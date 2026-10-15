package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

// codexLegacyPageVersion is the data version at which the Codex parser began
// recognizing revert pages whose header id is the page's own rollout.
const codexLegacyPageVersion = 134

// renameLegacyCodexPagesLocked renames stored Codex pages from
// "<prefix>codex:<rollout>" to "<prefix>codex:<thread>_<rollout>", the id the
// parser now emits. Codex Desktop 0.148.0-alpha.15 through 0.154.0-alpha.6.2
// wrote such pages with their own rollout id in the header, so older builds
// stored them as standalone sessions. The file name keyed on the row's
// file_path (or an exclusion's) names the thread and decides the rename.
//
// It runs once, before the scoped reparse that the version bump triggers, on
// archives below codexLegacyPageVersion. Renaming in place, as the Devin source
// uuid rewrite does, means curation, trash, exclusions, labels and
// relationships need no translation later: every consumer sees one identity.
// A page whose target id is already in use anywhere is skipped and left for
// the reparse to settle, so the rename never replaces a row.
//
// Every column that holds a session id is found from the schema: columns named
// session_id or *_session_id, and every foreign key into sessions. Foreign
// keys are deferred so parent and child rows can move in one transaction.
//
// The old id is journaled as a deletion, as trg_sessions_deletion_journal_delete
// would, and the new row's transcript revision advances, so mirrors drop the
// old row and publish the new one. The scoped reparse does not do this on its
// own: sessions that referenced a renamed page advance local_modified_at so an
// incremental mirror re-sends them, and an initialized conversation export gets
// tombstones for the old id and fresh revisions for the moved messages.
func renameLegacyCodexPagesLocked(ctx context.Context, w *writerHandle) error {
	var version int
	if err := w.QueryRowContext(
		ctx, "PRAGMA user_version",
	).Scan(&version); err != nil {
		return fmt.Errorf("probing data version: %w", err)
	}
	version, _ = DecodeUserVersion(version)
	if version >= codexLegacyPageVersion {
		return nil
	}

	tx, err := w.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning legacy codex page rename: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	renames := map[string]string{}
	for _, q := range []string{
		`SELECT id, file_path FROM sessions
		 WHERE agent IN ('codex', 'traex', 'augure-code')
		   AND file_path IS NOT NULL AND file_path != ''`,
		`SELECT id, file_path FROM excluded_sessions
		 WHERE file_path IS NOT NULL AND file_path != ''`,
	} {
		if err := collectLegacyCodexPages(ctx, tx, q, renames); err != nil {
			return err
		}
	}
	if len(renames) == 0 {
		return nil
	}

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("deferring foreign keys: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE _codex_page_renames (
		old_id TEXT PRIMARY KEY, new_id TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("creating rename map: %w", err)
	}
	defer func() {
		_, _ = tx.ExecContext(ctx, `DROP TABLE IF EXISTS temp._codex_page_renames`)
	}()
	cols, err := sessionIDColumnsLocked(ctx, tx)
	if err != nil {
		return err
	}
	for oldID, newID := range renames {
		// A target id already present in any table that holds session ids
		// could collide with a moved row. Leave such a page for the reparse.
		taken, err := sessionIDInUseLocked(ctx, tx, cols, newID)
		if err != nil {
			return err
		}
		if taken {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO _codex_page_renames
			VALUES (?1, ?2)`, oldID, newID,
		); err != nil {
			return fmt.Errorf("recording legacy codex page rename: %w", err)
		}
	}

	pages, err := journaledLegacyPages(ctx, tx)
	if err != nil {
		return err
	}
	for _, p := range pages {
		if err := journalSessionDeletionTx(ctx, tx, p.id, p.project); err != nil {
			return fmt.Errorf("journaling legacy codex page: %w", err)
		}
	}

	// Sessions that merely reference a renamed page keep their identity, but
	// the id they mirror changes. Advancing local_modified_at, which the
	// sync_marker trigger turns into an incremental mirror candidate, has to
	// happen before the rewrite finds them.
	for _, c := range cols {
		owner := "session_id"
		if c[0] == "sessions" {
			owner = "id"
		}
		if c[1] == "session_id" || !columnExists(ctx, tx, c[0], owner) {
			continue
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE sessions
			SET local_modified_at = strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now')
			WHERE id IN (SELECT %[3]s FROM %[1]s
			             WHERE %[2]s IN (SELECT old_id FROM _codex_page_renames))
			  AND id NOT IN (SELECT old_id FROM _codex_page_renames)`,
			quoteIdent(c[0]), quoteIdent(c[1]), quoteIdent(owner))); err != nil {
			return fmt.Errorf("touching sessions referencing %s.%s: %w", c[0], c[1], err)
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE sessions
		SET id = (SELECT new_id FROM _codex_page_renames WHERE old_id = sessions.id)
		WHERE id IN (SELECT old_id FROM _codex_page_renames)`); err != nil {
		return fmt.Errorf("renaming legacy codex pages: %w", err)
	}
	for _, c := range cols {
		stmt := fmt.Sprintf(`UPDATE %[1]s SET %[2]s = (
			SELECT new_id FROM _codex_page_renames WHERE old_id = %[1]s.%[2]s)
			WHERE %[2]s IN (SELECT old_id FROM _codex_page_renames)`,
			quoteIdent(c[0]), quoteIdent(c[1]))
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("renaming %s.%s: %w", c[0], c[1], err)
		}
	}
	active, err := conversationExportActiveTx(contextTransaction{ctx: ctx, tx: tx})
	if err != nil {
		return err
	}
	for _, p := range pages {
		if err := bumpTranscriptRevision(tx, p.newID, true); err != nil {
			return err
		}
		if active {
			if err := republishConversationRenameTx(ctx, tx, p.id, p.newID); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing legacy codex page rename: %w", err)
	}
	return nil
}

// journaledLegacyPage is a stored session about to move to newID.
type journaledLegacyPage struct {
	id, project, newID string
}

func journaledLegacyPages(
	ctx context.Context, tx *sql.Tx,
) ([]journaledLegacyPage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT s.id, s.project, r.new_id
		FROM sessions s JOIN _codex_page_renames r ON r.old_id = s.id`)
	if err != nil {
		return nil, fmt.Errorf("finding journaled legacy codex pages: %w", err)
	}
	defer rows.Close()
	var pages []journaledLegacyPage
	for rows.Next() {
		var p journaledLegacyPage
		if err := rows.Scan(&p.id, &p.project, &p.newID); err != nil {
			return nil, fmt.Errorf("scanning journaled legacy codex page: %w", err)
		}
		pages = append(pages, p)
	}
	return pages, rows.Err()
}

// collectLegacyCodexPages adds to renames every (id, file_path) row of query
// whose id is a legacy page.
func collectLegacyCodexPages(
	ctx context.Context, tx *sql.Tx, query string, renames map[string]string,
) error {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("finding legacy codex pages: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, path string
		if err := rows.Scan(&id, &path); err != nil {
			return fmt.Errorf("scanning legacy codex page: %w", err)
		}
		if target, ok := legacyCodexPageID(id, path); ok {
			renames[id] = target
		}
	}
	return rows.Err()
}

func columnExists(ctx context.Context, tx *sql.Tx, table, col string) bool {
	var has bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM pragma_table_info(?) WHERE name = ?)`, table, col,
	).Scan(&has)
	return err == nil && has
}

// legacyCodexPageID returns the canonical id for a stored Codex id whose raw
// part is the rollout segment of its file name's <thread>_<rollout> key.
func legacyCodexPageID(id, path string) (string, bool) {
	prefixEnd := strings.LastIndex(id, ":") + 1
	if prefixEnd == 0 {
		return "", false
	}
	name := path[strings.LastIndexAny(path, `/\`)+1:]
	key := parser.CodexSessionUUIDFromFilename(name)
	thread := parser.CodexThreadIDFromSessionKey(key)
	if key == "" || key == thread || key != thread+"_"+id[prefixEnd:] {
		return "", false
	}
	return id[:prefixEnd] + key, true
}

// sessionIDColumnsLocked lists [table, column] pairs, other than sessions.id,
// that hold a session id: columns named session_id or *_session_id, and
// foreign keys into sessions, plus excluded_sessions.id. The change journals
// are excluded because they record ids that no longer exist.
func sessionIDColumnsLocked(
	ctx context.Context, tx *sql.Tx,
) ([][2]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT m.name, p.name FROM sqlite_master m, pragma_table_info(m.name) p
		WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
		  AND m.sql NOT LIKE 'CREATE VIRTUAL%'
		  AND (p.name = 'session_id' OR p.name LIKE '%\_session\_id' ESCAPE '\')
		UNION
		SELECT m.name, f."from" FROM sqlite_master m, pragma_foreign_key_list(m.name) f
		WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
		  AND f."table" = 'sessions'
		ORDER BY 1, 2`)
	if err != nil {
		return nil, fmt.Errorf("finding session id columns: %w", err)
	}
	defer rows.Close()
	var cols [][2]string
	for rows.Next() {
		var c [2]string
		if err := rows.Scan(&c[0], &c[1]); err != nil {
			return nil, fmt.Errorf("scanning session id column: %w", err)
		}
		// The change journals record ids that no longer exist; triggers
		// already journal the snapshot rename.
		if c[0] == "session_deletion_changes" ||
			c[0] == "session_project_identity_snapshot_changes" ||
			(c[0] == "sessions" && c[1] == "source_session_id") {
			continue
		}
		cols = append(cols, c)
	}
	// Exclusions are keyed by the session id itself.
	return append(cols, [2]string{"excluded_sessions", "id"}), rows.Err()
}

// republishConversationRenameTx tells conversation export consumers that
// oldID is gone and newID carries its messages. The rewrite of session_id
// alone is not a publishable change, so a client holding a checkpoint would
// see an empty delta. Tombstones for the old messages and a session-level
// tombstone follow the shape conversation_session_delete emits; the moved
// messages and session state take fresh revisions.
func republishConversationRenameTx(
	ctx context.Context, tx *sql.Tx, oldID, newID string,
) error {
	// The insert trigger gives each tombstone its own revision.
	if _, err := tx.ExecContext(ctx, `INSERT INTO conversation_messages
		(session_id, message_id, ordinal, role, timestamp, source_id, body, digest,
		 text_bytes, gap, deleted, removed, revision)
		SELECT ?1, message_id, ordinal, role, timestamp, source_id, NULL, '',
		 0, gap, 1, 1, 0
		FROM conversation_messages WHERE session_id = ?2 AND removed = 0`,
		oldID, newID); err != nil {
		return fmt.Errorf("tombstoning renamed conversation messages: %w", err)
	}
	next := func() (int64, error) {
		var rev int64
		err := tx.QueryRowContext(ctx, `INSERT INTO archive_metadata(key,value) VALUES(?,'1')
			ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)
			RETURNING CAST(value AS INTEGER)`, conversationRevisionKey).Scan(&rev)
		return rev, err
	}
	rev, err := next()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO conversation_session_changes
		(session_id, revision, deleted) VALUES (?, ?, 1)
		ON CONFLICT(session_id) DO UPDATE SET revision = excluded.revision, deleted = 1`,
		oldID, rev); err != nil {
		return fmt.Errorf("tombstoning renamed conversation session: %w", err)
	}
	ids, err := liveConversationMessageIDs(ctx, tx, newID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if rev, err = next(); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE conversation_messages SET revision = ?
			WHERE session_id = ? AND message_id = ?`, rev, newID, id); err != nil {
			return fmt.Errorf("republishing renamed conversation message: %w", err)
		}
	}
	if rev, err = next(); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE conversation_session_changes SET revision = ?
		WHERE session_id = ?`, rev, newID)
	return err
}

func liveConversationMessageIDs(
	ctx context.Context, tx *sql.Tx, sessionID string,
) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT message_id FROM conversation_messages
		WHERE session_id = ? AND removed = 0 ORDER BY ordinal`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("listing renamed conversation messages: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sessionIDInUseLocked reports whether id appears in sessions.id or any
// listed session id column.
func sessionIDInUseLocked(
	ctx context.Context, tx *sql.Tx, cols [][2]string, id string,
) (bool, error) {
	for _, c := range append([][2]string{{"sessions", "id"}}, cols...) {
		var used bool
		err := tx.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT EXISTS (SELECT 1 FROM %s WHERE %s = ?)`,
			quoteIdent(c[0]), quoteIdent(c[1])), id,
		).Scan(&used)
		if err != nil {
			return false, fmt.Errorf("checking %s.%s for %s: %w", c[0], c[1], id, err)
		}
		if used {
			return true, nil
		}
	}
	return false, nil
}
