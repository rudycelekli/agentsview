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
// A page whose target id already exists is left for the reparse to settle.
//
// Every column that holds a session id is found from the schema: columns named
// session_id or *_session_id, and every foreign key into sessions. Foreign
// keys are deferred so parent and child rows can move in one transaction.
//
// The old id is journaled as a deletion, with the statements of
// trg_sessions_deletion_journal_delete, and the new row's transcript revision
// advances, so mirrors drop the old row and publish the new one.
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
		rows, err := tx.QueryContext(ctx, q)
		if err != nil {
			return fmt.Errorf("finding legacy codex pages: %w", err)
		}
		for rows.Next() {
			var id, path string
			if err := rows.Scan(&id, &path); err != nil {
				rows.Close()
				return fmt.Errorf("scanning legacy codex page: %w", err)
			}
			if target, ok := legacyCodexPageID(id, path); ok {
				renames[id] = target
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("listing legacy codex pages: %w", err)
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
	for oldID, newID := range renames {
		if _, err := tx.ExecContext(ctx, `INSERT INTO _codex_page_renames
			SELECT ?1, ?2
			WHERE NOT EXISTS (SELECT 1 FROM sessions WHERE id = ?2)
			  AND NOT EXISTS (SELECT 1 FROM excluded_sessions WHERE id = ?2)`,
			oldID, newID,
		); err != nil {
			return fmt.Errorf("recording legacy codex page rename: %w", err)
		}
	}

	// Journal the old ids as deletions before the rows move.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO archive_metadata (key, value)
			SELECT 'session_deletion_publication_revision', '0'
		ON CONFLICT(key) DO NOTHING`); err != nil {
		return fmt.Errorf("seeding deletion journal revision: %w", err)
	}
	var journaled []string
	jrows, err := tx.QueryContext(ctx, `SELECT s.id, s.project FROM sessions s
		JOIN _codex_page_renames r ON r.old_id = s.id`)
	if err != nil {
		return fmt.Errorf("finding journaled legacy codex pages: %w", err)
	}
	var projects []string
	for jrows.Next() {
		var id, project string
		if err := jrows.Scan(&id, &project); err != nil {
			jrows.Close()
			return fmt.Errorf("scanning journaled legacy codex page: %w", err)
		}
		journaled = append(journaled, id)
		projects = append(projects, project)
	}
	err = jrows.Err()
	jrows.Close()
	if err != nil {
		return fmt.Errorf("listing journaled legacy codex pages: %w", err)
	}
	for i, id := range journaled {
		if _, err := tx.ExecContext(ctx, `
			UPDATE archive_metadata SET
				value = CAST(CAST(value AS INTEGER) + 1 AS TEXT),
				updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
			WHERE key = 'session_deletion_publication_revision'`); err != nil {
			return fmt.Errorf("advancing deletion journal revision: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO session_deletion_changes (session_id, project, revision, deleted)
				SELECT ?1, ?2, CAST(value AS INTEGER), 1
				FROM archive_metadata WHERE key = 'session_deletion_publication_revision'
			ON CONFLICT(session_id) DO UPDATE SET
				project = excluded.project, revision = excluded.revision, deleted = 1`,
			id, projects[i],
		); err != nil {
			return fmt.Errorf("journaling legacy codex page: %w", err)
		}
	}

	cols, err := sessionIDColumnsLocked(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions
		SET id = (SELECT new_id FROM _codex_page_renames WHERE old_id = sessions.id)
		WHERE id IN (SELECT old_id FROM _codex_page_renames)`); err != nil {
		return fmt.Errorf("renaming legacy codex pages: %w", err)
	}
	for _, c := range cols {
		stmt := fmt.Sprintf(`UPDATE OR REPLACE %[1]s SET %[2]s = (
			SELECT new_id FROM _codex_page_renames WHERE old_id = %[1]s.%[2]s)
			WHERE %[2]s IN (SELECT old_id FROM _codex_page_renames)`,
			quoteIdent(c[0]), quoteIdent(c[1]))
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("renaming %s.%s: %w", c[0], c[1], err)
		}
	}
	for _, id := range journaled {
		var newID string
		if err := tx.QueryRowContext(ctx,
			`SELECT new_id FROM _codex_page_renames WHERE old_id = ?`, id,
		).Scan(&newID); err != nil {
			return fmt.Errorf("reading renamed codex page: %w", err)
		}
		if err := bumpTranscriptRevision(tx, newID, true); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing legacy codex page rename: %w", err)
	}
	return nil
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
// foreign keys into sessions, plus excluded_sessions.id. The deletion journal is excluded because it
// records ids that no longer exist.
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

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
