package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	legacyThread  = "11111111-1111-4111-8111-111111111111"
	legacyRollout = "22222222-2222-4222-8222-222222222222"
	legacyThread2 = "33333333-3333-4333-8333-333333333333"
	legacyRoll2   = "44444444-4444-4444-8444-444444444444"
	legacyThread3 = "55555555-5555-4555-8555-555555555555"
	legacyRoll3   = "66666666-6666-4666-8666-666666666666"
	legacyThread4 = "77777777-7777-4777-8777-777777777777"
	legacyRoll4   = "88888888-8888-4888-8888-888888888888"
	legacyThread5 = "99999999-9999-4999-8999-999999999999"
	legacyRoll5   = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	legacyThread6 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	legacyRoll6   = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

// legacyRenamedColumns is every [table, column] that holds a session id and
// moves with a renamed page, other than sessions.id. legacyJournalColumns
// are session id columns the rename deliberately leaves alone: the change
// journals record ids that no longer exist, and sessions.source_session_id
// is a provider-side id, not a session id.
var (
	legacyRenamedColumns = [][2]string{
		{"artifact_export_queue", "session_id"},
		{"artifact_imported_sessions", "imported_session_id"},
		{"artifact_publications", "session_id"},
		{"claude_subagent_sources", "session_id"},
		{"conversation_messages", "session_id"},
		{"conversation_session_changes", "session_id"},
		{"excluded_sessions", "id"},
		{"local_session_source_baselines", "session_id"},
		{"messages", "session_id"},
		{"parser_checkpoint_blobs", "session_id"},
		{"parser_checkpoints", "session_id"},
		{"pinned_messages", "session_id"},
		{"project_identity_observations", "session_id"},
		{"raw_archive_sessions", "session_id"},
		{"recall_entries", "source_session_id"},
		{"recall_evidence", "session_id"},
		{"recall_extract_progress", "session_id"},
		{"secret_findings", "session_id"},
		{"session_external_parents", "parent_session_id"},
		{"session_external_parents", "session_id"},
		{"session_labels", "session_id"},
		{"session_project_assignments", "session_id"},
		{"session_project_identity_snapshots", "session_id"},
		{"session_signal_state", "session_id"},
		{"sessions", "parent_session_id"},
		{"sessions", "parser_parent_session_id"},
		{"starred_sessions", "session_id"},
		{"subagent_parent_cleanup_queue", "session_id"},
		{"subagent_parent_repair_queue", "session_id"},
		{"tool_call_occurrence_agent_state", "session_id"},
		{"tool_calls", "session_id"},
		{"tool_calls", "subagent_session_id"},
		{"tool_result_events", "session_id"},
		{"tool_result_events", "subagent_session_id"},
		{"usage_events", "session_id"},
	}
	legacyJournalColumns = [][2]string{
		{"session_deletion_changes", "session_id"},
		{"session_project_identity_snapshot_changes", "session_id"},
		{"sessions", "source_session_id"},
	}
)

// TestLegacyCodexPageRenameCoversSessionIDColumns finds every column that can
// hold a session id straight from the schema, with its own broader pattern,
// and requires the result to equal the lists above and the migration's walk
// to equal the renamed list. A new table with a session id column fails here
// until someone decides whether the rename must move it.
func TestLegacyCodexPageRenameCoversSessionIDColumns(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "test.db")
	d, err := Open(ctx, path)
	require.NoError(t, err)
	d.Close()
	conn, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, `
		SELECT m.name, p.name FROM sqlite_master m, pragma_table_info(m.name) p
		WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
		  AND m.sql NOT LIKE 'CREATE VIRTUAL%'
		  AND (p.name LIKE '%session%id%'
		       OR (m.name = 'excluded_sessions' AND p.name = 'id'))
		UNION
		SELECT m.name, f."from" FROM sqlite_master m, pragma_foreign_key_list(m.name) f
		WHERE m.type = 'table' AND f."table" = 'sessions'`)
	require.NoError(t, err)
	var found [][2]string
	for rows.Next() {
		var c [2]string
		require.NoError(t, rows.Scan(&c[0], &c[1]))
		found = append(found, c)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	want := append(slices.Clone(legacyRenamedColumns), legacyJournalColumns...)
	sortPairs := func(p [][2]string) {
		slices.SortFunc(p, func(a, b [2]string) int {
			return strings.Compare(a[0]+"."+a[1], b[0]+"."+b[1])
		})
	}
	sortPairs(found)
	sortPairs(want)
	assert.Equal(t, want, found, "session id columns in the schema")

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	walked, err := sessionIDColumnsLocked(ctx, tx)
	require.NoError(t, err)
	renamed := slices.Clone(legacyRenamedColumns)
	sortPairs(walked)
	sortPairs(renamed)
	assert.Equal(t, renamed, walked, "columns the migration renames")
}

func legacyPagePath(thread, rollout string) *string {
	p := "/sessions/2026/09/01/rollout-2026-09-01T10-00-00-" + thread + "_" + rollout + ".jsonl"
	return &p
}

// TestOpenRenamesLegacyCodexPages covers an archive written before data
// version 134, which stored a revert page whose header names its own rollout
// as codex:<rollout>. The open renames the page, and everything attached to
// it, to codex:<thread>_<rollout> in place, whether its source file is
// present, gone, or the archive is archive-only. A page whose canonical row
// already exists is left alone.
func TestOpenRenamesLegacyCodexPages(t *testing.T) {
	for _, archiveOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("archiveOnly=%t", archiveOnly), func(t *testing.T) {
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "test.db")
			d, err := Open(ctx, path)
			require.NoError(t, err)

			const (
				page     = "codex:" + legacyRollout
				pageNew  = "codex:" + legacyThread + "_" + legacyRollout
				hosted   = "host~codex:" + legacyRoll2
				hostedNw = "host~codex:" + legacyThread2 + "_" + legacyRoll2
				taken    = "codex:" + legacyRoll3
				takenNw  = "codex:" + legacyThread3 + "_" + legacyRoll3
				gone     = "codex:" + legacyRoll4
				goneNew  = "codex:" + legacyThread4 + "_" + legacyRoll4
				child    = "codex:child"
				pathless = "codex:" + legacyRoll5
				pathNew  = "codex:" + legacyThread5 + "_" + legacyRoll5
				claimed  = "codex:" + legacyRoll6
				claimNew = "codex:" + legacyThread6 + "_" + legacyRoll6
			)
			codexOpt := func(p *string, parent string) func(*Session) {
				return func(s *Session) {
					s.Agent = "codex"
					s.FilePath = p
					if parent != "" {
						s.ParentSessionID = &parent
					}
				}
			}
			insertSession(t, d, page, "proj", codexOpt(legacyPagePath(legacyThread, legacyRollout), ""))
			insertSession(t, d, hosted, "proj", codexOpt(legacyPagePath(legacyThread2, legacyRoll2), ""))
			insertSession(t, d, taken, "proj", codexOpt(legacyPagePath(legacyThread3, legacyRoll3), ""))
			insertSession(t, d, takenNw, "proj", codexOpt(legacyPagePath(legacyThread3, legacyRoll3), ""))
			insertSession(t, d, child, "proj", codexOpt(nil, page))
			insertMessages(t, d, Message{
				SessionID: page, Ordinal: 0, Role: "user", Content: "page prompt", Timestamp: tsZero,
			})
			msgs, err := d.GetAllMessages(ctx, page)
			require.NoError(t, err)
			_, err = d.PinMessage(ctx, page, msgs[0].ID, nil)
			require.NoError(t, err)
			_, err = d.StarSession(ctx, page)
			require.NoError(t, err)
			_, err = d.SetSessionLabels(ctx, page, []string{"keep"})
			require.NoError(t, err)
			require.NoError(t, d.SoftDeleteSession(ctx, hosted))
			// The source of gone is missing and its row was permanently deleted;
			// only the exclusion remembers it.
			_, err = d.getWriter().Exec(ctx,
				`INSERT INTO excluded_sessions (id, file_path) VALUES (?, ?)`,
				gone, *legacyPagePath(legacyThread4, legacyRoll4))
			require.NoError(t, err)
			// An exclusion from before file names were recorded has no path to
			// rename it by; the page's new id must still read as excluded.
			_, err = d.getWriter().Exec(ctx,
				`INSERT INTO excluded_sessions (id, file_path) VALUES (?, NULL)`, pathless)
			require.NoError(t, err)
			// A child already points at claimNew, so renaming claimed into it
			// could merge two identities. The page stays as it is.
			insertSession(t, d, claimed, "proj", codexOpt(legacyPagePath(legacyThread6, legacyRoll6), ""))
			insertSession(t, d, "codex:claimant", "proj", codexOpt(nil, claimNew))
			before, err := d.GetSession(ctx, page)
			require.NoError(t, err)
			if archiveOnly {
				require.NoError(t, d.EnableArchiveOnly(ctx))
			}
			d.Close()

			conn, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			v := codexLegacyPageVersion - 1
			if archiveOnly {
				v += archiveOnlyVersionBase
			}
			_, err = conn.ExecContext(ctx, "PRAGMA user_version = "+itoa(v))
			require.NoError(t, err)
			conn.Close()

			d, err = Open(ctx, path)
			require.NoError(t, err)
			defer d.Close()

			old, err := d.GetSession(ctx, page)
			require.NoError(t, err)
			assert.Nil(t, old, "the bare-rollout row is gone")
			moved, err := d.GetSession(ctx, pageNew)
			require.NoError(t, err)
			require.NotNil(t, moved)
			assert.NotEqual(t, *before.TranscriptRevision, *moved.TranscriptRevision)
			msgs, err = d.GetAllMessages(ctx, pageNew)
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			pins, err := d.ListPinnedMessages(ctx, pageNew, "")
			require.NoError(t, err)
			assert.Len(t, pins, 1)
			starred, err := d.ListStarredSessionIDs(ctx)
			require.NoError(t, err)
			assert.Equal(t, []string{pageNew}, starred)
			var label string
			require.NoError(t, d.getReader().QueryRow(ctx,
				`SELECT label FROM session_labels WHERE session_id = ?`, pageNew,
			).Scan(&label))
			assert.Equal(t, "keep", label)
			kid, err := d.GetSession(ctx, child)
			require.NoError(t, err)
			require.NotNil(t, kid.ParentSessionID)
			assert.Equal(t, pageNew, *kid.ParentSessionID)
			trashed, err := d.GetSessionFull(ctx, hostedNw)
			require.NoError(t, err)
			require.NotNil(t, trashed)
			assert.NotNil(t, trashed.DeletedAt, "trash follows the page")

			assert.True(t, d.IsSessionExcluded(ctx, goneNew))
			assert.False(t, d.IsSessionExcluded(ctx, gone))
			assert.True(t, d.IsSessionExcluded(ctx, pathNew), "a pathless exclusion still covers the page")
			assert.False(t, d.IsSessionExcluded(ctx, "codex:"+legacyThread5+"_"+legacyRoll6))
			kept, err := d.GetSession(ctx, claimed)
			require.NoError(t, err)
			assert.NotNil(t, kept, "a page whose target is referenced elsewhere is not renamed")

			still, err := d.GetSession(ctx, taken)
			require.NoError(t, err)
			assert.NotNil(t, still, "a page whose target exists is not renamed")

			var journaled int
			require.NoError(t, d.getReader().QueryRow(ctx,
				`SELECT COUNT(*) FROM session_deletion_changes
				 WHERE deleted = 1 AND session_id IN (?, ?)`, page, hosted,
			).Scan(&journaled))
			assert.Equal(t, 2, journaled, "old ids reach mirrors as deletions")

			// No column that holds a session id still holds a renamed id. The
			// column list is written out here rather than read from the
			// migration, see TestLegacyCodexPageRenameCoversSessionIDColumns.
			d.Close()
			conn, err = sql.Open("sqlite3", path)
			require.NoError(t, err)
			defer conn.Close()
			for _, c := range append(slices.Clone(legacyRenamedColumns), [2]string{"sessions", "id"}) {
				var n int
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					`SELECT COUNT(*) FROM %s WHERE CAST(%s AS TEXT) IN (?, ?, ?)`,
					quoteIdent(c[0]), quoteIdent(c[1])), page, hosted, gone,
				).Scan(&n))
				assert.Zero(t, n, "%s.%s still holds a legacy page id", c[0], c[1])
			}
		})
	}
}

// TestLegacyCodexPageRenamePublishesToIncrementalConsumers covers the two
// consumers that only read what advanced since their last pass. A conversation
// export client holding a checkpoint from before the rename must see the old
// id removed and the new id's messages again, and a mirror that only selects
// sessions whose sync marker moved must pick up a child of another provider
// whose parent_session_id was rewritten.
func TestLegacyCodexPageRenamePublishesToIncrementalConsumers(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "test.db")
	d, err := Open(ctx, path)
	require.NoError(t, err)

	const (
		page    = "codex:" + legacyRollout
		pageNew = "codex:" + legacyThread + "_" + legacyRollout
		child   = "claude:child"
	)
	insertSession(t, d, page, "proj", func(s *Session) {
		s.Agent = "codex"
		s.FilePath = legacyPagePath(legacyThread, legacyRollout)
	})
	parent := page
	insertSession(t, d, child, "proj", func(s *Session) {
		s.Agent = "claude"
		s.ParentSessionID = &parent
	})
	insertMessages(t, d, userMsg(page, 0, "page prompt"), asstMsg(page, 1, "page answer"))

	checkpoint, err := d.ExportConversationChanges(ctx, ConversationExportOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, checkpoint.Checkpoint)
	require.Len(t, checkpoint.Changes, 2)

	var childMarker, childModified string
	require.NoError(t, d.getReader().QueryRow(ctx,
		`SELECT sync_marker, COALESCE(local_modified_at, '') FROM sessions WHERE id = ?`, child,
	).Scan(&childMarker, &childModified))
	time.Sleep(20 * time.Millisecond)
	cutoff := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	require.Less(t, childMarker, cutoff)
	time.Sleep(20 * time.Millisecond)
	d.Close()

	conn, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "PRAGMA user_version = "+itoa(codexLegacyPageVersion-1))
	require.NoError(t, err)
	conn.Close()

	d, err = Open(ctx, path)
	require.NoError(t, err)
	defer d.Close()

	// The conversation delta since the old checkpoint.
	delta, err := d.ExportConversationChanges(ctx, ConversationExportOptions{
		Checkpoint: checkpoint.Checkpoint,
	})
	require.NoError(t, err)
	type key struct {
		session string
		deleted bool
		kind    string
	}
	got := map[key]int{}
	for _, c := range delta.Changes {
		got[key{c.SessionID, c.Deleted, c.Type}]++
	}
	assert.Equal(t, 1, got[key{page, true, "session"}], "old id is announced as gone")
	assert.Equal(t, 2, got[key{page, true, "message"}], "old messages are tombstoned")
	assert.Equal(t, 2, got[key{pageNew, false, "message"}], "new id republishes its messages")
	assert.Zero(t, got[key{pageNew, true, "message"}])
	revisions := map[string]bool{}
	for _, c := range delta.Changes {
		assert.False(t, revisions[c.Revision], "revision %s repeats", c.Revision)
		revisions[c.Revision] = true
	}
	again, err := d.ExportConversationChanges(ctx, ConversationExportOptions{
		Checkpoint: delta.Checkpoint,
	})
	require.NoError(t, err)
	assert.Empty(t, again.Changes, "the delta is consumed once")

	// The mirror's incremental window.
	var marker, modified string
	require.NoError(t, d.getReader().QueryRow(ctx,
		`SELECT sync_marker, COALESCE(local_modified_at, '') FROM sessions WHERE id = ?`, child,
	).Scan(&marker, &modified))
	assert.NotEqual(t, childModified, modified, "the referencing child advanced")
	assert.Greater(t, marker, childMarker)
	window, err := d.ListSessionsForMirrorWindow(ctx, cutoff, nil, nil)
	require.NoError(t, err)
	var ids []string
	for _, s := range window {
		ids = append(ids, s.ID)
	}
	assert.ElementsMatch(t, []string{child, pageNew}, ids)
	for _, s := range window {
		if s.ID == child {
			require.NotNil(t, s.ParentSessionID)
			assert.Equal(t, pageNew, *s.ParentSessionID)
		}
	}
}
