package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

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
)

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

			still, err := d.GetSession(ctx, taken)
			require.NoError(t, err)
			assert.NotNil(t, still, "a page whose target exists is not renamed")

			var journaled int
			require.NoError(t, d.getReader().QueryRow(ctx,
				`SELECT COUNT(*) FROM session_deletion_changes
				 WHERE deleted = 1 AND session_id IN (?, ?)`, page, hosted,
			).Scan(&journaled))
			assert.Equal(t, 2, journaled, "old ids reach mirrors as deletions")

			// Schema walk: no column of any table, however named, still holds
			// an id that was renamed.
			d.Close()
			conn, err = sql.Open("sqlite3", path)
			require.NoError(t, err)
			defer conn.Close()
			rows, err := conn.QueryContext(ctx, `
				SELECT m.name, p.name FROM sqlite_master m, pragma_table_info(m.name) p
				WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
				  AND m.sql NOT LIKE 'CREATE VIRTUAL%'
				  AND m.name NOT IN ('session_deletion_changes',
				      'session_project_identity_snapshot_changes')`)
			require.NoError(t, err)
			var cols [][2]string
			for rows.Next() {
				var c [2]string
				require.NoError(t, rows.Scan(&c[0], &c[1]))
				cols = append(cols, c)
			}
			require.NoError(t, rows.Err())
			rows.Close()
			for _, c := range cols {
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
