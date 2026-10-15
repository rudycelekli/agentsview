//go:build pgtest

package postgres

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/storage"
)

// A page published before data version 134 sits under codex:<rollout>. The
// next push publishes it at codex:<thread>_<rollout>, keeps its remote
// curation, and moves a remote exclusion for a deleted page along.
func TestLegacyRolloutPageMovesToThreadKeyInPostgres(t *testing.T) {
	const (
		thread  = "11111111-1111-4111-8111-111111111111"
		rollout = "22222222-2222-4222-8222-222222222222"
		oldID   = "codex:" + rollout
		pageID  = "codex:" + thread + "_" + rollout
		goneOld = "codex:33333333-3333-4333-8333-333333333333"
		goneNew = "codex:44444444-4444-4444-8444-444444444444_33333333-3333-4333-8333-333333333333"
		schema  = "agentsview_codex_legacy_page_test"
	)
	ctx := t.Context()
	pgURL := testPGURL(t)
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	path := filepath.Join(t.TempDir(), "rollout-2026-09-25T12-00-00-"+thread+"_"+rollout+".jsonl")
	seed := func(d *db.DB, id string) db.Session {
		sess := db.Session{
			ID: id, Agent: "codex", Project: "sample", Machine: "machine",
			FilePath: &path, MessageCount: 1, CreatedAt: "2026-09-25T12:00:00Z",
			StartedAt: new("2026-09-25T12:00:00Z"),
		}
		require.NoError(t, d.UpsertSession(ctx, sess))
		require.NoError(t, d.InsertMessages(ctx, []db.Message{{
			SessionID: id, Role: "assistant", Content: "page answer",
			Timestamp: "2026-09-25T12:00:00Z",
		}}))
		return sess
	}

	legacy := testDB(t)
	seed(legacy, oldID)
	require.NoError(t, legacy.SetSessionDataVersion(ctx, oldID, 133))
	syncer, err := New(pgURL, schema, legacy, "machine", true, storage.PusherOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syncer.Close()) })
	pushed, err := syncer.Push(ctx, true, nil)
	require.NoError(t, err)
	require.Zero(t, pushed.Errors)
	store := &Store{pg: syncer.pg}
	require.NoError(t, store.RenameSession(ctx, oldID, new("remote title")))
	starred, err := store.StarSession(ctx, oldID)
	require.NoError(t, err)
	require.True(t, starred)
	_, err = store.PinMessage(ctx, oldID, 0, new("remote note"))
	require.NoError(t, err)
	_, err = syncer.pg.ExecContext(ctx, `INSERT INTO excluded_sessions (id) VALUES ($1)`, goneOld)
	require.NoError(t, err)

	renamed := testDB(t)
	seed(renamed, pageID)
	require.NoError(t, renamed.SetSessionDataVersion(ctx, pageID, db.CurrentDataVersion()))
	upgraded, err := New(pgURL, schema, renamed, "machine", true, storage.PusherOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, upgraded.Close()) })
	// The exclusion carry-over runs for each page in the batch, so a page
	// at goneNew brings the remote exclusion for goneOld along.
	seed(renamed, goneNew)
	for range 2 {
		pushed, err = upgraded.Push(ctx, true, nil)
		require.NoError(t, err)
		require.Zero(t, pushed.Errors)
	}

	var n int
	require.NoError(t, upgraded.pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sessions WHERE id = $1`, oldID).Scan(&n))
	assert.Zero(t, n, "the bare-rollout row is retired")
	var name string
	require.NoError(t, upgraded.pg.QueryRowContext(ctx,
		`SELECT COALESCE(display_name, '') FROM sessions WHERE id = $1`, pageID).Scan(&name))
	assert.Equal(t, "remote title", name)
	require.NoError(t, upgraded.pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM starred_sessions WHERE session_id = $1`, pageID).Scan(&n))
	assert.Equal(t, 1, n)
	require.NoError(t, upgraded.pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pinned_messages WHERE session_id = $1`, pageID).Scan(&n))
	assert.Equal(t, 1, n)
	require.NoError(t, upgraded.pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM excluded_sessions WHERE id = $1`, goneNew).Scan(&n))
	assert.Equal(t, 1, n, "the exclusion follows the page id")
	require.NoError(t, upgraded.pg.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM excluded_sessions WHERE id = $1`, goneOld).Scan(&n))
	assert.Zero(t, n)
}
