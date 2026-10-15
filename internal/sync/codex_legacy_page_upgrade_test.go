package sync

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/testjsonl"
)

// upgradedLegacyPage is an archive that stored a revert page as
// codex:<rollout> (data version 133), with a Claude session whose parent is
// that page, reopened and fully synced the way the version bump does it.
type upgradedLegacyPage struct {
	database *db.DB
	oldID    string
	newID    string
	child    string
	// Taken before the upgrade.
	checkpoint string
	cutoff     string
}

func upgradeLegacyCodexPage(t *testing.T) upgradedLegacyPage {
	t.Helper()
	ctx := t.Context()
	const rollout = "22222222-2222-4222-8222-222222222222"
	root := filepath.Join(t.TempDir(), "sessions")
	pagePath := writeCodexRollout(t, root, "25",
		"rollout-2026-09-25T10-00-00-"+paginationThread+"_"+rollout+".jsonl",
		testjsonl.JoinJSONL(
			testjsonl.CodexSessionMetaWithFieldsJSON(rollout, "/work/project", "codex_cli_rs", "2026-09-25T10:00:00Z",
				map[string]any{
					"session_id": paginationThread, "history_mode": "paginated",
					"history_base": map[string]any{"thread_id": paginationThread},
				}),
			codexUserEntryJSON("2026-09-25T10:00:01Z", "page prompt"),
			codexAssistantEntryJSON("2026-09-25T10:00:02Z", "page answer"),
		))

	dbPath := filepath.Join(t.TempDir(), "test.db")
	database := dbtest.OpenTestDBAt(t, dbPath)
	u := upgradedLegacyPage{
		database: database,
		oldID:    "codex:" + rollout,
		newID:    "codex:" + paginationThread + "_" + rollout,
		child:    "claude:child",
	}
	require.NoError(t, database.UpsertSession(ctx, db.Session{
		ID: u.oldID, Agent: "codex", Project: "project", Machine: "local",
		FilePath: &pagePath, MessageCount: 2,
	}))
	require.NoError(t, database.UpsertSession(ctx, db.Session{
		ID: u.child, Agent: "claude", Project: "project", Machine: "local",
		ParentSessionID: &u.oldID,
	}))
	require.NoError(t, database.InsertMessages(ctx, []db.Message{
		{SessionID: u.oldID, Ordinal: 0, Role: "user", Content: "page prompt", Timestamp: "2026-09-25T10:00:01Z"},
		{SessionID: u.oldID, Ordinal: 1, Role: "assistant", Content: "page answer", Timestamp: "2026-09-25T10:00:02Z"},
	}))

	exported, err := database.ExportConversationChanges(ctx, db.ConversationExportOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, exported.Checkpoint)
	u.checkpoint = exported.Checkpoint
	// Every write so far carries an earlier marker; the upgrade below takes
	// longer than the millisecond that separates them.
	u.cutoff = time.Now().Add(time.Millisecond).UTC().Format("2006-01-02T15:04:05.000Z")
	require.NoError(t, database.Close())

	raw, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	_, err = raw.ExecContext(ctx, "PRAGMA user_version=133")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	u.database, err = db.Open(ctx, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { u.database.Close() })

	engine := newCodexRevertEngine(t, u.database, root)
	require.Zero(t, engine.SyncAll(ctx, nil).Failed)
	return u
}

// A Claude session whose parent is a legacy page mirrors the page's id. After
// the rename and the scoped resync, an incremental mirror pass that only
// selects sessions changed since its last cutoff must still pick it up.
func TestCodexLegacyPageUpgradeAdvancesReferencingSessionMarker(t *testing.T) {
	u := upgradeLegacyCodexPage(t)
	window, err := u.database.ListSessionsForMirrorWindow(t.Context(), u.cutoff, nil, nil)
	require.NoError(t, err)
	var ids []string
	for _, s := range window {
		ids = append(ids, s.ID)
		if s.ID == u.child {
			require.NotNil(t, s.ParentSessionID)
			assert.Equal(t, u.newID, *s.ParentSessionID)
		}
	}
	assert.Contains(t, ids, u.child, "the referencing session is an incremental mirror candidate")
	assert.Contains(t, ids, u.newID)
}

// A conversation export client holding a checkpoint from before the upgrade
// must see the old id removed and the new id's messages, after the rename and
// the scoped resync.
func TestCodexLegacyPageUpgradeRepublishesConversationExport(t *testing.T) {
	u := upgradeLegacyCodexPage(t)
	delta, err := u.database.ExportConversationChanges(t.Context(), db.ConversationExportOptions{
		Checkpoint: u.checkpoint,
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
	assert.Equal(t, 1, got[key{u.oldID, true, "session"}], "old id is announced as gone")
	assert.Equal(t, 2, got[key{u.oldID, true, "message"}], "old messages are tombstoned")
	assert.Equal(t, 2, got[key{u.newID, false, "message"}], "new id republishes its messages")
	again, err := u.database.ExportConversationChanges(t.Context(), db.ConversationExportOptions{
		Checkpoint: delta.Checkpoint,
	})
	require.NoError(t, err)
	assert.Empty(t, again.Changes, "the delta is consumed once")
}
