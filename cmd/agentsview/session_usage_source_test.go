package main

import (
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/server"
	agentsync "go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestSessionUsage_FirstUseCodexBareID(t *testing.T) {
	const rawID = "11111111-1111-1111-1111-111111111111"
	for _, ownOnly := range []bool{false, true} {
		t.Run(fmt.Sprint("ownOnly=", ownOnly), func(t *testing.T) {
			dataDir := t.TempDir()
			codexDir := filepath.Join(dataDir, "codex", "sessions")
			require.NoError(t, os.MkdirAll(codexDir, 0o755))
			transcript := testjsonl.NewSessionBuilder().
				AddCodexMeta("2026-01-01T00:00:00Z", rawID, "/project-a", "codex").
				AddCodexMessage("2026-01-01T00:00:01Z", "user", "hello").
				AddCodexMessage("2026-01-01T00:00:02Z", "assistant", "done").
				String() + testjsonl.CodexTokenCountJSON("2026-01-01T00:00:03Z", 100, 42, 0) + "\n"
			require.NoError(t, os.WriteFile(filepath.Join(codexDir,
				"rollout-2026-01-01T00-00-00-"+rawID+".jsonl"), []byte(transcript), 0o600))
			cfg := config.Config{
				Host: "127.0.0.1", DataDir: dataDir,
				DBPath: filepath.Join(dataDir, "sessions.db"), WriteTimeout: 30 * time.Second,
				AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {codexDir}},
			}
			database := dbtest.OpenTestDBAt(t, cfg.DBPath)
			engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{
				AgentDirs: cfg.AgentDirs, Machine: "host-a", Ephemeral: true,
			})
			t.Cleanup(engine.Close)
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			cfg.Port = ln.Addr().(*net.TCPAddr).Port
			ts := httptest.NewUnstartedServer(server.New(cfg, database, engine).Handler())
			ts.Listener = ln
			ts.Start()
			t.Cleanup(ts.Close)
			backend := daemonArchiveQueryBackend{tr: transport{URL: ts.URL, Runtime: daemonRuntimeFromTestURL(t, ts.URL)}}
			stderr := captureStderr(t, func() {
				out, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: rawID, OwnOnly: ownOnly})
				require.NoError(t, err)
				require.Equal(t, tokenUseExitOK, code)
				require.NotNil(t, out)
				assert.Equal(t, "codex:"+rawID, out.SessionID)
				assert.Equal(t, 42, out.TotalOutputTokens)
			})
			assert.Empty(t, stderr)
			stored, err := database.GetSession(t.Context(), "codex:"+rawID)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, "codex", stored.Agent)
		})
	}
}
