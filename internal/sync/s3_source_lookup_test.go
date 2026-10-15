package sync

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	gosync "sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestSyncSingleSessionFirstUseS3Source(t *testing.T) {
	const rawID = "11111111-1111-4111-8111-111111111111"
	const sessionID = "host-a~codex:" + rawID
	const root = "s3://bucket/host-a/raw/codex"
	const key = "host-a/raw/codex/sessions/2026/06/24/rollout-2026-06-24T00-00-00-" + rawID + ".jsonl"
	const archivedKey = "host-a/raw/codex/archived_sessions/rollout-2026-06-24T00-00-00-" + rawID + ".jsonl"
	const uri = "s3://bucket/" + key
	content := testjsonl.JoinJSONL(
		testjsonl.CodexSessionMetaJSON(rawID, t.TempDir(), "user", "2026-06-24T00:00:00Z"),
		testjsonl.CodexMsgJSON("assistant", "selected transcript", "2026-06-24T00:00:01Z"),
		testjsonl.CodexTokenCountJSON("2026-06-24T00:00:02Z", 100, 42, 0),
	)
	archivedContent := testjsonl.JoinJSONL(
		testjsonl.CodexSessionMetaJSON(rawID, t.TempDir(), "user", "2026-06-24T00:00:00Z"),
		testjsonl.CodexMsgJSON("assistant", "archived transcript", "2026-06-24T00:00:01Z"),
		testjsonl.CodexTokenCountJSON("2026-06-24T00:00:02Z", 10, 5, 0),
	)
	var mu gosync.Mutex
	var listed, fetched []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/bucket/" {
			prefix := r.URL.Query().Get("prefix")
			mu.Lock()
			listed = append(listed, prefix)
			mu.Unlock()
			if !assert.Equal(t, "host-a/raw/codex/", prefix) {
				http.Error(w, "unexpected source root", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			_, err := fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>%s</Prefix><IsTruncated>false</IsTruncated>`, prefix)
			assert.NoError(t, err)
			for _, objectKey := range []string{
				"host-a/raw/codex/" + rawID + ".jsonl",
				archivedKey,
				key,
				"host-a/raw/codex/rollout-2026-06-24T00-00-00-22222222-2222-4222-8222-222222222222.jsonl",
			} {
				size := len(content)
				if objectKey == archivedKey {
					size = len(archivedContent)
				}
				_, err := fmt.Fprintf(w, `<Contents><Key>%s</Key><LastModified>2026-06-24T00:00:00Z</LastModified><ETag>"fixture"</ETag><Size>%d</Size></Contents>`, objectKey, size)
				assert.NoError(t, err)
			}
			_, err = fmt.Fprint(w, `</ListBucketResult>`)
			assert.NoError(t, err)
			return
		}
		if r.Method == http.MethodGet {
			mu.Lock()
			fetched = append(fetched, r.URL.Path)
			mu.Unlock()
		}
		body := content
		switch r.URL.Path {
		case "/bucket/" + key:
		case "/bucket/" + archivedKey:
			body = archivedContent
		default:
			http.Error(w, `<Error><Code>NoSuchKey</Code></Error>`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Last-Modified", "Wed, 24 Jun 2026 00:00:00 GMT")
		w.Header().Set("ETag", `"fixture"`)
		if r.Method == http.MethodHead {
			return
		}
		if !assert.Equal(t, http.MethodGet, r.Method) {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, err := fmt.Fprint(w, body)
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	t.Setenv("AWS_S3_ENDPOINT", server.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")

	database := openTestDB(t)
	engine := NewEngine(t.Context(), database, EngineConfig{
		AgentDirs: map[parser.AgentType][]string{
			parser.AgentCodex:  {"s3://bucket/host-b/raw/codex", root, t.TempDir()},
			parser.AgentClaude: {"s3://bucket/host-a/raw/claude"},
		},
		Machine: "local", DisableFilesystemProjectDiscovery: true,
	})
	t.Cleanup(engine.Close)

	t.Run("lookup uses provider discovery without fetching", func(t *testing.T) {
		assert.Equal(t, uri, engine.FindSourceFile(t.Context(), sessionID))
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []string{"host-a/raw/codex/"}, listed)
		assert.Empty(t, fetched)
	})
	t.Run("refresh ingests only requested canonical session", func(t *testing.T) {
		require.NoError(t, engine.SyncSingleSessionForUsageContext(t.Context(), sessionID))
		session, err := database.GetSessionFull(t.Context(), sessionID)
		require.NoError(t, err)
		require.NotNil(t, session)
		assert.Equal(t, "host-a", session.Machine)
		require.NotNil(t, session.FilePath)
		assert.Equal(t, uri, *session.FilePath)
		messages, err := database.GetAllMessages(t.Context(), sessionID)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		assert.Equal(t, "selected transcript", messages[0].Content)
		usage, err := database.GetSessionUsage(t.Context(), sessionID, false)
		require.NoError(t, err)
		require.NotNil(t, usage)
		assert.Equal(t, 42, usage.TotalOutputTokens)
		page, err := database.ListSessions(t.Context(), db.SessionFilter{Limit: 10})
		require.NoError(t, err)
		require.Len(t, page.Sessions, 1)
		assert.Equal(t, sessionID, page.Sessions[0].ID)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []string{
			"/bucket/" + key,
			"/bucket/host-a/raw/codex/session_index.jsonl",
		}, fetched)
	})
	t.Run("unconfigured machine stays unresolved", func(t *testing.T) {
		assert.Empty(t, engine.FindSourceFile(t.Context(), "host-c~codex:"+rawID))
		err := engine.SyncSingleSessionForUsageContext(t.Context(), "host-c~codex:"+rawID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot sync remote session")
	})
}

func TestFindSourceFileS3UsesCursorDiscoveryPreferences(t *testing.T) {
	const rawID = "11111111-1111-4111-8111-111111111111"
	const root = "s3://bucket/host-a/raw/cursor"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.Equal(t, http.MethodGet, r.Method) ||
			!assert.Equal(t, "/bucket/", r.URL.Path) ||
			!assert.Equal(t, "host-a/raw/cursor/", r.URL.Query().Get("prefix")) {
			http.Error(w, "unexpected source request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, err := fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><IsTruncated>false</IsTruncated>`)
		assert.NoError(t, err)
		for _, rel := range []string{
			"project/junk/" + rawID + ".jsonl",
			"project/" + rawID + ".txt",
			"project/" + rawID + ".jsonl",
		} {
			_, err := fmt.Fprintf(w, `<Contents><Key>host-a/raw/cursor/%s</Key><LastModified>2026-06-24T00:00:00Z</LastModified><ETag>"fixture"</ETag><Size>10</Size></Contents>`, rel)
			assert.NoError(t, err)
		}
		_, err = fmt.Fprint(w, `</ListBucketResult>`)
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	t.Setenv("AWS_S3_ENDPOINT", server.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	engine := NewEngine(t.Context(), openTestDB(t), EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentCursor: {root}},
		Machine:   "local", DisableFilesystemProjectDiscovery: true,
	})
	t.Cleanup(engine.Close)
	assert.Equal(t, root+"/project/"+rawID+".jsonl",
		engine.FindSourceFile(t.Context(), "host-a~cursor:"+rawID))
}
