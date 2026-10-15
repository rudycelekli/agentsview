package sync

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	gosync "sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestSubagentRefreshS3CodexInventoryScalesWithRoots(t *testing.T) {
	for _, count := range []int{2, 32} {
		for _, splitRoots := range []bool{false, true} {
			t.Run(fmt.Sprintf("children=%d/split=%t", count, splitRoots), func(t *testing.T) {
				const root = "s3://bucket/host-a/raw/codex"
				const parent = "11111111-1111-4111-8111-111111111111"
				const parentID = "host-a~codex:" + parent
				const timestamp = "2026-06-24T00:00:00Z"
				const parentKey = "host-a/raw/codex/sessions/rollout-2026-06-24T00-00-00-" + parent + ".jsonl"
				const parentPath = "s3://bucket/" + parentKey
				objects := map[string]string{parentKey: testjsonl.JoinJSONL(
					testjsonl.CodexSessionMetaJSON(parent, "/workspace/project", "user", timestamp),
					testjsonl.CodexMsgJSON("user", "parent task", timestamp),
				)}
				var childIDs, selectedKeys []string
				for i := range count + 3 {
					rawID := fmt.Sprintf("22222222-2222-4222-8222-%012d", i)
					childIDs = append(childIDs, "host-a~codex:"+rawID)
					objects[parentKey] += testjsonl.JoinJSONL(
						testjsonl.CodexFunctionCallWithCallIDJSON("spawn_agent", "spawn-"+rawID, map[string]any{"message": "child task"}, timestamp),
						testjsonl.CodexFunctionCallOutputJSON("spawn-"+rawID, `{"agent_id":"`+rawID+`"}`, timestamp),
					)
					key := "host-a/raw/codex/sessions/rollout-2026-06-24T00-00-00-" + rawID + ".jsonl"
					selectedKeys = append(selectedKeys, key)
					if i == count+2 { // An unresolved spawn must not list every root again.
						continue
					}
					for _, archive := range []bool{false, true} {
						if i == 0 && !archive {
							continue
						}
						objectKey, output := key, 42
						if archive {
							objectKey = strings.Replace(key, "/sessions/", "/archived_sessions/", 1)
							output = 7
						}
						objects[objectKey] = testjsonl.JoinJSONL(
							testjsonl.CodexSubagentSessionMetaJSON(rawID, parent, "/workspace/project", "user", timestamp),
							testjsonl.CodexTurnContextJSON("gpt-5", timestamp),
							testjsonl.CodexMsgJSON("assistant", "child answer", timestamp),
							testjsonl.CodexTokenCountJSON(timestamp, 20, output, 0),
						)
					}
					if i == 0 {
						selectedKeys[i] = strings.Replace(key, "/sessions/", "/archived_sessions/", 1)
					}
				}
				var mu gosync.Mutex
				listings, fetches := map[string]int{}, map[string]int{}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet && r.URL.Path == "/bucket/" {
						prefix := r.URL.Query().Get("prefix")
						mu.Lock()
						listings[prefix]++
						mu.Unlock()
						w.Header().Set("Content-Type", "application/xml")
						_, err := fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
						assert.NoError(t, err)
						var keys []string
						for key := range objects {
							if strings.HasPrefix(key, prefix) {
								keys = append(keys, key)
							}
						}
						slices.Sort(keys)
						for _, key := range keys {
							_, err := fmt.Fprintf(w, `<Contents><Key>%s</Key><LastModified>2026-06-24T00:00:00Z</LastModified><ETag>"fixture"</ETag><Size>%d</Size></Contents>`, key, len(objects[key]))
							assert.NoError(t, err)
						}
						_, err = fmt.Fprint(w, `</ListBucketResult>`)
						assert.NoError(t, err)
						return
					}
					key := strings.TrimPrefix(r.URL.Path, "/bucket/")
					content, ok := objects[key]
					if !ok {
						http.Error(w, `<Error><Code>NoSuchKey</Code></Error>`, http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(content)))
					w.Header().Set("Last-Modified", "Wed, 24 Jun 2026 00:00:00 GMT")
					w.Header().Set("ETag", `"fixture"`)
					if r.Method == http.MethodHead {
						return
					}
					if !assert.Equal(t, http.MethodGet, r.Method) {
						w.WriteHeader(http.StatusMethodNotAllowed)
						return
					}
					mu.Lock()
					fetches[key]++
					mu.Unlock()
					_, err := fmt.Fprint(w, content)
					assert.NoError(t, err)
				}))
				t.Cleanup(server.Close)
				t.Setenv("AWS_S3_ENDPOINT", server.URL)
				t.Setenv("AWS_REGION", "us-east-1")
				t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
				t.Setenv("AWS_SESSION_TOKEN", "")
				database := openTestDB(t)
				require.NoError(t, database.UpsertSession(t.Context(), db.Session{
					ID: parentID, Agent: "codex", Machine: "host-a", Project: "project", FilePath: new(parentPath),
				}))
				for _, id := range childIDs[count : count+2] {
					require.NoError(t, database.UpsertSession(t.Context(), db.Session{
						ID: id, Agent: "codex", Machine: "host-a", Project: "project", ParentSessionID: new(parentID),
					}))
				}
				require.NoError(t, database.DeleteSession(t.Context(), childIDs[count]))
				require.NoError(t, database.SoftDeleteSession(t.Context(), childIDs[count+1]))
				roots := []string{root}
				wantListings := map[string]int{"host-a/raw/codex/": 1}
				if splitRoots {
					roots = []string{root + "/archived_sessions", root + "/sessions"}
					wantListings = map[string]int{"host-a/raw/codex/archived_sessions/": 1, "host-a/raw/codex/sessions/": 1}
				}
				roots = append(roots, "s3://bucket/host-b/raw/codex")
				engine := NewEngine(t.Context(), database, EngineConfig{
					AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: roots},
					Machine:   "local", DeferStartupMaintenance: true, DisableFilesystemProjectDiscovery: true,
				})
				t.Cleanup(engine.Close)
				err := engine.SyncSessionWithSubagentsContext(t.Context(), parentID)
				require.ErrorContains(t, err, "cannot sync remote session "+childIDs[count+2]+" locally")
				for i, id := range childIDs[:count] {
					child, err := database.GetSessionFull(t.Context(), id)
					require.NoError(t, err)
					require.NotNil(t, child)
					assert.Equal(t, parentID, derefString(child.ParentSessionID))
					assert.Equal(t, "s3://bucket/"+selectedKeys[i], derefString(child.FilePath))
					usage, err := database.GetSessionUsage(t.Context(), id, false)
					require.NoError(t, err)
					require.NotNil(t, usage)
					if i == 0 {
						assert.Equal(t, 7, usage.TotalOutputTokens)
					} else {
						assert.Equal(t, 42, usage.TotalOutputTokens)
					}
				}
				assert.True(t, database.IsSessionExcluded(t.Context(), childIDs[count]))
				assert.True(t, database.IsSessionTrashed(t.Context(), childIDs[count+1]))
				assert.False(t, engine.StartupReconciled())
				mu.Lock()
				defer mu.Unlock()
				assert.Equal(t, wantListings, listings)
				assert.Len(t, fetches, count+1)
				assert.Equal(t, 1, fetches[parentKey])
				for _, key := range selectedKeys[:count] {
					assert.Equal(t, 1, fetches[key])
				}
			})
		}
	}
}
