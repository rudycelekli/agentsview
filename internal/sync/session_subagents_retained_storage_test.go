package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
)

func TestSubagentRefreshDiscoversStorageGrandchildAfterSourceRemoved(t *testing.T) {
	for _, tc := range []struct {
		agent  parser.AgentType
		subdir string
		prefix string
	}{
		{parser.AgentOpenCode, "session", "opencode:"},
		{parser.AgentKilo, "session", "kilo:"},
		{parser.AgentMiMoCode, "session_diff", "mimocode:"},
		{parser.AgentIcodemate, "session_diff", "icodemate:"},
	} {
		for _, missing := range []string{"parent", "child", "parent and child"} {
			t.Run(string(tc.agent)+"/"+missing, func(t *testing.T) {
				root := t.TempDir()
				database := openTestDB(t)
				engine := NewEngine(t.Context(), database, EngineConfig{
					AgentDirs: map[parser.AgentType][]string{tc.agent: {root}},
					Machine:   "local", DeferStartupMaintenance: true,
				})
				t.Cleanup(engine.Close)
				write := func(id, parent string, output int) string {
					path := filepath.Join(root, "storage", tc.subdir, "global", id+".json")
					message := filepath.Join(root, "storage", "message", id, "msg_"+id+".json")
					part := filepath.Join(root, "storage", "part", "msg_"+id, "part_text.json")
					for path, content := range map[string]string{
						path:    fmt.Sprintf(`{"id":%q,"parentID":%q,"directory":"/workspace/project","time":{"created":1,"updated":2}}`, id, parent),
						message: fmt.Sprintf(`{"id":%q,"sessionID":%q,"role":"assistant","modelID":"model-a","providerID":"provider-a","tokens":{"input":20,"output":%d},"time":{"created":1,"completed":2}}`, "msg_"+id, id, output),
						part:    fmt.Sprintf(`{"id":"part_text","sessionID":%q,"messageID":%q,"type":"text","text":"answer"}`, id, "msg_"+id),
					} {
						require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
						require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
					}
					return path
				}
				parentPath := write("ses_parent", "", 10)
				childPath := write("ses_child", "ses_parent", 20)
				require.NoError(t, engine.SyncPathsContext(t.Context(), []string{parentPath, childPath}))
				children, err := database.GetChildSessions(t.Context(), tc.prefix+"ses_parent")
				require.NoError(t, err)
				require.Len(t, children, 1)
				if missing != "child" {
					require.NoError(t, os.Remove(parentPath))
				}
				if missing != "parent" {
					require.NoError(t, os.Remove(childPath))
				}
				write("ses_grandchild", "ses_child", 42)
				write("ses_unrelated", "", 100)

				err = engine.SyncSessionWithSubagentsContext(t.Context(), tc.prefix+"ses_parent")
				if missing != "child" {
					require.ErrorContains(t, err, "source file not found for "+tc.prefix+"ses_parent")
				} else {
					require.NoError(t, err)
				}

				grandchild, err := database.GetSession(t.Context(), tc.prefix+"ses_grandchild")
				require.NoError(t, err)
				require.NotNil(t, grandchild, "refresh must discover descendants through the retained parent")
				require.NotNil(t, grandchild.ParentSessionID)
				assert.Equal(t, tc.prefix+"ses_child", *grandchild.ParentSessionID)
				usage, err := database.GetSessionUsage(t.Context(), grandchild.ID, false)
				require.NoError(t, err)
				require.NotNil(t, usage)
				assert.Equal(t, 42, usage.TotalOutputTokens)
				parent, err := database.GetSession(t.Context(), tc.prefix+"ses_parent")
				require.NoError(t, err)
				assert.NotNil(t, parent)
				unrelated, err := database.GetSession(t.Context(), tc.prefix+"ses_unrelated")
				require.NoError(t, err)
				assert.Nil(t, unrelated)
				assert.False(t, engine.StartupReconciled())
			})
		}
	}
}
