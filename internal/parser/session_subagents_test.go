package parser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubagentTranscriptDiscoveryReusesStorageInventoryForRetainedParent(t *testing.T) {
	project := filepath.Join(t.TempDir(), "storage", "session", "project")
	require.NoError(t, os.MkdirAll(project, 0o700))
	grandchildPath := filepath.Join(project, "ses_grandchild.json")
	require.NoError(t, os.WriteFile(grandchildPath,
		[]byte(`{"id":"ses_grandchild","parentID":"ses_child"}`), 0o600))
	var discovery SubagentTranscriptDiscovery
	paths, err := discovery.Paths(t.Context(), AgentOpenCode,
		filepath.Join(project, "ses_parent.json"), "ses_parent")
	require.NoError(t, err)
	assert.Empty(t, paths, "the source inventory lacks the intermediate child's metadata")

	// A later traversal uses the same inventory even if the directory changes.
	require.NoError(t, os.Remove(grandchildPath))
	paths, err = discovery.Paths(t.Context(), AgentOpenCode,
		filepath.Join(project, "ses_child.json"), "ses_child")
	require.NoError(t, err)
	assert.Equal(t, []string{grandchildPath}, paths)

	var nextRefresh SubagentTranscriptDiscovery
	paths, err = nextRefresh.Paths(t.Context(), AgentOpenCode,
		filepath.Join(project, "ses_child.json"), "ses_child")
	require.NoError(t, err)
	assert.Empty(t, paths, "the next refresh must observe source deletion")
}
