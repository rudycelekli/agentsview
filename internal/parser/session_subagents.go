package parser

import (
	"context"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// SubagentTranscriptRoot returns the source scope holding a session's children.
func SubagentTranscriptRoot(agent AgentType, sessionPath string) string {
	if sessionPath == "" {
		return ""
	}
	switch agent {
	case AgentCodex, AgentTraeX, AgentAugureCode:
		return ""
	case AgentShelley:
		if _, _, ok := parseShelleyVirtualPath(sessionPath); ok {
			return sessionPath
		}
		return ""
	case AgentOpenCode, AgentKilo, AgentMiMoCode, AgentIcodemate:
		if _, ok := openCodeFamilySubagentRoot(agent, sessionPath); ok {
			return sessionPath
		}
		return ClaudeSubagentTranscriptRoot(sessionPath)
	case AgentPositAssistant:
		return filepath.Join(filepath.Dir(sessionPath), positAssistantSubagentsDir)
	case AgentWorkBuddy:
		if stem, ok := strings.CutSuffix(sessionPath, ".jsonl"); ok {
			return filepath.Join(stem, "subagents")
		}
		return ""
	case AgentPi, AgentOMP, AgentOMO, AgentStepCode:
		if root, ok := strings.CutSuffix(sessionPath, ".jsonl"); ok {
			return root
		}
		return ""
	case AgentCursor:
		if isS3URI(sessionPath) {
			return CursorS3SubagentRoot(CursorS3SourceRoot(sessionPath), sessionPath)
		}
		loc, ok := cursorTranscriptLocationFromPath(sessionPath)
		if !ok {
			return ""
		}
		dir := filepath.Dir(sessionPath)
		if loc.ParentRawID != "" {
			dir = filepath.Dir(filepath.Dir(dir))
		} else if filepath.Base(dir) == loc.RawID {
			dir = filepath.Dir(dir)
		}
		return filepath.Join(dir, loc.RawID, cursorSubagentsDirName)
	default:
		return ClaudeSubagentTranscriptRoot(sessionPath)
	}
}

// SubagentTranscriptDiscovery shares source inventories within one refresh.
type SubagentTranscriptDiscovery struct {
	storage map[string]openCodeStorageSubagents
}

// Paths discovers children within the requested session's source tree.
// rawSessionID keeps retained storage parents reachable after their source disappears.
func (d *SubagentTranscriptDiscovery) Paths(ctx context.Context, agent AgentType, sessionPath, rawSessionID string) ([]string, error) {
	root := SubagentTranscriptRoot(agent, sessionPath)
	if root == "" {
		return nil, nil
	}
	switch agent {
	case AgentWorkBuddy:
		return workBuddySubagentTranscriptPaths(ctx, root)
	case AgentShelley:
		dbPath, parentID, _ := parseShelleyVirtualPath(sessionPath)
		conn, err := OpenShelleyDB(dbPath)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		rows, err := conn.QueryContext(ctx,
			`WITH RECURSIVE parents AS MATERIALIZED (
				SELECT conversation_id AS id, TRIM(parent_conversation_id) AS parent FROM conversations
			), descendants(id) AS (
				SELECT id FROM parents WHERE parent = ?
				UNION
				SELECT c.id FROM parents c JOIN descendants d ON c.parent = d.id
			)
			SELECT id FROM descendants ORDER BY id`,
			parentID,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var paths []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			if IsValidSessionID(id) {
				paths = append(paths, ShelleyVirtualPath(dbPath, id))
			}
		}
		return paths, rows.Err()
	case AgentOpenCode, AgentKilo, AgentMiMoCode, AgentIcodemate:
		if dbPath, parentID, ok := parseOpenCodeFormatVirtualPath(openCodeFormatForAgent(agent).dbName, sessionPath); ok {
			return openCodeDescendantPaths(ctx, dbPath, parentID)
		}
		if projectDir, ok := openCodeFamilySubagentRoot(agent, sessionPath); ok {
			inventory, listed := d.storage[projectDir]
			if !listed {
				var err error
				inventory, err = discoverOpenCodeStorageSubagents(projectDir)
				if err != nil {
					return nil, err
				}
				if d.storage == nil {
					d.storage = make(map[string]openCodeStorageSubagents)
				}
				d.storage[projectDir] = inventory
			}
			return inventory.descendantPaths(rawSessionID), nil
		}
		return ClaudeSubagentTranscriptPaths(sessionPath), nil
	case AgentPositAssistant:
		var sources []SourceRef
		positAssistantSourceSet{}.collectConversationSources("", filepath.Dir(sessionPath), "", "", &sources, make(map[string]struct{}))
		paths := make([]string, 0, len(sources))
		for _, source := range sources {
			if source.DisplayPath != sessionPath {
				paths = append(paths, source.DisplayPath)
			}
		}
		slices.Sort(paths)
		return paths, nil
	case AgentPi, AgentOMP, AgentOMO, AgentStepCode:
		sources, err := NewJSONLSourceSet(agent, []string{root}, WithRecursive(), WithSymlinkFollowing(), WithIncludePath(isPiSourcePath)).Discover(ctx)
		paths := make([]string, 0, len(sources))
		for _, source := range sources {
			paths = append(paths, source.DisplayPath)
		}
		return paths, err
	case AgentCursor:
		seen := make(map[string]string)
		cursorCollectSubagentTranscripts(seen, root)
		paths := make([]string, 0, len(seen))
		for _, path := range seen {
			paths = append(paths, path)
		}
		slices.Sort(paths)
		return paths, nil
	default:
		return ClaudeSubagentTranscriptPaths(sessionPath), nil
	}
}

// SubagentPathsIncludeDescendants reports whether SubagentTranscriptDiscovery.Paths
// returns every descendant of sessionPath, not only direct children, so
// callers need not search the returned children again.
func SubagentPathsIncludeDescendants(agent AgentType, sessionPath string) bool {
	switch agent {
	case AgentShelley:
		return true
	case AgentOpenCode, AgentKilo, AgentMiMoCode, AgentIcodemate:
		_, ok := openCodeFamilySubagentRoot(agent, sessionPath)
		return ok
	default:
		return false
	}
}

// openCodeFamilySubagentRoot returns the database or storage project folder
// that links a session to its descendants through their parent IDs. Other
// paths, such as Icodemate JSONL transcripts, use Claude-style subagent
// folders instead.
func openCodeFamilySubagentRoot(agent AgentType, sessionPath string) (string, bool) {
	format := openCodeFormatForAgent(agent)
	if _, _, ok := parseOpenCodeFormatVirtualPath(format.dbName, sessionPath); ok {
		return sessionPath, true
	}
	// Storage sessions live at storage/<sessionSubdir>/<project>/<id>.json.
	projectDir := filepath.Dir(sessionPath)
	if filepath.Ext(sessionPath) == ".json" && filepath.Base(filepath.Dir(projectDir)) == format.sessionSubdir {
		return projectDir, true
	}
	return "", false
}

func openCodeFormatForAgent(agent AgentType) openCodeFormat {
	for _, format := range []openCodeFormat{openCodeFmt, kiloFmt, mimoFmt, icodemateFmt} {
		if format.agent == agent {
			return format
		}
	}
	return openCodeFormat{}
}

// openCodeDescendantPaths finds sessions linked to parentID only through their
// own parent_id, which parent parsing never sees.
func openCodeDescendantPaths(ctx context.Context, dbPath, parentID string) ([]string, error) {
	conn, err := openOpenCodeDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tables, idle, migration, err := openCodeSessionTablesCached(ctx, conn, dbPath)
	if err != nil {
		return nil, err
	}
	sources := make([]string, 0, len(tables))
	for _, table := range tables {
		sources = append(sources, "SELECT id, TRIM(parent_id) AS parent FROM "+openCodeSessionFrom(table, idle, migration))
	}
	rows, err := conn.QueryContext(ctx,
		`WITH RECURSIVE parents AS MATERIALIZED (`+strings.Join(sources, " UNION ")+`),
		descendants(id) AS (
			SELECT id FROM parents WHERE parent = ?
			UNION
			SELECT c.id FROM parents c JOIN descendants d ON c.parent = d.id
		)
		SELECT id FROM descendants ORDER BY id`,
		parentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if IsValidSessionID(id) {
			paths = append(paths, OpenCodeSQLiteVirtualPath(dbPath, id))
		}
	}
	return paths, rows.Err()
}

type openCodeStorageSubagents struct {
	children map[string][]string
	paths    map[string]string
}

func discoverOpenCodeStorageSubagents(projectDir string) (openCodeStorageSubagents, error) {
	inventory := openCodeStorageSubagents{
		children: make(map[string][]string),
		paths:    make(map[string]string),
	}
	entries, err := os.ReadDir(projectDir)
	if err != nil {
		return inventory, err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(projectDir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return inventory, err
		}
		var session openCodeStorageSessionFile
		if err := json.Unmarshal(raw, &session); err != nil || session.ID == "" {
			continue
		}
		inventory.paths[session.ID] = path
		if session.ParentID != "" {
			inventory.children[session.ParentID] = append(inventory.children[session.ParentID], session.ID)
		}
	}
	return inventory, nil
}

func (inventory openCodeStorageSubagents) descendantPaths(parentID string) []string {
	var descendants []string
	seen := map[string]bool{parentID: true}
	queue := inventory.children[parentID]
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		descendants = append(descendants, inventory.paths[id])
		queue = append(queue, inventory.children[id]...)
	}
	slices.Sort(descendants)
	return descendants
}
