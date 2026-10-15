package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
)

// Before data version 131, a thread row could hold a revert page's messages.
// Publish that page and move its remote pins before replacing the thread row.
// If the original rollout is absent, retire the duplicate thread transcript
// after preserving its curation. The transaction also covers later batches and
// leaves the old row intact if publication fails.
func (s *Sync) migrateCodexPages(
	ctx context.Context, tx *sql.Tx, batch []db.Session,
	markerID string, legacyMarkerMachines []string,
) error {
	threads := make(map[string]db.Session)
	// Before data version 134 a page whose header named its own rollout was
	// stored as codex:<rollout>. Map that old id to the page now stored as
	// codex:<thread>_<rollout>.
	rollouts := make(map[string]db.Session)
	for _, sess := range batch {
		switch sess.Agent {
		case "codex", "traex", "augure-code":
			prefixEnd := strings.LastIndex(sess.ID, ":") + 1
			if prefixEnd > 0 {
				key := sess.ID[prefixEnd:]
				id := sess.ID[:prefixEnd] + parser.CodexThreadIDFromSessionKey(key)
				if id != sess.ID {
					rollouts[sess.ID[:prefixEnd]+key[len(id)-prefixEnd+1:]] = sess
				}
				// A rebuilt trash anchor has no file and may still have an old
				// data version, even after restore. Migrate before publishing its
				// empty transcript when the retained page is in a later batch.
				if sess.ID == id && sess.DataVersion < 131 && sess.FilePath != nil {
					continue
				}
				threads[id] = sess
			}
		}
	}
	if len(threads) == 0 {
		return nil
	}
	if err := carryLegacyPageExclusions(ctx, tx, rollouts); err != nil {
		return err
	}
	// Purge inventories pages under the thread lock. Keep that same lock
	// through publication, including after the thread's migration or retirement.
	rows, err := tx.QueryContext(ctx, `
		SELECT id, COALESCE(file_path, ''), agent, machine, owner_marker, source_archive_id, data_version
		FROM sessions
		WHERE id = ANY($1) AND provenance_kind = 'legacy'
		ORDER BY id FOR UPDATE`, append(mapKeys(threads), mapKeys(rollouts)...))
	if err != nil {
		return fmt.Errorf("finding legacy Codex pages: %w", err)
	}
	defer rows.Close()
	type legacyPage struct {
		id, path, agent, machine, owner, archive string
		dataVersion                              int
	}
	var pages []legacyPage
	for rows.Next() {
		var page legacyPage
		if err := rows.Scan(&page.id, &page.path, &page.agent, &page.machine, &page.owner, &page.archive, &page.dataVersion); err != nil {
			return fmt.Errorf("reading legacy Codex pages: %w", err)
		}
		pages = append(pages, page)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading legacy Codex pages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, old := range pages {
		if rolloutPage, rolloutShape := rollouts[old.id]; rolloutShape {
			// The stored row and the batch page share one rollout uuid. When the
			// row recorded a file name, it must agree on <thread>_<rollout>; a
			// row with no path is matched by the uuid alone.
			prefixEnd := strings.LastIndex(rolloutPage.ID, ":") + 1
			key := parser.CodexSessionUUIDFromFilename(old.path[strings.LastIndexAny(old.path, `/\`)+1:])
			if old.dataVersion >= 134 || (old.path != "" && key != rolloutPage.ID[prefixEnd:]) ||
				!sameSessionOwner(
					old.owner, old.machine, markerID,
					db.MirroredSessionMachine(rolloutPage, s.machine), legacyMarkerMachines,
				) {
				continue
			}
			if err := s.renameLegacyRolloutPage(ctx, tx, old.id, old.agent, old.archive, rolloutPage.ID, markerID, legacyMarkerMachines); err != nil {
				return err
			}
			continue
		}
		syncScope := s.isFiltered() && !slices.ContainsFunc(batch, func(sess db.Session) bool { return sess.ID == old.id })
		migratePage := old.dataVersion < 131 && old.path != ""
		if !syncScope && !migratePage {
			continue
		}
		if !sameSessionOwner(
			old.owner, old.machine, markerID,
			db.MirroredSessionMachine(threads[old.id], s.machine), legacyMarkerMachines,
		) {
			continue
		}
		head, err := s.local.GetArtifactExportSession(ctx, old.id)
		if err != nil {
			return fmt.Errorf("reading original Codex rollout: %w", err)
		}
		if syncScope && head != nil && head.Agent == old.agent && threads[old.id].Agent == old.agent &&
			(old.archive == "" || old.archive == s.archiveID) && projectFailsFilter(head.Project, s.projects, s.excludeProjects) {
			// A filtered page restore still ends its source's thread scope.
			// Publish that dependency without the excluded project's content.
			if _, err := tx.ExecContext(ctx, `UPDATE sessions SET
				trash_includes_codex_pages = CASE
					WHEN deleted_at IS DISTINCT FROM source_deleted_at
						OR trash_includes_codex_pages IS DISTINCT FROM source_trash_includes_codex_pages
					THEN trash_includes_codex_pages ELSE $2 END,
				source_trash_includes_codex_pages = $2, updated_at = NOW()
				WHERE id = $1 AND source_trash_includes_codex_pages IS DISTINCT FROM $2`, old.id, head.TrashIncludesCodexPages); err != nil {
				return fmt.Errorf("publishing Codex source trash scope: %w", err)
			}
		}
		if !migratePage {
			continue
		}
		prefixEnd := strings.LastIndex(old.id, ":") + 1
		key := parser.CodexSessionUUIDFromFilename(old.path[strings.LastIndexAny(old.path, `/\`)+1:])
		if key == "" || key == old.id[prefixEnd:] || parser.CodexThreadIDFromSessionKey(key) != old.id[prefixEnd:] {
			continue
		}
		pins, err := snapshotPinnedMessages(ctx, tx, old.id)
		if err != nil {
			return err
		}
		// An unpinned thread with a real original rollout is replaced normally,
		// even if the old page has since been removed or filtered out locally.
		hasHead := head != nil && head.FilePath != nil
		if hasHead && len(pins) == 0 {
			continue
		}
		if old.archive != "" && old.archive != s.archiveID {
			return errors.New("cannot migrate Codex pages from another source archive")
		}
		pageID := old.id[:prefixEnd] + key
		// Keep parser-owned names separate from user renames when publishing.
		page, err := s.local.GetArtifactExportSession(ctx, pageID)
		if err != nil {
			return fmt.Errorf("reading retained Codex page: %w", err)
		}
		if page == nil || page.Agent != old.agent {
			return fmt.Errorf("cannot replace legacy Codex thread: retained page %s is unavailable", pageID)
		}
		if projectFailsFilter(page.Project, s.projects, s.excludeProjects) {
			return fmt.Errorf("preserving the Codex page requires including project %q in the push", page.Project)
		}
		var pageExists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sessions WHERE id = $1)`, pageID).Scan(&pageExists); err != nil {
			return fmt.Errorf("checking retained Codex page: %w", err)
		}
		// Apply a selected empty anchor's current trash scope before the page
		// can inherit it. Its messages remain available for pin matching.
		if head != nil && !hasHead && slices.ContainsFunc(batch, func(sess db.Session) bool { return sess.ID == old.id }) {
			if err := s.pushSession(ctx, tx, *head, markerID, legacyMarkerMachines); err != nil {
				return fmt.Errorf("publishing Codex trash anchor metadata: %w", err)
			}
		}
		if err := s.publishRetainedCodexPage(ctx, tx, *page, pageID, pins, markerID, legacyMarkerMachines); err != nil {
			return err
		}
		// SQLite may keep a thread row without a file as a trash-scope anchor.
		// It does not represent a surviving original rollout.
		if !hasHead {
			if err := retireCodexPageThread(ctx, tx, old.id, pageID, pageExists); err != nil {
				return err
			}
		}
	}
	return nil
}

// carryLegacyPageExclusions moves a remote exclusion recorded under an old
// codex:<rollout> id to the page's current id, so a permanently deleted page
// stays deleted after the rename. Only a remote that still holds such a row
// needs it, which one lookup per batch tells; every row found moves, whatever
// its owner, since the exclusion follows the id and not a machine.
func carryLegacyPageExclusions(ctx context.Context, tx *sql.Tx, rollouts map[string]db.Session) error {
	if len(rollouts) == 0 {
		return nil
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM excluded_sessions WHERE id = ANY($1)`, mapKeys(rollouts))
	if err != nil {
		return fmt.Errorf("finding Codex page exclusions: %w", err)
	}
	defer rows.Close()
	var stale []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("reading Codex page exclusions: %w", err)
		}
		stale = append(stale, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading Codex page exclusions: %w", err)
	}
	for _, oldID := range stale {
		// A row already under the page's id wins; the stale one is dropped.
		if _, err := tx.ExecContext(ctx, `
			WITH moved AS (DELETE FROM excluded_sessions WHERE id = $1 RETURNING *)
			INSERT INTO excluded_sessions (id, include_codex_pages, owner_marker, machine, agent, created_at)
			SELECT $2, include_codex_pages, owner_marker, machine, agent, created_at FROM moved
			ON CONFLICT (id) DO NOTHING`, oldID, rollouts[oldID].ID); err != nil {
			return fmt.Errorf("moving Codex page exclusion: %w", err)
		}
	}
	return nil
}

// publishRetainedCodexPage publishes the page row, its messages and findings
// at pageID and restores the pins taken from the row it replaces.
func (s *Sync) publishRetainedCodexPage(
	ctx context.Context, tx *sql.Tx, page db.Session, pageID string,
	pins []savedPostgresPin, markerID string, legacyMarkerMachines []string,
) error {
	if err := s.pushSession(ctx, tx, page, markerID, legacyMarkerMachines); err != nil {
		return fmt.Errorf("publishing retained Codex page: %w", err)
	}
	if _, err := s.pushMessages(ctx, tx, pageID, true, nil, nil); err != nil {
		return err
	}
	if _, err := s.pushSecretFindings(ctx, tx, pageID); err != nil {
		return err
	}
	return restorePinnedMessages(ctx, tx, pageID, pins)
}

// renameLegacyRolloutPage replaces a remote page row stored as
// codex:<rollout> with the page at its current id, keeping its pins, star,
// name, and trash state.
func (s *Sync) renameLegacyRolloutPage(
	ctx context.Context, tx *sql.Tx, oldID, agent, archive, pageID string,
	markerID string, legacyMarkerMachines []string,
) error {
	if archive != "" && archive != s.archiveID {
		return errors.New("cannot migrate Codex pages from another source archive")
	}
	page, err := s.local.GetArtifactExportSession(ctx, pageID)
	if err != nil {
		return fmt.Errorf("reading retained Codex page: %w", err)
	}
	if page == nil || page.Agent != agent {
		return fmt.Errorf("cannot replace legacy Codex page: retained page %s is unavailable", pageID)
	}
	if projectFailsFilter(page.Project, s.projects, s.excludeProjects) {
		return fmt.Errorf("preserving the Codex page requires including project %q in the push", page.Project)
	}
	pins, err := snapshotPinnedMessages(ctx, tx, oldID)
	if err != nil {
		return err
	}
	var pageExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sessions WHERE id = $1)`, pageID).Scan(&pageExists); err != nil {
		return fmt.Errorf("checking retained Codex page: %w", err)
	}
	if err := s.publishRetainedCodexPage(ctx, tx, *page, pageID, pins, markerID, legacyMarkerMachines); err != nil {
		return err
	}
	return retireCodexPageThread(ctx, tx, oldID, pageID, pageExists)
}

func retireCodexPageThread(ctx context.Context, tx *sql.Tx, threadID, pageID string, pageExists bool) error {
	// Existing page curation is newer than the old thread projection. For a
	// newly published page, transfer remote overrides while keeping the page's
	// source baselines, so the next push does not undo those choices.
	if !pageExists {
		if _, err := tx.ExecContext(ctx, `
			UPDATE sessions page SET
				display_name = CASE WHEN NOT page.prompt_evidence_discarded
					AND old.display_name IS DISTINCT FROM old.source_display_name
					THEN old.display_name ELSE page.display_name END,
				deleted_at = CASE WHEN old.deleted_at IS DISTINCT FROM old.source_deleted_at
					THEN old.deleted_at ELSE page.deleted_at END,
				deletion_cause = CASE WHEN old.deleted_at IS DISTINCT FROM old.source_deleted_at
					THEN old.deletion_cause ELSE page.deletion_cause END
			FROM sessions old WHERE page.id = $1 AND old.id = $2`, pageID, threadID); err != nil {
			return fmt.Errorf("moving Codex page curation: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO starred_sessions (session_id, created_at)
			SELECT $1, created_at FROM starred_sessions WHERE session_id = $2
			ON CONFLICT (session_id) DO NOTHING`, pageID, threadID); err != nil {
			return fmt.Errorf("moving Codex page star: %w", err)
		}
	}
	if err := clearSessionVectorsTx(ctx, tx, threadID); err != nil {
		return err
	}
	// Cascades discard all duplicate content and derived usage. A trashed
	// thread-wide scope still needs an empty anchor for returning pages and
	// restore/purge; retain its ownership and source/remote trash baselines.
	_, err := tx.ExecContext(ctx, `
		WITH retired AS (DELETE FROM sessions WHERE id = $1 RETURNING *)
		INSERT INTO sessions (
			id, machine, owner_marker, project, agent, created_at,
			deleted_at, source_deleted_at, deletion_cause,
			trash_includes_codex_pages, source_trash_includes_codex_pages,
			source_archive_id, source_database_generation, data_version)
		SELECT id, machine, owner_marker, project, agent, created_at,
			deleted_at, source_deleted_at, deletion_cause,
			trash_includes_codex_pages, source_trash_includes_codex_pages,
			source_archive_id, source_database_generation, 131
		FROM retired WHERE trash_includes_codex_pages AND deleted_at IS NOT NULL`, threadID)
	if err != nil {
		return fmt.Errorf("retiring duplicate Codex thread: %w", err)
	}
	return nil
}
