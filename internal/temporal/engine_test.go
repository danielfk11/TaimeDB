package temporal

import (
	"path/filepath"
	"testing"

	"taimedb/internal/snapshot"
	"taimedb/internal/storage"
	"taimedb/internal/wal"
)

func TestEngineBranchAndMergeFlow(t *testing.T) {
	t.Parallel()

	walPath := filepath.Join(t.TempDir(), "taimedb.wal")
	walWriter, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("new WAL writer: %v", err)
	}

	engine := NewEngine(storage.NewMemoryEngine(), snapshot.NewEngine(2), walWriter)
	t.Cleanup(func() {
		_ = engine.Close()
	})

	mainCommit, _, err := engine.Upsert("users", "1", map[string]any{"name": "Joao", "plan": "free"}, "test")
	if err != nil {
		t.Fatalf("upsert main: %v", err)
	}

	if _, err := engine.CreateBranch("users", "1", "main", "feature"); err != nil {
		t.Fatalf("create branch: %v", err)
	}

	featureCommit, _, err := engine.UpsertOnBranch("users", "1", "feature", map[string]any{"name": "Joao", "plan": "enterprise"}, "test")
	if err != nil {
		t.Fatalf("upsert feature: %v", err)
	}

	stateMainBeforeMerge, _, err := engine.GetCurrentOnBranch("users", "1", "main")
	if err != nil {
		t.Fatalf("get current main before merge: %v", err)
	}
	if got := stateMainBeforeMerge["plan"]; got != "free" {
		t.Fatalf("unexpected main plan before merge: got %v", got)
	}

	mergeCommit, _, err := engine.MergeBranches("users", "1", "feature", "main", "test")
	if err != nil {
		t.Fatalf("merge branches: %v", err)
	}
	if mergeCommit.MergeParent != featureCommit.ID {
		t.Fatalf("unexpected merge parent: got %s want %s", mergeCommit.MergeParent, featureCommit.ID)
	}
	if mergeCommit.Parent != mainCommit.ID {
		t.Fatalf("unexpected merge parent chain: got %s want %s", mergeCommit.Parent, mainCommit.ID)
	}

	stateMainAfterMerge, _, err := engine.GetCurrentOnBranch("users", "1", "main")
	if err != nil {
		t.Fatalf("get current main after merge: %v", err)
	}
	if got := stateMainAfterMerge["plan"]; got != "enterprise" {
		t.Fatalf("unexpected main plan after merge: got %v", got)
	}
}

func TestEngineReplayFromWAL(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "taimedb.wal")
	storePath1 := filepath.Join(baseDir, "pebble-a")
	storePath2 := filepath.Join(baseDir, "pebble-b")

	store1, err := storage.NewPebbleEngine(storePath1)
	if err != nil {
		t.Fatalf("new pebble store 1: %v", err)
	}
	walWriter1, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("new wal writer 1: %v", err)
	}
	engine1 := NewEngine(store1, snapshot.NewEngine(2), walWriter1)

	_, _, err = engine1.Upsert("users", "1", map[string]any{"name": "Joao", "plan": "free"}, "test")
	if err != nil {
		t.Fatalf("seed commit 1: %v", err)
	}
	_, _, err = engine1.Upsert("users", "1", map[string]any{"name": "Joao", "plan": "pro"}, "test")
	if err != nil {
		t.Fatalf("seed commit 2: %v", err)
	}
	if _, err := engine1.CreateBranch("users", "1", "main", "feature"); err != nil {
		t.Fatalf("seed branch create: %v", err)
	}
	featureCommit, _, err := engine1.UpsertOnBranch("users", "1", "feature", map[string]any{"name": "Joao", "plan": "enterprise"}, "test")
	if err != nil {
		t.Fatalf("seed feature commit: %v", err)
	}

	if err := engine1.Close(); err != nil {
		t.Fatalf("close engine1: %v", err)
	}

	store2, err := storage.NewPebbleEngine(storePath2)
	if err != nil {
		t.Fatalf("new pebble store 2: %v", err)
	}
	walWriter2, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("new wal writer 2: %v", err)
	}
	engine2 := NewEngine(store2, snapshot.NewEngine(2), walWriter2)
	t.Cleanup(func() {
		_ = engine2.Close()
	})

	if err := engine2.ReplayFromWAL(walPath); err != nil {
		t.Fatalf("replay wal: %v", err)
	}

	mainState, _, err := engine2.GetCurrentOnBranch("users", "1", "main")
	if err != nil {
		t.Fatalf("get main after replay: %v", err)
	}
	if got := mainState["plan"]; got != "pro" {
		t.Fatalf("unexpected main plan after replay: got %v", got)
	}

	featureState, featureHead, err := engine2.GetCurrentOnBranch("users", "1", "feature")
	if err != nil {
		t.Fatalf("get feature after replay: %v", err)
	}
	if got := featureState["plan"]; got != "enterprise" {
		t.Fatalf("unexpected feature plan after replay: got %v", got)
	}
	if featureHead != featureCommit.ID {
		t.Fatalf("unexpected feature head after replay: got %s want %s", featureHead, featureCommit.ID)
	}

	nextCommit, _, err := engine2.Upsert("users", "1", map[string]any{"name": "Joao", "plan": "ultimate"}, "test")
	if err != nil {
		t.Fatalf("upsert after replay: %v", err)
	}
	if nextCommit.ID != "a4" {
		t.Fatalf("unexpected post-replay commit id: got %s want a4", nextCommit.ID)
	}
}
