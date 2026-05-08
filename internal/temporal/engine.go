package temporal

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"taimedb/internal/commit"
	"taimedb/internal/snapshot"
	"taimedb/internal/storage"
	"taimedb/internal/wal"
)

const defaultBranch = "main"

var (
	ErrDocumentNotFound   = errors.New("document not found")
	ErrCommitNotFound     = errors.New("commit not found")
	ErrCommitMismatch     = errors.New("commit does not match requested document")
	ErrBranchNotFound     = errors.New("branch not found")
	ErrBranchExists       = errors.New("branch already exists")
	ErrInvalidBranch      = errors.New("invalid branch")
	ErrBranchesEquivalent = errors.New("branches already equivalent")
)

// Engine coordinates temporal semantics on top of storage and WAL.
type Engine struct {
	mu        sync.RWMutex
	repo      *commit.Repository
	snapshots *snapshot.Engine
	storage   storage.Engine
	wal       *wal.Writer
	idSeq     uint64
}

func NewEngine(store storage.Engine, snaps *snapshot.Engine, walWriter *wal.Writer) *Engine {
	if store == nil {
		store = storage.NewMemoryEngine()
	}
	if snaps == nil {
		snaps = snapshot.NewEngine(10)
	}
	return &Engine{
		repo:      commit.NewRepository(),
		snapshots: snaps,
		storage:   store,
		wal:       walWriter,
	}
}

func (e *Engine) Upsert(collection, document string, newState map[string]any, author string) (commit.Commit, *snapshot.Snapshot, error) {
	return e.UpsertOnBranch(collection, document, defaultBranch, newState, author)
}

func (e *Engine) UpsertOnBranch(collection, document, branch string, newState map[string]any, author string) (commit.Commit, *snapshot.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if collection == "" || document == "" {
		return commit.Commit{}, nil, errors.New("collection and document are required")
	}

	branch = normalizeBranch(branch)
	if branch == "" {
		return commit.Commit{}, nil, ErrInvalidBranch
	}

	normalizedNewState := cloneMap(newState)
	if normalizedNewState == nil {
		normalizedNewState = make(map[string]any)
	}

	headID, hasHead := e.repo.Head(collection, document, branch)
	if !hasHead && branch != defaultBranch {
		return commit.Commit{}, nil, ErrBranchNotFound
	}

	oldState := map[string]any{}
	operation := "create"
	parent := ""
	if hasHead {
		head, ok := e.repo.Get(headID)
		if !ok {
			return commit.Commit{}, nil, ErrCommitNotFound
		}
		oldState = cloneMap(head.State)
		operation = "update"
		parent = head.ID
	}

	c := commit.Commit{
		ID:         e.nextCommitID(),
		Parent:     parent,
		Timestamp:  time.Now().UTC(),
		Collection: collection,
		Document:   document,
		Operation:  operation,
		Author:     normalizeAuthor(author),
		Branch:     branch,
		Diff:       computeDiff(oldState, normalizedNewState),
		State:      cloneMap(normalizedNewState),
	}

	snap, err := e.persistCommitLocked(c)
	if err != nil {
		return commit.Commit{}, nil, err
	}
	return c, snap, nil
}

func (e *Engine) GetCurrent(collection, document string) (map[string]any, string, error) {
	return e.GetCurrentOnBranch(collection, document, defaultBranch)
}

func (e *Engine) GetCurrentOnBranch(collection, document, branch string) (map[string]any, string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	branch = normalizeBranch(branch)
	headID, ok := e.repo.Head(collection, document, branch)
	if !ok {
		if branch != defaultBranch {
			return nil, "", ErrBranchNotFound
		}
		return nil, "", ErrDocumentNotFound
	}

	state, found, err := e.storage.GetDocument(collection, storageDocumentKey(document, branch))
	if err != nil {
		return nil, "", err
	}
	if found {
		return state, headID, nil
	}

	head, ok := e.repo.Get(headID)
	if !ok {
		return nil, "", ErrCommitNotFound
	}
	return cloneMap(head.State), head.ID, nil
}

func (e *Engine) GetAtCommit(collection, document, commitID string) (map[string]any, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	c, ok := e.repo.Get(commitID)
	if !ok {
		return nil, ErrCommitNotFound
	}
	if c.Collection != collection || c.Document != document {
		return nil, ErrCommitMismatch
	}
	return cloneMap(c.State), nil
}

func (e *Engine) GetAtTime(collection, document string, at time.Time) (map[string]any, string, error) {
	return e.GetAtTimeOnBranch(collection, document, defaultBranch, at)
}

func (e *Engine) GetAtTimeOnBranch(collection, document, branch string, at time.Time) (map[string]any, string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	branch = normalizeBranch(branch)
	history := e.repo.History(collection, document, branch)
	if len(history) == 0 {
		if branch != defaultBranch {
			return nil, "", ErrBranchNotFound
		}
		return nil, "", ErrDocumentNotFound
	}

	var selected *commit.Commit
	for i := range history {
		c := history[i]
		if !c.Timestamp.After(at) {
			copyC := c
			selected = &copyC
		}
	}
	if selected == nil {
		return nil, "", ErrDocumentNotFound
	}

	return cloneMap(selected.State), selected.ID, nil
}

func (e *Engine) History(collection, document string) ([]commit.Commit, error) {
	return e.HistoryOnBranch(collection, document, defaultBranch)
}

func (e *Engine) HistoryOnBranch(collection, document, branch string) ([]commit.Commit, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	branch = normalizeBranch(branch)
	history := e.repo.History(collection, document, branch)
	if len(history) == 0 {
		if branch != defaultBranch {
			return nil, ErrBranchNotFound
		}
		return nil, ErrDocumentNotFound
	}
	return history, nil
}

func (e *Engine) Diff(fromCommitID, toCommitID string) (commit.Diff, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	fromCommit, ok := e.repo.Get(fromCommitID)
	if !ok {
		return nil, ErrCommitNotFound
	}
	toCommit, ok := e.repo.Get(toCommitID)
	if !ok {
		return nil, ErrCommitNotFound
	}

	return computeDiff(fromCommit.State, toCommit.State), nil
}

func (e *Engine) Rollback(targetCommitID, author string) (commit.Commit, *snapshot.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	target, ok := e.repo.Get(targetCommitID)
	if !ok {
		return commit.Commit{}, nil, ErrCommitNotFound
	}

	branch := normalizeBranch(target.Branch)
	headID, ok := e.repo.Head(target.Collection, target.Document, branch)
	if !ok {
		return commit.Commit{}, nil, ErrBranchNotFound
	}
	head, ok := e.repo.Get(headID)
	if !ok {
		return commit.Commit{}, nil, ErrCommitNotFound
	}

	revertedState := cloneMap(target.State)
	rollbackCommit := commit.Commit{
		ID:         e.nextCommitID(),
		Parent:     head.ID,
		Timestamp:  time.Now().UTC(),
		Collection: target.Collection,
		Document:   target.Document,
		Operation:  "rollback",
		Author:     normalizeAuthor(author),
		Branch:     branch,
		Diff:       computeDiff(head.State, revertedState),
		State:      cloneMap(revertedState),
	}

	snap, err := e.persistCommitLocked(rollbackCommit)
	if err != nil {
		return commit.Commit{}, nil, err
	}
	return rollbackCommit, snap, nil
}

func (e *Engine) CreateBranch(collection, document, fromBranch, newBranch string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	fromBranch = normalizeBranch(fromBranch)
	newBranch = normalizeBranch(newBranch)
	if newBranch == "" {
		return "", ErrInvalidBranch
	}
	if e.repo.ExistsBranch(collection, document, newBranch) {
		return "", ErrBranchExists
	}

	fromHeadID, ok := e.repo.Head(collection, document, fromBranch)
	if !ok {
		if fromBranch != defaultBranch {
			return "", ErrBranchNotFound
		}
		return "", ErrDocumentNotFound
	}
	fromHead, ok := e.repo.Get(fromHeadID)
	if !ok {
		return "", ErrCommitNotFound
	}

	e.repo.CloneBranchHistory(collection, document, fromBranch, newBranch)
	e.repo.SetHead(collection, document, newBranch, fromHeadID)
	if err := e.storage.PutDocument(collection, storageDocumentKey(document, newBranch), fromHead.State); err != nil {
		return "", err
	}

	return fromHeadID, nil
}

func (e *Engine) MergeBranches(collection, document, fromBranch, toBranch, author string) (commit.Commit, *snapshot.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	fromBranch = normalizeBranch(fromBranch)
	toBranch = normalizeBranch(toBranch)
	if fromBranch == "" || toBranch == "" || fromBranch == toBranch {
		return commit.Commit{}, nil, ErrInvalidBranch
	}

	fromHeadID, ok := e.repo.Head(collection, document, fromBranch)
	if !ok {
		return commit.Commit{}, nil, ErrBranchNotFound
	}
	toHeadID, ok := e.repo.Head(collection, document, toBranch)
	if !ok {
		return commit.Commit{}, nil, ErrBranchNotFound
	}

	fromHead, ok := e.repo.Get(fromHeadID)
	if !ok {
		return commit.Commit{}, nil, ErrCommitNotFound
	}
	toHead, ok := e.repo.Get(toHeadID)
	if !ok {
		return commit.Commit{}, nil, ErrCommitNotFound
	}

	mergedState := mergeStates(toHead.State, fromHead.State)
	diff := computeDiff(toHead.State, mergedState)
	if len(diff) == 0 && fromHeadID == toHeadID {
		return commit.Commit{}, nil, ErrBranchesEquivalent
	}

	mergeCommit := commit.Commit{
		ID:          e.nextCommitID(),
		Parent:      toHead.ID,
		MergeParent: fromHead.ID,
		Timestamp:   time.Now().UTC(),
		Collection:  collection,
		Document:    document,
		Operation:   "merge",
		Author:      normalizeAuthor(author),
		Branch:      toBranch,
		Diff:        diff,
		State:       cloneMap(mergedState),
	}

	snap, err := e.persistCommitLocked(mergeCommit)
	if err != nil {
		return commit.Commit{}, nil, err
	}
	return mergeCommit, snap, nil
}

func (e *Engine) BranchHeads(collection, document string) (map[string]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	heads := e.repo.BranchHeads(collection, document)
	if len(heads) == 0 {
		return nil, ErrDocumentNotFound
	}
	return heads, nil
}

func (e *Engine) ReplayFromWAL(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := wal.Replay(path, func(record wal.Record) error {
		recovered := record.ToCommit()
		recovered.Branch = normalizeBranch(recovered.Branch)
		recovered.Author = normalizeAuthor(recovered.Author)
		if recovered.State == nil {
			base := map[string]any{}
			if recovered.Parent != "" {
				if parent, ok := e.repo.Get(recovered.Parent); ok {
					base = cloneMap(parent.State)
				}
			}
			recovered.State = applyDiff(base, recovered.Diff)
		}

		e.repo.PutRecovered(recovered)
		e.repo.SetHead(recovered.Collection, recovered.Document, recovered.Branch, recovered.ID)
		if err := e.storage.PutDocument(recovered.Collection, storageDocumentKey(recovered.Document, recovered.Branch), recovered.State); err != nil {
			return err
		}

		commitCount := e.repo.HistoryCount(recovered.Collection, recovered.Document, recovered.Branch)
		_, _ = e.snapshots.MaybeCreate(recovered.Collection, recovered.Document, recovered.ID, recovered.State, commitCount)
		return nil
	}); err != nil {
		return err
	}

	atomic.StoreUint64(&e.idSeq, e.repo.MaxCommitIDNumeric())
	return nil
}

func (e *Engine) Close() error {
	var errs []error
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.storage != nil {
		if err := e.storage.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) persistCommitLocked(c commit.Commit) (*snapshot.Snapshot, error) {
	if e.wal != nil {
		if err := e.wal.Append(wal.FromCommit(c)); err != nil {
			return nil, err
		}
	}

	if err := e.storage.PutDocument(c.Collection, storageDocumentKey(c.Document, c.Branch), c.State); err != nil {
		return nil, err
	}

	e.repo.Put(c)
	e.repo.SetHead(c.Collection, c.Document, c.Branch, c.ID)

	commitCount := e.repo.HistoryCount(c.Collection, c.Document, c.Branch)
	snap, _ := e.snapshots.MaybeCreate(c.Collection, c.Document, c.ID, c.State, commitCount)
	return snap, nil
}

func (e *Engine) nextCommitID() string {
	n := atomic.AddUint64(&e.idSeq, 1)
	return fmt.Sprintf("a%x", n)
}

func normalizeAuthor(author string) string {
	if author == "" {
		return "system"
	}
	return author
}

func normalizeBranch(branch string) string {
	if branch == "" {
		return defaultBranch
	}
	return branch
}

func storageDocumentKey(document, branch string) string {
	return document + "@@" + normalizeBranch(branch)
}

func mergeStates(targetState, sourceState map[string]any) map[string]any {
	merged := cloneMap(targetState)
	if merged == nil {
		merged = make(map[string]any)
	}
	for key, value := range sourceState {
		merged[key] = value
	}
	return merged
}

func applyDiff(base map[string]any, diff commit.Diff) map[string]any {
	state := cloneMap(base)
	if state == nil {
		state = make(map[string]any)
	}
	for field, change := range diff {
		if change.New == nil {
			delete(state, field)
			continue
		}
		state[field] = change.New
	}
	return state
}

func computeDiff(oldState, newState map[string]any) commit.Diff {
	if oldState == nil {
		oldState = map[string]any{}
	}
	if newState == nil {
		newState = map[string]any{}
	}

	keys := make(map[string]struct{}, len(oldState)+len(newState))
	for k := range oldState {
		keys[k] = struct{}{}
	}
	for k := range newState {
		keys[k] = struct{}{}
	}

	diff := make(commit.Diff)
	for k := range keys {
		oldValue, hadOld := oldState[k]
		newValue, hasNew := newState[k]

		switch {
		case hadOld && !hasNew:
			diff[k] = commit.FieldChange{Old: oldValue, New: nil}
		case !hadOld && hasNew:
			diff[k] = commit.FieldChange{Old: nil, New: newValue}
		case !reflect.DeepEqual(oldValue, newValue):
			diff[k] = commit.FieldChange{Old: oldValue, New: newValue}
		}
	}
	return diff
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	buf, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(buf, &out)
	return out
}
