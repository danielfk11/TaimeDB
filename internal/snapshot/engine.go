package snapshot

import (
	"encoding/json"
	"sync"
	"time"
)

// Snapshot captures a frozen document state at a specific commit.
type Snapshot struct {
	Snapshot   bool           `json:"snapshot"`
	Commit     string         `json:"commit"`
	Timestamp  time.Time      `json:"timestamp"`
	Collection string         `json:"collection"`
	Document   string         `json:"document"`
	State      map[string]any `json:"state"`
}

// Engine stores periodic snapshots for faster historical reconstruction.
type Engine struct {
	mu       sync.RWMutex
	interval int
	byCommit map[string]Snapshot
	byDoc    map[string][]Snapshot
}

func NewEngine(interval int) *Engine {
	if interval <= 0 {
		interval = 10
	}
	return &Engine{
		interval: interval,
		byCommit: make(map[string]Snapshot),
		byDoc:    make(map[string][]Snapshot),
	}
}

func (e *Engine) MaybeCreate(collection, document, commitID string, state map[string]any, docCommitCount int) (*Snapshot, bool) {
	if docCommitCount <= 0 || docCommitCount%e.interval != 0 {
		return nil, false
	}

	s := Snapshot{
		Snapshot:   true,
		Commit:     commitID,
		Timestamp:  time.Now().UTC(),
		Collection: collection,
		Document:   document,
		State:      cloneMap(state),
	}

	e.mu.Lock()
	e.byCommit[commitID] = s
	e.byDoc[docKey(collection, document)] = append(e.byDoc[docKey(collection, document)], s)
	e.mu.Unlock()

	out := s
	return &out, true
}

func (e *Engine) GetByCommit(commitID string) (Snapshot, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.byCommit[commitID]
	if !ok {
		return Snapshot{}, false
	}
	s.State = cloneMap(s.State)
	return s, true
}

func (e *Engine) List(collection, document string) []Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	snaps := e.byDoc[docKey(collection, document)]
	out := make([]Snapshot, 0, len(snaps))
	for _, s := range snaps {
		copyS := s
		copyS.State = cloneMap(s.State)
		out = append(out, copyS)
	}
	return out
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

func docKey(collection, document string) string {
	return collection + "/" + document
}
