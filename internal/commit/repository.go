package commit

import (
	"strconv"
	"strings"
	"sync"
)

// Repository stores commits and branch heads in-memory.
type Repository struct {
	mu      sync.RWMutex
	commits map[string]Commit
	heads   map[string]string
	history map[string][]string
}

func NewRepository() *Repository {
	return &Repository{
		commits: make(map[string]Commit),
		heads:   make(map[string]string),
		history: make(map[string][]string),
	}
}

func (r *Repository) Put(c Commit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits[c.ID] = c
	key := historyKey(c.Collection, c.Document, normalizeBranch(c.Branch))
	r.history[key] = append(r.history[key], c.ID)
}

// PutRecovered stores a commit during WAL replay.
func (r *Repository) PutRecovered(c Commit) {
	r.Put(c)
}

func (r *Repository) Get(id string) (Commit, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.commits[id]
	return c, ok
}

func (r *Repository) SetHead(collection, document, branch, commitID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heads[headKey(collection, document, normalizeBranch(branch))] = commitID
}

func (r *Repository) Head(collection, document, branch string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	head, ok := r.heads[headKey(collection, document, normalizeBranch(branch))]
	return head, ok
}

func (r *Repository) BranchHeads(collection, document string) map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	prefix := headKey(collection, document, "")
	out := make(map[string]string)
	for key, commitID := range r.heads {
		if !strings.HasPrefix(key, prefix) || len(key) <= len(prefix) {
			continue
		}
		branch := key[len(prefix):]
		out[branch] = commitID
	}
	return out
}

func (r *Repository) History(collection, document, branch string) []Commit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.history[historyKey(collection, document, normalizeBranch(branch))]
	out := make([]Commit, 0, len(ids))
	for _, id := range ids {
		if c, ok := r.commits[id]; ok {
			out = append(out, c)
		}
	}
	return out
}

func (r *Repository) HistoryCount(collection, document, branch string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.history[historyKey(collection, document, normalizeBranch(branch))])
}

func (r *Repository) ExistsBranch(collection, document, branch string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.heads[headKey(collection, document, normalizeBranch(branch))]
	return ok
}

func (r *Repository) CloneBranchHistory(collection, document, fromBranch, toBranch string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fromKey := historyKey(collection, document, normalizeBranch(fromBranch))
	toKey := historyKey(collection, document, normalizeBranch(toBranch))
	ids := r.history[fromKey]
	cloned := make([]string, len(ids))
	copy(cloned, ids)
	r.history[toKey] = cloned
}

func (r *Repository) MaxCommitIDNumeric() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var max uint64
	for id := range r.commits {
		if len(id) < 2 || id[0] != 'a' {
			continue
		}
		parsed, err := strconv.ParseUint(id[1:], 16, 64)
		if err != nil {
			continue
		}
		if parsed > max {
			max = parsed
		}
	}
	return max
}

func normalizeBranch(branch string) string {
	if branch == "" {
		return "main"
	}
	return branch
}

func headKey(collection, document, branch string) string {
	return collection + "/" + document + "/" + branch
}

func historyKey(collection, document, branch string) string {
	return collection + "/" + document + "/" + branch
}
