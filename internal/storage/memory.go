package storage

import (
	"encoding/json"
	"sync"
)

// MemoryEngine is an in-memory storage engine for current heads.
type MemoryEngine struct {
	mu   sync.RWMutex
	docs map[string]map[string]any
}

func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{docs: make(map[string]map[string]any)}
}

func (m *MemoryEngine) PutDocument(collection, document string, state map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docs[docKey(collection, document)] = cloneMap(state)
	return nil
}

func (m *MemoryEngine) GetDocument(collection, document string) (map[string]any, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.docs[docKey(collection, document)]
	if !ok {
		return nil, false, nil
	}
	return cloneMap(state), true, nil
}

func (m *MemoryEngine) DeleteDocument(collection, document string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.docs, docKey(collection, document))
	return nil
}

func (m *MemoryEngine) Close() error {
	return nil
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
