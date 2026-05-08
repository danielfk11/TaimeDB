package storage

// Engine abstracts how current document states are persisted.
type Engine interface {
	PutDocument(collection, document string, state map[string]any) error
	GetDocument(collection, document string) (map[string]any, bool, error)
	DeleteDocument(collection, document string) error
	Close() error
}
