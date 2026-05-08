package storage

import (
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
)

// PebbleEngine persists current document heads in PebbleDB.
type PebbleEngine struct {
	db *pebble.DB
}

func NewPebbleEngine(path string) (*PebbleEngine, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleEngine{db: db}, nil
}

func (p *PebbleEngine) PutDocument(collection, document string, state map[string]any) error {
	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return p.db.Set(docKeyBytes(collection, document), buf, pebble.Sync)
}

func (p *PebbleEngine) GetDocument(collection, document string) (map[string]any, bool, error) {
	value, closer, err := p.db.Get(docKeyBytes(collection, document))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()

	var out map[string]any
	if err := json.Unmarshal(value, &out); err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (p *PebbleEngine) DeleteDocument(collection, document string) error {
	return p.db.Delete(docKeyBytes(collection, document), pebble.Sync)
}

func (p *PebbleEngine) Close() error {
	if p.db == nil {
		return nil
	}
	return p.db.Close()
}

func docKeyBytes(collection, document string) []byte {
	return []byte("docs/" + collection + "/" + document)
}
