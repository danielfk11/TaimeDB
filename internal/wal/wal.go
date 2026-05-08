package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"taimedb/internal/commit"
)

// Record is appended to the write-ahead log before commits are persisted.
type Record struct {
	Timestamp   time.Time      `json:"timestamp"`
	CommitID    string         `json:"commit"`
	Parent      string         `json:"parent,omitempty"`
	MergeParent string         `json:"merge_parent,omitempty"`
	Collection  string         `json:"collection"`
	Document    string         `json:"document"`
	Operation   string         `json:"operation"`
	Author      string         `json:"author"`
	Branch      string         `json:"branch"`
	Diff        commit.Diff    `json:"diff"`
	State       map[string]any `json:"state,omitempty"`
}

func FromCommit(c commit.Commit) Record {
	return Record{
		Timestamp:   c.Timestamp,
		CommitID:    c.ID,
		Parent:      c.Parent,
		MergeParent: c.MergeParent,
		Collection:  c.Collection,
		Document:    c.Document,
		Operation:   c.Operation,
		Author:      c.Author,
		Branch:      c.Branch,
		Diff:        c.Diff,
		State:       cloneMap(c.State),
	}
}

func (r Record) ToCommit() commit.Commit {
	return commit.Commit{
		ID:          r.CommitID,
		Parent:      r.Parent,
		MergeParent: r.MergeParent,
		Timestamp:   r.Timestamp,
		Collection:  r.Collection,
		Document:    r.Document,
		Operation:   r.Operation,
		Author:      r.Author,
		Branch:      r.Branch,
		Diff:        r.Diff,
		State:       cloneMap(r.State),
	}
}

// Writer appends WAL records as JSON lines.
type Writer struct {
	mu   sync.Mutex
	file *os.File
}

func NewWriter(path string) (*Writer, error) {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{file: f}, nil
}

func (w *Writer) Append(record Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

// Replay reads a WAL file and calls apply for each record in order.
func Replay(path string, apply func(record Record) error) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	lineNo := 0
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			var record Record
			if unmarshalErr := json.Unmarshal(line, &record); unmarshalErr != nil {
				return fmt.Errorf("decode WAL line %d: %w", lineNo, unmarshalErr)
			}
			if applyErr := apply(record); applyErr != nil {
				return fmt.Errorf("apply WAL line %d: %w", lineNo, applyErr)
			}
		}

		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
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
