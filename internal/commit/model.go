package commit

import "time"

// FieldChange stores how a single field changed between two states.
type FieldChange struct {
	Old any `json:"old,omitempty"`
	New any `json:"new,omitempty"`
}

// Diff maps field names to the corresponding change.
type Diff map[string]FieldChange

// Commit is the atomic unit of historical change in TaimeDB.
type Commit struct {
	ID          string         `json:"commit"`
	Parent      string         `json:"parent,omitempty"`
	MergeParent string         `json:"merge_parent,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
	Collection  string         `json:"collection"`
	Document    string         `json:"document"`
	Operation   string         `json:"operation"`
	Author      string         `json:"author"`
	Branch      string         `json:"branch"`
	Diff        Diff           `json:"diff"`
	State       map[string]any `json:"state,omitempty"`
}
