package realtime

import (
	"sync"
	"time"
)

// Event is delivered to websocket subscribers when a document changes.
type Event struct {
	Event      string         `json:"event"`
	Commit     string         `json:"commit"`
	Collection string         `json:"collection"`
	Document   string         `json:"document"`
	Branch     string         `json:"branch"`
	Timestamp  time.Time      `json:"timestamp"`
	Changes    map[string]any `json:"changes"`
}

// Hub manages subscriptions by collection/document key.
type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan Event]struct{}
}

func NewHub() *Hub {
	return &Hub{subscribers: make(map[string]map[chan Event]struct{})}
}

func (h *Hub) Subscribe(collection, document, branch string) (<-chan Event, func()) {
	key := docKey(collection, document, normalizeBranch(branch))
	ch := make(chan Event, 32)

	h.mu.Lock()
	if _, ok := h.subscribers[key]; !ok {
		h.subscribers[key] = make(map[chan Event]struct{})
	}
	h.subscribers[key][ch] = struct{}{}
	h.mu.Unlock()

	unsubscribe := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		subs, ok := h.subscribers[key]
		if !ok {
			return
		}
		if _, ok := subs[ch]; ok {
			delete(subs, ch)
			close(ch)
		}
		if len(subs) == 0 {
			delete(h.subscribers, key)
		}
	}

	return ch, unsubscribe
}

func (h *Hub) Publish(event Event) {
	key := docKey(event.Collection, event.Document, normalizeBranch(event.Branch))

	h.mu.RLock()
	subs := h.subscribers[key]
	channels := make([]chan Event, 0, len(subs))
	for ch := range subs {
		channels = append(channels, ch)
	}
	h.mu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- event:
		default:
			// Drop slow subscriber events to avoid backpressure on writes.
		}
	}
}

func docKey(collection, document, branch string) string {
	return collection + "/" + document + "/" + branch
}

func normalizeBranch(branch string) string {
	if branch == "" {
		return "main"
	}
	return branch
}
