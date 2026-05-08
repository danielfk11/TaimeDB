package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"taimedb/internal/commit"
	"taimedb/internal/realtime"
	"taimedb/internal/temporal"
)

// Server exposes TaimeDB temporal capabilities via HTTP and websocket.
type Server struct {
	engine   *temporal.Engine
	hub      *realtime.Hub
	logger   *zap.Logger
	upgrader websocket.Upgrader
}

func NewServer(engine *temporal.Engine, hub *realtime.Hub, logger *zap.Logger) *Server {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Server{
		engine: engine,
		hub:    hub,
		logger: logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(_ *http.Request) bool {
				return true
			},
		},
	}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", s.handleHealth)
	r.Put("/collections/{collection}/{document}", s.handlePutDocument)
	r.Get("/collections/{collection}/{document}", s.handleGetDocument)
	r.Get("/history/{collection}/{document}", s.handleHistory)
	r.Get("/branches/{collection}/{document}", s.handleListBranches)
	r.Post("/branches/{collection}/{document}/{branch}", s.handleCreateBranch)
	r.Post("/merge/{collection}/{document}", s.handleMerge)
	r.Get("/diff/{from}/{to}", s.handleDiff)
	r.Post("/rollback/{commit}", s.handleRollback)
	r.Get("/ws", s.handleWS)
	return r
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handlePutDocument(w http.ResponseWriter, r *http.Request) {
	collection := chi.URLParam(r, "collection")
	document := chi.URLParam(r, "document")

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	author := r.URL.Query().Get("author")
	branch := queryBranch(r)
	createdCommit, createdSnapshot, err := s.engine.UpsertOnBranch(collection, document, branch, payload, author)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	s.hub.Publish(realtime.Event{
		Event:      "update",
		Commit:     createdCommit.ID,
		Collection: createdCommit.Collection,
		Document:   createdCommit.Document,
		Branch:     createdCommit.Branch,
		Timestamp:  createdCommit.Timestamp,
		Changes:    changesFromDiff(createdCommit.Diff),
	})

	writeJSON(w, http.StatusCreated, map[string]any{
		"commit":   createdCommit,
		"snapshot": createdSnapshot,
		"branch":   branch,
	})
}

func (s *Server) handleGetDocument(w http.ResponseWriter, r *http.Request) {
	collection := chi.URLParam(r, "collection")
	document := chi.URLParam(r, "document")
	commitID := r.URL.Query().Get("commit")
	at := r.URL.Query().Get("at")
	branch := queryBranch(r)

	if commitID != "" && at != "" {
		writeError(w, http.StatusBadRequest, "use either commit or at, not both")
		return
	}

	if commitID != "" {
		state, err := s.engine.GetAtCommit(collection, document, commitID)
		if err != nil {
			s.handleEngineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"collection": collection,
			"document":   document,
			"commit":     commitID,
			"branch":     branch,
			"data":       state,
		})
		return
	}

	if at != "" {
		timestamp, err := time.Parse(time.RFC3339, at)
		if err != nil {
			writeError(w, http.StatusBadRequest, "at must be RFC3339")
			return
		}
		state, selectedCommit, err := s.engine.GetAtTimeOnBranch(collection, document, branch, timestamp)
		if err != nil {
			s.handleEngineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"collection": collection,
			"document":   document,
			"commit":     selectedCommit,
			"branch":     branch,
			"at":         timestamp,
			"data":       state,
		})
		return
	}

	state, selectedCommit, err := s.engine.GetCurrentOnBranch(collection, document, branch)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"collection": collection,
		"document":   document,
		"commit":     selectedCommit,
		"branch":     branch,
		"data":       state,
	})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	collection := chi.URLParam(r, "collection")
	document := chi.URLParam(r, "document")
	branch := queryBranch(r)

	history, err := s.engine.HistoryOnBranch(collection, document, branch)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"branch": branch, "history": history})
}

func (s *Server) handleListBranches(w http.ResponseWriter, r *http.Request) {
	collection := chi.URLParam(r, "collection")
	document := chi.URLParam(r, "document")

	heads, err := s.engine.BranchHeads(collection, document)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"collection": collection,
		"document":   document,
		"heads":      heads,
	})
}

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	collection := chi.URLParam(r, "collection")
	document := chi.URLParam(r, "document")
	newBranch := chi.URLParam(r, "branch")
	fromBranch := r.URL.Query().Get("from")
	if fromBranch == "" {
		fromBranch = "main"
	}

	headCommit, err := s.engine.CreateBranch(collection, document, fromBranch, newBranch)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"collection": collection,
		"document":   document,
		"from":       fromBranch,
		"branch":     newBranch,
		"head":       headCommit,
	})
}

func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	collection := chi.URLParam(r, "collection")
	document := chi.URLParam(r, "document")
	fromBranch := r.URL.Query().Get("from")
	toBranch := r.URL.Query().Get("to")
	author := r.URL.Query().Get("author")

	if fromBranch == "" || toBranch == "" {
		writeError(w, http.StatusBadRequest, "from and to query params are required")
		return
	}

	mergeCommit, createdSnapshot, err := s.engine.MergeBranches(collection, document, fromBranch, toBranch, author)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	s.hub.Publish(realtime.Event{
		Event:      "merge",
		Commit:     mergeCommit.ID,
		Collection: mergeCommit.Collection,
		Document:   mergeCommit.Document,
		Branch:     mergeCommit.Branch,
		Timestamp:  mergeCommit.Timestamp,
		Changes:    changesFromDiff(mergeCommit.Diff),
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"commit":   mergeCommit,
		"snapshot": createdSnapshot,
	})
}

func (s *Server) handleDiff(w http.ResponseWriter, r *http.Request) {
	fromCommit := chi.URLParam(r, "from")
	toCommit := chi.URLParam(r, "to")

	diff, err := s.engine.Diff(fromCommit, toCommit)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"from": fromCommit,
		"to":   toCommit,
		"diff": diff,
	})
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	targetCommit := chi.URLParam(r, "commit")
	author := r.URL.Query().Get("author")

	rolledCommit, createdSnapshot, err := s.engine.Rollback(targetCommit, author)
	if err != nil {
		s.handleEngineError(w, err)
		return
	}

	s.hub.Publish(realtime.Event{
		Event:      "rollback",
		Commit:     rolledCommit.ID,
		Collection: rolledCommit.Collection,
		Document:   rolledCommit.Document,
		Branch:     rolledCommit.Branch,
		Timestamp:  rolledCommit.Timestamp,
		Changes:    changesFromDiff(rolledCommit.Diff),
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"commit":   rolledCommit,
		"snapshot": createdSnapshot,
	})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	collection := r.URL.Query().Get("collection")
	document := r.URL.Query().Get("document")
	branch := queryBranch(r)
	if collection == "" || document == "" {
		writeError(w, http.StatusBadRequest, "collection and document query params are required")
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("failed to upgrade websocket", zap.Error(err))
		return
	}
	defer conn.Close()

	events, unsubscribe := s.hub.Subscribe(collection, document, branch)
	defer unsubscribe()

	_ = conn.WriteJSON(map[string]any{
		"event":      "subscribed",
		"collection": collection,
		"document":   document,
		"branch":     branch,
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			if err := conn.WriteJSON(event); err != nil {
				return
			}
		case <-pingTicker.C:
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (s *Server) handleEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, temporal.ErrDocumentNotFound),
		errors.Is(err, temporal.ErrCommitNotFound),
		errors.Is(err, temporal.ErrCommitMismatch),
		errors.Is(err, temporal.ErrBranchNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, temporal.ErrBranchExists),
		errors.Is(err, temporal.ErrInvalidBranch),
		errors.Is(err, temporal.ErrBranchesEquivalent):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		s.logger.Error("request failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func queryBranch(r *http.Request) string {
	branch := r.URL.Query().Get("branch")
	if branch == "" {
		return "main"
	}
	return branch
}

func changesFromDiff(diff commit.Diff) map[string]any {
	changes := make(map[string]any, len(diff))
	for field, change := range diff {
		changes[field] = change.New
	}
	return changes
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
