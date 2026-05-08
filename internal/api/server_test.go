package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"go.uber.org/zap"

	"taimedb/internal/realtime"
	"taimedb/internal/snapshot"
	"taimedb/internal/storage"
	"taimedb/internal/temporal"
	"taimedb/internal/wal"
)

func TestServerDocumentLifecycleAndTemporalRoutes(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	router := server.Router()

	mustRequest(t, router, http.MethodPut, "/collections/users/1", map[string]any{"name": "Joao", "plan": "free"}, http.StatusCreated)
	mustRequest(t, router, http.MethodPut, "/collections/users/1", map[string]any{"name": "Joao", "plan": "pro"}, http.StatusCreated)

	historyResp := mustRequest(t, router, http.MethodGet, "/history/users/1", nil, http.StatusOK)
	history := historyResp["history"].([]any)
	if len(history) != 2 {
		t.Fatalf("unexpected history size: got %d want 2", len(history))
	}

	diffResp := mustRequest(t, router, http.MethodGet, "/diff/a1/a2", nil, http.StatusOK)
	diffMap := diffResp["diff"].(map[string]any)
	planDiff := diffMap["plan"].(map[string]any)
	if planDiff["old"] != "free" || planDiff["new"] != "pro" {
		t.Fatalf("unexpected diff payload: %#v", planDiff)
	}

	mustRequest(t, router, http.MethodPost, "/rollback/a1", nil, http.StatusOK)
	currentResp := mustRequest(t, router, http.MethodGet, "/collections/users/1", nil, http.StatusOK)
	currentData := currentResp["data"].(map[string]any)
	if currentData["plan"] != "free" {
		t.Fatalf("unexpected plan after rollback: %v", currentData["plan"])
	}
}

func TestServerBranchAndMergeRoutes(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	router := server.Router()

	mustRequest(t, router, http.MethodPut, "/collections/users/1", map[string]any{"name": "Joao", "plan": "free"}, http.StatusCreated)
	mustRequest(t, router, http.MethodPost, "/branches/users/1/feature?from=main", nil, http.StatusCreated)
	mustRequest(t, router, http.MethodPut, "/collections/users/1?branch=feature", map[string]any{"name": "Joao", "plan": "enterprise"}, http.StatusCreated)

	mainBefore := mustRequest(t, router, http.MethodGet, "/collections/users/1?branch=main", nil, http.StatusOK)
	if mainBefore["data"].(map[string]any)["plan"] != "free" {
		t.Fatalf("unexpected main plan before merge")
	}

	mustRequest(t, router, http.MethodPost, "/merge/users/1?from=feature&to=main", nil, http.StatusOK)

	mainAfter := mustRequest(t, router, http.MethodGet, "/collections/users/1?branch=main", nil, http.StatusOK)
	if mainAfter["data"].(map[string]any)["plan"] != "enterprise" {
		t.Fatalf("unexpected main plan after merge")
	}

	branches := mustRequest(t, router, http.MethodGet, "/branches/users/1", nil, http.StatusOK)
	heads := branches["heads"].(map[string]any)
	if _, ok := heads["main"]; !ok {
		t.Fatalf("main head not found")
	}
	if _, ok := heads["feature"]; !ok {
		t.Fatalf("feature head not found")
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()

	walPath := filepath.Join(t.TempDir(), "test.wal")
	walWriter, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("new wal writer: %v", err)
	}

	engine := temporal.NewEngine(storage.NewMemoryEngine(), snapshot.NewEngine(2), walWriter)
	t.Cleanup(func() {
		_ = engine.Close()
	})

	return NewServer(engine, realtime.NewHub(), zap.NewNop())
}

func mustRequest(t *testing.T, handler http.Handler, method, path string, payload any, expectedStatus int) map[string]any {
	t.Helper()

	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}

	req := httptest.NewRequest(method, path, &body)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != expectedStatus {
		t.Fatalf("unexpected status for %s %s: got %d want %d, body=%s", method, path, rr.Code, expectedStatus, rr.Body.String())
	}

	if rr.Body.Len() == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response JSON: %v", err)
	}
	return out
}
