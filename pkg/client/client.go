package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"taimedb/internal/commit"
)

// Client provides a small HTTP wrapper around TaimeDB API endpoints.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

func New(baseURL string) *Client {
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) PutDocument(collection, document string, payload map[string]any) (map[string]any, error) {
	return c.PutDocumentOnBranch(collection, document, "main", payload)
}

func (c *Client) PutDocumentOnBranch(collection, document, branch string, payload map[string]any) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s/collections/%s/%s", c.baseURL, url.PathEscape(collection), url.PathEscape(document))
	if branch != "" {
		endpoint = endpoint + "?branch=" + url.QueryEscape(branch)
	}
	req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doJSON(req)
}

func (c *Client) History(collection, document string) ([]commit.Commit, error) {
	return c.HistoryOnBranch(collection, document, "main")
}

func (c *Client) HistoryOnBranch(collection, document, branch string) ([]commit.Commit, error) {
	endpoint := fmt.Sprintf("%s/history/%s/%s", c.baseURL, url.PathEscape(collection), url.PathEscape(document))
	if branch != "" {
		endpoint = endpoint + "?branch=" + url.QueryEscape(branch)
	}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	raw, err := c.doJSON(req)
	if err != nil {
		return nil, err
	}
	buf, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var response struct {
		History []commit.Commit `json:"history"`
	}
	if err := json.Unmarshal(buf, &response); err != nil {
		return nil, err
	}
	return response.History, nil
}

func (c *Client) Diff(fromCommit, toCommit string) (commit.Diff, error) {
	endpoint := fmt.Sprintf("%s/diff/%s/%s", c.baseURL, url.PathEscape(fromCommit), url.PathEscape(toCommit))
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	raw, err := c.doJSON(req)
	if err != nil {
		return nil, err
	}
	buf, err := json.Marshal(raw["diff"])
	if err != nil {
		return nil, err
	}
	var diff commit.Diff
	if err := json.Unmarshal(buf, &diff); err != nil {
		return nil, err
	}
	return diff, nil
}

func (c *Client) CreateBranch(collection, document, newBranch, fromBranch string) (map[string]any, error) {
	endpoint := fmt.Sprintf("%s/branches/%s/%s/%s", c.baseURL, url.PathEscape(collection), url.PathEscape(document), url.PathEscape(newBranch))
	if fromBranch != "" {
		endpoint = endpoint + "?from=" + url.QueryEscape(fromBranch)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, err
	}
	return c.doJSON(req)
}

func (c *Client) MergeBranches(collection, document, fromBranch, toBranch string) (map[string]any, error) {
	values := url.Values{}
	values.Set("from", fromBranch)
	values.Set("to", toBranch)

	endpoint := fmt.Sprintf("%s/merge/%s/%s?%s", c.baseURL, url.PathEscape(collection), url.PathEscape(document), values.Encode())
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, err
	}
	return c.doJSON(req)
}

func (c *Client) Rollback(targetCommit string) (map[string]any, error) {
	endpoint := fmt.Sprintf("%s/rollback/%s", c.baseURL, url.PathEscape(targetCommit))
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, err
	}
	return c.doJSON(req)
}

func (c *Client) doJSON(req *http.Request) (map[string]any, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("request failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if len(body) == 0 {
		return map[string]any{}, nil
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}
