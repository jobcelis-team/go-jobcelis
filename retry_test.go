package jobcelis

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeServer answers each request with the next handler in the list (the
// last one repeats) and records every request it receives.
type fakeServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []*recordedRequest
}

type recordedRequest struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
}

func newFakeServer(t *testing.T, handlers ...http.HandlerFunc) *fakeServer {
	t.Helper()
	fs := &fakeServer{}
	fs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fs.mu.Lock()
		n := len(fs.requests)
		fs.requests = append(fs.requests, &recordedRequest{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(), Body: body})
		fs.mu.Unlock()
		if n >= len(handlers) {
			n = len(handlers) - 1
		}
		handlers[n](w, r)
	}))
	t.Cleanup(fs.Close)
	return fs
}

func (fs *fakeServer) count() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.requests)
}

func (fs *fakeServer) request(i int) *recordedRequest {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.requests[i]
}

func status(code int, headers ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"error":"status"}`))
	}
}

func okJSON(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

// dropConnection closes the connection without a response, which the client
// sees as a network error.
func dropConnection(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("hijacking not supported")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		panic(err)
	}
	_ = conn.Close()
}

func testClient(fs *fakeServer) *Client {
	c := NewClient("test_key").WithBaseURL(fs.URL)
	c.retryBaseDelay = time.Millisecond
	return c
}

func TestRetriesGetOn503ThenSucceeds(t *testing.T) {
	fs := newFakeServer(t, status(503), status(503), okJSON(`{"id":"evt_1","topic":"a"}`))
	ev, err := testClient(fs).GetEvent(context.Background(), "evt_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.ID != "evt_1" {
		t.Fatalf("got event %q", ev.ID)
	}
	if fs.count() != 3 {
		t.Fatalf("expected 3 requests, got %d", fs.count())
	}
}

func TestRetries429WithRetryAfter(t *testing.T) {
	fs := newFakeServer(t, status(429, "Retry-After", "0"), okJSON(`{"status":"ok"}`))
	start := time.Now()
	resp, err := testClient(fs).Health(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}
	if fs.count() != 2 {
		t.Fatalf("expected 2 requests, got %d", fs.count())
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("Retry-After: 0 should not wait")
	}
}

func TestSendEventKeepsIdempotencyKeyAcrossRetries(t *testing.T) {
	fs := newFakeServer(t, status(502), dropConnection, okJSON(`{"event_id":"evt_1"}`))
	resp, err := testClient(fs).SendEvent(context.Background(), EventRequest{Topic: "order.created"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.EventID != "evt_1" {
		t.Fatalf("got event %q", resp.EventID)
	}
	if fs.count() != 3 {
		t.Fatalf("expected 3 requests, got %d", fs.count())
	}
	key := fs.request(0).Header.Get("X-Idempotency-Key")
	if len(key) != 36 || key[14] != '4' {
		t.Fatalf("expected a UUID v4 key, got %q", key)
	}
	for i := 1; i < fs.count(); i++ {
		if got := fs.request(i).Header.Get("X-Idempotency-Key"); got != key {
			t.Fatalf("request %d key %q, want %q", i, got, key)
		}
	}
}

func TestSendEventUsesCallerIdempotencyKey(t *testing.T) {
	fs := newFakeServer(t, status(503), okJSON(`{"event_id":"evt_1"}`))
	_, err := testClient(fs).SendEvent(context.Background(), EventRequest{Topic: "a", IdempotencyKey: "my-key"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < fs.count(); i++ {
		if got := fs.request(i).Header.Get("X-Idempotency-Key"); got != "my-key" {
			t.Fatalf("request %d key %q, want my-key", i, got)
		}
	}
}

func TestSendEventsAddsIdempotencyKeys(t *testing.T) {
	fs := newFakeServer(t, status(503), okJSON(`{"accepted":2}`))
	req := BatchRequest{Events: []EventRequest{
		{Topic: "a"},
		{Topic: "b", IdempotencyKey: "existing"},
	}}
	if _, err := testClient(fs).SendEvents(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fs.count() != 2 {
		t.Fatalf("expected 2 requests, got %d", fs.count())
	}

	var first, second BatchRequest
	if err := json.Unmarshal(fs.request(0).Body, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fs.request(1).Body, &second); err != nil {
		t.Fatal(err)
	}
	generated := first.Events[0].IdempotencyKey
	if len(generated) != 36 {
		t.Fatalf("expected a generated UUID, got %q", generated)
	}
	if first.Events[1].IdempotencyKey != "existing" {
		t.Fatalf("existing key replaced: %q", first.Events[1].IdempotencyKey)
	}
	if second.Events[0].IdempotencyKey != generated || second.Events[1].IdempotencyKey != "existing" {
		t.Fatalf("retry changed the keys: %+v", second.Events)
	}
	if req.Events[0].IdempotencyKey != "" {
		t.Fatalf("caller's request was modified")
	}
}

func TestPostWithoutIdempotencyKeyNotRetriedOnNetworkError(t *testing.T) {
	fs := newFakeServer(t, dropConnection, okJSON(`{"id":"wh_1"}`))
	_, err := testClient(fs).CreateWebhook(context.Background(), WebhookRequest{URL: "https://example.com"})
	if err == nil {
		t.Fatal("expected an error")
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		t.Fatalf("expected a network error, got %v", err)
	}
	if fs.count() != 1 {
		t.Fatalf("expected 1 request, got %d", fs.count())
	}
}

func TestPostWithoutIdempotencyKeyRetriedOn503(t *testing.T) {
	fs := newFakeServer(t, status(503), okJSON(`{"id":"wh_1"}`))
	wh, err := testClient(fs).CreateWebhook(context.Background(), WebhookRequest{URL: "https://example.com"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wh.ID != "wh_1" || fs.count() != 2 {
		t.Fatalf("expected success after 2 requests, got %q after %d", wh.ID, fs.count())
	}
}

func TestMaxRetriesZeroMakesOneRequest(t *testing.T) {
	fs := newFakeServer(t, status(503))
	_, err := testClient(fs).WithMaxRetries(0).GetEvent(context.Background(), "evt_1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("expected APIError 503, got %v", err)
	}
	if fs.count() != 1 {
		t.Fatalf("expected 1 request, got %d", fs.count())
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	fs := newFakeServer(t, status(503))
	_, err := testClient(fs).WithMaxRetries(2).GetEvent(context.Background(), "evt_1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("expected APIError 503, got %v", err)
	}
	if fs.count() != 3 {
		t.Fatalf("expected 3 requests, got %d", fs.count())
	}
}

func TestNonRetryableStatusNotRetried(t *testing.T) {
	for _, code := range []int{400, 404, 500} {
		fs := newFakeServer(t, status(code), okJSON(`{}`))
		_, err := testClient(fs).GetEvent(context.Background(), "evt_1")
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != code {
			t.Fatalf("expected APIError %d, got %v", code, err)
		}
		if fs.count() != 1 {
			t.Fatalf("status %d: expected 1 request, got %d", code, fs.count())
		}
	}
}

func TestDefaultMaxRetries(t *testing.T) {
	if got := NewClient("k").MaxRetries; got != 6 {
		t.Fatalf("expected 6 retries by default, got %d", got)
	}
}

func TestRetryDelay(t *testing.T) {
	c := NewClient("k")
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		d := c.retryDelay(i+1, nil)
		lo := time.Duration(float64(w*time.Second) * 0.8)
		hi := time.Duration(float64(w*time.Second) * 1.2)
		if d < lo || d > hi {
			t.Fatalf("attempt %d: delay %v not within [%v, %v]", i+1, d, lo, hi)
		}
	}

	if d, ok := parseRetryAfter("120"); !ok || d != 60*time.Second {
		t.Fatalf("Retry-After should be capped at 60s, got %v", d)
	}
	date := time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)
	if d, ok := parseRetryAfter(date); !ok || d <= 0 || d > 11*time.Second {
		t.Fatalf("unexpected HTTP-date Retry-After delay %v", d)
	}
}
