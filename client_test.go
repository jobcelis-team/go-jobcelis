package jobcelis

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func decodeBody(t *testing.T, body []byte) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, body)
	}
	return m
}

func TestGetPipelineUnwrapsData(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"data":{"id":"p1","name":"Orders","webhook_id":"w1"}}`))
	p, err := testClient(fs).GetPipeline(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != "p1" || p.Name != "Orders" || p.WebhookID == nil || *p.WebhookID != "w1" {
		t.Fatalf("pipeline not decoded: %+v", p)
	}
}

func TestCreateEventSchemaUnwrapsDataWithIntegerVersion(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"data":{"id":"s1","topic":"order.created","version":2}}`))
	s, err := testClient(fs).CreateEventSchema(context.Background(), EventSchemaRequest{Topic: "order.created", Version: 2})
	if err != nil {
		t.Fatal(err)
	}
	if s.ID != "s1" || s.Version != 2 {
		t.Fatalf("schema not decoded: %+v", s)
	}
	if got := decodeBody(t, fs.request(0).Body)["version"]; got != float64(2) {
		t.Fatalf("version sent as %v", got)
	}
}

func TestAddMemberDefaultsToViewerAndUnwrapsData(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"data":{"id":"m1","role":"viewer"}}`))
	c := testClient(fs).SetAuthToken("jwt")
	m, err := c.AddMember(context.Background(), "proj 1", "a@example.com", "")
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "m1" {
		t.Fatalf("member not decoded: %+v", m)
	}
	req := fs.request(0)
	if got := decodeBody(t, req.Body)["role"]; got != "viewer" {
		t.Fatalf("role sent as %v", got)
	}
	if req.Path != "/api/v1/projects/proj 1/members" {
		t.Fatalf("unexpected path %q", req.Path)
	}
}

func TestRestrictProcessingSendsPassword(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"status":"restricted"}`))
	c := testClient(fs).SetAuthToken("jwt")
	if err := c.RestrictProcessing(context.Background(), "secret"); err != nil {
		t.Fatal(err)
	}
	if got := decodeBody(t, fs.request(0).Body)["password"]; got != "secret" {
		t.Fatalf("password sent as %v", got)
	}
}

func TestTestPipelineWrapsPayload(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"status":"passed"}`))
	_, err := testClient(fs).TestPipeline(context.Background(), "p1", map[string]interface{}{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := decodeBody(t, fs.request(0).Body)["payload"].(map[string]interface{})
	if !ok || payload["a"] != float64(1) {
		t.Fatalf("payload not wrapped: %s", fs.request(0).Body)
	}
}

func TestUpdateRetentionPolicyWrapsPolicy(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"retention_policy":{"events_days":30}}`))
	_, err := testClient(fs).UpdateRetentionPolicy(context.Background(), RetentionPolicy{EventsDays: 30})
	if err != nil {
		t.Fatal(err)
	}
	policy, ok := decodeBody(t, fs.request(0).Body)["retention_policy"].(map[string]interface{})
	if !ok || policy["events_days"] != float64(30) {
		t.Fatalf("policy not wrapped: %s", fs.request(0).Body)
	}
}

func TestUpdateJobSendsOnlySetFieldsAndSingularTopic(t *testing.T) {
	fs := newFakeServer(t, okJSON(`{"id":"j1"}`))
	_, err := testClient(fs).UpdateJob(context.Background(), "j1", JobRequest{Topics: []string{"order.created"}})
	if err != nil {
		t.Fatal(err)
	}
	body := decodeBody(t, fs.request(0).Body)
	if _, ok := body["name"]; ok {
		t.Fatalf("empty name was sent: %v", body)
	}
	if body["topic"] != "order.created" {
		t.Fatalf("topic sent as %v", body["topic"])
	}
	if _, ok := body["topics"]; ok {
		t.Fatalf("topics was sent: %v", body)
	}
}

func TestPathParametersAreEscaped(t *testing.T) {
	var escaped string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escaped = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient("test_key").WithBaseURL(srv.URL)
	if _, err := c.GetEvent(context.Background(), "a/b"); err != nil {
		t.Fatal(err)
	}
	if escaped != "/api/v1/events/a%2Fb" {
		t.Fatalf("unexpected path %q", escaped)
	}
}
