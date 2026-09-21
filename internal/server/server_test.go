package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"incident-timeline/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return httptest.NewServer(New(st)), st
}

func doJSON(t *testing.T, h *httptest.Server, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, h.URL+path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &out)
	}
	return res.StatusCode, out
}

func createIncident(t *testing.T, h *httptest.Server) string {
	status, body := doJSON(t, h, "POST", "/api/incidents", map[string]string{"title": "inc"})
	if status != http.StatusCreated {
		t.Fatalf("create incident status=%d body=%v", status, body)
	}
	return body["id"].(string)
}

func TestBatchPartialFailure(t *testing.T) {
	h, _ := newTestServer(t)
	defer h.Close()
	incID := createIncident(t, h)

	events := []any{
		map[string]any{"source": "app", "event_id": "ok1", "payload": map[string]any{
			"occurred_at": "2026-09-21T10:00:00Z", "msg": "fine"}},
		map[string]any{"source": "app", "event_id": "bad-no-time", "payload": map[string]any{
			"msg": "missing occurred_at"}},
		map[string]any{"source": "app", "payload": map[string]any{
			"occurred_at": "2026-09-21T10:02:00Z"}}, // missing event_id
		map[string]any{"source": "app", "event_id": "ok2", "payload": map[string]any{
			"occurred_at": "not-a-timestamp"}}, // unparseable time
		map[string]any{"source": "db", "event_id": "ok3", "payload": map[string]any{
			"occurred_at": "2026-09-21T09:55:00Z", "msg": "also fine"}},
		"this-is-not-even-an-object",
	}
	status, body := doJSON(t, h, "POST", "/api/incidents/"+incID+"/events/batch",
		map[string]any{"events": events})
	if status != http.StatusOK {
		t.Fatalf("batch status = %d, want 200 with per-row results", status)
	}
	if body["total"].(float64) != 6 || body["accepted"].(float64) != 2 || body["failed"].(float64) != 4 {
		t.Fatalf("unexpected counts: total=%v accepted=%v failed=%v",
			body["total"], body["accepted"], body["failed"])
	}
	rows := body["results"].([]any)
	for i, r := range rows {
		row := r.(map[string]any)
		if row["line"].(float64) != float64(i+1) {
			t.Fatalf("row %d missing line number: %v", i, row)
		}
		wantOK := i == 0 || i == 4
		if row["ok"] != wantOK {
			t.Fatalf("row %d ok=%v want %v, error=%v", i, row["ok"], wantOK, row["error"])
		}
		if !wantOK && (row["error"] == nil || row["error"] == "") {
			t.Fatalf("row %d failure lacks error message", i)
		}
	}

	// Valid rows must have landed despite the bad ones; incident view returns
	// the late-arriving 09:55 event first when sorted by original time.
	res, err := http.Get(h.URL + "/api/incidents/" + incID + "?sort=original")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var view map[string]any
	raw, _ := io.ReadAll(res.Body)
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	evs := view["events"].([]any)
	if len(evs) != 2 {
		t.Fatalf("valid rows not persisted: got %d events", len(evs))
	}
	first := evs[0].(map[string]any)
	if first["event_id"] != "ok3" {
		t.Fatalf("time ordering wrong, first = %v", first["event_id"])
	}
}

func TestDuplicateIngestAndConflictStatuses(t *testing.T) {
	h, _ := newTestServer(t)
	defer h.Close()
	incID := createIncident(t, h)

	one := map[string]any{"source": "app", "event_id": "E1", "payload": map[string]any{
		"occurred_at": "2026-09-21T10:00:00Z"}}
	status, body := doJSON(t, h, "POST", "/api/incidents/"+incID+"/events", one)
	if status != http.StatusCreated || body["duplicate"] != false {
		t.Fatalf("first ingest status=%d body=%v", status, body)
	}
	status, body = doJSON(t, h, "POST", "/api/incidents/"+incID+"/events", one)
	if status != http.StatusOK || body["duplicate"] != true {
		t.Fatalf("redeliver status=%d body=%v", status, body)
	}
	ev := body["event"].(map[string]any)
	if ev["receive_count"].(float64) != 2 {
		t.Fatalf("receive_count = %v, want 2", ev["receive_count"])
	}

	// 409 on a stale correction version.
	status, body = doJSON(t, h, "POST", "/api/events/"+ev["id"].(string)+"/correct",
		map[string]any{"occurred_at": "2026-09-21T08:00:00Z", "expected_version": 1})
	if status != http.StatusCreated {
		t.Fatalf("first correct status=%d body=%v", status, body)
	}
	status, body = doJSON(t, h, "POST", "/api/events/"+ev["id"].(string)+"/correct",
		map[string]any{"occurred_at": "2026-09-21T08:30:00Z", "expected_version": 1})
	if status != http.StatusConflict || !strings.Contains(body["error"].(string), "version conflict") {
		t.Fatalf("stale correction status=%d body=%v", status, body)
	}

	// 409 on cycle.
	other := map[string]any{"source": "app", "event_id": "E2", "payload": map[string]any{
		"occurred_at": "2026-09-21T10:01:00Z"}}
	_, b2 := doJSON(t, h, "POST", "/api/incidents/"+incID+"/events", other)
	id2 := b2["event"].(map[string]any)["id"].(string)
	id1 := ev["id"].(string)
	edge := func(cause, effect string) (int, map[string]any) {
		return doJSON(t, h, "POST", "/api/incidents/"+incID+"/edges", map[string]any{
			"cause_id": cause, "effect_id": effect, "rationale": "why"})
	}
	if s, b := edge(id1, id2); s != http.StatusCreated {
		t.Fatalf("edge1 status=%d body=%v", s, b)
	}
	if s, b := edge(id2, id1); s != http.StatusConflict ||
		!strings.Contains(b["error"].(string), "cycle") {
		t.Fatalf("cycle edge status=%d body=%v", s, b)
	}

	// 409 revoking an event referenced by an active edge.
	if s, b := doJSON(t, h, "POST", "/api/events/"+id1+"/revoke", map[string]any{}); s != http.StatusConflict {
		t.Fatalf("revoke referenced event status=%d body=%v", s, b)
	}
}

func TestConcurrentCorrectionOverHTTP(t *testing.T) {
	h, _ := newTestServer(t)
	defer h.Close()
	incID := createIncident(t, h)
	_, body := doJSON(t, h, "POST", "/api/incidents/"+incID+"/events", map[string]any{
		"source": "app", "event_id": "E", "payload": map[string]any{
			"occurred_at": "2026-09-21T10:00:00Z"}})
	id := body["event"].(map[string]any)["id"].(string)

	const n = 5
	var wg sync.WaitGroup
	statuses := make([]int, n)
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			s, _ := doJSON(t, h, "POST", "/api/events/"+id+"/correct", map[string]any{
				"occurred_at":      "2026-09-21T11:00:0" + string(rune('0'+i)) + "Z",
				"expected_version": 1,
			})
			statuses[i] = s
		}()
	}
	close(start)
	wg.Wait()

	created, conflict := 0, 0
	for _, s := range statuses {
		switch s {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflict++
		default:
			t.Fatalf("unexpected status %d", s)
		}
	}
	if created != 1 || conflict != n-1 {
		t.Fatalf("created=%d conflict=%d", created, conflict)
	}
}

func TestUIServesTimelinePage(t *testing.T) {
	h, _ := newTestServer(t)
	defer h.Close()
	res, err := http.Get(h.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "因果时间线") {
		t.Fatalf("page missing 因果时间线 marker")
	}
}
