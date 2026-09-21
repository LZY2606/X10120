package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mkEvent(occurredAt, extra string) json.RawMessage {
	s := `{"occurred_at":"` + occurredAt + `","msg":"` + extra + `"}`
	return json.RawMessage(s)
}

func mkIncident(t *testing.T, s *Store) string {
	t.Helper()
	inc, err := s.CreateIncident("test incident")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	return inc.ID
}

func ingest(t *testing.T, s *Store, incID, source, eid, at string) *Event {
	t.Helper()
	ev, dup, err := s.IngestEvent(incID, source, eid, mkEvent(at, eid), "")
	if err != nil {
		t.Fatalf("ingest %s: %v", eid, err)
	}
	if dup {
		t.Fatalf("event %s unexpectedly reported duplicate", eid)
	}
	return ev
}

func TestDuplicateDeliveryKeepsSingleRawFact(t *testing.T) {
	s := openTestStore(t)
	incID := mkIncident(t, s)

	first := mkEvent("2026-09-21T10:00:00Z", "original")
	ev1, dup, err := s.IngestEvent(incID, "app", "E1", first, "")
	if err != nil || dup {
		t.Fatalf("first ingest: dup=%v err=%v", dup, err)
	}

	// A redelivery with *different* payload content must not overwrite Raw.
	second := mkEvent("2026-09-21T11:00:00Z", "TAMPERED CONTENT")
	ev2, dup, err := s.IngestEvent(incID, "app", "E1", second, "retry after timeout")
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if !dup {
		t.Fatal("redelivery must be flagged duplicate")
	}
	if ev2.ID != ev1.ID {
		t.Fatal("redelivery created a second event")
	}
	if ev2.ReceiveCount != 2 {
		t.Fatalf("receive count = %d, want 2", ev2.ReceiveCount)
	}
	if string(ev2.Raw) != string(first) {
		t.Fatalf("raw fact was overwritten:\n got %s\nwant %s", ev2.Raw, first)
	}
	if len(ev2.Receipts) != 2 || ev2.Receipts[1].Reason != "retry after timeout" {
		t.Fatalf("receipt history not accumulated: %+v", ev2.Receipts)
	}
	tFirst, _ := time.Parse(time.RFC3339Nano, ev2.FirstSeenAt)
	tLast, _ := time.Parse(time.RFC3339Nano, ev2.LastSeenAt)
	if tLast.Before(tFirst) {
		t.Fatalf("last_seen_at %s before first_seen_at %s", ev2.LastSeenAt, ev2.FirstSeenAt)
	}

	snap := s.Snapshot()
	count := 0
	for _, e := range snap.Events {
		if e.Source == "app" && e.EventID == "E1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("snapshot contains %d raw facts for app/E1, want 1", count)
	}
}

func TestLateArrivalSortingAndTimestampTie(t *testing.T) {
	s := openTestStore(t)
	incID := mkIncident(t, s)

	// Received in this order:
	e10 := ingest(t, s, incID, "app", "10:00", "2026-09-21T10:00:00Z")
	e12 := ingest(t, s, incID, "app", "12:00", "2026-09-21T12:00:00Z")
	// Late arrival: event time earlier than both, but received last.
	e09 := ingest(t, s, incID, "app", "09:00-late", "2026-09-21T09:00:00Z")
	// Two events with the identical timestamp: receive order must win.
	e11a := ingest(t, s, incID, "app", "tie-a", "2026-09-21T11:00:00Z")
	e11b := ingest(t, s, incID, "app", "tie-b", "2026-09-21T11:00:00Z")

	gotReception := SortEvents(eventsFor(s, incID), SortReception)
	wantReception := []string{e10.ID, e12.ID, e09.ID, e11a.ID, e11b.ID}
	assertOrder(t, gotReception, wantReception, "reception")

	gotTime := SortEvents(eventsFor(s, incID), SortOriginal)
	wantTime := []string{e09.ID, e10.ID, e11a.ID, e11b.ID, e12.ID}
	assertOrder(t, gotTime, wantTime, "original-time")
}

func eventsFor(s *Store, incID string) []Event {
	snap := s.Snapshot()
	out := []Event{}
	for _, e := range snap.Events {
		if e.IncidentID == incID {
			out = append(out, e)
		}
	}
	return out
}

func assertOrder(t *testing.T, got []Event, wantIDs []string, name string) {
	t.Helper()
	if len(got) != len(wantIDs) {
		t.Fatalf("%s: got %d events, want %d", name, len(got), len(wantIDs))
	}
	for i, want := range wantIDs {
		if got[i].ID != want {
			gotIDs := make([]string, len(got))
			for j, e := range got {
				gotIDs[j] = e.EventID
			}
			t.Fatalf("%s order = %v, want %v at position %d", name, gotIDs, wantIDs, i)
		}
	}
}

func TestEdgeCycleRejectedWithReadablePath(t *testing.T) {
	s := openTestStore(t)
	incID := mkIncident(t, s)
	a := ingest(t, s, incID, "app", "A", "2026-09-21T10:00:00Z")
	b := ingest(t, s, incID, "app", "B", "2026-09-21T10:01:00Z")
	c := ingest(t, s, incID, "app", "C", "2026-09-21T10:02:00Z")

	for _, p := range [][2]string{{a.ID, b.ID}, {b.ID, c.ID}} {
		if _, err := s.AddEdge(incID, p[0], p[1], "ordered in logs"); err != nil {
			t.Fatalf("edge: %v", err)
		}
	}
	_, err := s.AddEdge(incID, c.ID, a.ID, "would close the loop")
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("want ConflictError, got %v", err)
	}
	msg := err.Error()
	if !contains(msg, "cycle") || !contains(msg, "A") || !contains(msg, "C") {
		t.Fatalf("cycle error not readable/path missing: %q", msg)
	}

	// Removing an edge opens the graph again.
	var edgeID string
	for _, e := range s.Snapshot().Edges {
		if e.Active() && e.CauseID == b.ID && e.EffectID == c.ID {
			edgeID = e.ID
		}
	}
	if _, err := s.RemoveEdge(edgeID); err != nil {
		t.Fatalf("remove edge: %v", err)
	}
	if _, err := s.AddEdge(incID, c.ID, a.ID, "now acyclic"); err != nil {
		t.Fatalf("edge after removal should succeed: %v", err)
	}
}

func TestRevokeBlockedWhileReferenced(t *testing.T) {
	s := openTestStore(t)
	incID := mkIncident(t, s)
	a := ingest(t, s, incID, "app", "A", "2026-09-21T10:00:00Z")
	b := ingest(t, s, incID, "app", "B", "2026-09-21T10:01:00Z")
	if _, err := s.AddEdge(incID, a.ID, b.ID, "caused"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RevokeEvent(a.ID); err == nil {
		t.Fatal("revoke of referenced event must fail")
	}
	for _, e := range s.Snapshot().Edges {
		if e.Active() {
			if _, err := s.RemoveEdge(e.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	ev, err := s.RevokeEvent(a.ID)
	if err != nil || !ev.Revoked {
		t.Fatalf("revoke after edge removal: ev=%+v err=%v", ev, err)
	}
	// Raw fact survives logical revocation.
	got, err := s.GetEvent(a.ID)
	if err != nil || string(got.Raw) == "" {
		t.Fatalf("revoked event/raw fact disappeared: %v", err)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestAuditLogPathIsInsideDataDir documents the on-disk layout.
func TestAuditLogPath(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mkIncident(t, s)
	s.Close()
	p := filepath.Join(dir, "audit.log")
	if fi, err := os.Stat(p); err != nil {
		t.Fatalf("audit log missing at %s: %v", p, err)
	} else if fi.Size() == 0 {
		t.Fatal("audit log is empty")
	}
}
