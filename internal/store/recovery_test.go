package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCrashRecoveryRestoresIdenticalState(t *testing.T) {
	dir := t.TempDir()

	func() {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		inc, err := s.CreateIncident("outage")
		if err != nil {
			t.Fatal(err)
		}
		e1 := ingest(t, s, inc.ID, "app", "E1", "2026-09-21T10:00:00Z")
		e2 := ingest(t, s, inc.ID, "db", "E2", "2026-09-21T09:59:00Z")
		if _, _, err := s.IngestEvent(inc.ID, "app", "E1",
			mkEvent("2026-09-21T10:00:00Z", "E1"), "redelivered"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CorrectEvent(e2.ID, 1, "2026-09-21T09:30:00Z", "clock skew"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AddEdge(inc.ID, e2.ID, e1.ID, "pool exhaustion triggered timeout"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RevokeEvent(e1.ID); err == nil {
			t.Fatal("revoke of referenced event should have failed before shutdown")
		}
		// Simulate a killed process: Close() still flushes/fsyncs, and the log
		// is the only durable state.
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	snap := s2.Snapshot()
	if len(snap.Incidents) != 1 || snap.Incidents[0].Title != "outage" {
		t.Fatalf("incidents not restored: %+v", snap.Incidents)
	}
	incID := snap.Incidents[0].ID
	byKey := map[string]*Event{}
	for i := range snap.Events {
		e := &snap.Events[i]
		byKey[e.Source+"/"+e.EventID] = e
	}
	e1 := byKey["app/E1"]
	e2 := byKey["db/E2"]
	if e1 == nil || e2 == nil {
		t.Fatalf("events not restored: %+v", snap.Events)
	}
	if e1.ReceiveCount != 2 || len(e1.Receipts) != 2 {
		t.Fatalf("receipt history lost: %+v", e1)
	}
	if string(e1.Raw) == "" {
		t.Fatal("raw fact lost across restart")
	}
	if e2.CurrentVer != 2 || len(e2.Versions) != 2 ||
		e2.Versions[0].OccurredAt != "2026-09-21T09:59:00Z" ||
		e2.Versions[1].OccurredAt != "2026-09-21T09:30:00Z" {
		t.Fatalf("version history not restored: %+v", e2.Versions)
	}
	active := 0
	for _, e := range snap.Edges {
		if e.Active() && e.IncidentID == incID {
			active++
			if e.Rationale != "pool exhaustion triggered timeout" {
				t.Fatalf("edge rationale lost: %+v", e)
			}
		}
	}
	if active != 1 {
		t.Fatalf("active edges after restart = %d, want 1", active)
	}

	// The store keeps accepting new writes after replay.
	e3 := ingest(t, s2, incID, "deploy", "E3", "2026-09-21T09:00:00Z")
	if e3 == nil {
		t.Fatal("write after replay failed")
	}
}

func TestTornTailRecordIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	inc, _ := s.CreateIncident("inc")
	ingest(t, s, inc.ID, "app", "E1", "2026-09-21T10:00:00Z")
	path := filepath.Join(dir, "audit.log")
	s.Close()

	// Append a partial, garbage record like a crash mid-write.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":99,"at":"2026-09-21T10:05:00Z","type":"event","payl`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer s2.Close()

	snap := s2.Snapshot()
	if len(snap.Events) != 1 || snap.Events[0].EventID != "E1" {
		t.Fatalf("valid state not preserved around torn tail: %+v", snap.Events)
	}

	// New writes must continue from the last valid sequence.
	e2 := ingest(t, s2, inc.ID, "app", "E2", "2026-09-21T10:02:00Z")
	if e2.Seq <= snap.Seq {
		t.Fatalf("sequence did not continue after torn tail: %d <= %d", e2.Seq, snap.Seq)
	}
}

func TestConcurrentCorrectionOneWinnerOneConflict(t *testing.T) {
	s := openTestStore(t)
	incID := mkIncident(t, s)
	ev := ingest(t, s, incID, "app", "E1", "2026-09-21T10:00:00Z")

	const n = 8
	var wg sync.WaitGroup
	var winners, conflicts int
	var mu sync.Mutex
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			newAt := "2026-09-21T11:0" + string(rune('0'+i)) + ":00Z"
			_, err := s.CorrectEvent(ev.ID, 1, newAt, "concurrent correction")
			mu.Lock()
			if err == nil {
				winners++
			} else {
				if _, ok := err.(*ConflictError); !ok {
					t.Errorf("non-conflict error: %v", err)
				}
				conflicts++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if winners != 1 || conflicts != n-1 {
		t.Fatalf("winners=%d conflicts=%d, want 1 and %d", winners, conflicts, n-1)
	}
	got, _ := s.GetEvent(ev.ID)
	if got.CurrentVer != 2 {
		t.Fatalf("current version = %d, want 2 (no clobbering)", got.CurrentVer)
	}
	if len(got.Versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(got.Versions))
	}

	// A stale base version also conflicts explicitly.
	if _, err := s.CorrectEvent(ev.ID, 1, "2026-09-21T12:00:00Z", "late retry"); err == nil {
		t.Fatal("expected conflict on stale version 1")
	}
	if _, err := s.CorrectEvent(ev.ID, 2, "2026-09-21T12:00:00Z", "chained"); err != nil {
		t.Fatalf("correction based on current version 2 should succeed: %v", err)
	}
}
