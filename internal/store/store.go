package store

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ConflictError marks a request that is well-formed but conflicts with the
// current state (duplicate edge, cycle, stale version, ...). HTTP handlers map
// it to 409; validation problems map to 400.
type ConflictError struct{ Msg string }

func (e *ConflictError) Error() string { return e.Msg }

func conflictf(format string, args ...any) error {
	return &ConflictError{Msg: fmt.Sprintf(format, args...)}
}

var ErrNotFound = errors.New("not found")

// Version describes one historical occurred_at of an event. Version 1 is the
// timestamp declared when the raw event was first received; later corrections
// add higher versions without removing older ones.
type Version struct {
	Version    int    `json:"version"`
	OccurredAt string `json:"occurred_at"`
	Note       string `json:"note,omitempty"`
	CreatedAt  string `json:"created_at"`
}

// Receipt records one delivery of a raw fact. Re-deliveries of the same
// (source, event_id) never overwrite Raw; they only extend this history.
type Receipt struct {
	Seq    int    `json:"seq"`
	At     string `json:"at"`
	Reason string `json:"reason,omitempty"`
}

// Event is one immutable raw fact plus its mutable metadata (incident
// assignment, timestamp corrections, logical revocation).
type Event struct {
	ID           string          `json:"id"`
	IncidentID   string          `json:"incident_id"`
	Source       string          `json:"source"`
	EventID      string          `json:"event_id"`
	Raw          json.RawMessage `json:"raw"`
	Seq          int             `json:"seq"`
	FirstSeenAt  string          `json:"first_seen_at"`
	LastSeenAt   string          `json:"last_seen_at"`
	ReceiveCount int             `json:"receive_count"`
	Receipts     []Receipt       `json:"receipts"`
	Versions     []Version       `json:"versions"`
	CurrentVer   int             `json:"current_version"`
	Revoked      bool            `json:"revoked"`
	RevokedAt    string          `json:"revoked_at,omitempty"`
}

// EffectiveAt returns the timestamp of the current version.
func (e *Event) EffectiveAt() string {
	if len(e.Versions) == 0 {
		return ""
	}
	return e.Versions[e.CurrentVer-1].OccurredAt
}

// Incident groups events and edges.
type Incident struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	CreatedAt string `json:"created_at"`
}

// Edge is one declared ordering/causal relationship: Cause happened before and
// may have caused Effect.
type Edge struct {
	ID         string `json:"id"`
	IncidentID string `json:"incident_id"`
	CauseID    string `json:"cause_id"`
	EffectID   string `json:"effect_id"`
	Rationale  string `json:"rationale"`
	Seq        int    `json:"seq"`
	CreatedAt  string `json:"created_at"`
	RemovedAt  string `json:"removed_at,omitempty"`
}

func (e *Edge) Active() bool { return e.RemovedAt == "" }

// Snapshot is the full, JSON-friendly state used by the API and tests.
type Snapshot struct {
	Seq       int        `json:"seq"`
	Incidents []Incident `json:"incidents"`
	Events    []Event    `json:"events"`
	Edges     []Edge     `json:"edges"`
}

type rec struct {
	Seq     int             `json:"seq"`
	At      string          `json:"at"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type pIncident struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	At    string `json:"at"`
}

type pEvent struct {
	ID         string          `json:"id"`
	IncidentID string          `json:"incident_id"`
	Source     string          `json:"source"`
	EventID    string          `json:"event_id"`
	Raw        json.RawMessage `json:"raw"`
	At         string          `json:"at"`
}

type pReceipt struct {
	Source  string `json:"source"`
	EventID string `json:"event_id"`
	Reason  string `json:"reason,omitempty"`
	At      string `json:"at"`
}

type pCorrect struct {
	EventID    string `json:"event_id"`
	OccurredAt string `json:"occurred_at"`
	Note       string `json:"note,omitempty"`
	ExpectedV  int    `json:"expected_version"`
	At         string `json:"at"`
}

type pRevoke struct {
	EventID string `json:"event_id"`
	At      string `json:"at"`
}

type pEdge struct {
	ID         string `json:"id"`
	IncidentID string `json:"incident_id"`
	CauseID    string `json:"cause_id"`
	EffectID   string `json:"effect_id"`
	Rationale  string `json:"rationale"`
	At         string `json:"at"`
}

type pEdgeRemove struct {
	EdgeID string `json:"id"`
	At     string `json:"at"`
}

// Store is an append-only audit log with an in-memory index rebuilt on open.
type Store struct {
	mu  sync.RWMutex
	dir string
	f   *os.File
	w   *bufio.Writer

	seq       int
	incidents map[string]*Incident
	events    map[string]*Event
	edges     map[string]*Edge
	dedup     map[string]string // source + "\x00" + event_id -> event ID
}

// Open opens (or creates) the audit log in dir and replays it.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:       dir,
		incidents: map[string]*Incident{},
		events:    map[string]*Event{},
		edges:     map[string]*Edge{},
		dedup:     map[string]string{},
	}
	path := filepath.Join(dir, "audit.log")
	if err := s.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.w = bufio.NewWriter(f)
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}

func (s *Store) replay(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	var valid int64
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			valid += int64(len(line)) + 1
			continue
		}
		var r rec
		if err := json.Unmarshal(line, &r); err != nil || r.Seq != s.seq+1 {
			break
		}
		if err := s.applyRecord(r); err != nil {
			return fmt.Errorf("audit log corrupted at line %d: %w", lineNo, err)
		}
		valid += int64(len(line)) + 1
	}
	if sc.Err() != nil {
		return sc.Err()
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() > valid {
		// Drop a torn trailing record (process crash mid-write).
		if err := f.Truncate(valid); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) appendLocked(typ string, payload any) (rec, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return rec{}, err
	}
	s.seq++
	r := rec{Seq: s.seq, At: now(), Type: typ, Payload: raw}
	line, err := json.Marshal(r)
	if err != nil {
		s.seq--
		return rec{}, err
	}
	if _, err := s.w.Write(line); err != nil {
		s.seq--
		return rec{}, err
	}
	if err := s.w.WriteByte('\n'); err != nil {
		s.seq--
		return rec{}, err
	}
	if err := s.w.Flush(); err != nil {
		s.seq--
		return rec{}, err
	}
	if err := s.f.Sync(); err != nil {
		s.seq--
		return rec{}, err
	}
	if err := s.applyRecord(r); err != nil {
		return rec{}, err
	}
	return r, nil
}

func (s *Store) applyRecord(r rec) error {
	switch r.Type {
	case "incident":
		var p pIncident
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		s.incidents[p.ID] = &Incident{ID: p.ID, Title: p.Title, CreatedAt: p.At}
	case "event":
		var p pEvent
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		ev := &Event{
			ID:           p.ID,
			IncidentID:   p.IncidentID,
			Source:       p.Source,
			EventID:      p.EventID,
			Raw:          append(json.RawMessage(nil), p.Raw...),
			Seq:          r.Seq,
			FirstSeenAt:  p.At,
			LastSeenAt:   p.At,
			ReceiveCount: 1,
			Receipts:     []Receipt{{Seq: r.Seq, At: p.At}},
			Versions:     []Version{{Version: 1, OccurredAt: firstOccurredAt(p.Raw), CreatedAt: p.At}},
			CurrentVer:   1,
		}
		s.events[p.ID] = ev
		s.dedup[dedupKey(p.Source, p.EventID)] = p.ID
	case "receipt":
		var p pReceipt
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		if id, ok := s.dedup[dedupKey(p.Source, p.EventID)]; ok {
			if ev, ok := s.events[id]; ok {
				ev.ReceiveCount++
				ev.LastSeenAt = p.At
				ev.Receipts = append(ev.Receipts, Receipt{Seq: r.Seq, At: p.At, Reason: p.Reason})
			}
		}
	case "correct":
		var p pCorrect
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		if ev, ok := s.events[p.EventID]; ok {
			ev.Versions = append(ev.Versions, Version{
				Version: len(ev.Versions) + 1, OccurredAt: p.OccurredAt,
				Note: p.Note, CreatedAt: p.At,
			})
			ev.CurrentVer = len(ev.Versions)
		}
	case "revoke":
		var p pRevoke
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		if ev, ok := s.events[p.EventID]; ok {
			ev.Revoked = true
			ev.RevokedAt = p.At
		}
	case "edge":
		var p pEdge
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		s.edges[p.ID] = &Edge{
			ID: p.ID, IncidentID: p.IncidentID, CauseID: p.CauseID, Seq: r.Seq,
			EffectID: p.EffectID, Rationale: p.Rationale, CreatedAt: p.At,
		}
	case "edge_remove":
		var p pEdgeRemove
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return err
		}
		if e, ok := s.edges[p.EdgeID]; ok {
			e.RemovedAt = p.At
		}
	default:
		return fmt.Errorf("unknown record type %q", r.Type)
	}
	s.seq = r.Seq
	return nil
}

func dedupKey(source, eventID string) string { return source + "\x00" + eventID }

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func newID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

func firstOccurredAt(raw json.RawMessage) string {
	var probe struct {
		OccurredAt string `json:"occurred_at"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	return probe.OccurredAt
}
