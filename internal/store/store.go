package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Edge kinds.
const (
	EdgeBefore = "before" // claimed temporal ordering constraint
	EdgeCauses = "causes" // possible causal edge
)

// Record types written to the audit stream.
const (
	RecIncidentCreated = "incident_created"
	RecEventReceived   = "event_received"
	RecEventDuplicate  = "event_duplicate_received"
	RecEventCorrected  = "event_corrected"
	RecEventAssigned   = "event_assigned"
	RecEventRevoked    = "event_revoked"
	RecEdgeDeclared    = "edge_declared"
	RecEdgeRemoved     = "edge_removed"
)

// Reception records one delivery of a (source, event_id) pair.
type Reception struct {
	At  time.Time `json:"at"`
	Seq int64     `json:"seq"`
}

// Version is one timestamp revision of an event. Version 0 is the
// timestamp parsed from the original payload and is never deleted.
type Version struct {
	Version   int       `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"created_at"`
	Seq       int64     `json:"seq"`
}

// Event is the deduplicated raw fact.
type Event struct {
	Source       string          `json:"source"`
	EventID      string          `json:"event_id"`
	Key          string          `json:"key"`
	Raw          json.RawMessage `json:"raw"`
	RawTimestamp time.Time       `json:"raw_timestamp"`
	Versions     []Version       `json:"versions"`
	FirstSeenSeq int64           `json:"first_seen_seq"`
	Receptions   []Reception     `json:"receptions"`
	ReceiveCount int             `json:"receive_count"`
	Revoked      bool            `json:"revoked"`
}

// Edge is a declared relation between two events.
type Edge struct {
	From      string    `json:"from"`
	To        string    `json:"to"`
	Kind      string    `json:"kind"`
	Rationale string    `json:"rationale"`
	CreatedAt time.Time `json:"created_at"`
	Seq       int64     `json:"seq"`
	Removed   bool      `json:"removed"`
}

// Incident groups events and their edges.
type Incident struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	Seq       int64     `json:"seq"`
	Members   []string  `json:"members"`
	Edges     []*Edge   `json:"edges"`
}

// Store is the durable, in-memory event-sourced application state.
type Store struct {
	dir string
	log *auditLog

	mu        sync.Mutex
	seq       int64
	now       func() time.Time
	events    map[string]*Event
	incidents map[string]*Incident
}

// Open loads state from the audit stream in dir and resumes appending to it.
func Open(dir string) (*Store, error) {
	log, err := openAuditLog(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:       dir,
		log:       log,
		now:       func() time.Time { return time.Now().UTC() },
		events:    map[string]*Event{},
		incidents: map[string]*Incident{},
	}
	lastSeq, lastOffset, torn, err := replay(dir, func(r Record) (int64, error) {
		return 0, s.apply(r)
	})
	if err != nil {
		return nil, err
	}
	s.seq = lastSeq
	if torn {
		if err := s.log.truncate(lastOffset); err != nil {
			return nil, fmt.Errorf("truncate torn audit line: %w", err)
		}
	}
	return s, nil
}

// Close releases the audit file.
func (s *Store) Close() error { return s.log.close() }

func EventKey(source, eventID string) string { return source + "/" + eventID }

func SplitEventKey(key string) (source, eventID string, ok bool) {
	i := strings.Index(key, "/")
	if i < 0 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// NextSeq exposes the high-water mark of the audit stream.
func (s *Store) NextSeq() int64 { return s.seq }

func (s *Store) commit(recType string, data any, raw []byte, apply func(Record)) error {
	rawData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	rec := Record{Seq: s.seq + 1, At: s.now(), Type: recType, Data: rawData, Raw: raw}
	if err := s.log.append(&rec); err != nil {
		return err
	}
	s.seq = rec.Seq
	apply(rec)
	return nil
}

func (s *Store) lock()   { s.mu.Lock() }
func (s *Store) unlock() { s.mu.Unlock() }

// CycleError describes a rejected edge declaration with a readable path.
type CycleError struct {
	Path []string
}

func (e *CycleError) Error() string {
	return "edge would create a cycle: " + strings.Join(e.Path, " -> ")
}
func (e *CycleError) Unwrap() error { return errors.New("cycle") }
