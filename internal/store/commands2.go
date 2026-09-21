package store

import (
	"fmt"
	"strings"
	"time"
)

// CorrectEvent appends a new timestamp version. expectedVersion must equal the
// event's current version, otherwise ErrConflict is returned; the original
// version 0 always remains inspectable.
func (s *Store) CorrectEvent(key, tsText, note string, expectedVersion int) (int, error) {
	s.lock()
	defer s.unlock()
	ev := s.events[key]
	if ev == nil {
		return 0, fmt.Errorf("event %q not found", key)
	}
	current := ev.Versions[len(ev.Versions)-1].Version
	if expectedVersion != current {
		return current, &ConflictError{CurrentVersion: current}
	}
	ts, err := time.Parse(time.RFC3339Nano, tsText)
	if err != nil {
		return current, fmt.Errorf("timestamp must be RFC3339 (e.g. 2026-09-21T10:00:00Z)")
	}
	d := eventCorrectedData{
		Source: ev.Source, EventID: ev.EventID,
		Version: current + 1, Timestamp: ts.UTC(), Note: note,
	}
	err = s.commit(RecEventCorrected, d, nil, func(r Record) {
		s.applyEventCorrected(r, d)
	})
	if err != nil {
		return current, err
	}
	return current + 1, nil
}

// AssignEvent adds an existing event to an incident (idempotent).
func (s *Store) AssignEvent(incidentID, key string) error {
	s.lock()
	defer s.unlock()
	ev := s.events[key]
	if ev == nil {
		return fmt.Errorf("event %q not found", key)
	}
	if ev.Revoked {
		return fmt.Errorf("event %s is revoked", key)
	}
	inc := s.incidents[incidentID]
	if inc == nil {
		return fmt.Errorf("incident %q not found", incidentID)
	}
	for _, m := range inc.Members {
		if m == key {
			return nil
		}
	}
	d := eventAssignedData{IncidentID: incidentID, Source: ev.Source, EventID: ev.EventID}
	return s.commit(RecEventAssigned, d, nil, func(r Record) {
		s.applyEventAssigned(r, d)
	})
}

// DeclareEdge adds a before/causes edge. It is rejected if the edge would
// close a cycle or reference missing/revoked/mis-grouped events.
func (s *Store) DeclareEdge(incidentID, from, to, kind, rationale string) (*Edge, error) {
	s.lock()
	defer s.unlock()
	if from == to {
		return nil, fmt.Errorf("self-loops are not allowed")
	}
	if kind != EdgeBefore && kind != EdgeCauses {
		return nil, fmt.Errorf("kind must be %q or %q", EdgeBefore, EdgeCauses)
	}
	inc := s.incidents[incidentID]
	if inc == nil {
		return nil, fmt.Errorf("incident %q not found", incidentID)
	}
	if err := s.edgeEndpointOK(inc, from); err != nil {
		return nil, err
	}
	if err := s.edgeEndpointOK(inc, to); err != nil {
		return nil, err
	}
	for _, e := range inc.Edges {
		if !e.Removed && e.From == from && e.To == to {
			return nil, fmt.Errorf("active edge %s -> %s already exists", from, to)
		}
	}
	if path := s.cyclePath(inc, from, to); path != nil {
		return nil, &CycleError{Path: path}
	}
	d := edgeDeclaredData{
		IncidentID: incidentID, From: from, To: to, Kind: kind, Rationale: rationale,
	}
	err := s.commit(RecEdgeDeclared, d, nil, func(r Record) {
		s.applyEdgeDeclared(r, d)
	})
	if err != nil {
		return nil, err
	}
	return inc.Edges[len(inc.Edges)-1], nil
}

func (s *Store) edgeEndpointOK(inc *Incident, key string) error {
	ev := s.events[key]
	if ev == nil {
		return fmt.Errorf("event %q not found", key)
	}
	if ev.Revoked {
		return fmt.Errorf("event %s is revoked", key)
	}
	member := false
	for _, m := range inc.Members {
		if m == key {
			member = true
			break
		}
	}
	if !member {
		return fmt.Errorf("event %s does not belong to incident %s", key, inc.ID)
	}
	return nil
}

// RemoveEdge logically removes an edge.
func (s *Store) RemoveEdge(incidentID, from, to string) error {
	s.lock()
	defer s.unlock()
	inc := s.incidents[incidentID]
	if inc == nil {
		return fmt.Errorf("incident %q not found", incidentID)
	}
	var target *Edge
	for _, e := range inc.Edges {
		if !e.Removed && e.From == from && e.To == to {
			target = e
			break
		}
	}
	if target == nil {
		return fmt.Errorf("active edge %s -> %s not found", from, to)
	}
	d := edgeRemovedData{IncidentID: incidentID, From: from, To: to}
	return s.commit(RecEdgeRemoved, d, nil, func(r Record) {
		s.applyEdgeRemoved(r, d)
	})
}

// RevokeEvent performs a logical withdrawal. It fails while any active edge in
// any incident still references the event.
func (s *Store) RevokeEvent(key string) error {
	s.lock()
	defer s.unlock()
	ev := s.events[key]
	if ev == nil {
		return fmt.Errorf("event %q not found", key)
	}
	if ev.Revoked {
		return fmt.Errorf("event %s already revoked", key)
	}
	var blocked []string
	for _, inc := range s.incidents {
		for _, e := range inc.Edges {
			if !e.Removed && (e.From == key || e.To == key) {
				blocked = append(blocked, fmt.Sprintf("%s: %s -> %s", inc.ID, e.From, e.To))
			}
		}
	}
	if len(blocked) > 0 {
		return fmt.Errorf("event %s is still referenced by active edge(s): %s", key, strings.Join(blocked, "; "))
	}
	d := eventRevokedData{Source: ev.Source, EventID: ev.EventID}
	return s.commit(RecEventRevoked, d, nil, func(r Record) {
		s.applyEventRevoked(r, d)
	})
}

type eventCorrectedData struct {
	Source    string    `json:"source"`
	EventID   string    `json:"event_id"`
	Version   int       `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Note      string    `json:"note"`
}

type eventAssignedData struct {
	IncidentID string `json:"incident_id"`
	Source     string `json:"source"`
	EventID    string `json:"event_id"`
}

type eventRevokedData struct {
	Source  string `json:"source"`
	EventID string `json:"event_id"`
}

type edgeDeclaredData struct {
	IncidentID string `json:"incident_id"`
	From       string `json:"from"`
	To         string `json:"to"`
	Kind       string `json:"kind"`
	Rationale  string `json:"rationale"`
}

type edgeRemovedData struct {
	IncidentID string `json:"incident_id"`
	From       string `json:"from"`
	To         string `json:"to"`
}

// ConflictError signals an optimistic-concurrency mismatch on correction.
type ConflictError struct {
	CurrentVersion int
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("version conflict: expected base version is stale, current version is %d", e.CurrentVersion)
}
