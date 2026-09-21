package store

import (
	"encoding/json"
	"sort"
	"time"
)

// CreateIncident opens a new incident.
func (s *Store) CreateIncident(title string) (*Incident, error) {
	if title == "" {
		return nil, conflictf("title is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p := pIncident{ID: newID("inc"), Title: title, At: now()}
	if _, err := s.appendLocked("incident", p); err != nil {
		return nil, err
	}
	return cloneIncident(s.incidents[p.ID]), nil
}

// IngestEvent stores a raw fact. If (source, eventID) was already received,
// the original fact is kept untouched and only a new receipt is recorded; the
// returned duplicate flag is true in that case.
func (s *Store) IngestEvent(incidentID, source, eventID string, raw json.RawMessage, reason string) (*Event, bool, error) {
	if incidentID == "" || source == "" || eventID == "" {
		return nil, false, conflictf("incident_id, source and event_id are required")
	}
	if !isJSONObject(raw) {
		return nil, false, conflictf("event payload must be a JSON object")
	}
	at := firstOccurredAt(raw)
	if at == "" {
		return nil, false, conflictf("payload.occurred_at is required")
	}
	if _, err := parseTime(at); err != nil {
		return nil, false, conflictf("payload.occurred_at must be RFC3339: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.incidents[incidentID]; !ok {
		return nil, false, conflictf("incident %q not found", incidentID)
	}
	if id, ok := s.dedup[dedupKey(source, eventID)]; ok {
		if _, err := s.appendLocked("receipt", pReceipt{
			Source: source, EventID: eventID, Reason: reason, At: now(),
		}); err != nil {
			return nil, false, err
		}
		return cloneEvent(s.events[id]), true, nil
	}

	p := pEvent{
		ID:         newID("evt"),
		IncidentID: incidentID,
		Source:     source,
		EventID:    eventID,
		Raw:        append(json.RawMessage(nil), raw...),
		At:         now(),
	}
	if _, err := s.appendLocked("event", p); err != nil {
		return nil, false, err
	}
	return cloneEvent(s.events[p.ID]), false, nil
}

// CorrectEvent creates a new timestamp version. expectedVersion must equal the
// event's current version, otherwise a ConflictError is returned.
func (s *Store) CorrectEvent(eventID string, expectedVersion int, occurredAt, note string) (*Event, error) {
	if _, err := parseTime(occurredAt); err != nil {
		return nil, conflictf("occurred_at must be RFC3339: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return nil, ErrNotFound
	}
	if expectedVersion <= 0 {
		return nil, conflictf("expected_version must be a positive version number")
	}
	if expectedVersion != ev.CurrentVer {
		return nil, conflictf("version conflict: event is at version %d, request was based on %d",
			ev.CurrentVer, expectedVersion)
	}
	p := pCorrect{
		EventID: eventID, OccurredAt: occurredAt, Note: note,
		ExpectedV: expectedVersion, At: now(),
	}
	if _, err := s.appendLocked("correct", p); err != nil {
		return nil, err
	}
	return cloneEvent(ev), nil
}

// RevokeEvent logically removes an event. Events still referenced by active
// edges cannot be revoked.
func (s *Store) RevokeEvent(eventID string) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[eventID]
	if !ok {
		return nil, ErrNotFound
	}
	if ev.Revoked {
		return nil, conflictf("event %s is already revoked", eventID)
	}
	for _, e := range s.edges {
		if e.Active() && e.IncidentID == ev.IncidentID && (e.CauseID == eventID || e.EffectID == eventID) {
			return nil, conflictf("event %s is still referenced by active edge %s", eventID, e.ID)
		}
	}
	if _, err := s.appendLocked("revoke", pRevoke{EventID: eventID, At: now()}); err != nil {
		return nil, err
	}
	return cloneEvent(ev), nil
}

// AddEdge declares that causeID happened before and may have caused effectID.
// Edges that would close a directed cycle are rejected with a human-readable
// path in the ConflictError message.
func (s *Store) AddEdge(incidentID, causeID, effectID, rationale string) (*Edge, error) {
	if rationale == "" {
		return nil, conflictf("rationale is required: explain why this edge holds")
	}
	if causeID == effectID {
		return nil, conflictf("an edge cannot connect an event to itself")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.incidents[incidentID]
	if !ok {
		return nil, conflictf("incident %q not found", incidentID)
	}
	cause, ok := s.events[causeID]
	if !ok || cause.IncidentID != inc.ID {
		return nil, conflictf("cause event %q not found in this incident", causeID)
	}
	effect, ok := s.events[effectID]
	if !ok || effect.IncidentID != inc.ID {
		return nil, conflictf("effect event %q not found in this incident", effectID)
	}
	if cause.Revoked || effect.Revoked {
		return nil, conflictf("revoked events cannot participate in edges")
	}
	for _, e := range s.edges {
		if e.Active() && e.IncidentID == incidentID && e.CauseID == causeID && e.EffectID == effectID {
			return nil, conflictf("edge %s already declares this relationship", e.ID)
		}
	}
	if path := s.cyclePath(incidentID, causeID, effectID); path != nil {
		return nil, &ConflictError{Msg: "edge would create a cycle: " + s.formatEventPath(path)}
	}
	p := pEdge{
		ID: newID("edge"), IncidentID: incidentID, CauseID: causeID,
		EffectID: effectID, Rationale: rationale, At: now(),
	}
	if _, err := s.appendLocked("edge", p); err != nil {
		return nil, err
	}
	return cloneEdge(s.edges[p.ID]), nil
}

// RemoveEdge logically removes an edge (its audit record stays).
func (s *Store) RemoveEdge(edgeID string) (*Edge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.edges[edgeID]
	if !ok {
		return nil, ErrNotFound
	}
	if !e.Active() {
		return nil, conflictf("edge %s is already removed", edgeID)
	}
	if _, err := s.appendLocked("edge_remove", pEdgeRemove{EdgeID: edgeID, At: now()}); err != nil {
		return nil, err
	}
	return cloneEdge(e), nil
}

// cyclePath returns a path from causeID back to itself once the proposed
// edge causeID -> effectID exists, i.e. an existing path effectID -> ... ->
// causeID. nil means the edge is acyclic.
func (s *Store) cyclePath(incidentID, causeID, effectID string) []string {
	adj := map[string][]string{}
	for _, e := range s.edges {
		if e.Active() && e.IncidentID == incidentID {
			adj[e.CauseID] = append(adj[e.CauseID], e.EffectID)
		}
	}
	visited := map[string]bool{}
	var dfs func(node string, trail []string) []string
	dfs = func(node string, trail []string) []string {
		trail = append(trail, node)
		visited[node] = true
		for _, next := range adj[node] {
			if next == causeID {
				return append(append([]string{}, trail...), causeID)
			}
			if !visited[next] {
				if p := dfs(next, trail); p != nil {
					return p
				}
			}
		}
		return nil
	}
	return dfs(effectID, []string{causeID})
}

func (s *Store) formatEventPath(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += " -> "
		}
		if ev, ok := s.events[id]; ok {
			out += ev.Source + "/" + ev.EventID + " (" + id + ")"
			continue
		}
		out += id
	}
	return out
}

// GetEvent returns a copy of an event.
func (s *Store) GetEvent(id string) (*Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ev, ok := s.events[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneEvent(ev), nil
}

// GetIncident returns a copy of an incident.
func (s *Store) GetIncident(id string) (*Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inc, ok := s.incidents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneIncident(inc), nil
}

// Snapshot returns a deep, stable-ordered copy of the whole state.
func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := Snapshot{Seq: s.seq}
	for _, inc := range s.incidents {
		snap.Incidents = append(snap.Incidents, *cloneIncident(inc))
	}
	sort.Slice(snap.Incidents, func(i, j int) bool {
		return snap.Incidents[i].CreatedAt < snap.Incidents[j].CreatedAt
	})
	for _, ev := range s.events {
		snap.Events = append(snap.Events, *cloneEvent(ev))
	}
	sort.Slice(snap.Events, func(i, j int) bool { return snap.Events[i].Seq < snap.Events[j].Seq })
	for _, e := range s.edges {
		snap.Edges = append(snap.Edges, *cloneEdge(e))
	}
	sort.Slice(snap.Edges, func(i, j int) bool { return snap.Edges[i].Seq < snap.Edges[j].Seq })
	return snap
}

// SortMode controls event ordering in incident views.
const (
	SortReception = "reception" // global receive sequence
	SortOriginal  = "original"  // occurred_at of version 1, receive-seq tiebreak
	SortCorrected = "corrected" // current version's occurred_at, receive-seq tiebreak
)

// SortEvents orders copies of events according to mode. Equal timestamps keep
// receive order, which is the documented stable tiebreak.
func SortEvents(events []Event, mode string) []Event {
	out := append([]Event(nil), events...)
	key := func(e Event) string {
		switch mode {
		case SortOriginal:
			if len(e.Versions) > 0 {
				return e.Versions[0].OccurredAt
			}
		case SortCorrected:
			return e.EffectiveAt()
		}
		return ""
	}
	if mode == SortReception {
		sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
		return out
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti, errI := parseTime(key(out[i]))
		tj, errJ := parseTime(key(out[j]))
		if errI != nil || errJ != nil {
			if errI != nil && errJ != nil {
				return out[i].Seq < out[j].Seq
			}
			return errI != nil
		}
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

func parseTime(v string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, v)
}

func isJSONObject(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var m map[string]json.RawMessage
	return json.Unmarshal(raw, &m) == nil
}
