package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// inboundEvent is the parsed envelope of a posted event.
type inboundEvent struct {
	Source    string `json:"source"`
	EventID   string `json:"event_id"`
	Timestamp string `json:"timestamp"`
}

// IngestResult reports what happened to one posted event.
type IngestResult struct {
	Key          string `json:"key"`
	Duplicate    bool   `json:"duplicate"`
	ReceiveCount int    `json:"receive_count"`
	IncidentID   string `json:"incident_id,omitempty"`
}

// RowResult is the per-row outcome of a bulk import.
type RowResult struct {
	Index    int    `json:"index"`
	Accepted bool   `json:"accepted"`
	Key      string `json:"key,omitempty"`
	Error    string `json:"error,omitempty"`
}

// ValidateIngest parses and validates one raw event without touching state.
func ValidateIngest(raw []byte) (inboundEvent, time.Time, error) {
	var ev inboundEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return ev, time.Time{}, fmt.Errorf("invalid JSON object: %v", err)
	}
	if strings.TrimSpace(ev.Source) == "" {
		return ev, time.Time{}, fmt.Errorf("source is required")
	}
	if strings.TrimSpace(ev.EventID) == "" {
		return ev, time.Time{}, fmt.Errorf("event_id is required")
	}
	ts, err := time.Parse(time.RFC3339Nano, ev.Timestamp)
	if err != nil {
		return ev, time.Time{}, fmt.Errorf("timestamp must be RFC3339 (e.g. 2026-09-21T10:00:00Z)")
	}
	return ev, ts.UTC(), nil
}

// IngestEvent stores a raw event verbatim (deduplicated) and optionally adds
// it to an incident in the same atomic commit.
func (s *Store) IngestEvent(raw []byte, incidentID string) (IngestResult, error) {
	s.lock()
	defer s.unlock()
	return s.ingestEventLocked(raw, incidentID)
}

func (s *Store) ingestEventLocked(raw []byte, incidentID string) (IngestResult, error) {
	var res IngestResult
	ev, ts, err := ValidateIngest(raw)
	if err != nil {
		return res, err
	}
	key := EventKey(ev.Source, ev.EventID)

	var existing *Event
	var inc *Incident
	if existing, _ = s.events[key]; existing == nil {
		if incidentID != "" {
			inc = s.incidents[incidentID]
			if inc == nil {
				return res, fmt.Errorf("incident %q not found", incidentID)
			}
		}
		d := eventReceivedData{
			Source: ev.Source, EventID: ev.EventID, Timestamp: ts,
			IncidentID: incidentID,
		}
		err = s.commit(RecEventReceived, d, append([]byte(nil), raw...), func(r Record) {
			s.applyEventReceived(r, d)
		})
		if err != nil {
			return res, err
		}
		existing = s.events[key]
	} else {
		d := eventDuplicateData{
			Source: ev.Source, EventID: ev.EventID, IncidentID: incidentID,
		}
		if incidentID != "" {
			if s.incidents[incidentID] == nil {
				return res, fmt.Errorf("incident %q not found", incidentID)
			}
			if existing.Revoked {
				return res, fmt.Errorf("event %s is revoked", key)
			}
		}
		err = s.commit(RecEventDuplicate, d, nil, func(r Record) {
			s.applyEventDuplicate(r, d)
		})
		if err != nil {
			return res, err
		}
	}

	res.Key = key
	res.Duplicate = existing.ReceiveCount > 1
	res.ReceiveCount = existing.ReceiveCount
	if inc2, _ := s.incidentOf(key); inc2 != nil {
		res.IncidentID = inc2.ID
	}
	return res, nil
}

// BulkIngest ingests a batch; valid rows are committed independently so one
// bad row never prevents the others from landing.
func (s *Store) BulkIngest(rows [][]byte, incidentID string) []RowResult {
	s.lock()
	defer s.unlock()
	results := make([]RowResult, len(rows))
	for i, raw := range rows {
		results[i].Index = i
		ev, _, verr := ValidateIngest(raw)
		if verr == nil {
			results[i].Key = EventKey(ev.Source, ev.EventID)
		}
		ingestRes, ierr := s.ingestEventLocked(raw, incidentID)
		if ierr != nil {
			if verr != nil {
				results[i].Error = verr.Error()
			} else {
				results[i].Error = ierr.Error()
			}
			continue
		}
		results[i].Accepted = true
		results[i].Key = ingestRes.Key
	}
	return results
}

type eventReceivedData struct {
	Source     string    `json:"source"`
	EventID    string    `json:"event_id"`
	Timestamp  time.Time `json:"timestamp"`
	IncidentID string    `json:"incident_id,omitempty"`
}

type eventDuplicateData struct {
	Source     string `json:"source"`
	EventID    string `json:"event_id"`
	IncidentID string `json:"incident_id,omitempty"`
}
