package store

import (
	"encoding/json"
	"sort"
	"time"
)

type incidentCreatedData struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// CreateIncident opens a new incident with a generated id.
func (s *Store) CreateIncident(title string) (*Incident, error) {
	s.lock()
	defer s.unlock()
	if title == "" {
		title = "Untitled incident"
	}
	id := s.newIncidentID()
	d := incidentCreatedData{ID: id, Title: title}
	err := s.commit(RecIncidentCreated, d, nil, func(r Record) {
		s.applyIncidentCreated(r, d)
	})
	if err != nil {
		return nil, err
	}
	return s.incidents[id], nil
}

func (s *Store) newIncidentID() string {
	max := 0
	for id := range s.incidents {
		if len(id) > 1 && id[0] == 'i' && id[1] == '-' {
			n := parseSmallInt(id[2:])
			if n > max {
				max = n
			}
		}
	}
	return "i-" + formatSmallInt(max+1)
}

// cyclePath returns a readable loop if adding from->to closes one. A loop
// exists when `to` already reaches `from`; the readable path follows the
// declared edge direction: from -> to -> ... -> from.
func (s *Store) cyclePath(inc *Incident, from, to string) []string {
	adj := map[string][]string{}
	for _, e := range inc.Edges {
		if !e.Removed {
			adj[e.To] = append(adj[e.To], e.From)
		}
	}
	parent := map[string]string{}
	queue := []string{from}
	found := false
	for len(queue) > 0 && !found {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if _, seen := parent[next]; seen {
				continue
			}
			parent[next] = cur
			if next == to {
				found = true
				break
			}
			queue = append(queue, next)
		}
	}
	if !found {
		return nil
	}
	// Walk from `to` up the predecessor chain to `from`, reverse it, and
	// wrap with the new edge so the readable loop is from -> to -> ... -> from.
	chain := []string{}
	for cur := to; cur != from; cur = parent[cur] {
		chain = append(chain, cur)
	}
	path := []string{from, to}
	for i := len(chain) - 1; i >= 1; i-- {
		path = append(path, chain[i])
	}
	path = append(path, from)
	return path
}

func (s *Store) incidentOf(key string) (*Incident, int) {
	for _, inc := range s.incidents {
		for i, m := range inc.Members {
			if m == key {
				return inc, i
			}
		}
	}
	return nil, -1
}

// ---------- record application (shared by live commits and replay) ----------

func (s *Store) apply(rec Record) error {
	switch rec.Type {
	case RecIncidentCreated:
		var d incidentCreatedData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyIncidentCreated(rec, d)
	case RecEventReceived:
		var d eventReceivedData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEventReceived(rec, d)
	case RecEventDuplicate:
		var d eventDuplicateData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEventDuplicate(rec, d)
	case RecEventCorrected:
		var d eventCorrectedData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEventCorrected(rec, d)
	case RecEventAssigned:
		var d eventAssignedData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEventAssigned(rec, d)
	case RecEventRevoked:
		var d eventRevokedData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEventRevoked(rec, d)
	case RecEdgeDeclared:
		var d edgeDeclaredData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEdgeDeclared(rec, d)
	case RecEdgeRemoved:
		var d edgeRemovedData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s.applyEdgeRemoved(rec, d)
	default:
		return unknownRecord(rec.Type)
	}
	return nil
}

func (s *Store) applyIncidentCreated(r Record, d incidentCreatedData) {
	s.incidents[d.ID] = &Incident{ID: d.ID, Title: d.Title, CreatedAt: r.At, Seq: r.Seq}
}

func (s *Store) applyEventReceived(r Record, d eventReceivedData) {
	key := EventKey(d.Source, d.EventID)
	if s.events[key] != nil {
		s.applyEventDuplicate(r, eventDuplicateData{
			Source: d.Source, EventID: d.EventID, IncidentID: d.IncidentID,
		})
		return
	}
	ev := &Event{
		Source:       d.Source,
		EventID:      d.EventID,
		Key:          key,
		Raw:          append(json.RawMessage(nil), r.Raw...),
		RawTimestamp: d.Timestamp,
		FirstSeenSeq: r.Seq,
		Versions: []Version{{
			Version: 0, Timestamp: d.Timestamp,
			Note:      "timestamp from original payload",
			CreatedAt: r.At, Seq: r.Seq,
		}},
		Receptions:   []Reception{{At: r.At, Seq: r.Seq}},
		ReceiveCount: 1,
	}
	s.events[key] = ev
	if d.IncidentID != "" {
		if inc := s.incidents[d.IncidentID]; inc != nil {
			inc.Members = append(inc.Members, key)
		}
	}
}

func (s *Store) applyEventDuplicate(r Record, d eventDuplicateData) {
	key := EventKey(d.Source, d.EventID)
	ev := s.events[key]
	if ev == nil {
		return
	}
	ev.Receptions = append(ev.Receptions, Reception{At: r.At, Seq: r.Seq})
	ev.ReceiveCount++
	if d.IncidentID != "" {
		if inc := s.incidents[d.IncidentID]; inc != nil {
			for _, m := range inc.Members {
				if m == key {
					return
				}
			}
			inc.Members = append(inc.Members, key)
		}
	}
}

func (s *Store) applyEventCorrected(r Record, d eventCorrectedData) {
	key := EventKey(d.Source, d.EventID)
	ev := s.events[key]
	if ev == nil {
		return
	}
	for _, v := range ev.Versions {
		if v.Version == d.Version {
			return
		}
	}
	ev.Versions = append(ev.Versions, Version{
		Version: d.Version, Timestamp: d.Timestamp, Note: d.Note,
		CreatedAt: r.At, Seq: r.Seq,
	})
}

func (s *Store) applyEventAssigned(r Record, d eventAssignedData) {
	key := EventKey(d.Source, d.EventID)
	inc := s.incidents[d.IncidentID]
	if inc == nil || s.events[key] == nil {
		return
	}
	for _, m := range inc.Members {
		if m == key {
			return
		}
	}
	inc.Members = append(inc.Members, key)
}

func (s *Store) applyEventRevoked(r Record, d eventRevokedData) {
	if ev := s.events[EventKey(d.Source, d.EventID)]; ev != nil {
		ev.Revoked = true
	}
}

func (s *Store) applyEdgeDeclared(r Record, d edgeDeclaredData) {
	inc := s.incidents[d.IncidentID]
	if inc == nil {
		return
	}
	for _, e := range inc.Edges {
		if !e.Removed && e.From == d.From && e.To == d.To {
			return
		}
	}
	inc.Edges = append(inc.Edges, &Edge{
		From: d.From, To: d.To, Kind: d.Kind, Rationale: d.Rationale,
		CreatedAt: r.At, Seq: r.Seq,
	})
}

func (s *Store) applyEdgeRemoved(r Record, d edgeRemovedData) {
	inc := s.incidents[d.IncidentID]
	if inc == nil {
		return
	}
	for _, e := range inc.Edges {
		if e.From == d.From && e.To == d.To {
			e.Removed = true
		}
	}
}

// ---------- views ----------

// EventView is an event as presented in a timeline.
type EventView struct {
	Key          string          `json:"key"`
	Source       string          `json:"source"`
	EventID      string          `json:"event_id"`
	Raw          json.RawMessage `json:"raw"`
	Timestamp    time.Time       `json:"timestamp"`
	RawTimestamp time.Time       `json:"raw_timestamp"`
	FirstSeenSeq int64           `json:"first_seen_seq"`
	Order        int             `json:"order"`
	Version      int             `json:"version"`
	ReceiveCount int             `json:"receive_count"`
	Revoked      bool            `json:"revoked"`
	Corrected    bool            `json:"corrected"`
}

// EdgeView is an active edge with a human-readable justification.
type EdgeView struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Kind        string `json:"kind"`
	KindLabel   string `json:"kind_label"`
	Rationale   string `json:"rationale"`
	Explanation string `json:"explanation"`
	CreatedSeq  int64  `json:"created_seq"`
}

// IncidentView is the full working-set snapshot for one incident.
type IncidentView struct {
	ID        string      `json:"id"`
	Title     string      `json:"title"`
	CreatedAt time.Time   `json:"created_at"`
	Reception []EventView `json:"reception_order"`
	Timeline  []EventView `json:"timeline"`
	Edges     []EdgeView  `json:"edges"`
}

func (s *Store) eventView(ev *Event, order int, corrected bool) EventView {
	v := ev.Versions[len(ev.Versions)-1]
	ts := ev.RawTimestamp
	if corrected {
		ts = v.Timestamp
	}
	return EventView{
		Key: ev.Key, Source: ev.Source, EventID: ev.EventID, Raw: ev.Raw,
		Timestamp: ts, RawTimestamp: ev.RawTimestamp,
		FirstSeenSeq: ev.FirstSeenSeq, Order: order,
		Version: v.Version, ReceiveCount: ev.ReceiveCount,
		Revoked: ev.Revoked, Corrected: v.Version > 0,
	}
}

// ViewIncident builds reception order plus raw/corrected timeline (the API
// chooses via the corrected flag). Ties on timestamp are broken by original
// reception sequence, so ordering is total and stable.
func (s *Store) ViewIncident(id string, corrected bool) (*IncidentView, error) {
	inc := s.incidents[id]
	if inc == nil {
		return nil, &NotFoundError{What: "incident", ID: id}
	}
	v := &IncidentView{ID: inc.ID, Title: inc.Title, CreatedAt: inc.CreatedAt}

	recpKeys := append([]string(nil), inc.Members...)
	sort.SliceStable(recpKeys, func(i, j int) bool {
		return s.events[recpKeys[i]].FirstSeenSeq < s.events[recpKeys[j]].FirstSeenSeq
	})
	for i, key := range recpKeys {
		v.Reception = append(v.Reception, s.eventView(s.events[key], i, false))
	}

	tlKeys := append([]string(nil), inc.Members...)
	sort.SliceStable(tlKeys, func(i, j int) bool {
		a, b := s.events[tlKeys[i]], s.events[tlKeys[j]]
		ta, tb := a.RawTimestamp, b.RawTimestamp
		if corrected {
			ta = a.Versions[len(a.Versions)-1].Timestamp
			tb = b.Versions[len(b.Versions)-1].Timestamp
		}
		if !ta.Equal(tb) {
			return ta.Before(tb)
		}
		return a.FirstSeenSeq < b.FirstSeenSeq
	})
	for i, key := range tlKeys {
		v.Timeline = append(v.Timeline, s.eventView(s.events[key], i, corrected))
	}

	for _, e := range inc.Edges {
		if e.Removed {
			continue
		}
		ev := EdgeView{
			From: e.From, To: e.To, Kind: e.Kind, Rationale: e.Rationale,
			CreatedSeq: e.Seq,
		}
		if e.Kind == EdgeBefore {
			ev.KindLabel = "先后约束 (before)"
			ev.Explanation = "声明 " + e.From + " 的发生时间早于 " + e.To
		} else {
			ev.KindLabel = "可能因果 (causes)"
			ev.Explanation = "声明 " + e.From + " 可能导致 " + e.To
		}
		if e.Rationale != "" {
			ev.Explanation += "；理由：" + e.Rationale
		}
		ev.Explanation += "（依据使用者声明，于审计序号 " + itoa(e.Seq) + " 建立）"
		v.Edges = append(v.Edges, ev)
	}
	sort.SliceStable(v.Edges, func(i, j int) bool { return v.Edges[i].CreatedSeq < v.Edges[j].CreatedSeq })
	return v, nil
}

// ListIncidents returns incident summaries.
func (s *Store) ListIncidents() []*Incident {
	out := make([]*Incident, 0, len(s.incidents))
	for _, inc := range s.incidents {
		out = append(out, inc)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// GetEvent returns full version and reception history.
func (s *Store) GetEvent(key string) (*Event, error) {
	ev := s.events[key]
	if ev == nil {
		return nil, &NotFoundError{What: "event", ID: key}
	}
	return ev, nil
}
