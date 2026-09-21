package server

import (
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"

	"incident-timeline/internal/store"
)

//go:embed static/*
var staticFS embed.FS

// Server wires the store to HTTP handlers.
type Server struct {
	store *store.Store
	mux   *http.ServeMux
}

// New builds the HTTP handler.
func New(st *store.Store) http.Handler {
	s := &Server{store: st, mux: http.NewServeMux()}
	s.routes()
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/incidents", s.handleListIncidents)
	s.mux.HandleFunc("POST /api/incidents", s.handleCreateIncident)
	s.mux.HandleFunc("GET /api/incidents/{id}", s.handleGetIncident)
	s.mux.HandleFunc("POST /api/incidents/{id}/events", s.handleIngest)
	s.mux.HandleFunc("POST /api/incidents/{id}/events/batch", s.handleBatch)
	s.mux.HandleFunc("GET /api/events/{id}", s.handleGetEvent)
	s.mux.HandleFunc("POST /api/events/{id}/correct", s.handleCorrect)
	s.mux.HandleFunc("POST /api/events/{id}/revoke", s.handleRevoke)
	s.mux.HandleFunc("POST /api/incidents/{id}/edges", s.handleAddEdge)
	s.mux.HandleFunc("DELETE /api/edges/{edgeID}", s.handleRemoveEdge)

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	s.mux.Handle("GET /", http.FileServer(http.FS(sub)))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type incidentView struct {
	store.Incident
	EventCount int           `json:"event_count"`
	EdgeCount  int           `json:"edge_count"`
	Events     []store.Event `json:"events,omitempty"`
	Edges      []store.Edge  `json:"edges,omitempty"`
}

func (s *Server) handleListIncidents(w http.ResponseWriter, r *http.Request) {
	snap := s.store.Snapshot()
	counts := map[string]struct{ events, edges int }{}
	for _, ev := range snap.Events {
		c := counts[ev.IncidentID]
		c.events++
		counts[ev.IncidentID] = c
	}
	for _, e := range snap.Edges {
		if e.Active() {
			c := counts[e.IncidentID]
			c.edges++
			counts[e.IncidentID] = c
		}
	}
	out := make([]incidentView, 0, len(snap.Incidents))
	for _, inc := range snap.Incidents {
		c := counts[inc.ID]
		out = append(out, incidentView{Incident: inc, EventCount: c.events, EdgeCount: c.edges})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title string `json:"title"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	inc, err := s.store.CreateIncident(req.Title)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, inc)
}

func (s *Server) handleGetIncident(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	inc, err := s.store.GetIncident(id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	snap := s.store.Snapshot()
	mode := r.URL.Query().Get("sort")
	events := make([]store.Event, 0)
	edges := make([]store.Edge, 0)
	for _, ev := range snap.Events {
		if ev.IncidentID == id {
			events = append(events, ev)
		}
	}
	for _, e := range snap.Edges {
		if e.IncidentID == id {
			edges = append(edges, e)
		}
	}
	events = store.SortEvents(events, mode)
	writeJSON(w, http.StatusOK, incidentView{
		Incident:   *inc,
		EventCount: len(events),
		EdgeCount:  countActive(edges),
		Events:     events,
		Edges:      edges,
	})
}

func countActive(edges []store.Edge) int {
	n := 0
	for _, e := range edges {
		if e.Active() {
			n++
		}
	}
	return n
}

type ingestRequest struct {
	Source  string          `json:"source"`
	EventID string          `json:"event_id"`
	Reason  string          `json:"reason,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	incidentID := r.PathValue("id")
	var req ingestRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	ev, duplicate, err := s.store.IngestEvent(incidentID, req.Source, req.EventID, req.Payload, req.Reason)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"ok":        true,
		"duplicate": duplicate,
		"event":     ev,
	})
}

type batchRow struct {
	Line      int          `json:"line"`
	OK        bool         `json:"ok"`
	Source    string       `json:"source,omitempty"`
	EventID   string       `json:"event_id,omitempty"`
	EventRef  string       `json:"event_ref,omitempty"`
	Duplicate bool         `json:"duplicate,omitempty"`
	Error     string       `json:"error,omitempty"`
	Event     *store.Event `json:"event,omitempty"`
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	incidentID := r.PathValue("id")
	var req struct {
		Events []json.RawMessage `json:"events"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Events == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "body must contain an \"events\" array",
		})
		return
	}
	rows := make([]batchRow, 0, len(req.Events))
	for i, raw := range req.Events {
		row := batchRow{Line: i + 1}
		var item ingestRequest
		if err := json.Unmarshal(raw, &item); err != nil {
			row.Error = "invalid JSON row: " + err.Error()
			rows = append(rows, row)
			continue
		}
		row.Source = item.Source
		row.EventID = item.EventID
		ev, duplicate, err := s.store.IngestEvent(incidentID, item.Source, item.EventID, item.Payload, item.Reason)
		if err != nil {
			row.Error = err.Error()
			rows = append(rows, row)
			continue
		}
		row.OK = true
		row.Duplicate = duplicate
		row.EventRef = ev.ID
		row.Event = ev
		rows = append(rows, row)
	}
	failed := 0
	for _, row := range rows {
		if !row.OK {
			failed++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":    len(rows),
		"accepted": len(rows) - failed,
		"failed":   failed,
		"results":  rows,
	})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	ev, err := s.store.GetEvent(r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) handleCorrect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OccurredAt string `json:"occurred_at"`
		Note       string `json:"note"`
		ExpectedV  int    `json:"expected_version"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	ev, err := s.store.CorrectEvent(r.PathValue("id"), req.ExpectedV, req.OccurredAt, req.Note)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"ok": true, "event": ev, "version": ev.CurrentVer,
	})
}

func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	ev, err := s.store.RevokeEvent(r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "event": ev})
}

func (s *Server) handleAddEdge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CauseID   string `json:"cause_id"`
		EffectID  string `json:"effect_id"`
		Rationale string `json:"rationale"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	edge, err := s.store.AddEdge(r.PathValue("id"), req.CauseID, req.EffectID, req.Rationale)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "edge": edge})
}

func (s *Server) handleRemoveEdge(w http.ResponseWriter, r *http.Request) {
	edge, err := s.store.RemoveEdge(r.PathValue("edgeID"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "edge": edge})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return false
	}
	return true
}

func writeStoreError(w http.ResponseWriter, err error) {
	var conflict *store.ConflictError
	switch {
	case errors.As(err, &conflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, store.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
