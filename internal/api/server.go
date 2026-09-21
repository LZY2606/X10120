package api

import (
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strings"

	"incident-timeline/internal/store"
)

//go:embed all:web
var webFS embed.FS

// Server wires the store to HTTP handlers.
type Server struct {
	Store *store.Store
	Mux   *http.ServeMux
}

// New builds the HTTP handler tree.
func New(st *store.Store) *Server {
	s := &Server{Store: st, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	sub, _ := fs.Sub(webFS, "web")
	s.Mux.Handle("GET /", http.FileServer(http.FS(sub)))

	s.Mux.HandleFunc("GET /api/incidents", s.listIncidents)
	s.Mux.HandleFunc("POST /api/incidents", s.createIncident)
	s.Mux.HandleFunc("GET /api/incidents/", s.incidentSubrouter)
	s.Mux.HandleFunc("POST /api/incidents/", s.incidentSubrouter)
	s.Mux.HandleFunc("DELETE /api/incidents/", s.incidentSubrouter)

	s.Mux.HandleFunc("POST /api/events/ingest", func(w http.ResponseWriter, r *http.Request) {
		s.ingest(w, r, "")
	})
	s.Mux.HandleFunc("POST /api/events/bulk", func(w http.ResponseWriter, r *http.Request) {
		s.bulk(w, r, "")
	})
	s.Mux.HandleFunc("GET /api/event", s.getEvent)
	s.Mux.HandleFunc("POST /api/event/correct", s.correctEvent)
	s.Mux.HandleFunc("POST /api/event/revoke", s.revokeEvent)
}

func (s *Server) incidentSubrouter(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/incidents/")
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	rest := ""
	if len(parts) == 2 {
		rest = parts[1]
	}
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing incident id")
		return
	}
	switch {
	case rest == "" && r.Method == http.MethodGet:
		corrected := r.URL.Query().Get("corrected") == "1"
		view, err := s.Store.ViewIncident(id, corrected)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	case rest == "ingest" && r.Method == http.MethodPost:
		s.ingest(w, r, id)
	case rest == "bulk" && r.Method == http.MethodPost:
		s.bulk(w, r, id)
	case rest == "members" && r.Method == http.MethodPost:
		s.addMember(w, r, id)
	case rest == "edges" && r.Method == http.MethodPost:
		s.declareEdge(w, r, id)
	case rest == "edges" && r.Method == http.MethodDelete:
		s.removeEdge(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "no such API route")
	}
}

func (s *Server) listIncidents(w http.ResponseWriter, r *http.Request) {
	incs := s.Store.ListIncidents()
	type summary struct {
		ID        string `json:"id"`
		Title     string `json:"title"`
		Events    int    `json:"events"`
		Edges     int    `json:"active_edges"`
		CreatedAt string `json:"created_at"`
	}
	out := make([]summary, 0, len(incs))
	for _, inc := range incs {
		edges := 0
		for _, e := range inc.Edges {
			if !e.Removed {
				edges++
			}
		}
		out = append(out, summary{
			ID: inc.ID, Title: inc.Title, Events: len(inc.Members),
			Edges: edges, CreatedAt: inc.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) createIncident(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title string `json:"title"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	inc, err := s.Store.CreateIncident(strings.TrimSpace(req.Title))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, inc)
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request, incidentID string) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.Store.IngestEvent(raw, incidentID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) bulk(w http.ResponseWriter, r *http.Request, incidentID string) {
	var req struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "body must be {\"events\":[...]}: "+err.Error())
		return
	}
	if req.Events == nil {
		writeError(w, http.StatusBadRequest, "events array is required")
		return
	}
	rows := make([][]byte, len(req.Events))
	for i, e := range req.Events {
		rows[i] = e
	}
	results := s.Store.BulkIngest(rows, incidentID)
	accepted := 0
	for _, rr := range results {
		if rr.Accepted {
			accepted++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":    len(results),
		"accepted": accepted,
		"failed":   len(results) - accepted,
		"results":  results,
	})
}

func (s *Server) addMember(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		Key string `json:"key"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.Store.AssignEvent(id, strings.TrimSpace(req.Key)); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "assigned"})
}

func (s *Server) declareEdge(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		From      string `json:"from"`
		To        string `json:"to"`
		Kind      string `json:"kind"`
		Rationale string `json:"rationale"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	edge, err := s.Store.DeclareEdge(id, strings.TrimSpace(req.From),
		strings.TrimSpace(req.To), strings.TrimSpace(req.Kind), req.Rationale)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, edge)
}

func (s *Server) removeEdge(w http.ResponseWriter, r *http.Request, id string) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	if from == "" || to == "" {
		writeError(w, http.StatusBadRequest, "from and to query parameters are required")
		return
	}
	if err := s.Store.RemoveEdge(id, from, to); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key query parameter is required")
		return
	}
	ev, err := s.Store.GetEvent(key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) correctEvent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key         string `json:"key"`
		Timestamp   string `json:"timestamp"`
		Note        string `json:"note"`
		BaseVersion *int   `json:"base_version"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.BaseVersion == nil {
		writeError(w, http.StatusBadRequest, "base_version is required")
		return
	}
	newVersion, err := s.Store.CorrectEvent(strings.TrimSpace(req.Key),
		strings.TrimSpace(req.Timestamp), req.Note, *req.BaseVersion)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": newVersion, "status": "corrected"})
}

func (s *Server) revokeEvent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.Store.RevokeEvent(strings.TrimSpace(req.Key)); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

func writeStoreError(w http.ResponseWriter, err error) {
	var cycle *store.CycleError
	if errors.As(err, &cycle) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": err.Error(),
			"code":  "cycle_detected",
			"path":  cycle.Path,
		})
		return
	}
	var conflict *store.ConflictError
	if errors.As(err, &conflict) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":           err.Error(),
			"code":            "version_conflict",
			"current_version": conflict.CurrentVersion,
		})
		return
	}
	var nf *store.NotFoundError
	if errors.As(err, &nf) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}
