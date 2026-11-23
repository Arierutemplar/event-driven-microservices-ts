package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

const (
	maxRedriveBody = 64 << 10
	redriveTimeout = 2 * time.Minute
	dlqReadTimeout = 15 * time.Second
)

// DLQHandler expone la inspección y el re-drive de la Dead Letter Queue.
// Debe montarse detrás de un middleware de autenticación de administrador.
type DLQHandler struct {
	mgr domain.DLQManager // nil si NATS no está activo
}

// NewDLQHandler crea el handler; mgr puede ser nil si NATS no está activo.
func NewDLQHandler(mgr domain.DLQManager) *DLQHandler { return &DLQHandler{mgr: mgr} }

func (h *DLQHandler) available(w http.ResponseWriter) bool {
	if h.mgr == nil {
		http.Error(w, "DLQ solo disponible con NATS", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// List atiende GET /api/dlq?limit=&after=&reason= y GET /api/dlq?seq=N (detalle con cuerpo).
func (h *DLQHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		return
	}
	if !h.available(w) {
		return
	}
	q := r.URL.Query()
	ctx, cancel := context.WithTimeout(r.Context(), dlqReadTimeout)
	defer cancel()

	if s := q.Get("seq"); s != "" {
		seq, err := strconv.ParseUint(s, 10, 64)
		if err != nil || seq == 0 {
			http.Error(w, "seq inválido", http.StatusBadRequest)
			return
		}
		d, err := h.mgr.Get(ctx, seq)
		if err != nil {
			h.fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, d)
		return
	}

	limit, ok := intParam(q.Get("limit"))
	if !ok {
		http.Error(w, "limit inválido", http.StatusBadRequest)
		return
	}
	var after uint64
	if s := q.Get("after"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			http.Error(w, "after inválido", http.StatusBadRequest)
			return
		}
		after = v
	}
	stats, err := h.mgr.Stats(ctx)
	if err != nil {
		h.fail(w, err)
		return
	}
	items, err := h.mgr.List(ctx, domain.DLQFilter{AfterSeq: after, Reason: q.Get("reason")}, limit)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stats": stats, "messages": items})
}

type redriveRequest struct {
	Seqs   []uint64 `json:"seqs"`
	Reason string   `json:"reason"`
	All    bool     `json:"all"`
	Limit  int      `json:"limit"`
	DryRun bool     `json:"dry_run"`
}

// Redrive atiende POST /api/dlq/redrive. Exige un selector explícito (seqs, reason o all)
// para que un cuerpo vacío nunca reinyecte toda la cola por accidente.
func (h *DLQHandler) Redrive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		return
	}
	if !h.available(w) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRedriveBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req redriveRequest
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "JSON inválido: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Limit < 0 {
		http.Error(w, "limit inválido", http.StatusBadRequest)
		return
	}
	if len(req.Seqs) == 0 && req.Reason == "" && !req.All {
		http.Error(w, "indica un selector: seqs, reason o all=true", http.StatusBadRequest)
		return
	}
	for _, s := range req.Seqs {
		if s == 0 {
			http.Error(w, "seqs inválido", http.StatusBadRequest)
			return
		}
	}

	slog.Info("dlq: re-drive solicitado", "remote", r.RemoteAddr, "seqs", len(req.Seqs),
		"reason", req.Reason, "all", req.All, "limit", req.Limit, "dry_run", req.DryRun)

	ctx, cancel := context.WithTimeout(r.Context(), redriveTimeout)
	defer cancel()
	res, err := h.mgr.Redrive(ctx, domain.DLQFilter{Seqs: req.Seqs, Reason: req.Reason}, req.Limit, req.DryRun)
	if err != nil {
		slog.Error("dlq: re-drive terminó con error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "result": res})
		return
	}
	status := http.StatusOK
	if res.Failed > 0 || res.DeleteFailed > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, res)
}

func (h *DLQHandler) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, domain.ErrNotFound) {
		http.Error(w, "mensaje no encontrado en la DLQ", http.StatusNotFound)
		return
	}
	slog.Error("dlq: operación fallida", "error", err)
	http.Error(w, "error interno", http.StatusInternalServerError)
}

func intParam(s string) (int, bool) {
	if s == "" {
		return 0, true
	}
	n, err := strconv.Atoi(s)
	return n, err == nil && n >= 0
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("escribir respuesta JSON", "error", err)
	}
}
