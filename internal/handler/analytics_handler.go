package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Arierutemplar/REPO/analytics-service/internal/domain"
	"github.com/Arierutemplar/REPO/analytics-service/internal/middleware"
	"github.com/Arierutemplar/REPO/analytics-service/internal/observability"
	"github.com/Arierutemplar/REPO/analytics-service/internal/usecase"
)

const historyTimeout = 5 * time.Second
const (
	maxEventBodyBytes = 1 << 20
	maxEventTypeBytes = 128
	maxUserIDBytes    = 256
	maxEventIDBytes   = 256
)

// AnalyticsHandler expone la ingesta, el historial y el resumen de métricas.
type AnalyticsHandler struct {
	service *usecase.AnalyticsService
}

// NewAnalyticsHandler crea el handler sobre el servicio de analíticas.
func NewAnalyticsHandler(service *usecase.AnalyticsService) *AnalyticsHandler {
	return &AnalyticsHandler{service: service}
}

// HandleIngest procesa la llegada de nuevos eventos de analíticas (POST)
func (h *AnalyticsHandler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxEventBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var event domain.Event
	if err := dec.Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_payload", "Payload JSON inválido", err)
		return
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_payload", "El cuerpo debe contener un único evento JSON", nil)
		return
	}
	if err := validateEvent(&event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_event", err.Error(), nil)
		return
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	// El tenant lo decide la autenticación, nunca el cuerpo de la petición
	event.TenantID = middleware.TenantFromContext(r.Context())

	// Ingesta asíncrona mediante el canal concurrente
	if err := h.service.IngestEvent(event); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	observability.RecordEventIngested(event.EventType)

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "event_accepted"})
}

// HandleHistory devuelve los eventos recientes persistidos (GET ?limit=N, por defecto 50, máx. 1000)
func (h *AnalyticsHandler) HandleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n < 1 {
			http.Error(w, "limit inválido", http.StatusBadRequest)
			return
		}
		limit = n
	}
	if limit > 1000 {
		limit = 1000
	}

	ctx, cancel := context.WithTimeout(r.Context(), historyTimeout)
	defer cancel()
	tenantID := middleware.TenantFromContext(r.Context())
	events, err := h.service.GetHistoryForTenant(ctx, tenantID, limit)
	if err != nil {
		slog.Error("leer historial", "error", err)
		http.Error(w, "No se pudo leer el historial", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, events)
}

// HandleSummary devuelve las métricas acumuladas (GET)
func (h *AnalyticsHandler) HandleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, h.service.GetSummaryForTenant(middleware.TenantFromContext(r.Context())))
}

func validateEvent(event *domain.Event) error {
	if strings.TrimSpace(event.EventType) == "" {
		return fmt.Errorf("event_type es obligatorio")
	}
	if len(event.ID) > maxEventIDBytes {
		return fmt.Errorf("id excede %d caracteres", maxEventIDBytes)
	}
	if len(event.EventType) > maxEventTypeBytes {
		return fmt.Errorf("event_type excede %d caracteres", maxEventTypeBytes)
	}
	if len(event.UserID) > maxUserIDBytes {
		return fmt.Errorf("user_id excede %d caracteres", maxUserIDBytes)
	}
	if len(event.Payload) > maxEventBodyBytes {
		return fmt.Errorf("payload excede el límite permitido")
	}
	if event.Timestamp.After(time.Now().Add(24 * time.Hour)) {
		return fmt.Errorf("timestamp no puede estar más de 24 horas en el futuro")
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, code, message string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	detail := struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{
		Code:    code,
		Message: message,
	}
	if err != nil {
		slog.Debug("HTTP error detail", "code", code, "error", err)
	}
	writeJSON(w, status, detail)
}
