package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/middleware"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/usecase"
)

type captureRepo struct{ got chan *domain.Event }

func (c *captureRepo) Save(_ context.Context, e *domain.Event) error {
	c.got <- e
	return nil
}
func (c *captureRepo) GetHistory(context.Context, int) ([]*domain.Event, error) { return nil, nil }

func TestIngestTenantComesFromAuthNotBody(t *testing.T) {
	repo := &captureRepo{got: make(chan *domain.Event, 4)}
	svc := usecase.NewAnalyticsService(10, repo)
	svc.StartWorkerPool(1)
	defer svc.Stop()

	auth, err := middleware.NewTenantAuth("", map[string]string{"key-a": "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	h := auth.Ingest(NewAnalyticsHandler(svc).HandleIngest)

	// El cuerpo intenta suplantar a otro tenant
	req := httptest.NewRequest(http.MethodPost, "/api/events",
		strings.NewReader(`{"id":"e1","event_type":"click","tenant_id":"victima"}`))
	req.Header.Set("X-API-Key", "key-a")
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	select {
	case e := <-repo.got:
		if e.TenantID != "tenant-a" {
			t.Fatalf("tenant = %q, quería tenant-a (el del API key, no el del cuerpo)", e.TenantID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("el evento no llegó al repositorio")
	}

	// Sin clave no se acepta nada
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(`{"id":"e2"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("sin clave: status = %d, quería 401", rec.Code)
	}
}
