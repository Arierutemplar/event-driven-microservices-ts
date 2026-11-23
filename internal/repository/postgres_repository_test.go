package repository

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

// Prueba de integración: requiere TEST_POSTGRES_DSN, p. ej.
// postgres://postgres:postgres@localhost:5432/analytics?sslmode=disable
func TestPostgresRepository(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN no definido")
	}
	ctx := context.Background()

	r, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// La migración debe ser idempotente
	r2, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatalf("segunda migración: %v", err)
	}
	r2.Close()

	marker := "it-" + time.Now().Format("150405.000000")
	base := time.Now().UTC().Truncate(time.Microsecond)
	events := []*domain.Event{
		{ID: marker + "-1", EventType: "click", UserID: "u1", Payload: json.RawMessage(`"texto"`), Timestamp: base},
		{ID: marker + "-2", EventType: "purchase", UserID: "u2", Payload: json.RawMessage(`{"a":{"b":[1,2]}}`), Timestamp: base.Add(time.Second)},
		{ID: marker + "-3", EventType: "view", Timestamp: base.Add(2 * time.Second)}, // sin payload
	}
	for _, e := range events {
		if err := r.Save(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	got, err := r.GetHistory(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].EventType != "view" || got[2].EventType != "click" {
		t.Fatalf("orden inesperado: %+v", got)
	}
	if got[0].Payload != nil {
		t.Errorf("payload ausente debería ser nil, fue %s", got[0].Payload)
	}
	var obj map[string]any
	if err := json.Unmarshal(got[1].Payload, &obj); err != nil || obj["a"] == nil {
		t.Errorf("payload objeto no se conservó: %s", got[1].Payload)
	}
	if string(got[2].Payload) != `"texto"` {
		t.Errorf("payload string: %s", got[2].Payload)
	}
	if !got[2].Timestamp.Equal(base) {
		t.Errorf("timestamp: %v != %v", got[2].Timestamp, base)
	}

	// Idempotencia: el mismo event_id (ya guardado, o repetido dentro del lote) no duplica filas
	dup := marker + "-dup"
	batch := []*domain.Event{
		{ID: dup, EventType: "dupcheck", Timestamp: base.Add(3 * time.Second)},
		{ID: dup, EventType: "dupcheck", Timestamp: base.Add(4 * time.Second)},
		{ID: marker + "-1", EventType: "dupcheck", Timestamp: base.Add(5 * time.Second)},
	}
	if err := r.SaveBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := r.SaveBatch(ctx, batch); err != nil {
		t.Fatalf("reintento del lote: %v", err)
	}
	all, err := r.GetHistory(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range all {
		if e.EventType == "dupcheck" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("filas dupcheck = %d, quería 1", n)
	}
}

func TestPostgresTenantIsolation(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN no definido")
	}
	ctx := context.Background()
	r, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	marker := "tn-" + time.Now().Format("150405.000000")
	ts := time.Now().UTC().Truncate(time.Microsecond)
	batch := []*domain.Event{
		{ID: marker, TenantID: "acme", EventType: "tenantcheck", Timestamp: ts},
		{ID: marker, TenantID: "globex", EventType: "tenantcheck", Timestamp: ts}, // mismo id, otro tenant
		{ID: marker, TenantID: "acme", EventType: "tenantcheck", Timestamp: ts},   // duplicado real
		{ID: marker + "-d", EventType: "tenantcheck", Timestamp: ts},              // sin tenant => default
	}
	for i := 0; i < 2; i++ { // el reintento del lote tampoco duplica
		if err := r.SaveBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}

	all, err := r.GetHistory(ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, e := range all {
		if e.EventType == "tenantcheck" && strings.HasPrefix(e.ID, marker) {
			got[e.TenantID]++
		}
	}
	if got["acme"] != 1 || got["globex"] != 1 || got["default"] != 1 || len(got) != 3 {
		t.Fatalf("filas por tenant = %v, quería acme:1 globex:1 default:1", got)
	}
}
