package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/middleware"
)

type fakeDLQ struct {
	gotFilter domain.DLQFilter
	gotLimit  int
	gotDry    bool
	calls     int
	res       domain.RedriveResult
	err       error
}

func (f *fakeDLQ) Stats(context.Context) (domain.DLQStats, error) {
	return domain.DLQStats{Messages: 1}, nil
}
func (f *fakeDLQ) List(context.Context, domain.DLQFilter, int) ([]domain.DeadLetter, error) {
	return []domain.DeadLetter{{Seq: 1, Reason: "permanent_error"}}, nil
}
func (f *fakeDLQ) Get(_ context.Context, seq uint64) (*domain.DeadLetter, error) {
	if seq == 404 {
		return nil, fmt.Errorf("x: %w", domain.ErrNotFound)
	}
	return &domain.DeadLetter{Seq: seq, Body: "{}"}, nil
}
func (f *fakeDLQ) Redrive(_ context.Context, flt domain.DLQFilter, limit int, dry bool) (domain.RedriveResult, error) {
	f.calls++
	f.gotFilter, f.gotLimit, f.gotDry = flt, limit, dry
	return f.res, f.err
}

func dlqServer(mgr domain.DLQManager, key string) http.Handler {
	h := NewDLQHandler(mgr)
	admin := middleware.AdminOnly(key)
	mux := http.NewServeMux()
	mux.Handle("/api/dlq", admin(http.HandlerFunc(h.List)))
	mux.Handle("/api/dlq/redrive", admin(http.HandlerFunc(h.Redrive)))
	return mux
}

func do(h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestDLQAuthAndFailClosed(t *testing.T) {
	f := &fakeDLQ{}
	h := dlqServer(f, "admin")
	if rec := do(h, "POST", "/api/dlq/redrive", `{"all":true}`, ""); rec.Code != 401 {
		t.Fatalf("sin clave: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/dlq/redrive", `{"all":true}`, "otra"); rec.Code != 401 {
		t.Fatalf("clave errónea: %d", rec.Code)
	}
	if f.calls != 0 {
		t.Fatal("no debe ejecutarse sin autenticar")
	}
	// Sin API_KEY configurada se deniega todo, aunque el cliente envíe cabecera vacía
	closed := dlqServer(f, "")
	if rec := do(closed, "POST", "/api/dlq/redrive", `{"all":true}`, ""); rec.Code != 503 {
		t.Fatalf("fail-closed: %d", rec.Code)
	}
	if f.calls != 0 {
		t.Fatal("no debe ejecutarse con admin deshabilitado")
	}
}

func TestDLQRedriveValidation(t *testing.T) {
	f := &fakeDLQ{}
	h := dlqServer(f, "k")
	cases := []struct {
		name, method, body string
		want               int
	}{
		{"método", "GET", "", 405},
		{"cuerpo vacío", "POST", "", 400},
		{"sin selector", "POST", `{}`, 400},
		{"json roto", "POST", `{`, 400},
		{"campo desconocido", "POST", `{"all":true,"foo":1}`, 400},
		{"limit negativo", "POST", `{"all":true,"limit":-1}`, 400},
		{"seq cero", "POST", `{"seqs":[0]}`, 400},
	}
	for _, c := range cases {
		if rec := do(h, c.method, "/api/dlq/redrive", c.body, "k"); rec.Code != c.want {
			t.Errorf("%s: %d, want %d", c.name, rec.Code, c.want)
		}
	}
	if f.calls != 0 {
		t.Fatalf("las peticiones inválidas no deben llegar al gestor (%d)", f.calls)
	}
}

func TestDLQRedriveOK(t *testing.T) {
	f := &fakeDLQ{res: domain.RedriveResult{Matched: 2, Republished: 2}}
	h := dlqServer(f, "k")
	rec := do(h, "POST", "/api/dlq/redrive", `{"seqs":[3,4],"reason":"permanent_error","limit":10,"dry_run":true}`, "k")
	if rec.Code != 200 {
		t.Fatalf("code %d: %s", rec.Code, rec.Body)
	}
	if len(f.gotFilter.Seqs) != 2 || f.gotFilter.Reason != "permanent_error" || f.gotLimit != 10 || !f.gotDry {
		t.Fatalf("parámetros mal propagados: %+v %d %v", f.gotFilter, f.gotLimit, f.gotDry)
	}
	var out domain.RedriveResult
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out.Republished != 2 {
		t.Fatalf("respuesta: %s", rec.Body)
	}

	f.res = domain.RedriveResult{Matched: 1, Failed: 1}
	if rec := do(h, "POST", "/api/dlq/redrive", `{"all":true}`, "k"); rec.Code != http.StatusMultiStatus {
		t.Fatalf("fallos parciales: %d", rec.Code)
	}
	f.err = fmt.Errorf("nats caído")
	if rec := do(h, "POST", "/api/dlq/redrive", `{"all":true}`, "k"); rec.Code != 500 {
		t.Fatalf("error: %d", rec.Code)
	}
}

func TestDLQListAndGet(t *testing.T) {
	h := dlqServer(&fakeDLQ{}, "k")
	if rec := do(h, "GET", "/api/dlq?limit=5", "", "k"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "messages") {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "GET", "/api/dlq?seq=7", "", "k"); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"body"`) {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "GET", "/api/dlq?seq=404", "", "k"); rec.Code != 404 {
		t.Fatalf("404: %d", rec.Code)
	}
	if rec := do(h, "GET", "/api/dlq?seq=abc", "", "k"); rec.Code != 400 {
		t.Fatalf("seq inválido: %d", rec.Code)
	}
	if rec := do(h, "GET", "/api/dlq?limit=-3", "", "k"); rec.Code != 400 {
		t.Fatalf("limit inválido: %d", rec.Code)
	}
}

func TestDLQUnavailableWithoutNATS(t *testing.T) {
	h := dlqServer(nil, "k")
	if rec := do(h, "GET", "/api/dlq", "", "k"); rec.Code != 503 {
		t.Fatalf("list sin NATS: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/dlq/redrive", `{"all":true}`, "k"); rec.Code != 503 {
		t.Fatalf("redrive sin NATS: %d", rec.Code)
	}
}
