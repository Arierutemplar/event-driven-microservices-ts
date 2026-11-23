package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/middleware"
)

func dial(t *testing.T, srv *httptest.Server, query string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws" + query
	return websocket.DefaultDialer.Dial(url, nil)
}

func readText(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := c.ReadMessage()
	if err != nil {
		return ""
	}
	return string(msg)
}

func TestWebSocketTenantIsolationWithTickets(t *testing.T) {
	ticketer, err := middleware.NewWSTicketer("0123456789abcdef0123456789abcdef", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ws := NewWSHandlerWithOrigins("")
	go ws.HandleMessages()
	defer ws.Close()
	srv := httptest.NewServer(middleware.WSAuth(middleware.WSModeTicket, ticketer, "static")(ws.HandleConnections))
	defer srv.Close()

	if _, resp, err := dial(t, srv, "?token=static"); err == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatal("el token estático debe rechazarse en modo ticket")
	}

	ta, _ := ticketer.Issue("tenant-a")
	tb, _ := ticketer.Issue("tenant-b")
	a, _, err := dial(t, srv, "?ticket="+ta)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, _, err := dial(t, srv, "?ticket="+tb)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	deadline := time.Now().Add(2 * time.Second)
	for len(ws.Tenants()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	ws.BroadcastTenant("tenant-a", []byte("solo-a"))
	if got := readText(t, a); got != "solo-a" {
		t.Fatalf("tenant-a recibió %q", got)
	}
	if got := readText(t, b); got != "" {
		t.Fatalf("tenant-b recibió datos de otro tenant: %q", got)
	}
}

func TestWebSocketClosesOversizedClientMessage(t *testing.T) {
	ws := NewWSHandlerWithOrigins("")
	go ws.HandleMessages()
	defer ws.Close()
	srv := httptest.NewServer(http.HandlerFunc(ws.HandleConnections))
	defer srv.Close()

	c, _, err := dial(t, srv, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.WriteMessage(websocket.TextMessage, make([]byte, maxClientMessageBytes+1)); err != nil {
		t.Fatal(err)
	}

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
		t.Fatalf("se esperaba cierre 1009 (mensaje demasiado grande), got %v", err)
	}
}
