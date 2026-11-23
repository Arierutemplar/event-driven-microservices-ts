package handler

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/middleware"
)

const (
	writeWait = 5 * time.Second
	// El dashboard no envía datos: basta para frames de control y evita que un cliente
	// obligue al servidor a bufferizar mensajes arbitrariamente grandes.
	maxClientMessageBytes = 512
)

// WSHandler gestiona los clientes WebSocket conectados y difunde mensajes.
type WSHandler struct {
	upgrader  websocket.Upgrader
	clients   map[*websocket.Conn]string // conexión -> tenant ("" = vista global)
	mu        sync.Mutex
	closed    bool
	broadcast chan wsMessage
}

type wsMessage struct {
	tenant string
	data   []byte
}

// NewWSHandler crea un WSHandler con CORS abierto (CheckOrigin permisivo) y cola de difusión acotada.
func NewWSHandler() *WSHandler {
	return NewWSHandlerWithOrigins(os.Getenv("WS_ALLOWED_ORIGINS"))
}

// NewWSHandlerWithOrigins habilita restricciones de origen para producción.
// Una lista vacía conserva el modo permisivo por compatibilidad de desarrollo.
func NewWSHandlerWithOrigins(allowed string) *WSHandler {
	origins := make(map[string]struct{})
	for _, origin := range strings.Split(allowed, ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			origins[origin] = struct{}{}
		}
	}
	return &WSHandler{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				if len(origins) == 0 {
					return true
				}
				origin := r.Header.Get("Origin")
				if origin == "" {
					return true
				}
				if _, ok := origins["*"]; ok {
					return true
				}
				_, ok := origins[origin]
				return ok
			},
		},
		clients:   make(map[*websocket.Conn]string),
		broadcast: make(chan wsMessage, 256),
	}
}

// closeConn cierra la conexión; un error aquí solo indica que ya estaba cerrada.
func closeConn(conn *websocket.Conn) {
	if err := conn.Close(); err != nil {
		slog.Debug("ws: cerrar conexión", "error", err)
	}
}

// HandleConnections actualiza la conexión HTTP a WebSocket y registra al cliente.
func (h *WSHandler) HandleConnections(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("ws: error de upgrade", "error", err)
		return
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		closeConn(conn)
		return
	}
	h.clients[conn] = middleware.WSTenantFromContext(r.Context())
	h.mu.Unlock()

	defer h.removeClient(conn)
	conn.SetReadLimit(maxClientMessageBytes)

	// Lectura necesaria para procesar pings/close y detectar desconexiones.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// Broadcast encola un mensaje para los clientes de vista global sin bloquear.
func (h *WSHandler) Broadcast(msg []byte) { h.BroadcastTenant("", msg) }

// BroadcastTenant encola un mensaje solo para los clientes ligados a ese tenant.
func (h *WSHandler) BroadcastTenant(tenant string, msg []byte) {
	select {
	case h.broadcast <- wsMessage{tenant: tenant, data: msg}:
	default:
		slog.Warn("ws: cola de difusión llena, mensaje descartado")
	}
}

// Tenants lista los tenants con al menos un cliente conectado (excluye la vista global).
func (h *WSHandler) Tenants() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	seen := map[string]struct{}{}
	var out []string
	for _, t := range h.clients {
		if _, ok := seen[t]; t != "" && !ok {
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	return out
}

// HandleMessages difunde cada mensaje a todos los clientes. Bloquea; ejecútese en una goroutine.
func (h *WSHandler) HandleMessages() {
	for msg := range h.broadcast {
		h.mu.Lock()
		for conn, tenant := range h.clients {
			if tenant != msg.tenant {
				continue
			}
			err := conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err == nil {
				err = conn.WriteMessage(websocket.TextMessage, msg.data)
			}
			if err != nil {
				slog.Debug("ws: cliente descartado", "error", err)
				closeConn(conn)
				delete(h.clients, conn)
			}
		}
		h.mu.Unlock()
	}
}

func (h *WSHandler) removeClient(conn *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, conn)
	h.mu.Unlock()
	closeConn(conn)
}

// Close despide a los clientes con un cierre limpio (1001 going away) y rechaza nuevas
// conexiones. Http.Server.Shutdown no gestiona conexiones ya "hijacked", por eso es necesario.
func (h *WSHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	msg := websocket.FormatCloseMessage(websocket.CloseGoingAway, "servidor en apagado")
	for conn := range h.clients {
		if err := conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(writeWait)); err != nil {
			slog.Debug("ws: enviar cierre", "error", err)
		}
		closeConn(conn)
		delete(h.clients, conn)
	}
}
