package handler

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const writeWait = 5 * time.Second

// WSHandler gestiona los clientes WebSocket conectados y difunde mensajes.
type WSHandler struct {
	upgrader  websocket.Upgrader
	clients   map[*websocket.Conn]struct{}
	mu        sync.Mutex
	closed    bool
	broadcast chan []byte
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
		clients:   make(map[*websocket.Conn]struct{}),
		broadcast: make(chan []byte, 256),
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
	h.clients[conn] = struct{}{}
	h.mu.Unlock()

	defer h.removeClient(conn)

	// Lectura necesaria para procesar pings/close y detectar desconexiones.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// Broadcast encola un mensaje para todos los clientes sin bloquear.
func (h *WSHandler) Broadcast(msg []byte) {
	select {
	case h.broadcast <- msg:
	default:
		slog.Warn("ws: cola de difusión llena, mensaje descartado")
	}
}

// HandleMessages difunde cada mensaje a todos los clientes. Bloquea; ejecútese en una goroutine.
func (h *WSHandler) HandleMessages() {
	for msg := range h.broadcast {
		h.mu.Lock()
		for conn := range h.clients {
			err := conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err == nil {
				err = conn.WriteMessage(websocket.TextMessage, msg)
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
