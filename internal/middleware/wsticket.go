package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

// Modos de autenticación del WebSocket (WS_AUTH_MODE).
const (
	WSModeAny    = "any"    // ticket o token estático (compatibilidad)
	WSModeTicket = "ticket" // solo tickets efímeros firmados, ligados a un tenant
	WSModeToken  = "token"  // solo token estático global (legado)
)

type wsTenantKey struct{}

// WSTenantFromContext devuelve el tenant al que quedó ligada la conexión WebSocket.
// Una cadena vacía significa vista global (token estático / sin autenticación).
func WSTenantFromContext(ctx context.Context) string {
	t, _ := ctx.Value(wsTenantKey{}).(string)
	return t
}

// WSTicketer emite y verifica tickets de un solo propósito para abrir el WebSocket.
// Un ticket es `base64url(payload).base64url(HMAC-SHA256)`; caduca en segundos, así que
// aunque quede en la URL (logs de proxies) deja de servir casi de inmediato.
type WSTicketer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

type wsTicketClaims struct {
	Tenant string `json:"t"`
	Exp    int64  `json:"e"`
}

// NewWSTicketer exige un secreto de al menos 32 bytes.
func NewWSTicketer(secret string, ttl time.Duration) (*WSTicketer, error) {
	if len(secret) < 32 {
		return nil, errors.New("WS_TICKET_SECRET debe tener al menos 32 caracteres")
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &WSTicketer{secret: []byte(secret), ttl: ttl, now: time.Now}, nil
}

func (t *WSTicketer) sign(payload string) string {
	m := hmac.New(sha256.New, t.secret)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// Issue firma un ticket para el tenant indicado.
func (t *WSTicketer) Issue(tenant string) (string, time.Time) {
	exp := t.now().Add(t.ttl)
	raw, _ := json.Marshal(wsTicketClaims{Tenant: tenant, Exp: exp.Unix()})
	payload := base64.RawURLEncoding.EncodeToString(raw)
	return payload + "." + t.sign(payload), exp
}

// Verify devuelve el tenant del ticket si la firma es válida y no ha caducado.
func (t *WSTicketer) Verify(ticket string) (string, error) {
	payload, sig, ok := strings.Cut(ticket, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(t.sign(payload))) {
		return "", errors.New("firma inválida")
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return "", errors.New("ticket malformado")
	}
	var c wsTicketClaims
	if err := json.Unmarshal(raw, &c); err != nil || !ValidTenantID(c.Tenant) {
		return "", errors.New("ticket malformado")
	}
	if t.now().Unix() >= c.Exp {
		return "", errors.New("ticket caducado")
	}
	return c.Tenant, nil
}

// IssueHandler responde con un ticket para el tenant autorizado en el contexto. Debe ir detrás
// de TenantAuth.Read: así la clave de tenant (que nunca llega al bundle del navegador si el
// frontend la obtiene vía un BFF/sesión) es lo único que concede acceso a ese tenant.
func (t *WSTicketer) IssueHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
			return
		}
		tenant := TenantFromContext(r.Context())
		ticket, exp := t.Issue(tenant)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ticket":     ticket,
			"tenant":     tenant,
			"expires_in": int(time.Until(exp).Seconds()) + 1,
		})
	}
}

// WSAuth autentica el upgrade a WebSocket según mode. Los tickets ligan la conexión a un tenant;
// los tokens estáticos conceden la vista global. Sin ticketer ni tokens la autenticación queda
// desactivada (solo desarrollo).
func WSAuth(mode string, ticketer *WSTicketer, tokens ...string) func(http.HandlerFunc) http.HandlerFunc {
	static := WSToken(tokens...)
	staticEnabled := false
	for _, tk := range tokens {
		if tk != "" {
			staticEnabled = true
		}
	}
	return func(next http.HandlerFunc) http.HandlerFunc {
		staticNext := static(next)
		return func(w http.ResponseWriter, r *http.Request) {
			if ticket := r.URL.Query().Get("ticket"); ticket != "" && mode != WSModeToken && ticketer != nil {
				tenant, err := ticketer.Verify(ticket)
				if err != nil {
					observability.AuthRejected("/ws", "invalid_ticket")
					http.Error(w, "ticket inválido", http.StatusUnauthorized)
					return
				}
				next(w, r.WithContext(context.WithValue(r.Context(), wsTenantKey{}, tenant)))
				return
			}
			if mode == WSModeTicket {
				observability.AuthRejected("/ws", "missing_ticket")
				http.Error(w, "ticket ausente", http.StatusUnauthorized)
				return
			}
			if !staticEnabled && ticketer != nil && mode == WSModeAny {
				observability.AuthRejected("/ws", "missing_ticket")
				http.Error(w, "ticket ausente", http.StatusUnauthorized)
				return
			}
			staticNext(w, r)
		}
	}
}
