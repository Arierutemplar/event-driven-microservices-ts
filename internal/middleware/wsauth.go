package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

// WSToken exige un token válido antes de aceptar el upgrade a WebSocket. El token se lee de,
// por orden: `Authorization: Bearer <t>`, `X-API-Key` (clientes no navegador) o `?token=<t>`
// (los navegadores no pueden fijar cabeceras en `new WebSocket`). Sin tokens configurados la
// verificación se desactiva. La comprobación ocurre antes del upgrade: un rechazo es un 401 HTTP.
//
// Nota: el token en la URL puede quedar en logs de proxies; no lo registres y rótalo si se filtra.
func WSToken(tokens ...string) func(http.HandlerFunc) http.HandlerFunc {
	var hashes [][sha256.Size]byte
	for _, t := range tokens {
		if t != "" {
			hashes = append(hashes, sha256.Sum256([]byte(t)))
		}
	}
	if len(hashes) == 0 {
		return func(next http.HandlerFunc) http.HandlerFunc { return next }
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			token := wsTokenFrom(r)
			if token == "" {
				observability.AuthRejected("/ws", "missing_token")
				http.Error(w, "token ausente", http.StatusUnauthorized)
				return
			}
			got := sha256.Sum256([]byte(token))
			valid := 0
			for _, h := range hashes {
				valid |= subtle.ConstantTimeCompare(got[:], h[:])
			}
			if valid != 1 {
				observability.AuthRejected("/ws", "invalid_token")
				http.Error(w, "token inválido", http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
}

func wsTokenFrom(r *http.Request) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	if k := r.Header.Get(HeaderAPIKey); k != "" {
		return k
	}
	return r.URL.Query().Get("token")
}
