package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// APIKey exige la cabecera X-API-Key. Si `key` está vacío, la verificación
// se desactiva y el handler pasa tal cual (modo opcional).
func APIKey(key string) func(http.Handler) http.Handler {
	if key == "" {
		return func(next http.Handler) http.Handler { return next }
	}
	want := sha256.Sum256([]byte(key))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
					key = strings.TrimSpace(auth[len("Bearer "):])
				}
			}
			got := sha256.Sum256([]byte(key))
			// Comparar hashes de longitud fija evita filtrar datos por tiempo.
			if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
				http.Error(w, "API key inválida o ausente", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// AdminOnly protege operaciones destructivas: solo acepta la clave de administración y, a
// diferencia de APIKey, deniega todo (503) si no hay clave configurada en lugar de dejar pasar.
func AdminOnly(key string) func(http.Handler) http.Handler {
	want := sha256.Sum256([]byte(key))
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if key == "" {
				http.Error(w, "endpoints administrativos deshabilitados: API_KEY no configurada", http.StatusServiceUnavailable)
				return
			}
			got := sha256.Sum256([]byte(r.Header.Get("X-API-Key")))
			if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
				http.Error(w, "se requiere la API key de administración", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// APIKeyFunc adapta APIKey a http.HandlerFunc.
func APIKeyFunc(key string) func(http.HandlerFunc) http.HandlerFunc {
	mw := APIKey(key)
	return func(next http.HandlerFunc) http.HandlerFunc {
		return mw(next).ServeHTTP
	}
}
