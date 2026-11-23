package middleware

import (
	"net/http"
	"os"
	"strings"
)

// CORS conserva el modo permisivo solo por compatibilidad explícita.
func CORS(next http.Handler) http.Handler {
	return CORSAllowed(os.Getenv("CORS_ALLOWED_ORIGINS"), next)
}

// CORSAllowed permite orígenes separados por comas. "*" se mantiene para desarrollo.
func CORSAllowed(allowed string, next http.Handler) http.Handler {
	origins := make(map[string]struct{})
	for _, origin := range strings.Split(allowed, ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			origins[origin] = struct{}{}
		}
	}
	allowAll := len(origins) == 0
	if _, ok := origins["*"]; ok {
		allowAll = true
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestOrigin := r.Header.Get("Origin")
		allowedOrigin := ""
		if allowAll {
			allowedOrigin = "*"
		} else if _, ok := origins[requestOrigin]; ok {
			allowedOrigin = requestOrigin
		}
		if allowedOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-Tenant-ID, Authorization")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		if r.Method == http.MethodOptions {
			if allowedOrigin == "" {
				http.Error(w, "origen no permitido", http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if requestOrigin != "" && allowedOrigin == "" {
			http.Error(w, "origen no permitido", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
