package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

const (
	HeaderAPIKey   = "X-API-Key"
	HeaderTenantID = "X-Tenant-ID"
)

var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidTenantID comprueba el formato de un tenant: 1-64 caracteres alfanuméricos, '.', '_' o '-'.
// Es deliberadamente estricto: el valor acaba en cabeceras de NATS y en la clave de deduplicación.
func ValidTenantID(s string) bool { return tenantIDPattern.MatchString(s) }

type tenantCtxKey struct{}

// TenantFromContext devuelve el tenant que fijó TenantAuth.Ingest, o DefaultTenant si no hay.
func TenantFromContext(ctx context.Context) string {
	if t, ok := ctx.Value(tenantCtxKey{}).(string); ok && t != "" {
		return t
	}
	return domain.DefaultTenant
}

type credential struct {
	hash   [sha256.Size]byte
	tenant string // vacío = clave de administración (cualquier tenant)
}

// TenantAuth valida X-API-Key y resuelve el tenant de las peticiones de ingesta.
//
//   - Clave de tenant ("clave:tenant"): fija el tenant. X-Tenant-ID es opcional, pero si viene
//     debe coincidir; si no, 403.
//   - Clave de administración (API_KEY): puede ingestar para cualquier tenant indicado en
//     X-Tenant-ID (por defecto "default").
//   - Sin ninguna clave configurada la autenticación queda desactivada y el tenant sale de
//     X-Tenant-ID (informativo, no verificable).
type TenantAuth struct {
	creds []credential
}

// NewTenantAuth crea el validador. adminKey puede ser vacío; tenantKeys mapea clave -> tenant.
func NewTenantAuth(adminKey string, tenantKeys map[string]string) (*TenantAuth, error) {
	a := &TenantAuth{}
	if adminKey != "" {
		a.creds = append(a.creds, credential{hash: sha256.Sum256([]byte(adminKey))})
	}
	for key, tenant := range tenantKeys {
		if key == "" {
			return nil, fmt.Errorf("tenant keys: clave vacía para el tenant %q", tenant)
		}
		if !ValidTenantID(tenant) {
			return nil, fmt.Errorf("tenant keys: tenant inválido %q", tenant)
		}
		a.creds = append(a.creds, credential{hash: sha256.Sum256([]byte(key)), tenant: tenant})
	}
	return a, nil
}

// ParseTenantKeys interpreta "clave1:tenantA,clave2:tenantB". El tenant va tras el último ':',
// así que las claves pueden contener ':'.
func ParseTenantKeys(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		i := strings.LastIndex(pair, ":")
		if i <= 0 || i == len(pair)-1 {
			return nil, fmt.Errorf("entrada inválida (se esperaba clave:tenant)")
		}
		key, tenant := pair[:i], pair[i+1:]
		if !ValidTenantID(tenant) {
			return nil, fmt.Errorf("tenant inválido %q", tenant)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("clave repetida para el tenant %q", tenant)
		}
		out[key] = tenant
	}
	return out, nil
}

// Enabled indica si hay alguna clave configurada.
func (a *TenantAuth) Enabled() bool { return len(a.creds) > 0 }

// lookup compara contra todas las credenciales sin cortar en la primera coincidencia.
func (a *TenantAuth) lookup(key string) (credential, bool) {
	got := sha256.Sum256([]byte(key))
	var found credential
	ok := false
	for _, c := range a.creds {
		if subtle.ConstantTimeCompare(got[:], c.hash[:]) == 1 {
			found, ok = c, true
		}
	}
	return found, ok
}

// Ingest protege la ingesta (POST): valida la clave, resuelve el tenant y lo deja en el contexto.
// El resto de métodos pasan al handler, que responde 405.
func (a *TenantAuth) Ingest(next http.HandlerFunc) http.HandlerFunc {
	const endpoint = "/api/events"
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next(w, r)
			return
		}

		headerTenant := strings.TrimSpace(r.Header.Get(HeaderTenantID))
		tenant := headerTenant

		if a.Enabled() {
			key := r.Header.Get(HeaderAPIKey)
			if key == "" {
				observability.AuthRejected(endpoint, "missing_key")
				http.Error(w, "API key ausente", http.StatusUnauthorized)
				return
			}
			cred, ok := a.lookup(key)
			if !ok {
				observability.AuthRejected(endpoint, "invalid_key")
				http.Error(w, "API key inválida", http.StatusUnauthorized)
				return
			}
			if cred.tenant != "" {
				if headerTenant != "" && headerTenant != cred.tenant {
					observability.AuthRejected(endpoint, "tenant_mismatch")
					http.Error(w, "la API key no pertenece a ese tenant", http.StatusForbidden)
					return
				}
				tenant = cred.tenant
			}
		}

		if tenant == "" {
			tenant = domain.DefaultTenant
		} else if !ValidTenantID(tenant) {
			observability.AuthRejected(endpoint, "invalid_tenant")
			http.Error(w, "X-Tenant-ID inválido (1-64 caracteres: letras, dígitos, '.', '_' o '-')", http.StatusBadRequest)
			return
		}

		next(w, r.WithContext(context.WithValue(r.Context(), tenantCtxKey{}, tenant)))
	}
}

// Read protege los endpoints de lectura y fija el tenant autorizado en el contexto.
// Las claves de tenant solo pueden leer su propio tenant; API_KEY puede elegir
// X-Tenant-ID y, sin cabecera, usa el tenant default.
func (a *TenantAuth) Read(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		headerTenant := strings.TrimSpace(r.Header.Get(HeaderTenantID))
		tenant := headerTenant

		if a.Enabled() {
			key := r.Header.Get(HeaderAPIKey)
			if key == "" {
				observability.AuthRejected(r.URL.Path, "missing_key")
				http.Error(w, "API key ausente", http.StatusUnauthorized)
				return
			}
			cred, ok := a.lookup(key)
			if !ok {
				observability.AuthRejected(r.URL.Path, "invalid_key")
				http.Error(w, "API key inválida", http.StatusUnauthorized)
				return
			}
			if cred.tenant != "" {
				if headerTenant != "" && headerTenant != cred.tenant {
					observability.AuthRejected(r.URL.Path, "tenant_mismatch")
					http.Error(w, "la API key no pertenece a ese tenant", http.StatusForbidden)
					return
				}
				tenant = cred.tenant
			}
		}

		if tenant == "" {
			tenant = domain.DefaultTenant
		} else if !ValidTenantID(tenant) {
			http.Error(w, "X-Tenant-ID inválido", http.StatusBadRequest)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), tenantCtxKey{}, tenant)))
	}
}
