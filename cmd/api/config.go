package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/secrets"
)

func loadConfiguration() error {
	names := []string{"API_KEY", "TENANT_API_KEYS", "WS_TOKEN", "WS_TICKET_SECRET", "POSTGRES_PASSWORD",
		"NATS_URL", "REDIS_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "OTEL_EXPORTER_OTLP_HEADERS"}
	if err := secrets.Load(secrets.Environment{}, names); err != nil {
		return err
	}
	if !strings.EqualFold(os.Getenv("ENVIRONMENT"), "production") {
		return nil
	}
	for _, name := range []string{"API_KEY", "WS_TICKET_SECRET", "POSTGRES_HOST", "POSTGRES_PASSWORD", "NATS_URL", "REDIS_URL"} {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			return fmt.Errorf("producción requiere %s", name)
		}
	}
	if len(os.Getenv("WS_TICKET_SECRET")) < 32 {
		return fmt.Errorf("producción requiere WS_TICKET_SECRET de al menos 32 caracteres")
	}
	if !strings.EqualFold(os.Getenv("WS_AUTH_MODE"), "ticket") {
		return fmt.Errorf("producción requiere WS_AUTH_MODE=ticket (sin token compartido en el navegador)")
	}
	if os.Getenv("POSTGRES_SSLMODE") != "verify-full" {
		return fmt.Errorf("producción requiere POSTGRES_SSLMODE=verify-full")
	}
	for _, item := range []struct{ name, scheme string }{{"NATS_URL", "tls"}, {"REDIS_URL", "rediss"}} {
		u, err := url.Parse(os.Getenv(item.name))
		if err != nil || u.Host == "" || u.Scheme != item.scheme {
			return fmt.Errorf("producción requiere %s con esquema %s y host válido", item.name, item.scheme)
		}
		if item.name == "REDIS_URL" {
			if u.User == nil {
				return fmt.Errorf("producción requiere credenciales en REDIS_URL")
			}
			if password, ok := u.User.Password(); !ok || password == "" {
				return fmt.Errorf("producción requiere contraseña en REDIS_URL")
			}
		}
		if item.name == "NATS_URL" && u.User == nil && os.Getenv("NATS_CREDS_FILE") == "" {
			return fmt.Errorf("producción requiere credenciales de NATS")
		}
	}
	for _, name := range []string{"CORS_ALLOWED_ORIGINS", "WS_ALLOWED_ORIGINS"} {
		origins := strings.Split(os.Getenv(name), ",")
		for _, origin := range origins {
			u, err := url.Parse(strings.TrimSpace(origin))
			if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
				return fmt.Errorf("producción requiere orígenes HTTPS explícitos en %s", name)
			}
		}
	}
	if os.Getenv("S3_ENDPOINT") != "" {
		if os.Getenv("S3_USE_SSL") != "true" || os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
			return fmt.Errorf("producción requiere TLS y credenciales S3")
		}
	}
	if os.Getenv("TLS_CERT_FILE") == "" && os.Getenv("TRUSTED_PROXY_CIDRS") == "" {
		return fmt.Errorf("producción requiere TLS_CERT_FILE o TRUSTED_PROXY_CIDRS")
	}
	if os.Getenv("OTEL_TRACING_ENABLED") == "true" {
		endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
		if endpoint == "" {
			endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("producción requiere un endpoint OTLP HTTPS explícito")
		}
		if os.Getenv("OTEL_EXPORTER_OTLP_INSECURE") == "true" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE") == "true" {
			return fmt.Errorf("producción no permite OTLP inseguro")
		}
	}
	return nil
}
