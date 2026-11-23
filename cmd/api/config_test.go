package main

import "testing"

func TestProductionConfiguration(t *testing.T) {
	values := map[string]string{
		"ENVIRONMENT":          "production",
		"API_KEY":              "admin-secret",
		"WS_TICKET_SECRET":     "0123456789abcdef0123456789abcdef",
		"WS_AUTH_MODE":         "ticket",
		"POSTGRES_HOST":        "db.example",
		"POSTGRES_PASSWORD":    "db-secret",
		"POSTGRES_SSLMODE":     "verify-full",
		"NATS_URL":             "tls://user:password@nats.example:4222",
		"REDIS_URL":            "rediss://:password@redis.example:6379",
		"CORS_ALLOWED_ORIGINS": "https://app.example",
		"WS_ALLOWED_ORIGINS":   "https://app.example",
		"TRUSTED_PROXY_CIDRS":  "10.0.0.0/8",
		"S3_ENDPOINT":          "",
		"OTEL_TRACING_ENABLED": "false",
	}
	for k, v := range values {
		t.Setenv(k, v)
	}
	for _, name := range []string{"API_KEY", "WS_TOKEN", "WS_TICKET_SECRET", "POSTGRES_PASSWORD", "NATS_URL", "REDIS_URL", "TENANT_API_KEYS", "S3_ACCESS_KEY", "S3_SECRET_KEY", "OTEL_EXPORTER_OTLP_HEADERS"} {
		t.Setenv(name+"_FILE", "")
	}
	if err := loadConfiguration(); err != nil {
		t.Fatalf("valid production config: %v", err)
	}
	for _, tc := range []struct{ key, value string }{
		{"API_KEY", ""},
		{"WS_AUTH_MODE", "any"},
		{"WS_TICKET_SECRET", "short"},
		{"POSTGRES_SSLMODE", "require"},
		{"NATS_URL", "nats://server:4222"},
		{"REDIS_URL", "rediss://server:6379"},
		{"WS_ALLOWED_ORIGINS", "https://app.example,*"},
		{"CORS_ALLOWED_ORIGINS", "http://app.example"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if err := loadConfiguration(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
