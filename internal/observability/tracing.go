package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// InitTracing enables OTLP when explicitly configured; W3C propagation is always enabled.
func InitTracing(ctx context.Context) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	enabled := os.Getenv("OTEL_TRACING_ENABLED")
	if enabled == "" || enabled == "false" {
		return func(context.Context) error { return nil }, nil
	}
	if enabled != "true" {
		return nil, fmt.Errorf("OTEL_TRACING_ENABLED must be true or false")
	}
	protocol := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	if protocol == "" {
		protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	var exporter sdktrace.SpanExporter
	var err error
	switch strings.ToLower(protocol) {
	case "", "grpc":
		exporter, err = otlptracegrpc.New(ctx)
	case "http/protobuf":
		exporter, err = otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP traces protocol %q", protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("initialize OTLP exporter: %w", err)
	}
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = "event-driven-microservices-ts"
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes("", attribute.String("service.name", name)))
	if err != nil {
		return nil, fmt.Errorf("initialize telemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func TraceHTTP(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "http.request",
		otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" && r.URL.Path != "/ready" }))
}

// ContextHandler adds trace identifiers to records emitted with a context.
type ContextHandler struct{ slog.Handler }

func (h ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		record.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, record)
}

func (h ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return ContextHandler{h.Handler.WithAttrs(attrs)}
}

func (h ContextHandler) WithGroup(name string) slog.Handler {
	return ContextHandler{h.Handler.WithGroup(name)}
}
