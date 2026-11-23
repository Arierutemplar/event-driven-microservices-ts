package queue

import (
	"context"
	"testing"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextAcrossJetStream(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	oldProvider, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldProp)
	})
	repo := &fakeRepo{}
	c, _, _ := startManual(t, repo, nil, nil)
	ctx, parent := provider.Tracer("test").Start(context.Background(), "request")
	if err := c.Publish(ctx, &domain.Event{ID: "traced", EventType: "click"}); err != nil {
		t.Fatal(err)
	}
	parent.End()
	eventually(t, "consumer span with producer link", func() bool {
		var producer trace.SpanContext
		for _, span := range exporter.GetSpans() {
			if span.Name == "nats publish" {
				producer = span.SpanContext
			}
		}
		if !producer.IsValid() {
			return false
		}
		for _, span := range exporter.GetSpans() {
			if span.Name == "nats process batch" && len(span.Links) == 1 && span.Links[0].SpanContext.TraceID() == producer.TraceID() && span.Links[0].SpanContext.SpanID() == producer.SpanID() {
				return true
			}
		}
		return false
	})
}
