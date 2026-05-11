// Package telemetry provides OpenTelemetry tracing setup for the cloudberry operator.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	protocolGRPC = "grpc"
	protocolHTTP = "http"
)

// Config holds telemetry configuration.
type Config struct {
	// Enabled controls whether telemetry is active.
	Enabled bool
	// OTLPEndpoint is the OTLP collector endpoint.
	OTLPEndpoint string
	// OTLPProtocol is the OTLP exporter protocol (grpc, http).
	OTLPProtocol string
	// SamplingRate is the trace sampling rate (0.0 to 1.0).
	SamplingRate float64
	// ServiceName is the service name for traces.
	ServiceName string
	// ServiceVersion is the service version for traces.
	ServiceVersion string
	// Namespace is the Kubernetes namespace.
	Namespace string
}

// ShutdownFunc is a function that shuts down the trace provider.
type ShutdownFunc func(ctx context.Context) error

// InitTracer initializes the OpenTelemetry trace provider.
// Returns a shutdown function that must be called on application exit.
// If telemetry is disabled, a no-op tracer is configured.
func InitTracer(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(_ context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String(cfg.ServiceVersion),
			attribute.String("k8s.namespace", cfg.Namespace),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating resource: %w", err)
	}

	exporter, err := createExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating exporter: %w", err)
	}

	sampler := createSampler(cfg.SamplingRate)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// createExporter creates the appropriate OTLP exporter based on protocol.
func createExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	switch cfg.OTLPProtocol {
	case protocolHTTP:
		return otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
			otlptracehttp.WithInsecure(),
		)
	default:
		return otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
	}
}

// createSampler creates a trace sampler based on the sampling rate.
func createSampler(rate float64) sdktrace.Sampler {
	if rate <= 0 {
		return sdktrace.NeverSample()
	}
	if rate >= 1.0 {
		return sdktrace.AlwaysSample()
	}
	return sdktrace.TraceIDRatioBased(rate)
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// StartSpan starts a new span with the given name and returns the context and span.
func StartSpan(
	ctx context.Context,
	tracerName, spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	return Tracer(tracerName).Start(ctx, spanName, opts...)
}

// SetSpanError marks a span as having an error.
func SetSpanError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// AddSpanEvent adds an event to the current span.
func AddSpanEvent(span trace.Span, name string, attrs ...attribute.KeyValue) {
	span.AddEvent(name, trace.WithAttributes(attrs...))
}
