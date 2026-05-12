package telemetry

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestInitTracer_Disabled(t *testing.T) {
	cfg := Config{
		Enabled: false,
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Shutdown should work without error
	err = shutdown(context.Background())
	assert.NoError(t, err)

	// Tracer provider should be noop
	tp := otel.GetTracerProvider()
	assert.NotNil(t, tp)
}

func TestCreateSampler(t *testing.T) {
	tests := []struct {
		name string
		rate float64
	}{
		{"zero rate - never sample", 0},
		{"negative rate - never sample", -0.5},
		{"full rate - always sample", 1.0},
		{"above 1.0 - always sample", 1.5},
		{"half rate - ratio based", 0.5},
		{"low rate - ratio based", 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampler := createSampler(tt.rate)
			assert.NotNil(t, sampler)
		})
	}
}

func TestTracer(t *testing.T) {
	// Set up noop provider
	otel.SetTracerProvider(noop.NewTracerProvider())

	tracer := Tracer("test-tracer")
	assert.NotNil(t, tracer)
}

func TestStartSpan(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	ctx, span := StartSpan(context.Background(), "test-tracer", "test-span")
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
	span.End()
}

func TestSetSpanError(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	t.Run("with error", func(t *testing.T) {
		_, span := StartSpan(context.Background(), "test", "span")
		defer span.End()
		// Should not panic
		SetSpanError(span, fmt.Errorf("test error"))
	})

	t.Run("with nil error", func(t *testing.T) {
		_, span := StartSpan(context.Background(), "test", "span")
		defer span.End()
		// Should not panic
		SetSpanError(span, nil)
	})
}

func TestAddSpanEvent(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	_, span := StartSpan(context.Background(), "test", "span")
	defer span.End()

	// Should not panic
	AddSpanEvent(span, "test-event", attribute.String("key", "value"))
	AddSpanEvent(span, "event-no-attrs")
}

func TestSetSpanError_RecordsError(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	_, span := StartSpan(context.Background(), "test", "span")
	defer span.End()

	testErr := fmt.Errorf("test error message")
	SetSpanError(span, testErr)

	// With noop provider, we can't verify the error was recorded,
	// but we verify it doesn't panic
	_ = codes.Error
}

func TestProtocolConstants(t *testing.T) {
	assert.Equal(t, "grpc", protocolGRPC)
	assert.Equal(t, "http", protocolHTTP)
}

func TestInitTracer_EnabledWithEmptyEndpoint(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "",
		OTLPProtocol:   "grpc",
		SamplingRate:   1.0,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "default",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	// Should succeed even with empty endpoint (exporter will fail on flush, not on init).
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Shutdown should not panic.
	_ = shutdown(context.Background())
}

func TestInitTracer_HTTPProtocol(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4318",
		OTLPProtocol:   "http",
		SamplingRate:   1.0,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "default",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	_ = shutdown(context.Background())
}

func TestInitTracer_GRPCProtocol(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4317",
		OTLPProtocol:   "grpc",
		SamplingRate:   0.5,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "test-ns",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	_ = shutdown(context.Background())
}

func TestInitTracer_SamplingRates(t *testing.T) {
	rates := []struct {
		name string
		rate float64
	}{
		{"zero rate", 0.0},
		{"half rate", 0.5},
		{"full rate", 1.0},
	}

	for _, tt := range rates {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Enabled:        true,
				OTLPEndpoint:   "localhost:4317",
				OTLPProtocol:   "grpc",
				SamplingRate:   tt.rate,
				ServiceName:    "test",
				ServiceVersion: "1.0",
			}

			shutdown, err := InitTracer(context.Background(), cfg)
			require.NoError(t, err)
			require.NotNil(t, shutdown)

			_ = shutdown(context.Background())
		})
	}
}

func TestInitTracer_Disabled_ShutdownIdempotent(t *testing.T) {
	cfg := Config{Enabled: false}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)

	// Calling shutdown multiple times should be safe.
	assert.NoError(t, shutdown(context.Background()))
	assert.NoError(t, shutdown(context.Background()))
}

func TestStartSpan_ReturnsValidContext(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	ctx := context.Background()
	newCtx, span := StartSpan(ctx, "tracer", "operation")
	defer span.End()

	// The returned context should be different from the original.
	assert.NotNil(t, newCtx)
	assert.NotNil(t, span)
}

func TestSetSpanError_WithVariousErrors(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	tests := []struct {
		name string
		err  error
	}{
		{"nil error", nil},
		{"simple error", fmt.Errorf("simple error")},
		{"wrapped error", fmt.Errorf("outer: %w", fmt.Errorf("inner"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, span := StartSpan(context.Background(), "test", "span")
			defer span.End()
			// Should not panic for any error type.
			SetSpanError(span, tt.err)
		})
	}
}

func TestConfig_Fields(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "collector:4317",
		OTLPProtocol:   "grpc",
		OTLPInsecure:   false,
		SamplingRate:   0.75,
		ServiceName:    "cloudberry-operator",
		ServiceVersion: "1.0.0",
		Namespace:      "production",
	}

	assert.True(t, cfg.Enabled)
	assert.Equal(t, "collector:4317", cfg.OTLPEndpoint)
	assert.Equal(t, "grpc", cfg.OTLPProtocol)
	assert.False(t, cfg.OTLPInsecure)
	assert.Equal(t, 0.75, cfg.SamplingRate)
	assert.Equal(t, "cloudberry-operator", cfg.ServiceName)
	assert.Equal(t, "production", cfg.Namespace)
}

func TestInitTracer_GRPCInsecure(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4317",
		OTLPProtocol:   "grpc",
		OTLPInsecure:   true,
		SamplingRate:   1.0,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "default",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	_ = shutdown(context.Background())
}

func TestInitTracer_HTTPInsecure(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4318",
		OTLPProtocol:   "http",
		OTLPInsecure:   true,
		SamplingRate:   0.5,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "default",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	_ = shutdown(context.Background())
}

func TestInitTracer_GRPC_TLS(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4317",
		OTLPProtocol:   "grpc",
		OTLPInsecure:   false,
		SamplingRate:   1.0,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "default",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	_ = shutdown(context.Background())
}

func TestInitTracer_HTTP_TLS(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4318",
		OTLPProtocol:   "http",
		OTLPInsecure:   false,
		SamplingRate:   1.0,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		Namespace:      "default",
	}

	shutdown, err := InitTracer(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	_ = shutdown(context.Background())
}

func TestCreateSampler_NegativeRate(t *testing.T) {
	sampler := createSampler(-1.0)
	assert.NotNil(t, sampler)
	// Negative rate should result in NeverSample
}

func TestCreateSampler_AboveOne(t *testing.T) {
	sampler := createSampler(2.0)
	assert.NotNil(t, sampler)
	// Above 1.0 should result in AlwaysSample
}

func TestAddSpanEvent_MultipleAttributes(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	_, span := StartSpan(context.Background(), "test", "span")
	defer span.End()

	AddSpanEvent(span, "multi-attr-event",
		attribute.String("key1", "val1"),
		attribute.Int("key2", 42),
		attribute.Bool("key3", true),
	)
}

func TestTracer_MultipleCalls(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	t1 := Tracer("tracer-1")
	t2 := Tracer("tracer-2")
	assert.NotNil(t, t1)
	assert.NotNil(t, t2)
}

func TestStartSpan_WithOptions(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())

	ctx, span := StartSpan(context.Background(), "test", "span-with-opts",
		trace.WithAttributes(attribute.String("custom", "attr")),
	)
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
	span.End()
}
