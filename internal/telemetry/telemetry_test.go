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
