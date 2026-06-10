package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// TestInstallSpanRecorder verifies the shared test-support harness records
// spans through the global provider and the restore function reinstates the
// previous provider.
func TestInstallSpanRecorder(t *testing.T) {
	prev := otel.GetTracerProvider()

	sr, restore := telemetry.InstallSpanRecorder()
	_, span := telemetry.StartSpan(context.Background(), "test-tracer", "test.span")
	span.End()

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "test.span", spans[0].Name())

	restore()
	assert.Equal(t, prev, otel.GetTracerProvider(), "restore must reinstate the previous provider")
}
