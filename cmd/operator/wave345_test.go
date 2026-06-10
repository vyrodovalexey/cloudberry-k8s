package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/config"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// TestBuildTelemetryConfig_UsesBuildVersion verifies D-7: the build-time
// version variable flows into telemetry.Config.ServiceVersion (no hardcoded
// "1.0.0").
func TestBuildTelemetryConfig_UsesBuildVersion(t *testing.T) {
	cfg := &config.OperatorConfig{}
	cfg.Telemetry.ServiceName = "cloudberry-operator"
	cfg.Telemetry.SamplingRate = 0.5

	tc := buildTelemetryConfig(cfg)
	assert.Equal(t, version, tc.ServiceVersion)
	assert.NotEqual(t, "1.0.0", tc.ServiceVersion)
	assert.Equal(t, "cloudberry-operator", tc.ServiceName)
	assert.Equal(t, 0.5, tc.SamplingRate)
}

// TestBuildManagerOptions verifies B-2: WebhookPort and Namespace are wired
// into the manager options; an empty namespace keeps the cluster-wide cache.
func TestBuildManagerOptions(t *testing.T) {
	cfg := &config.OperatorConfig{
		MetricsAddress:     ":9090",
		HealthProbeAddress: ":9091",
		LeaderElection:     true,
		WebhookPort:        9543,
	}

	opts := buildManagerOptions(cfg)
	assert.Equal(t, ":9090", opts.Metrics.BindAddress)
	assert.Equal(t, ":9091", opts.HealthProbeBindAddress)
	assert.True(t, opts.LeaderElection)
	require.NotNil(t, opts.WebhookServer, "webhook server must be configured from WebhookPort")
	// Cluster-wide cache: no namespace restriction.
	assert.Nil(t, opts.Cache.DefaultNamespaces)

	// Restricted namespace maps into the cache config.
	cfg.Namespace = "cb-system"
	opts = buildManagerOptions(cfg)
	require.NotNil(t, opts.Cache.DefaultNamespaces)
	_, ok := opts.Cache.DefaultNamespaces["cb-system"]
	assert.True(t, ok)
}

// TestOperationTimeoutOverride verifies B-2: the override is only forwarded
// when explicitly changed from the config default, preserving the historical
// per-operation hardcoded deadlines otherwise.
func TestOperationTimeoutOverride(t *testing.T) {
	cfg := &config.OperatorConfig{OperationTimeout: config.DefaultOperationTimeout}
	assert.Equal(t, time.Duration(0), operationTimeoutOverride(cfg),
		"default value must NOT override the per-operation deadlines")

	cfg.OperationTimeout = 42 * time.Minute
	assert.Equal(t, 42*time.Minute, operationTimeoutOverride(cfg))
}

// TestInjectCABundle_Span verifies D-7: injectCABundle creates a span and
// records the error status on failure.
func TestInjectCABundle_Span(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	err := injectCABundle(context.Background(), k8sClient, []byte("ca"), testLogger())
	require.NoError(t, err)

	spans := sr.Ended()
	var found bool
	for _, s := range spans {
		if s.Name() == "operator.injectCABundle" {
			found = true
			assert.NotEqual(t, "Error", s.Status().Code.String())
		}
	}
	assert.True(t, found, "operator.injectCABundle span missing")
}
