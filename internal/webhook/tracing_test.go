package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// validWebhookCluster returns a cluster that passes admission validation.
func validWebhookCluster() *cbv1alpha1.CloudberryCluster {
	c := newValidCluster()
	return c
}

// TestWebhookValidateSpans verifies D-5: validating admission produces a
// webhook.validate span with bounded kind/operation/allowed attributes; a
// denial sets allowed=false and error status.
func TestWebhookValidateSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	v := NewCloudberryClusterValidator(nil)

	// Allowed create.
	_, err := v.ValidateCreate(context.Background(), validWebhookCluster())
	require.NoError(t, err)

	// Denied create (invalid segment count).
	bad := validWebhookCluster()
	bad.Spec.Segments.Count = 0
	_, err = v.ValidateCreate(context.Background(), bad)
	require.Error(t, err)

	spans := sr.Ended()
	var allowed, denied bool
	for _, s := range spans {
		if s.Name() != "webhook.validate" {
			continue
		}
		attrs := map[string]string{}
		var allowedAttr *bool
		for _, a := range s.Attributes() {
			if string(a.Key) == "webhook.allowed" {
				v := a.Value.AsBool()
				allowedAttr = &v
				continue
			}
			attrs[string(a.Key)] = a.Value.Emit()
		}
		require.NotNil(t, allowedAttr)
		assert.Equal(t, "CloudberryCluster", attrs["webhook.kind"])
		assert.Equal(t, "create", attrs["webhook.operation"])
		if *allowedAttr {
			allowed = true
		} else {
			denied = true
			assert.Equal(t, "Error", s.Status().Code.String())
		}
	}
	assert.True(t, allowed, "allowed validate span missing")
	assert.True(t, denied, "denied validate span missing")

	// E-5 PII gate: admission spans must not carry resource secret material.
	telemetry.AssertNoPII(t, spans)
}

// TestWebhookMutateSpan verifies the mutating webhook span.
func TestWebhookMutateSpan(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	d := NewCloudberryClusterDefaulter()
	require.NoError(t, d.Default(context.Background(), validWebhookCluster()))

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "webhook.mutate" {
			found = true
		}
	}
	assert.True(t, found, "webhook.mutate span missing")
}

// TestWebhookUpdateDeleteSpans covers the update and delete operations.
func TestWebhookUpdateDeleteSpans(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	v := NewCloudberryClusterValidator(nil)
	old := validWebhookCluster()
	updated := validWebhookCluster()
	_, err := v.ValidateUpdate(context.Background(), old, updated)
	require.NoError(t, err)
	_, err = v.ValidateDelete(context.Background(), old)
	require.NoError(t, err)

	ops := map[string]bool{}
	for _, s := range sr.Ended() {
		if s.Name() != "webhook.validate" {
			continue
		}
		for _, a := range s.Attributes() {
			if string(a.Key) == "webhook.operation" {
				ops[a.Value.Emit()] = true
			}
		}
	}
	assert.True(t, ops["update"])
	assert.True(t, ops["delete"])
}
