package main

// E-1 P8: runCertRotation tick behavior, driven through the
// certRotationInterval seam so the 12h production cadence shrinks to
// milliseconds in tests.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// admissionWebhookFixture returns an operator-labeled validating webhook
// configuration for CA-injection tests.
func admissionWebhookFixture() *admissionregistrationv1.ValidatingWebhookConfiguration {
	se := admissionregistrationv1.SideEffectClassNone
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "operator-vwc",
			Labels: map[string]string{util.LabelPartOf: util.LabelPartOfValue},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name:                    "validate.cloudberry.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
				SideEffects:             &se,
			},
		},
	}
}

// atomicCertManager is a race-safe certmanager.CertManager stub for the
// rotation-loop tests (counters are read while the loop is still running).
type atomicCertManager struct {
	needsRotation    atomic.Bool
	needsRotationErr atomic.Bool
	ensureErr        atomic.Bool

	needsCalls  atomic.Int64
	ensureCalls atomic.Int64
}

func (m *atomicCertManager) EnsureCertificates(_ context.Context) ([]byte, error) {
	m.ensureCalls.Add(1)
	if m.ensureErr.Load() {
		return nil, fmt.Errorf("rotation failed")
	}
	return []byte("ca"), nil
}

func (m *atomicCertManager) NeedsRotation(_ context.Context) (bool, error) {
	m.needsCalls.Add(1)
	if m.needsRotationErr.Load() {
		return false, fmt.Errorf("check failed")
	}
	return m.needsRotation.Load(), nil
}

// shrinkRotationInterval makes the rotation ticker fire fast.
func shrinkRotationInterval(t *testing.T) {
	t.Helper()
	prev := certRotationInterval
	certRotationInterval = 2 * time.Millisecond
	t.Cleanup(func() { certRotationInterval = prev })
}

// runRotation runs runCertRotation until the predicate holds (or times out),
// then cancels and joins the loop.
func runRotation(t *testing.T, cm *atomicCertManager, pred func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runCertRotation(ctx, cm, testLogger())
		close(done)
	}()

	require.Eventually(t, pred, 5*time.Second, time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after cancel")
	}
}

func TestRunCertRotation_TickRotatesWhenNeeded(t *testing.T) {
	shrinkRotationInterval(t)
	cm := &atomicCertManager{}
	cm.needsRotation.Store(true)

	runRotation(t, cm, func() bool { return cm.ensureCalls.Load() >= 1 })

	assert.GreaterOrEqual(t, cm.needsCalls.Load(), int64(1))
}

func TestRunCertRotation_TickRotationFailure_LoopContinues(t *testing.T) {
	shrinkRotationInterval(t)
	cm := &atomicCertManager{}
	cm.needsRotation.Store(true)
	cm.ensureErr.Store(true)

	// The loop must survive EnsureCertificates failures: wait for at least
	// two failed rotations.
	runRotation(t, cm, func() bool { return cm.ensureCalls.Load() >= 2 })
}

func TestRunCertRotation_TickCheckError_LoopContinues(t *testing.T) {
	shrinkRotationInterval(t)
	cm := &atomicCertManager{}
	cm.needsRotationErr.Store(true)

	runRotation(t, cm, func() bool { return cm.needsCalls.Load() >= 2 })

	assert.Zero(t, cm.ensureCalls.Load(),
		"a failed rotation check must not trigger a rotation")
}

func TestRunCertRotation_TickNoRotationNeeded(t *testing.T) {
	shrinkRotationInterval(t)
	cm := &atomicCertManager{} // needsRotation=false

	runRotation(t, cm, func() bool { return cm.needsCalls.Load() >= 2 })

	assert.Zero(t, cm.ensureCalls.Load())
}
