package main

// Tests for the H-1 pending-CA-bundle retry mechanic in rotateOnce, the
// rotation-outcome metrics, the leader gating of runCertRotation and the
// jitter bounds (handoff gaps UT-8..UT-14). rotateOnce is driven DIRECTLY
// (no goroutine, no ticks) so every branch is deterministic; the injection
// failure is produced with a pre-canceled context, which makes the retry
// helper fail fast without burning its 5-retry/30s backoff budget.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// captureRecorder is a race-safe metrics.Recorder capturing the DEV-4
// rotation outcome metrics. It embeds metrics.NoopRecorder so only the
// relevant methods are overridden.
type captureRecorder struct {
	*metrics.NoopRecorder

	injectionSuccess atomic.Int64
	injectionError   atomic.Int64

	checkErrors   atomic.Int64
	lastComponent atomic.Value // string
}

func newCaptureRecorder() *captureRecorder {
	return &captureRecorder{NoopRecorder: &metrics.NoopRecorder{}}
}

func (c *captureRecorder) RecordCABundleInjection(result string) {
	if result == metrics.ResultSuccess {
		c.injectionSuccess.Add(1)
		return
	}
	c.injectionError.Add(1)
}

func (c *captureRecorder) IncCertRotationCheckError(component string) {
	c.checkErrors.Add(1)
	c.lastComponent.Store(component)
}

// getVWC fetches the operator webhook fixture back from the fake client.
func getVWC(t *testing.T, k8sClient client.Client) *admissionregistrationv1.ValidatingWebhookConfiguration {
	t.Helper()
	vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	require.NoError(t, k8sClient.Get(context.Background(),
		client.ObjectKey{Name: "operator-vwc"}, vwc))
	return vwc
}

// spanAttrBool returns the value of a bool attribute on the span (and whether
// it was present).
func spanAttrBool(span sdktrace.ReadOnlySpan, key string) (value, found bool) {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.AsBool(), true
		}
	}
	return false, false
}

// lastSpanByName returns the LAST ended span with the given name, or nil.
func lastSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].Name() == name {
			return spans[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// UT-8 / UT-9: rotateOnce pending-bundle retry across ticks (H-1 core)
// ---------------------------------------------------------------------------

// TestRotateOnce_InjectionFails_ThenPendingBundleInjectedOnNextTick drives
// the full H-1 failure/recovery sequence: tick 1 rotates successfully but the
// injection fails (canceled ctx) and the FRESH bundle is returned as pending
// with needs_rotation/rotated span attributes set; tick 2 (healed client,
// live ctx) injects the pending bundle, returns nil, and the webhook
// configuration ends up carrying the rotated bundle.
func TestRotateOnce_InjectionFails_ThenPendingBundleInjectedOnNextTick(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	cm := &atomicCertManager{}
	cm.needsRotation.Store(true)
	k8sClient := newFakeClient(admissionWebhookFixture())
	rec := newCaptureRecorder()

	// --- Tick 1: rotation succeeds, injection fails (canceled context makes
	// injectCABundleWithRetry return an error before any Update is issued).
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	pending := rotateOnce(canceledCtx, cm, k8sClient, nil, rec, testLogger())

	require.Equal(t, []byte("ca"), pending,
		"the freshly rotated bundle must be returned as pending on injection failure")
	assert.Equal(t, int64(1), cm.ensureCalls.Load(), "tick 1 must have rotated")
	assert.Equal(t, int64(1), rec.injectionError.Load(),
		"the failed injection must be recorded as an error outcome")
	assert.Nil(t, getVWC(t, k8sClient).Webhooks[0].ClientConfig.CABundle,
		"the webhook config must not carry a bundle after the failed injection")

	// UT-9: the tick span carries needs_rotation=true and rotated=true.
	span := lastSpanByName(sr.Ended(), "operator.certRotationCheck")
	require.NotNil(t, span, "every tick must produce an operator.certRotationCheck span")
	needsRotation, ok := spanAttrBool(span, "needs_rotation")
	require.True(t, ok, "needs_rotation attribute must be present")
	assert.True(t, needsRotation)
	rotated, ok := spanAttrBool(span, "rotated")
	require.True(t, ok, "rotated attribute must be present")
	assert.True(t, rotated, "the rotation itself succeeded, only the injection failed")

	// --- Tick 2: the client is healthy again; the pending bundle must be
	// injected and cleared. No further rotation is needed.
	cm.needsRotation.Store(false)

	got := rotateOnce(context.Background(), cm, k8sClient, pending, rec, testLogger())

	assert.Nil(t, got, "an injected pending bundle must be cleared")
	assert.Equal(t, int64(1), cm.ensureCalls.Load(), "tick 2 must not rotate again")
	assert.Equal(t, int64(1), rec.injectionSuccess.Load(),
		"the healed injection must be recorded as a success outcome")
	assert.Equal(t, []byte("ca"), getVWC(t, k8sClient).Webhooks[0].ClientConfig.CABundle,
		"the webhook config must carry the rotated bundle after the pending retry (H-1)")
}

// TestRotateOnce_PendingInjectionFailsAgain_StaysPending pins the
// retry-until-healed contract: when the pending injection fails AGAIN the
// same bundle is carried over to the next tick (never dropped).
func TestRotateOnce_PendingInjectionFailsAgain_StaysPending(t *testing.T) {
	cm := &atomicCertManager{} // needsRotation=false: only the pending path runs
	k8sClient := newFakeClient(admissionWebhookFixture())
	rec := newCaptureRecorder()
	pending := []byte("stranded-bundle")

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	got := rotateOnce(canceledCtx, cm, k8sClient, pending, rec, testLogger())

	assert.Equal(t, pending, got,
		"a still-failing pending injection must keep the bundle pending")
	assert.Equal(t, int64(1), rec.injectionError.Load())
	assert.Zero(t, cm.ensureCalls.Load(), "no rotation was needed")
	assert.Nil(t, getVWC(t, k8sClient).Webhooks[0].ClientConfig.CABundle)
}

// ---------------------------------------------------------------------------
// UT-13: injectCABundleWithRetry outcome metric (G-1)
// ---------------------------------------------------------------------------

func TestInjectCABundleWithRetry_OutcomeMetric(t *testing.T) {
	t.Run("success records ResultSuccess", func(t *testing.T) {
		k8sClient := newFakeClient(admissionWebhookFixture())
		rec := newCaptureRecorder()

		err := injectCABundleWithRetry(context.Background(), k8sClient,
			[]byte("bundle"), nil, rec, testLogger())

		require.NoError(t, err)
		assert.Equal(t, int64(1), rec.injectionSuccess.Load())
		assert.Zero(t, rec.injectionError.Load())
	})

	t.Run("exhausted budget records ResultError", func(t *testing.T) {
		// A canceled context exhausts the retry helper immediately (fast,
		// deterministic) and must surface as an error outcome.
		k8sClient := newFakeClient(admissionWebhookFixture())
		rec := newCaptureRecorder()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := injectCABundleWithRetry(ctx, k8sClient, []byte("bundle"), nil, rec, testLogger())

		require.Error(t, err)
		assert.Equal(t, int64(1), rec.injectionError.Load())
		assert.Zero(t, rec.injectionSuccess.Load())
	})

	t.Run("nil recorder does not panic", func(t *testing.T) {
		k8sClient := newFakeClient(admissionWebhookFixture())

		require.NoError(t, injectCABundleWithRetry(context.Background(), k8sClient,
			[]byte("bundle"), nil, nil, testLogger()))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, injectCABundleWithRetry(ctx, k8sClient,
			[]byte("bundle"), nil, nil, testLogger()))
	})
}

// ---------------------------------------------------------------------------
// UT-14: IncCertRotationCheckError emission (DEV-4)
// ---------------------------------------------------------------------------

func TestRotateOnce_NeedsRotationError_IncrementsCheckErrorMetric(t *testing.T) {
	cm := &atomicCertManager{}
	cm.needsRotationErr.Store(true)
	k8sClient := newFakeClient(admissionWebhookFixture())
	rec := newCaptureRecorder()

	// Two direct ticks -> exactly one increment per tick.
	got1 := rotateOnce(context.Background(), cm, k8sClient, nil, rec, testLogger())
	got2 := rotateOnce(context.Background(), cm, k8sClient, nil, rec, testLogger())

	assert.Nil(t, got1)
	assert.Nil(t, got2)
	assert.Equal(t, int64(2), rec.checkErrors.Load(),
		"exactly one check-error increment per failed tick")
	assert.Equal(t, "webhook", rec.lastComponent.Load(),
		"the component label must be the bounded webhook value")
	assert.Zero(t, cm.ensureCalls.Load(), "a failed check must not rotate")
	assert.Zero(t, rec.injectionSuccess.Load()+rec.injectionError.Load(),
		"a failed check must not attempt an injection")
}

// ---------------------------------------------------------------------------
// UT-10: leader gating (L-10)
// ---------------------------------------------------------------------------

func TestRunCertRotation_LeaderGating_NoWorkUntilElected(t *testing.T) {
	shrinkRotationInterval(t)

	cm := &atomicCertManager{}
	cm.needsRotation.Store(true)
	elected := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runCertRotation(ctx, cm, newFakeClient(admissionWebhookFixture()),
			elected, &metrics.NoopRecorder{}, testLogger())
		close(done)
	}()

	// While NOT elected the loop must perform no work, even though dozens of
	// shrunk intervals (2ms) elapse within the observation window.
	assert.Never(t, func() bool { return cm.needsCalls.Load() > 0 },
		100*time.Millisecond, 5*time.Millisecond,
		"an un-elected replica must not run rotation checks (L-10)")

	// Election: ticks must begin.
	close(elected)
	require.Eventually(t, func() bool { return cm.needsCalls.Load() >= 1 },
		5*time.Second, time.Millisecond,
		"ticks must start once leadership is acquired")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runCertRotation did not return after cancel")
	}
}

// ---------------------------------------------------------------------------
// UT-12: jitteredRotationInterval bounds
// ---------------------------------------------------------------------------

func TestJitteredRotationInterval_Bounds(t *testing.T) {
	shrinkRotationInterval(t)
	base := certRotationInterval
	maxExclusive := base + time.Duration(float64(base)*rotationJitterFraction)

	for range 500 {
		v := jitteredRotationInterval()

		assert.GreaterOrEqual(t, v, base,
			"jitter must never shorten the interval")
		assert.Less(t, v, maxExclusive+1, // rand.Float64 < 1 keeps v < base*1.1
			"jitter must stay below base*(1+rotationJitterFraction)")
	}
}
