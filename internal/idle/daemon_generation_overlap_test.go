package idle

// Regression test for F-10.2 (handoff task T-6): Stop() nils d.cancel before
// the old scanLoop drains, so a concurrent Start() could previously spawn
// generation N+1 while generation N was still INSIDE runScanCycle — leaving
// d.consecutiveFails (and the attemptReconnect DBClient swap) racing across
// generations. Start now waits for the previous generation's done channel, so
// at most one scanLoop ever executes scan cycles. This test forces a scan
// cycle to straddle a Stop/Start overlap and must stay clean under -race.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// scanFailureCounter counts RecordIdleScanFailure calls, attributing scan
// cycles to a generation without touching daemon internals.
type scanFailureCounter struct {
	metrics.NoopRecorder
	failures atomic.Int64
}

func (s *scanFailureCounter) RecordIdleScanFailure(_, _ string) {
	s.failures.Add(1)
}

func TestDaemon_OverlappingGenerationScanSerialized(t *testing.T) {
	var (
		inScan        atomic.Int32
		maxConcurrent atomic.Int32
	)
	entered := make(chan struct{}, 64)
	release := make(chan struct{})

	// Every scan cycle: track concurrency, signal entry, block until the
	// gate opens (a closed channel lets later cycles pass immediately), and
	// FAIL — failing scans execute the exact statements F-10.2 flagged
	// (consecutiveFails writes) in every generation.
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			cur := inScan.Add(1)
			for {
				seen := maxConcurrent.Load()
				if cur <= seen || maxConcurrent.CompareAndSwap(seen, cur) {
					break
				}
			}
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			inScan.Add(-1)
			return nil, errors.New("scan boom")
		},
	}

	rec := &scanFailureCounter{}
	d := New(Config{
		ClusterName:  "overlap-cluster",
		Namespace:    "default",
		ScanInterval: time.Millisecond,
		DBClient:     mock,
		Metrics:      rec,
	})
	d.UpdateRules([]IdleRule{{
		Name:        "r1",
		Enabled:     true,
		IdleTimeout: time.Minute,
	}})

	ctx := context.Background()
	d.Start(ctx)

	// Generation 1 is INSIDE runScanCycle (blocked on the gate).
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("generation 1 never entered a scan cycle")
	}

	// Overlap: Stop blocks on the draining generation; Start must NOT spawn
	// generation 2 until generation 1 has fully exited.
	stopReturned := make(chan struct{})
	go func() { d.Stop(); close(stopReturned) }()

	// Wait until Stop has DEREGISTERED generation 1 (d.cancel == nil) while
	// its scanLoop is still parked inside the blocked scan cycle. Without
	// this ordering pin the concurrent Start could legally observe the
	// daemon as still running and no-op (documented Start semantics), which
	// would skip the overlap entirely. From this point Start must take the
	// drain-wait path — the pre-fix code instead spawned an overlapping
	// generation right here.
	require.Eventually(t, func() bool {
		d.mu.RLock()
		defer d.mu.RUnlock()
		return d.cancel == nil
	}, 10*time.Second, time.Millisecond,
		"Stop never deregistered the draining generation")

	startReturned := make(chan struct{})
	go func() { d.Start(ctx); close(startReturned) }()

	// Bounded overlap-observation window (fail-detection only, never a
	// pass-path wait-for-state): with the fix, generation 2 CANNOT scan while
	// generation 1 is parked inside its cycle, so no token can arrive here.
	// Under the pre-fix code, Start returns immediately and generation 2's
	// 1ms ticker enters a concurrent scan within the window.
	select {
	case <-entered:
		t.Fatal("a second generation executed a scan cycle while generation 1 was still draining")
	case <-time.After(250 * time.Millisecond):
	}

	// Open the gate: generation 1 finishes its cycle and exits, Stop returns,
	// and the pending Start spawns generation 2.
	close(release)

	select {
	case <-stopReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop hung waiting for the draining generation")
	}
	select {
	case <-startReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Start hung waiting for the previous generation to drain")
	}

	// Generation 2 must actually run scan cycles: Start only returns after
	// generation 1 fully exited, so any failure increment observed from here
	// on is attributable to generation 2 alone.
	base := rec.failures.Load()
	require.Eventually(t, func() bool { return rec.failures.Load() > base },
		5*time.Second, time.Millisecond,
		"generation 2 never executed a scan cycle after the handover")

	d.Stop()

	assert.Equal(t, int32(1), maxConcurrent.Load(),
		"scan cycles of different generations must never run concurrently")
}

// TestDaemon_StartAfterStopReusesCleanState pins the happy handover: a plain
// Stop-then-Start sequence (previous done channel already closed) must start
// the next generation without waiting and keep scanning.
func TestDaemon_StartAfterStopReusesCleanState(t *testing.T) {
	scans := make(chan struct{}, 64)
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			select {
			case scans <- struct{}{}:
			default:
			}
			return []db.SessionWithGroup{}, nil
		},
	}
	d := New(Config{
		ClusterName:  "handover-cluster",
		Namespace:    "default",
		ScanInterval: time.Millisecond,
		DBClient:     mock,
		Metrics:      &metrics.NoopRecorder{},
	})
	d.UpdateRules([]IdleRule{{Name: "r1", Enabled: true, IdleTimeout: time.Minute}})

	ctx := context.Background()
	d.Start(ctx)
	select {
	case <-scans:
	case <-time.After(10 * time.Second):
		t.Fatal("generation 1 never scanned")
	}
	d.Stop()

	// Drain any buffered generation-1 tokens so the next observation is
	// attributable to generation 2 (generation 1 is fully exited after Stop).
	for {
		select {
		case <-scans:
			continue
		default:
		}
		break
	}

	d.Start(ctx)
	select {
	case <-scans:
	case <-time.After(10 * time.Second):
		t.Fatal("generation 2 never scanned after a clean Stop/Start handover")
	}
	d.Stop()
}
