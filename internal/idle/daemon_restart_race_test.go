package idle

// Regression tests for T7/B4: scanLoop closes the done channel of ITS OWN
// generation (passed as a parameter), so rapid or concurrent Stop/Start
// cycles can neither double-close a channel (panic) nor leave a Stop hanging
// on a channel nobody closes. Run under -race.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

func newRaceTestDaemon() *Daemon {
	return New(Config{
		ClusterName:  "race-cluster",
		Namespace:    "default",
		ScanInterval: time.Millisecond,
		DBClient:     &mockDBClient{},
		Metrics:      &metrics.NoopRecorder{},
	})
}

// TestDaemon_RapidStopStartCycles drives many back-to-back Stop->Start
// generations: every Stop must return (guarded by the test timeout) and no
// generation may close another generation's channel.
func TestDaemon_RapidStopStartCycles(t *testing.T) {
	d := newRaceTestDaemon()
	ctx := context.Background()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i := 0; i < 50; i++ {
			d.Start(ctx)
			d.Stop()
		}
	}()

	select {
	case <-finished:
		// Success: 50 generations, no panic, no hang.
	case <-time.After(10 * time.Second):
		t.Fatal("rapid Stop/Start cycles hung — done-channel generation bug")
	}
}

// TestDaemon_ConcurrentStartStop hammers Start/Stop from multiple goroutines
// concurrently; the run must be free of double-close panics and data races.
func TestDaemon_ConcurrentStartStop(t *testing.T) {
	d := newRaceTestDaemon()
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				d.Start(ctx)
				d.Stop()
			}
		}()
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()

	select {
	case <-finished:
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent Start/Stop hung")
	}

	// Leave the daemon stopped; a final Stop must be a safe no-op.
	d.Stop()
}
