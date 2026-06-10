package idle

// Cycle-2 fix test (T14, L-6): a session matching MULTIPLE enabled rules is
// terminated exactly once — the rule loop breaks after the first successful
// termination.

import (
	"context"
	"testing"
	"time"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
)

func TestScanAndEnforce_SessionMatchingTwoRules_SingleTerminateCall(t *testing.T) {
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        77,
						Username:   "testuser",
						State:      "idle",
						QueryStart: time.Now().Add(-10 * time.Minute),
					},
					ResourceGroup: "etl_group",
				},
			}, nil
		},
	}
	recorder := &trackingMetricsRecorder{}
	d := New(Config{
		ClusterName:  "test-cluster",
		Namespace:    "default",
		ScanInterval: time.Second,
		DBClient:     mock,
		Metrics:      recorder,
	})

	// BOTH rules match the session (same resource group, both timeouts long
	// exceeded): only ONE terminate call may be issued.
	d.UpdateRules([]IdleRule{
		{Name: "rule-a", Enabled: true, ResourceGroup: "etl_group", IdleTimeout: time.Minute},
		{Name: "rule-b", Enabled: true, ResourceGroup: "etl_group", IdleTimeout: 2 * time.Minute},
	})

	_, terminated, err := d.scanAndEnforce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if terminated != 1 {
		t.Errorf("expected 1 terminated session, got %d", terminated)
	}
	pids := mock.getTerminatedPIDs()
	if len(pids) != 1 || pids[0] != 77 {
		t.Errorf("expected exactly one terminate call for PID 77, got %v", pids)
	}
	if terms := recorder.getTerminations(); len(terms) != 1 {
		t.Errorf("expected exactly one termination metric, got %d", len(terms))
	}
}

func TestScanAndEnforce_FailedTerminationKeepsEvaluatingRules(t *testing.T) {
	calls := 0
	mock := &mockDBClient{
		listSessionsWithResourceGroupFn: func(_ context.Context) ([]db.SessionWithGroup, error) {
			return []db.SessionWithGroup{
				{
					Session: db.Session{
						PID:        78,
						Username:   "testuser",
						State:      "idle",
						QueryStart: time.Now().Add(-10 * time.Minute),
					},
					ResourceGroup: "etl_group",
				},
			}, nil
		},
		terminateSessionFn: func(_ context.Context, _ int32) (bool, error) {
			calls++
			// First rule's attempt reports "already gone"; the second rule
			// may still try (only a SUCCESSFUL termination breaks the loop).
			return calls > 1, nil
		},
	}
	d := New(Config{
		ClusterName:  "test-cluster",
		Namespace:    "default",
		ScanInterval: time.Second,
		DBClient:     mock,
	})
	d.UpdateRules([]IdleRule{
		{Name: "rule-a", Enabled: true, ResourceGroup: "etl_group", IdleTimeout: time.Minute},
		{Name: "rule-b", Enabled: true, ResourceGroup: "etl_group", IdleTimeout: 2 * time.Minute},
	})

	_, terminated, err := d.scanAndEnforce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("an unsuccessful termination must not break the rule loop; got %d calls", calls)
	}
	if terminated != 1 {
		t.Errorf("expected 1 terminated session, got %d", terminated)
	}
}
