package controller

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces goroutine-leak verification for the controller package:
// the HA rebalance dispatcher, idle-daemon lifecycle management, and
// deletion-backup state machine all spawn goroutines that must be joined
// before each test exits (E-3 / H-1).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// klog's background flush daemon is process-wide and started lazily
		// by client-go; it is intentional third-party behavior.
		goleak.IgnoreTopFunction("k8s.io/klog/v2.(*flushDaemon).run"),
	)
}
