package main

// TestMain hardens the whole test binary against the dangerous production
// defaults of the testability seams declared in main.go:
//
//   - getRestConfig defaults to ctrl.GetConfigOrDie, which — when no
//     kubeconfig or in-cluster environment exists (e.g. GitHub Actions) —
//     logs an error and calls os.Exit(1), killing the ENTIRE test binary
//     with a package-level FAIL and no individual test attribution.
//   - newManager defaults to ctrl.NewManager, which builds a manager wired
//     to a real cluster and can block or touch live infrastructure.
//   - metricsRegistry defaults to the controller-runtime global registry;
//     a second unseamed run() invocation would panic on re-registration.
//
// Replacing them here makes the real ctrl.GetConfigOrDie unreachable from
// any test, regardless of seam install/restore ordering or goroutine
// lifetimes. Tests that need specific behavior (e.g. installRunSeams)
// install their own seams on top and may restore to these safe defaults.

import (
	"fmt"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestMain(m *testing.M) {
	// Never resolve a real kubeconfig: return an unroutable localhost config.
	getRestConfig = func() *rest.Config {
		return &rest.Config{Host: "http://127.0.0.1:1"}
	}
	// Never construct a real controller manager: fail fast with a marker
	// error so any test that accidentally reaches manager construction
	// without installing its own seam fails loudly and attributably.
	newManager = func(_ *rest.Config, _ ctrl.Options) (ctrl.Manager, error) {
		return nil, fmt.Errorf("cmd/operator TestMain: real manager construction is disabled in tests; install a manager seam")
	}
	// Keep operator metrics out of the controller-runtime global registry.
	metricsRegistry = prometheus.NewRegistry()

	os.Exit(m.Run())
}
