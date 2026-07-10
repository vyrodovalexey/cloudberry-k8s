//go:build e2e

package e2e

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// liveClusterProbeTimeout bounds the kubectl existence probe so a hung
// apiserver can never stall a gated test.
const liveClusterProbeTimeout = 30 * time.Second

// requireLiveClusterDeployed gates a heavy live journey (the scenario 77-88
// backup lifecycle scripts and friends) on its DEDICATED target cluster being
// deployed: when the named CloudberryCluster is not served in the namespace,
// the journey cannot even preflight (its script would fail resolving the
// admin-password Secret / coordinator), so the honest outcome is a clean SKIP
// — matching the suite-wide "skips cleanly when the live env is absent"
// convention used by every other live Part B (scenarios 92-122).
//
// The gate NEVER weakens the journey: whenever the dedicated cluster IS
// deployed (or the test is pointed at a compatible one via its
// SCENARIO*_CLUSTER env override), the full original assertions run.
func requireLiveClusterDeployed(t *testing.T, cluster, namespace string) {
	t.Helper()

	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skipf("kubectl not found on PATH, cannot probe for cluster %q [CONFIG-ONLY]", cluster)
	}

	ctx, cancel := context.WithTimeout(context.Background(), liveClusterProbeTimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, "kubectl", "get", "cloudberrycluster", cluster,
		"-n", namespace, "-o", "name").CombinedOutput()
	if err != nil {
		t.Skipf("CloudberryCluster %q not deployed in namespace %q [CONFIG-ONLY: the live "+
			"journey needs its dedicated, already-deployed cluster — deploy it or point the "+
			"scenario's cluster env override at a compatible deployed cluster]: %v: %s",
			cluster, namespace, err, strings.TrimSpace(string(out)))
	}
}
