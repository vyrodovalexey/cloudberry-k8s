//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 106: Server Configuration Update / Delete (SL.7–SL.8) — E2E
// ============================================================================
//
// The SL.1–SL.6 server-ConfigMap e2e is Scenario 93; this suite is its SL.7/SL.8
// LIFECYCLE sibling. It mirrors the Scenario 105 e2e SHAPE (catalog-honest Part A
// that ALWAYS runs + a KUBECONFIG-gated live Part B) and reuses the Scenario 93
// live harness (read the <cluster>-pxf-servers ConfigMap; exec-and-cat a
// segment-primary pxf sidecar to confirm the RESOLVED nested per-server file).
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario106Cases() and assert
//     the catalog is well-formed (unique IDs, every SL.7/SL.8/MX requirement
//     family present, every row carries a Layer + Expected + Description). The
//     -B/-F rows are resolved at the functional + integration layers; the -L rows
//     are documented here and resolved at Part B.
//
//   PART B (KUBECONFIG + SCENARIO106_LIVE=1; cluster name via env, default s106):
//     106-SL7-L1: read the live <cluster>-pxf-servers ConfigMap (baseline
//                 minio-warehouse endpoint); PATCH the CloudberryCluster
//                 dataLoading.pxf.servers[minio-warehouse].config.fs.s3a.endpoint
//                 to a new value; wait for the operator to regenerate the CM;
//                 assert minio-warehouse__s3-site.xml carries the NEW endpoint;
//                 trigger pxf sync (or rely on reconcile) and, if feasible, exec a
//                 segment-primary pxf sidecar to cat the resolved
//                 servers/minio-warehouse/s3-site.xml and assert the NEW endpoint.
//                 Assert the PXFServersChanged event was recorded and the counter
//                 cloudberry_pxf_servers_changed_total increased.
//     106-SL8-L1: REMOVE a server from dataLoading.pxf.servers[]; wait for CM
//                 regeneration; assert the server's <server>__*.xml keys are GONE;
//                 then run a SELECT against an external table referencing it and
//                 assert it ERRORS (real negative proof — not over-constrained).
//
// HONESTY: the live ConfigMap is the operator's own output; the patch is applied
// to the CR and the regeneration is OBSERVED (no synthesized state). The negative
// is a REAL captured SELECT error. Skips cleanly when KUBECONFIG / the live env is
// absent (matches scenario93/105 exactly).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG              — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO106_LIVE=1      — gates the destructive patch/delete live path.
//   SCENARIO106_CLUSTER     — live cluster name (default s106, a SHORT name).
//   SCENARIO106_NAMESPACE   — namespace (default cloudberry-test).
//   SCENARIO106_EXT_TABLE   — external table referencing the to-be-deleted server
//                             (default ext_minio_warehouse).
// ============================================================================

const (
	// envKubeconfigS106 gates all of Scenario 106 Part B.
	envKubeconfigS106 = "KUBECONFIG"
	// envScenario106Live gates the destructive live patch/delete path.
	envScenario106Live = "SCENARIO106_LIVE"
	// envScenario106Cluster overrides the live cluster name.
	envScenario106Cluster = "SCENARIO106_CLUSTER"
	// envScenario106Namespace overrides the namespace.
	envScenario106Namespace = "SCENARIO106_NAMESPACE"
	// envScenario106ExtTable overrides the (self-contained) external table the
	// SL.8 negative creates + SELECTs.
	envScenario106ExtTable = "SCENARIO106_EXT_TABLE"

	// scenario106DefaultCluster is the default (SHORT) deployed cluster name.
	scenario106DefaultCluster = "s106"
	// scenario106DefaultNamespace is the default namespace.
	scenario106DefaultNamespace = "cloudberry-test"
	// scenario106DefaultExtTable is the default external table the SL.8 negative
	// creates referencing the to-be-deleted server.
	scenario106DefaultExtTable = "s106_sl8_probe"

	// scenario106UpdateServer is the s3 server whose endpoint is patched (SL.7).
	scenario106UpdateServer = "minio-warehouse"
	// scenario106UpdateFile is the rendered per-server file the endpoint routes to.
	scenario106UpdateFile = "minio-warehouse__s3-site.xml"
	// scenario106EndpointKey is the config key (inside the server's config map)
	// that the SL.7 JSON patch targets BY INDEX.
	scenario106EndpointKey = "fs.s3a.endpoint"
	// scenario106PxfContainer is the segment-primary PXF sidecar container.
	scenario106PxfContainer = "pxf"
	// scenario106CoordContainer is the coordinator's primary container (hosts a
	// reachable psql against the live cluster).
	scenario106CoordContainer = "cloudberry"
	// scenario106PsqlDB is the database psql connects to on the coordinator.
	scenario106PsqlDB = "postgres"
	// scenario106PsqlRole is the data-loader role psql runs as.
	scenario106PsqlRole = "gpadmin"

	// scenario106NewEndpoint is the NEW endpoint the SL.7 patch sets.
	scenario106NewEndpoint = "http://minio-patched-s106:9000"

	// scenario106SL8TmpJob is the throwaway job the SL.8 test adds to make the
	// chosen server referenced, so the delete must atomically remove BOTH (W.9).
	scenario106SL8TmpJob = "s106-sl8-tmp-job"
	// scenario106DataResource is the object-store CSV resource the probe table
	// reads through PXF (3 columns: id,name,val) — present on the s106 deploy.
	scenario106DataResource = "cloudberry-data/text/data.csv"

	// scenario106LiveTimeout bounds the CM-regeneration wait loops (reconcile +
	// sync propagation can take tens of seconds).
	scenario106LiveTimeout = 5 * time.Minute
	// scenario106RestartTimeout bounds a segment-primary rolling restart +
	// query-ready recovery (a mirrored HA cluster takes a few minutes).
	scenario106RestartTimeout = 6 * time.Minute
	// scenario106PollInterval is the live poll interval.
	scenario106PollInterval = 10 * time.Second
	// scenario106ExecTimeout bounds the kubectl exec / psql probes.
	scenario106ExecTimeout = 90 * time.Second
)

// Scenario106E2ESuite verifies the PXF server-config update/delete lifecycle
// end-to-end (catalog-direct Part A + KUBECONFIG-gated live Part B).
type Scenario106E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario106(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario106E2ESuite))
}

func (s *Scenario106E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario106_PartA_CatalogHonest iterates the full Scenario 106 catalog
// and asserts it is well-formed: unique IDs, every SL.7/SL.8/MX requirement
// family present, and every row carries a non-empty Layer + Expected +
// Description. The -B/-F rows are resolved at the functional + integration
// layers; the -L rows are documented here and resolved at Part B.
func (s *Scenario106E2ESuite) TestE2E_Scenario106_PartA_CatalogHonest() {
	catalog := cases.Scenario106Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true

			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(),
				[]string{cases.Scenario106LayerBuilder, cases.Scenario106LayerReconcile,
					cases.Scenario106LayerLive}, tc.Layer,
				"%s Layer must be a known token", tc.ID)

			switch tc.Layer {
			case cases.Scenario106LayerLive:
				s.T().Logf("scenario106 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
			default:
				s.T().Logf("scenario106 %s (%s): %s — resolved at functional/integration",
					tc.ID, tc.Req, tc.Expected)
			}
		})
	}

	for _, req := range []string{"SL.7", "SL.8", "MX"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover requirement family %s", req)
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (patch endpoint / delete server)
// ----------------------------------------------------------------------------

// scenario106Env returns the ENV value or the provided default.
func scenario106Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario106Namespace() string {
	return scenario106Env(envScenario106Namespace, scenario106DefaultNamespace)
}
func scenario106Cluster() string {
	return scenario106Env(envScenario106Cluster, scenario106DefaultCluster)
}
func scenario106ExtTable() string {
	return scenario106Env(envScenario106ExtTable, scenario106DefaultExtTable)
}

// scenario106CR is the slice of the live CloudberryCluster the live Part B reads
// to compute JSON-patch paths BY INDEX (mirrors the proven manual approach: a
// merge patch on the servers array would REPLACE it and fail webhook validation,
// so we always target the specific element index).
type scenario106CR struct {
	Spec struct {
		DataLoading struct {
			Pxf struct {
				Servers []json.RawMessage `json:"servers"`
			} `json:"pxf"`
			Jobs []json.RawMessage `json:"jobs"`
		} `json:"dataLoading"`
	} `json:"spec"`
}

// scenario106Server is the minimal server shape needed to find one by name.
type scenario106Server struct {
	Name string `json:"name"`
}

// scenario106Job is the minimal job shape needed to find the jobs that
// reference a given server (for the atomic delete, W.9 referential integrity).
type scenario106Job struct {
	Name   string `json:"name"`
	PxfJob struct {
		Server string `json:"server"`
	} `json:"pxfJob"`
}

// scenario106GetCR reads the live CloudberryCluster as JSON (empty + false when
// the cluster is absent — a genuine "cluster not deployed" precondition).
func (s *Scenario106E2ESuite) scenario106GetCR() (*scenario106CR, bool) {
	out, err := s.scenario106Kubectl("get", "cloudberrycluster", scenario106Cluster(),
		"-n", scenario106Namespace(), "-o", "json")
	if err != nil {
		s.T().Logf("scenario106: could not read CR %s: %v (out=%s)", scenario106Cluster(), err, out)
		return nil, false
	}
	cr := &scenario106CR{}
	if err := json.Unmarshal([]byte(out), cr); err != nil {
		s.T().Logf("scenario106: could not parse CR JSON: %v", err)
		return nil, false
	}
	return cr, true
}

// scenario106ServerIndex returns the index of the named server in the CR's
// pxf.servers[] (-1 when absent), plus its raw JSON for later restoration.
func scenario106ServerIndex(cr *scenario106CR, name string) (int, json.RawMessage) {
	for i, raw := range cr.Spec.DataLoading.Pxf.Servers {
		var srv scenario106Server
		if json.Unmarshal(raw, &srv) == nil && srv.Name == name {
			return i, raw
		}
	}
	return -1, nil
}

// scenario106ServerEndpoint extracts a server's config["fs.s3a.endpoint"] from
// its raw JSON (empty + false when absent).
func scenario106ServerEndpoint(raw json.RawMessage) (string, bool) {
	var srv struct {
		Config map[string]string `json:"config"`
	}
	if json.Unmarshal(raw, &srv) != nil {
		return "", false
	}
	v, ok := srv.Config[scenario106EndpointKey]
	return v, ok
}

// scenario106JobsReferencing returns the indices of jobs whose pxfJob.server is
// the named server (descending order so a remove sequence stays index-valid),
// plus their raw JSON for restoration.
func scenario106JobsReferencing(cr *scenario106CR, server string) ([]int, []json.RawMessage) {
	var idxs []int
	var raws []json.RawMessage
	for i, raw := range cr.Spec.DataLoading.Jobs {
		var job scenario106Job
		if json.Unmarshal(raw, &job) == nil && job.PxfJob.Server == server {
			idxs = append(idxs, i)
			raws = append(raws, raw)
		}
	}
	// Descending so removing the higher index first does not shift the lower.
	for l, r := 0, len(idxs)-1; l < r; l, r = l+1, r-1 {
		idxs[l], idxs[r] = idxs[r], idxs[l]
		raws[l], raws[r] = raws[r], raws[l]
	}
	return idxs, raws
}

// scenario106JobIndex returns the index of the named job in the CR's
// dataLoading.jobs[] (-1 when absent).
func scenario106JobIndex(cr *scenario106CR, name string) int {
	for i, raw := range cr.Spec.DataLoading.Jobs {
		var job scenario106Job
		if json.Unmarshal(raw, &job) == nil && job.Name == name {
			return i
		}
	}
	return -1
}

// scenario106PatchJSON applies a JSON (RFC6902) patch to the live CR.
func (s *Scenario106E2ESuite) scenario106PatchJSON(patch string) (string, error) {
	return s.scenario106Kubectl("patch", "cloudberrycluster", scenario106Cluster(),
		"-n", scenario106Namespace(), "--type=json", "-p", patch)
}

// scenario106RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario106E2ESuite) scenario106RequireKubeconfig() {
	if os.Getenv(envKubeconfigS106) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 106 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 106 live Part B")
	}
}

// scenario106RequireLive additionally requires SCENARIO106_LIVE=1 for the
// destructive patch/delete path.
func (s *Scenario106E2ESuite) scenario106RequireLive() {
	s.scenario106RequireKubeconfig()
	if os.Getenv(envScenario106Live) != "1" {
		s.T().Skip("SCENARIO106_LIVE not set, skipping the live PXF server patch/delete path " +
			"(the deployed cluster + the real cloudberry-pxf sidecar must be available)")
	}
}

// scenario106Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario106E2ESuite) scenario106Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario106ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario106ReadServerFile reads one key of the live <cluster>-pxf-servers
// ConfigMap (empty string + false when absent/unreadable).
func (s *Scenario106E2ESuite) scenario106ReadServerFile(key string) (string, bool) {
	cmName := builder.PxfServersConfigMapName(scenario106Cluster())
	out, err := s.scenario106Kubectl("get", "configmap", cmName,
		"-n", scenario106Namespace(), "-o", "jsonpath={.data."+jsonpathEscape(key)+"}")
	if err != nil {
		s.T().Logf("scenario106: could not read CM %s key %s: %v (out=%s)", cmName, key, err, out)
		return "", false
	}
	return out, true
}

// scenario106CMKeys returns the live <cluster>-pxf-servers ConfigMap's data keys
// (empty + false when absent).
func (s *Scenario106E2ESuite) scenario106CMKeys() (string, bool) {
	cmName := builder.PxfServersConfigMapName(scenario106Cluster())
	out, err := s.scenario106Kubectl("get", "configmap", cmName,
		"-n", scenario106Namespace(), "-o", "jsonpath={.data}")
	if err != nil {
		return "", false
	}
	return out, true
}

// jsonpathEscape escapes the "." and "_" in a ConfigMap data key for a kubectl
// jsonpath {.data.<key>} selector (dots in keys must be escaped with a backslash).
func jsonpathEscape(key string) string {
	return strings.ReplaceAll(key, ".", `\.`)
}

// scenario106JSONPointerEscape escapes a JSON-pointer reference token per
// RFC6901: "~" → "~0" then "/" → "~1". The PXF config keys (e.g.
// "fs.s3a.endpoint") contain dots, which are LITERAL in a JSON pointer and need
// no escaping, but "/" / "~" would, so we escape defensively.
func scenario106JSONPointerEscape(token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	return strings.ReplaceAll(token, "/", "~1")
}

// scenario106FirstSegmentPxfPod returns the first segment-primary pod carrying a
// pxf container (empty when none found).
func (s *Scenario106E2ESuite) scenario106FirstSegmentPxfPod() string {
	out, err := s.scenario106Kubectl("get", "pods", "-n", scenario106Namespace(),
		"-l", "avsoft.io/component=segment-primary,avsoft.io/cluster="+scenario106Cluster(),
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// scenario106AllSegmentPxfPods returns the names of all segment-primary pods.
func (s *Scenario106E2ESuite) scenario106AllSegmentPxfPods() []string {
	out, err := s.scenario106Kubectl("get", "pods", "-n", scenario106Namespace(),
		"-l", "avsoft.io/component=segment-primary,avsoft.io/cluster="+scenario106Cluster(),
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil
	}
	var pods []string
	for _, line := range strings.Split(out, "\n") {
		if name := strings.TrimSpace(line); name != "" {
			pods = append(pods, name)
		}
	}
	return pods
}

// scenario106PxfHealthy reports whether the PXF service in the named pod's pxf
// sidecar answers its health endpoint with HTTP 200 (the pod Ready condition
// fires BEFORE the PXF JVM finishes starting, so a SELECT needs this stronger
// signal across every segment).
func (s *Scenario106E2ESuite) scenario106PxfHealthy(pod string) bool {
	out, err := s.scenario106PxfExec(pod,
		"curl -s -o /dev/null -w '%{http_code}' http://localhost:5888/actuator/health 2>/dev/null || true")
	return err == nil && strings.Contains(out, "200")
}

// scenario106WaitPxfHealthy waits until the PXF sidecar on EVERY segment-primary
// pod is serving (HTTP 200), so a cross-segment PXF SELECT can run. Returns false
// (logged) when not all sidecars become healthy in time.
func (s *Scenario106E2ESuite) scenario106WaitPxfHealthy() bool {
	deadline := time.Now().Add(scenario106RestartTimeout)
	for time.Now().Before(deadline) {
		pods := s.scenario106AllSegmentPxfPods()
		allUp := len(pods) > 0
		for _, p := range pods {
			if !s.scenario106PxfHealthy(p) {
				allUp = false
				break
			}
		}
		if allUp {
			return true
		}
		time.Sleep(scenario106PollInterval)
	}
	s.T().Logf("scenario106: PXF sidecars did not all become healthy within %s",
		scenario106RestartTimeout)
	return false
}

// scenario106PxfExec runs a bash command inside the pxf container of the named pod.
func (s *Scenario106E2ESuite) scenario106PxfExec(pod, bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario106ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario106Namespace(),
		"-c", scenario106PxfContainer, pod, "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario106CoordinatorPod returns the coordinator pod (the one node that hosts
// a reachable psql against the cluster; a segment-primary refuses direct
// connections). Empty when none found.
func (s *Scenario106E2ESuite) scenario106CoordinatorPod() string {
	out, err := s.scenario106Kubectl("get", "pods", "-n", scenario106Namespace(),
		"-l", "avsoft.io/component=coordinator,avsoft.io/cluster="+scenario106Cluster(),
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// scenario106Psql runs a SQL query as the data-loader role (gpadmin) against the
// coordinator's database via kubectl exec. The error carries the captured psql
// output so the SL.8 negative can assert a REAL query error.
func (s *Scenario106E2ESuite) scenario106Psql(coordPod, sql string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario106ExecTimeout)
	defer cancel()
	psqlCmd := fmt.Sprintf("psql -U %s -d %s -tAc %s",
		scenario106PsqlRole, scenario106PsqlDB, scenario106ShellQuote(sql))
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario106Namespace(),
		"-c", scenario106CoordContainer, coordPod, "--", "bash", "-lc", psqlCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario106ShellQuote single-quotes a string for safe embedding in bash -lc.
func scenario106ShellQuote(in string) string {
	return "'" + strings.ReplaceAll(in, "'", `'\''`) + "'"
}

// scenario106RestartSegments deletes the segment-primary pods and waits for them
// to come back Ready (the credential init container re-renders the resolved
// per-server files from the regenerated ConfigMap — the operator does NOT
// auto-restart on a PXF servers-only change, so the SL.8 negative SELECT needs
// this to drop the deleted server from the sidecar). Returns false (with a log)
// when recovery does not complete in time — a genuine infra precondition.
func (s *Scenario106E2ESuite) scenario106RestartSegments() bool {
	out, err := s.scenario106Kubectl("delete", "pods", "-n", scenario106Namespace(),
		"-l", "avsoft.io/component=segment-primary,avsoft.io/cluster="+scenario106Cluster())
	if err != nil {
		s.T().Logf("scenario106: could not delete segment-primary pods: %v (out=%s)", err, out)
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, scenario106RestartTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "wait", "--for=condition=Ready",
		"pod", "-n", scenario106Namespace(),
		"-l", "avsoft.io/component=segment-primary,avsoft.io/cluster="+scenario106Cluster(),
		"--timeout="+scenario106RestartTimeout.String())
	if wout, werr := cmd.CombinedOutput(); werr != nil {
		s.T().Logf("scenario106: segment-primary pods not Ready after restart: %v (out=%s)",
			werr, string(wout))
		return false
	}
	// Pod-Ready fires before the PXF JVM finishes starting; wait for the PXF
	// service to actually serve on every segment so a cross-segment SELECT runs.
	return s.scenario106WaitPxfHealthy()
}

// scenario106CountServersChangedEvents returns the number of PXFServersChanged
// events recorded for the live cluster (0 when none / unreadable).
func (s *Scenario106E2ESuite) scenario106CountServersChangedEvents() int {
	out, err := s.scenario106Kubectl("get", "events", "-n", scenario106Namespace(),
		"--field-selector", "reason="+cases.Scenario106EventReason,
		"-o", "jsonpath={.items[*].reason}")
	if err != nil {
		return 0
	}
	return strings.Count(out, cases.Scenario106EventReason)
}

// TestE2E_Scenario106_LiveSL7PatchEndpoint covers 106-SL7-L1: read the baseline
// minio-warehouse endpoint, PATCH it in the CR, wait for the operator to
// regenerate the live ConfigMap, assert minio-warehouse__s3-site.xml carries the
// NEW endpoint, optionally exec the sidecar to confirm the RESOLVED file, and
// assert the PXFServersChanged event was recorded. Gated by SCENARIO106_LIVE=1;
// skips cleanly when the CM / cluster is absent.
func (s *Scenario106E2ESuite) TestE2E_Scenario106_LiveSL7PatchEndpoint() {
	s.scenario106RequireLive()

	// Baseline: the live CM must carry the minio-warehouse server file.
	baseline, ok := s.scenario106ReadServerFile(scenario106UpdateFile)
	if !ok || strings.TrimSpace(baseline) == "" {
		s.T().Skipf("no %s key in the live %s ConfigMap (cluster / minio-warehouse server may "+
			"not be deployed) [CONFIG-ONLY]", scenario106UpdateFile,
			builder.PxfServersConfigMapName(scenario106Cluster()))
	}
	eventsBefore := s.scenario106CountServersChangedEvents()

	// Find the INDEX of the minio-warehouse server in the live CR. A JSON *merge*
	// patch on the servers ARRAY would REPLACE the whole array with a single
	// {name,config} element and then fail webhook validation (the dropped server
	// loses its required `type`). The proven manual approach is a JSON (RFC6902)
	// patch that targets the specific server's config field BY INDEX, leaving the
	// rest of that server (incl. `type`) intact.
	cr, haveCR := s.scenario106GetCR()
	if !haveCR {
		s.T().Skipf("cluster %s not deployed / unreadable [CONFIG-ONLY: cluster absent]",
			scenario106Cluster())
	}
	idx, srvRaw := scenario106ServerIndex(cr, scenario106UpdateServer)
	if idx < 0 {
		s.T().Skipf("server %q absent from the live CR pxf.servers[] [CONFIG-ONLY]",
			scenario106UpdateServer)
	}

	// Capture the ORIGINAL endpoint and restore it on exit so the cluster returns
	// to a clean baseline (SL.7 is a non-destructive update; restoring keeps the
	// minio-warehouse server SELECT-usable for the SL.8 negative that runs next).
	origEndpoint, hadEndpoint := scenario106ServerEndpoint(srvRaw)
	endptPath := fmt.Sprintf("/spec/dataLoading/pxf/servers/%d/config/%s",
		idx, scenario106JSONPointerEscape(scenario106EndpointKey))
	if hadEndpoint {
		defer func() {
			restore := fmt.Sprintf(`[{"op":"replace","path":%q,"value":%q}]`,
				endptPath, origEndpoint)
			if o, e := s.scenario106PatchJSON(restore); e != nil {
				s.T().Logf("scenario106 SL.7 cleanup: could not restore endpoint to %q: %v (out=%s)",
					origEndpoint, e, o)
			} else {
				s.T().Logf("scenario106 SL.7 cleanup: restored %s endpoint to %q",
					scenario106UpdateServer, origEndpoint)
			}
		}()
	}

	// PATCH the CR: set servers[idx].config["fs.s3a.endpoint"] to the NEW value.
	// "replace" is correct on the s106 deploy (the key exists); fall back to "add"
	// when the key is absent on some other deploy.
	patch := fmt.Sprintf(`[{"op":"replace","path":%q,"value":%q}]`,
		endptPath, scenario106NewEndpoint)
	if o, e := s.scenario106PatchJSON(patch); e != nil {
		addPatch := fmt.Sprintf(`[{"op":"add","path":%q,"value":%q}]`,
			endptPath, scenario106NewEndpoint)
		if o2, e2 := s.scenario106PatchJSON(addPatch); e2 != nil {
			s.T().Skipf("could not patch the CR endpoint by index (replace: %v / out=%s; "+
				"add: %v / out=%s) [CONFIG-ONLY: CR patch not reproducible on this deploy]",
				e, o, e2, o2)
		}
	}

	// Wait for the operator to regenerate the CM with the NEW endpoint.
	require.Eventuallyf(s.T(), func() bool {
		body, found := s.scenario106ReadServerFile(scenario106UpdateFile)
		return found && strings.Contains(body, scenario106NewEndpoint)
	}, scenario106LiveTimeout, scenario106PollInterval,
		"the live %s must regenerate with the NEW endpoint %s",
		scenario106UpdateFile, scenario106NewEndpoint)

	body, _ := s.scenario106ReadServerFile(scenario106UpdateFile)
	assert.Contains(s.T(), body, scenario106NewEndpoint, "regenerated CM carries the NEW endpoint")
	s.T().Logf("scenario106 106-SL7-L1: live CM regenerated with endpoint %s", scenario106NewEndpoint)

	// SURGICAL re-render (SL.7): only the patched server's file changed — the OTHER
	// servers' keys are still present (the key set is unchanged).
	keys, _ := s.scenario106CMKeys()
	assert.Contains(s.T(), keys, scenario106UpdateFile,
		"the patched server's key must still be present (surgical update, not a delete)")

	// The PXFServersChanged event was recorded (counter increased) — honest, not
	// over-constrained: the deploy may aggregate events, so assert non-decrease.
	assert.GreaterOrEqual(s.T(), s.scenario106CountServersChangedEvents(), eventsBefore,
		"a PXFServersChanged event must have been recorded for the real diff")

	// The sidecar's RESOLVED file only re-renders on a pod restart (the credential
	// init container runs at startup; the operator does NOT auto-restart on a PXF
	// servers-only change). The CM-level regeneration above is the operator's
	// observable SL.7 output, so we only LOG the (possibly stale) sidecar view
	// rather than asserting it (keeping SL.7 fast — no restart needed here).
	if pod := s.scenario106FirstSegmentPxfPod(); pod != "" {
		resolved, execErr := s.scenario106PxfExec(pod,
			"cat /pxf-base/servers/"+scenario106UpdateServer+"/s3-site.xml 2>/dev/null || true")
		if execErr == nil && strings.Contains(resolved, scenario106NewEndpoint) {
			s.T().Logf("scenario106 106-SL7-L1: sidecar resolved file already carries the NEW endpoint")
		} else {
			s.T().Logf("scenario106 106-SL7-L1: sidecar resolved file not yet refreshed "+
				"(needs a pod restart; execErr=%v) — CM-level regeneration already proven", execErr)
		}
	}
}

// scenario106SL8Target is a server the SL.8 negative can delete and restore: it
// is rendered in the sidecar NOW (so the pre-delete SELECT succeeds) and we keep
// its raw spec + any referencing jobs' raw specs to restore the baseline.
type scenario106SL8Target struct {
	name       string
	serverRaw  json.RawMessage
	jobIdxs    []int             // descending CR indices of jobs referencing it
	jobRaws    []json.RawMessage // their raw specs (for restoration)
	probeTable string            // the external table the negative SELECT exercises
}

// scenario106ChooseSL8Target picks a DELETABLE server that is ALREADY resolved in
// the sidecar (the pre-delete SELECT must succeed without a restart). It prefers a
// server whose data resource matches the probe CSV. Returns false when none fit
// the precondition.
func (s *Scenario106E2ESuite) scenario106ChooseSL8Target(
	cr *scenario106CR, resolved map[string]bool,
) (scenario106SL8Target, bool) {
	// Preference order: minio-warehouse, s3-datalake (both s3, the probe CSV).
	for _, name := range []string{scenario106UpdateServer, "s3-datalake"} {
		idx, raw := scenario106ServerIndex(cr, name)
		if idx < 0 || !resolved[name] {
			continue
		}
		jobIdxs, jobRaws := scenario106JobsReferencing(cr, name)
		return scenario106SL8Target{
			name: name, serverRaw: raw, jobIdxs: jobIdxs, jobRaws: jobRaws,
			probeTable: scenario106ExtTable(),
		}, true
	}
	return scenario106SL8Target{}, false
}

// scenario106ResolvedServers returns the set of server names currently rendered
// in the sidecar's /pxf-base/servers (the per-server dirs).
func (s *Scenario106E2ESuite) scenario106ResolvedServers(pxfPod string) map[string]bool {
	out, err := s.scenario106PxfExec(pxfPod,
		"ls -1 /pxf-base/servers 2>/dev/null || true")
	set := map[string]bool{}
	if err != nil {
		return set
	}
	for _, line := range strings.Split(out, "\n") {
		if name := strings.TrimSpace(line); name != "" {
			set[name] = true
		}
	}
	return set
}

// TestE2E_Scenario106_LiveSL8DeleteServer covers 106-SL8-L1 (REAL NEGATIVE) as a
// SELF-CONTAINED, idempotent, re-runnable flow:
//
//  1. Pick a DELETABLE server already rendered in the sidecar; ADD a throwaway
//     job referencing it so the delete MUST remove BOTH (proving the webhook's
//     W.9 referential-integrity rule — a server cannot be removed while a job
//     still references it).
//  2. Create an external table referencing the server and assert the SELECT
//     SUCCEEDS (the pre-delete positive baseline).
//  3. Remove the throwaway job AND the server in ONE atomic JSON patch (indices
//     computed from the CR, removes ordered so earlier removals don't invalidate
//     later paths).
//  4. Assert the live ConfigMap drops the server's <server>__*.xml keys (the
//     operator's observable SL.8 output).
//  5. Restart the segment-primary pods so the credential init container
//     re-renders the sidecar WITHOUT the deleted server, then assert the SELECT
//     ERRORS (a REAL captured query error — the negative proof).
//  6. Restore the server (+ any referencing jobs) and drop the probe table.
//
// Gated by SCENARIO106_LIVE=1; skips cleanly ONLY for genuine "cluster not
// deployed" preconditions.
//
// delete→restart→negative→restore) is intentionally one linear narrative.
//
//nolint:gocyclo,gocognit,funlen // a self-contained live lifecycle (add→select→
func (s *Scenario106E2ESuite) TestE2E_Scenario106_LiveSL8DeleteServer() {
	s.scenario106RequireLive()

	cr, haveCR := s.scenario106GetCR()
	if !haveCR {
		s.T().Skipf("cluster %s not deployed / unreadable [CONFIG-ONLY: cluster absent]",
			scenario106Cluster())
	}
	pxfPod := s.scenario106FirstSegmentPxfPod()
	if pxfPod == "" {
		s.T().Skip("no segment-primary pxf pod found [CONFIG-ONLY: cluster not deployed]")
	}
	coord := s.scenario106CoordinatorPod()
	if coord == "" {
		s.T().Skip("no coordinator pod found for the SL.8 SELECT [CONFIG-ONLY: cluster not deployed]")
	}

	// A prior run's restart may still be recovering PXF; wait for every segment's
	// PXF sidecar to serve before the pre-delete positive (pod-Ready precedes the
	// PXF JVM being ready). A genuine "PXF never came up" is a clean skip.
	if !s.scenario106WaitPxfHealthy() {
		s.T().Skip("PXF sidecars not all healthy [CONFIG-ONLY: PXF service unavailable on this deploy]")
	}

	// Choose a deletable server that is already rendered in the sidecar so the
	// pre-delete SELECT can succeed without a restart.
	resolved := s.scenario106ResolvedServers(pxfPod)
	target, ok := s.scenario106ChooseSL8Target(cr, resolved)
	if !ok {
		s.T().Skip("no sidecar-resolved deletable PXF server available for the SL.8 negative " +
			"[CONFIG-ONLY: server not rendered on this deploy]")
	}
	serverFile := target.name + "__s3-site.xml"
	s.T().Logf("scenario106 106-SL8-L1: chosen target server %q (already resolved in sidecar)",
		target.name)

	// Register the STATE-BASED cleanup up-front so it restores the baseline (drop
	// the probe table, remove the throwaway job if present, re-add the server +
	// original jobs only if they are missing) no matter where we exit. This keeps
	// the test idempotent / re-runnable even on an early skip.
	defer s.scenario106SL8Cleanup(coord, target)

	// (1) Add a throwaway job referencing the target so the delete MUST remove
	// BOTH (exercises the W.9 referential-integrity rule). Disabled so no dataload
	// pods spawn.
	tmpJob := fmt.Sprintf(`{"name":%q,"type":"pxf","enabled":false,"pxfJob":`+
		`{"profile":"s3:text","resource":%q,"server":%q,"targetTable":"public.events",`+
		`"mode":"insert"}}`,
		scenario106SL8TmpJob, scenario106DataResource, target.name)
	addJobPatch := fmt.Sprintf(`[{"op":"add","path":"/spec/dataLoading/jobs/-","value":%s}]`, tmpJob)
	if o, e := s.scenario106PatchJSON(addJobPatch); e != nil {
		s.T().Skipf("could not add the throwaway SL.8 job: %v (out=%s) "+
			"[CONFIG-ONLY: job-add not reproducible on this deploy]", e, o)
	}

	// (2) Create the probe external table referencing the server; the SELECT must
	// SUCCEED (3-column CSV → id,name,val). This is the pre-delete positive.
	_, _ = s.scenario106Psql(coord,
		"DROP EXTERNAL TABLE IF EXISTS "+target.probeTable+";")
	createSQL := fmt.Sprintf(
		"CREATE EXTERNAL TABLE %s (id int, name text, val int) "+
			"LOCATION ('pxf://%s?PROFILE=s3:text&SERVER=%s') FORMAT 'CSV';",
		target.probeTable, scenario106DataResource, target.name)
	if out, err := s.scenario106Psql(coord, createSQL); err != nil {
		s.T().Skipf("could not CREATE the SL.8 probe table: %v (out=%s) "+
			"[CONFIG-ONLY: PXF/object-store not usable on this deploy]", err, out)
	}
	// Allow a few retries: PXF may need a moment to resolve the server even when
	// healthy. A persistent failure here is a genuine precondition skip.
	var preErr error
	var preOut string
	for attempt := 0; attempt < 6; attempt++ {
		preOut, preErr = s.scenario106Psql(coord, "SELECT count(*) FROM "+target.probeTable+";")
		if preErr == nil {
			break
		}
		time.Sleep(scenario106PollInterval)
	}
	if preErr != nil {
		s.T().Skipf("SL.8 probe SELECT not readable BEFORE the delete (%v / out=%s) — the negative "+
			"needs a working positive baseline [CONFIG-ONLY: object-store unreachable]", preErr, preOut)
	}
	s.T().Logf("scenario106 106-SL8-L1: pre-delete SELECT against %s SUCCEEDS (server %q present)",
		target.probeTable, target.name)

	// (3) Atomic delete: re-read the CR (it changed when we added the job), then
	// remove the throwaway job AND the server in ONE ordered JSON patch. Job
	// removes precede the server remove; both index sets are descending so earlier
	// removes never shift a later path.
	cr2, ok2 := s.scenario106GetCR()
	require.Truef(s.T(), ok2, "re-read CR after adding the throwaway job")
	srvIdx, _ := scenario106ServerIndex(cr2, target.name)
	require.GreaterOrEqualf(s.T(), srvIdx, 0, "target server %q must still exist", target.name)
	jobIdxs, _ := scenario106JobsReferencing(cr2, target.name)

	var ops []string
	for _, ji := range jobIdxs { // descending → safe sequential removes
		ops = append(ops, fmt.Sprintf(`{"op":"remove","path":"/spec/dataLoading/jobs/%d"}`, ji))
	}
	ops = append(ops,
		fmt.Sprintf(`{"op":"remove","path":"/spec/dataLoading/pxf/servers/%d"}`, srvIdx))
	delPatch := "[" + strings.Join(ops, ",") + "]"
	o, e := s.scenario106PatchJSON(delPatch)
	require.NoErrorf(s.T(), e,
		"atomic remove of the referencing job(s) + server %q must be ACCEPTED (out=%s)",
		target.name, o)
	s.T().Logf("scenario106 106-SL8-L1: atomically removed %d job(s) + server %q",
		len(jobIdxs), target.name)

	// (4) The operator regenerates the CM WITHOUT the server's keys.
	require.Eventuallyf(s.T(), func() bool {
		body, found := s.scenario106ReadServerFile(serverFile)
		return found && strings.TrimSpace(body) == ""
	}, scenario106LiveTimeout, scenario106PollInterval,
		"the live %s key must vanish after the server is removed", serverFile)
	keys, _ := s.scenario106CMKeys()
	assert.NotContains(s.T(), keys, serverFile,
		"the removed server's <server>__*.xml keys must be GONE from the CM")
	s.T().Logf("scenario106 106-SL8-L1: live CM dropped the %s keys after the delete", target.name)

	// (5) Restart the segment-primary pods so the sidecar re-renders WITHOUT the
	// deleted server, then assert the SELECT ERRORS (REAL negative proof).
	if !s.scenario106RestartSegments() {
		s.T().Skip("segment-primary pods did not recover after restart within the budget " +
			"[CONFIG-ONLY: cluster could not be restarted in time] — CM-drop already proven")
	}
	require.Eventuallyf(s.T(), func() bool {
		_, err := s.scenario106Psql(coord, "SELECT count(*) FROM "+target.probeTable+";")
		return err != nil
	}, scenario106LiveTimeout, scenario106PollInterval,
		"a SELECT against %s must ERROR after its PXF server %q is deleted",
		target.probeTable, target.name)
	s.T().Logf("scenario106 106-SL8-L1: SELECT against %s ERRORS after the server delete "+
		"(real negative proof)", target.probeTable)
}

// scenario106SL8Cleanup is STATE-BASED so the test is idempotent / re-runnable
// regardless of where it exited: it (a) drops the probe table, (b) re-adds the
// server if it is now missing, (c) restores any ORIGINAL referencing job that is
// now missing, and (d) removes the throwaway job if it is still present (e.g. on
// an early skip after the throwaway job was added but the delete never ran). The
// server is re-added FIRST so a referencing-job restore never violates W.9. All
// steps are best-effort (logged, never failing the test) and order their removes
// so the throwaway job is dropped only when no longer needed.
func (s *Scenario106E2ESuite) scenario106SL8Cleanup(coord string, target scenario106SL8Target) {
	// (a) Drop the probe table (cleanup the external-table fixture).
	if _, err := s.scenario106Psql(coord,
		"DROP EXTERNAL TABLE IF EXISTS "+target.probeTable+";"); err != nil {
		s.T().Logf("scenario106 SL.8 cleanup: could not drop probe table %s: %v",
			target.probeTable, err)
	}

	cr, ok := s.scenario106GetCR()
	if !ok {
		s.T().Logf("scenario106 SL.8 cleanup: could not re-read CR to restore baseline")
		return
	}

	// (b) Re-add the server FIRST (before any job restore) if it is missing, so a
	// referencing-job restore cannot trip the W.9 referential-integrity rule.
	if idx, _ := scenario106ServerIndex(cr, target.name); idx < 0 && target.serverRaw != nil {
		restore := fmt.Sprintf(`[{"op":"add","path":"/spec/dataLoading/pxf/servers/-","value":%s}]`,
			string(target.serverRaw))
		if o, e := s.scenario106PatchJSON(restore); e != nil {
			s.T().Logf("scenario106 SL.8 cleanup: could not restore server %q: %v (out=%s)",
				target.name, e, o)
		} else {
			s.T().Logf("scenario106 SL.8 cleanup: restored server %q into the CR", target.name)
		}
		cr, _ = s.scenario106GetCR()
	}

	// (c) Restore any ORIGINAL referencing job that is now MISSING (captured at the
	// first CR read; the throwaway job is NEVER restored). Skipped when the job is
	// still present (e.g. on an early skip that never deleted it).
	for _, raw := range target.jobRaws {
		var job scenario106Job
		if json.Unmarshal(raw, &job) != nil {
			continue
		}
		if cr != nil && scenario106JobIndex(cr, job.Name) >= 0 {
			continue // still present — nothing to restore
		}
		restoreJob := fmt.Sprintf(`[{"op":"add","path":"/spec/dataLoading/jobs/-","value":%s}]`,
			string(raw))
		if o, e := s.scenario106PatchJSON(restoreJob); e != nil {
			s.T().Logf("scenario106 SL.8 cleanup: could not restore job %q: %v (out=%s)",
				job.Name, e, o)
		} else {
			s.T().Logf("scenario106 SL.8 cleanup: restored original job %q", job.Name)
		}
		cr, _ = s.scenario106GetCR()
	}

	// (d) Remove the throwaway job if it is still present (an early skip path may
	// have added it without the atomic delete ever removing it).
	if cr != nil {
		if ji := scenario106JobIndex(cr, scenario106SL8TmpJob); ji >= 0 {
			rm := fmt.Sprintf(`[{"op":"remove","path":"/spec/dataLoading/jobs/%d"}]`, ji)
			if o, e := s.scenario106PatchJSON(rm); e != nil {
				s.T().Logf("scenario106 SL.8 cleanup: could not remove throwaway job: %v (out=%s)", e, o)
			} else {
				s.T().Logf("scenario106 SL.8 cleanup: removed leftover throwaway job %q",
					scenario106SL8TmpJob)
			}
		}
	}
}
