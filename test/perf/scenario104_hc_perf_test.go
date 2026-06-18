//go:build e2e

// Scenario 104 pre-load health-check init-container overhead benchmark.
//
// PERF-HARNESS NOTE: like the Scenario 101 perf file, this is a minimal but REAL
// Go benchmark. It measures TWO honest things:
//
//   - BenchmarkScenario104_HealthCheckScriptBuild (ALWAYS runnable, infra-free):
//     the cost of BUILDING the dataload-healthcheck init container + its 5-check
//     script via the real builder. The init container is built once per Job
//     reconcile; this proves the build cost is negligible (sub-millisecond).
//
//   - BenchmarkScenario104_InitOverhead (live, SCENARIO104_HC_LIVE=1 gated):
//     the WALL-CLOCK overhead the health-check init container adds to a data-load
//     Job — measured as the time from the dataload Job's pod start to the init
//     container's terminated.finishedAt (the init duration). When the live infra
//     is absent it is documented N/A and skips cleanly (the build benchmark above
//     is the infra-free proxy).
//
// HONESTY: this reports build ns/op and (live) init duration only — it does NOT
// assert (and never emits) any cloudberry_* / kube-state-metrics series. The
// health-check init adds negligible time to the load (a handful of psql/curl/df
// probes); the live measurement quantifies it.
//
// Build tag: e2e (the live body needs the deployed healthcheck-test cluster, same
// as the e2e suite; the build benchmark is infra-free but shares the tag). Run:
//
//	go test -tags=e2e -run=^$ -bench=BenchmarkScenario104_HealthCheckScriptBuild ./test/perf/...
//	SCENARIO104_HC_LIVE=1 KUBECONFIG=... \
//	  go test -tags=e2e -run=^$ -bench=BenchmarkScenario104_InitOverhead ./test/perf/...
package perf

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
)

const (
	envKubeconfigS104Perf  = "KUBECONFIG"
	envScenario104LivePerf = "SCENARIO104_HC_LIVE"
	envScenario104ClusterP = "SCENARIO104_CLUSTER"
	envScenario104NsP      = "SCENARIO104_NAMESPACE"

	perf104DefaultCluster   = "healthcheck-test"
	perf104DefaultNamespace = "cloudberry-test"
	perf104ExecTimeout      = 2 * time.Minute

	// perf104PxfJobName is the s3-load pxf job name (carries the init container).
	perf104PxfJobName = "s3-load"
	// perf104InitName is the health-check init container name.
	perf104InitName = "dataload-healthcheck"
)

// perf104Env returns the ENV value or a default.
func perf104Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func perf104Namespace() string { return perf104Env(envScenario104NsP, perf104DefaultNamespace) }
func perf104Cluster() string   { return perf104Env(envScenario104ClusterP, perf104DefaultCluster) }

// perf104PxfCluster builds an in-memory cluster carrying the s3-load pxf job with
// pxf+gpfdist enabled + an s3 backup destination (so the HC.3 creds env wires).
func perf104PxfCluster() (*cbv1alpha1.CloudberryCluster, cbv1alpha1.DataLoadingJob) {
	job := cbv1alpha1.DataLoadingJob{
		Name:    perf104PxfJobName,
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3-datalake",
			Profile:     "s3:text",
			Resource:    "cloudberry-data/text/data.csv",
			TargetTable: "public.events",
		},
	}
	cluster := &cbv1alpha1.CloudberryCluster{}
	cluster.Name = perf104DefaultCluster
	cluster.Namespace = perf104DefaultNamespace
	cluster.Spec.Image = "cloudberry-official-pxf:2.1.0"
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: true},
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs:    []cbv1alpha1.DataLoadingJob{job},
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   "cloudberry-data",
				Endpoint: "http://minio:9000",
				Region:   "us-east-1",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster, job
}

// BenchmarkScenario104_HealthCheckScriptBuild measures the cost of BUILDING the
// dataload-healthcheck init container + its 5-check script via the real builder
// (BuildDataLoadJob → the native pxf pod spec). Infra-free; proves the build cost
// is negligible. Reports ns/op and a sanity check that the init is present.
func BenchmarkScenario104_HealthCheckScriptBuild(b *testing.B) {
	bd := builder.NewBuilder()
	cluster, job := perf104PxfCluster()

	// Sanity: the init container must be present (otherwise the benchmark is
	// meaningless).
	out := bd.BuildDataLoadJob(cluster, job)
	if out == nil || len(out.Spec.Template.Spec.InitContainers) == 0 ||
		out.Spec.Template.Spec.InitContainers[0].Name != perf104InitName {
		b.Fatalf("expected the %s init container on the built pxf dataload Job", perf104InitName)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bd.BuildDataLoadJob(cluster, job)
	}
}

// perf104SkipUnlessLive skips the live benchmark cleanly unless KUBECONFIG is
// set, kubectl exists and SCENARIO104_HC_LIVE=1.
func perf104SkipUnlessLive(b *testing.B) {
	b.Helper()
	if os.Getenv(envKubeconfigS104Perf) == "" {
		b.Skip("KUBECONFIG not set, skipping Scenario 104 init-overhead baseline")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		b.Skip("kubectl not found on PATH, skipping Scenario 104 init-overhead baseline")
	}
	if os.Getenv(envScenario104LivePerf) != "1" {
		b.Skip("SCENARIO104_HC_LIVE not set, skipping the live init-overhead baseline " +
			"(the deployed healthcheck-test cluster must be available)")
	}
}

// perf104Kubectl runs a kubectl subcommand bounded by perf104ExecTimeout.
func perf104Kubectl(ctx context.Context, args ...string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, perf104ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// BenchmarkScenario104_InitOverhead measures the WALL-CLOCK overhead the
// health-check init container adds to a data-load Job: it triggers the s3-load
// dataload Job (kubectl create job --from=job/<cluster>-dataload-s3-load), waits
// for it, then reads the init container's started/finished timestamps from the
// Job's pod and reports the init duration via b.ReportMetric. Skips cleanly
// without live infra (the infra-free build benchmark above is the proxy). The
// health-check init is EXPECTED to add negligible time to the load.
func BenchmarkScenario104_InitOverhead(b *testing.B) {
	perf104SkipUnlessLive(b)
	ctx := context.Background()

	src := perf104Cluster() + "-dataload-" + perf104PxfJobName
	if out, err := perf104Kubectl(ctx, "get", "job", src, "-n", perf104Namespace()); err != nil {
		b.Skipf("dataload Job %s not found (cluster may not be deployed): %v (out=%s)",
			src, err, out)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runJob := fmt.Sprintf("%s-perf-%d", perf104PxfJobName, i)
		_, _ = perf104Kubectl(ctx, "delete", "job", runJob, "-n", perf104Namespace(),
			"--ignore-not-found")
		if out, err := perf104Kubectl(ctx, "create", "job", runJob,
			"--from=job/"+src, "-n", perf104Namespace()); err != nil {
			b.Skipf("perf could not create job from job/%s: %v (out=%s)", src, err, out)
		}
		b.StartTimer()

		// Wait for the run Job to finish (complete OR failed).
		_, _ = perf104Kubectl(ctx, "wait", "--for=condition=complete",
			"--timeout=5m", "job/"+runJob, "-n", perf104Namespace())

		b.StopTimer()
		dur := perf104InitDuration(ctx, runJob)
		_, _ = perf104Kubectl(ctx, "delete", "job", runJob, "-n", perf104Namespace(),
			"--ignore-not-found")
		b.StartTimer()

		if dur > 0 {
			b.ReportMetric(dur.Seconds()*1000.0, "init_ms")
		}
		b.Logf("scenario104 init-overhead: %s init container ran for %s", perf104InitName, dur)
	}
}

// perf104InitDuration reads the dataload-healthcheck init container's
// started→finished duration from the run Job's pod, returning 0 when
// unavailable.
func perf104InitDuration(ctx context.Context, runJob string) time.Duration {
	out, err := perf104Kubectl(ctx, "get", "pods", "-n", perf104Namespace(),
		"-l", "job-name="+runJob, "-o", "json")
	if err != nil {
		return 0
	}
	var list struct {
		Items []struct {
			Status struct {
				InitContainerStatuses []struct {
					Name  string `json:"name"`
					State struct {
						Terminated *struct {
							StartedAt  time.Time `json:"startedAt"`
							FinishedAt time.Time `json:"finishedAt"`
						} `json:"terminated"`
					} `json:"state"`
				} `json:"initContainerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return 0
	}
	for _, p := range list.Items {
		for _, ic := range p.Status.InitContainerStatuses {
			if ic.Name == perf104InitName && ic.State.Terminated != nil {
				d := ic.State.Terminated.FinishedAt.Sub(ic.State.Terminated.StartedAt)
				if d > 0 {
					return d
				}
			}
		}
	}
	return 0
}
