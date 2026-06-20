//go:build functional

package functional

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	webhookpkg "github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 118: Scan Scheduling and Duration Limit
// (reconciliation rules C.5, C.10, M.3 + webhook W.5) — functional
// ============================================================================
//
// This functional layer drives the PUBLIC AdminReconciler.Reconcile entrypoint
// (and CloudberryClusterValidator.ValidateCreate for W.5) over a fake-client
// TestK8sEnv with an injected dbFactory, mirroring
// scenario117_recommendation_scan_test.go:
//
//   - 118a-C5-schedule-F: a reconcile with the scan enabled + a configured cron
//     creates the `<cluster>-recommendation-scan` CronJob carrying that schedule
//     verbatim (C.5) and the recommendation_scan_cronjob gauge is 1.
//   - 118a-M3-duration-F: a reconcile populates the
//     recommendation_scan_duration_seconds histogram (M.3, _count +1).
//   - 118b-C10-cap-F / 118b-TRUNCATE-F / 118b-M3-capped-F: a tiny scanDuration
//     ("10ms") + a BLOCKING mock DB trips the cap -> reconcile is non-fatal,
//     status.recommendationScanTruncated=true, the truncation counter
//     increments, only completed types are counted, and the duration histogram
//     records the (capped) sample.
//   - 118b no-truncate: a generous scanDuration ("2h") + a fast DB leaves the
//     flag false (never sticky).
//   - 118-VALIDATE-duration-F (W.5): ValidateCreate rejects an invalid
//     scanDuration (enabled-gated) and admits valid/empty/disabled+invalid.
//   - 118-DISABLED-noop / 118-CONTROL: the disabled no-op and the healthy
//     control.
//
// It is catalog-honest via a coverage test that keeps the -F matrix from
// silently dropping a rule. The live proof is the KUBECONFIG/SCENARIO118_LIVE-
// gated Scenario 118 integration/e2e Part B.
// ============================================================================

// Scenario118Suite drives AdminReconciler.Reconcile + ValidateCreate over a
// recommendation-scan cluster and asserts the C.5/C.10/M.3/W.5 scan effects.
type Scenario118Suite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario118(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario118Suite))
}

func (s *Scenario118Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario118Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario118ScanCluster builds a base-valid running cluster with the
// recommendation scan enabled (DiskMonitoring on so the storage path engages),
// the configured schedule, and the supplied scanDuration.
func scenario118ScanCluster(name, schedule, scanDuration string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            schedule,
			BloatThreshold:      20,
			SkewThreshold:       30,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 40,
			ScanDuration:        scanDuration,
		},
	}
	return cluster
}

// scenario118Reconciler builds an AdminReconciler over a fake-client TestK8sEnv
// seeded with the supplied cluster, wired with the supplied dbFactory and a real
// PrometheusRecorder over reg so the metrics can be inspected.
func (s *Scenario118Suite) scenario118Reconciler(
	cluster *cbv1alpha1.CloudberryCluster,
	dbFactory db.DBClientFactory,
	reg *prometheus.Registry,
) *controller.AdminReconciler {
	s.env = testutil.NewTestK8sEnv(cluster)
	return controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), dbFactory, metrics.NewPrometheusRecorder(reg), s.env.Logger,
	)
}

// scenario118BlockingDB returns a MockDBClient whose four Get* recommendation
// queries BLOCK on the passed context (select on ctx.Done() with a long sleep)
// so a tiny shared scanDuration deadline trips context.DeadlineExceeded
// mid-scan. The block is far longer than any test cap so the deadline always
// wins.
func scenario118BlockingDB(block time.Duration) *testutil.MockDBClient {
	wait := func(ctx context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(block):
			return nil, nil
		}
	}
	return &testutil.MockDBClient{
		GetBloatRecommendationsFunc:      wait,
		GetSkewRecommendationsFunc:       wait,
		GetAgeRecommendationsFunc:        wait,
		GetIndexBloatRecommendationsFunc: wait,
	}
}

// scenario118Rec builds a recommendation row of a given type/schema/table/ratio.
func scenario118Rec(recType, schema, table string, ratio float64) db.Recommendation {
	return db.Recommendation{Type: recType, Schema: schema, Table: table, Ratio: ratio}
}

// storageConfiguredTrue118 reports whether StorageConfigured is True.
func storageConfiguredTrue118(cluster *cbv1alpha1.CloudberryCluster) bool {
	for _, c := range cluster.Status.Conditions {
		if c.Type == "StorageConfigured" {
			return string(c.Status) == "True"
		}
	}
	return false
}

// scenario118CounterValue gathers the counter series whose labels match all of
// the provided key/value pairs (found=false when no matching series is present).
func scenario118CounterValue(
	t require.TestingT, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if scenario118LabelsMatch(m.GetLabel(), want) {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// scenario118HistogramCount gathers the histogram sample count whose labels
// match all of the provided key/value pairs.
func scenario118HistogramCount(
	t require.TestingT, reg *prometheus.Registry, name string, want map[string]string,
) (uint64, bool) {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if scenario118LabelsMatch(m.GetLabel(), want) {
				return m.GetHistogram().GetSampleCount(), true
			}
		}
	}
	return 0, false
}

// scenario118GaugeValue gathers the gauge series whose labels match all of the
// provided key/value pairs.
func scenario118GaugeValue(
	t require.TestingT, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if scenario118LabelsMatch(m.GetLabel(), want) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// scenario118LabelsMatch reports whether every key/value in want is present in
// labels.
func scenario118LabelsMatch(labels []*dto.LabelPair, want map[string]string) bool {
	have := map[string]string{}
	for _, lp := range labels {
		have[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// TestFunctional_Scenario118_ScheduleCronJobAndDuration covers 118a-C5-schedule-F
// / 118a-M3-duration-F / 118-CONTROL: a reconcile with the scan enabled creates
// the `<cluster>-recommendation-scan` CronJob carrying the configured schedule
// verbatim (C.5), publishes the recommendation_scan_cronjob gauge, populates the
// recommendation_scan_duration_seconds histogram (M.3, _count +1), and returns
// no error (CONTROL) with the truncation flag false.
func (s *Scenario118Suite) TestFunctional_Scenario118_ScheduleCronJobAndDuration() {
	const schedule = cases.Scenario118NearFutureSchedule
	cluster := scenario118ScanCluster("s118-sched", schedule, "2h")
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario118Rec("bloat", "public", "t1", 25)}, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario118Reconciler(cluster, dbFactory, reg)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "118-CONTROL: the reconcile must return no error")
	assert.NotZero(s.T(), result.RequeueAfter, "reconcileStorage must proceed past the gate")

	// 118a-C5-schedule-F: the CronJob exists with the configured schedule verbatim.
	cj := &batchv1.CronJob{}
	err = s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj)
	require.NoError(s.T(), err, "118a-C5-schedule-F: the recommendation-scan CronJob must be created")
	assert.Equal(s.T(), cluster.Name+"-"+cases.Scenario118CronJobSuffix, cj.Name)
	assert.Equal(s.T(), schedule, cj.Spec.Schedule,
		"118a-C5-schedule-F: the CronJob must carry the configured cron verbatim")
	assert.Equal(s.T(), batchv1.ForbidConcurrent, cj.Spec.ConcurrencyPolicy)

	// recommendation_scan_cronjob gauge published as 1.
	g, found := scenario118GaugeValue(s.T(), reg, cases.Scenario118CronJobMetricName,
		map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace})
	require.True(s.T(), found, "the recommendation_scan_cronjob gauge must be published")
	assert.InDelta(s.T(), 1.0, g, 0.001)

	// 118a-M3-duration-F: the duration histogram recorded one sample.
	cnt, found := scenario118HistogramCount(s.T(), reg, cases.Scenario118DurationMetricName,
		map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace})
	require.True(s.T(), found, "118a-M3-duration-F: the duration histogram must be populated")
	assert.Equal(s.T(), uint64(1), cnt, "118a-M3-duration-F: histogram _count must be 1 after a scan")

	// 118-CONTROL: completed scan -> truncation flag false.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.False(s.T(), updated.Status.RecommendationScanTruncated,
		"118-CONTROL: a completed scan must leave RecommendationScanTruncated false")
	require.NotNil(s.T(), updated.Status.LastRecommendationScanTime,
		"118-CONTROL: LastRecommendationScanTime must be set each scan")
	assert.True(s.T(), storageConfiguredTrue118(updated), "118-CONTROL: StorageConfigured must be True")
}

// TestFunctional_Scenario118_CapTruncates covers 118b-C10-cap-F /
// 118b-TRUNCATE-F / 118b-M3-capped-F: a tiny scanDuration ("10ms") + a blocking
// mock DB trips the cap -> the reconcile is non-fatal, the truncation flag is
// set and persisted, the truncation counter increments, the un-run types
// contribute 0 (no fabrication), and the duration histogram records the
// (capped) sample.
func (s *Scenario118Suite) TestFunctional_Scenario118_CapTruncates() {
	cluster := scenario118ScanCluster("s118-cap", cases.Scenario118NearFutureSchedule,
		cases.Scenario118TinyScanDuration)
	cluster.Status.RecommendationCount = 42 // stale prior must be replaced.
	dbFactory := &testutil.MockDBClientFactory{Client: scenario118BlockingDB(2 * time.Second)}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario118Reconciler(cluster, dbFactory, reg)

	start := time.Now()
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	elapsed := time.Since(start)
	require.NoError(s.T(), err, "118b-C10-cap-F: a capped scan must be non-fatal")

	// The capped scan finishes near the cap, not the full 2s block (shared budget).
	assert.Less(s.T(), elapsed, 1500*time.Millisecond,
		"118b-C10-cap-F: the capped scan must not run the full block")

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// 118b-TRUNCATE-F: the flag is set + persisted.
	assert.True(s.T(), updated.Status.RecommendationScanTruncated,
		"118b-TRUNCATE-F: a capped scan must set status.recommendationScanTruncated=true")
	require.NotNil(s.T(), updated.Status.LastRecommendationScanTime,
		"118b-TRUNCATE-F: LastRecommendationScanTime must be set on a capped scan")
	assert.True(s.T(), storageConfiguredTrue118(updated),
		"118b-C10-cap-F: StorageConfigured must stay True under the cap")

	// The truncation counter incremented exactly once.
	v, found := scenario118CounterValue(s.T(), reg, cases.Scenario118TruncatedMetricName,
		map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace})
	require.True(s.T(), found, "118b-TRUNCATE-F: the truncation counter must be published")
	assert.InDelta(s.T(), 1.0, v, 0.001)

	// Un-run types contribute 0 -> no fabrication. The blocking DB returns no
	// rows, so every per-type gauge is 0 (a 0 recommendationCount is
	// omitempty-dropped from the merge-patch, so the honesty is asserted via the
	// per-type gauges, which are published every scan).
	for _, recType := range []string{"bloat", "skew", "age", "index_bloat"} {
		g, gFound := scenario118GaugeValue(s.T(), reg, "cloudberry_recommendations_total",
			map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace, "type": recType})
		require.Truef(s.T(), gFound, "118b-TRUNCATE-F: the %s gauge must still be published", recType)
		assert.InDeltaf(s.T(), 0.0, g, 0.001,
			"118b-TRUNCATE-F: type %s must be 0 (un-run, not fabricated)", recType)
	}

	// 118b-M3-capped-F: the histogram recorded one (capped) sample.
	cnt, found := scenario118HistogramCount(s.T(), reg, cases.Scenario118DurationMetricName,
		map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace})
	require.True(s.T(), found, "118b-M3-capped-F: the duration histogram must record the capped run")
	assert.Equal(s.T(), uint64(1), cnt)
}

// TestFunctional_Scenario118_NoTruncateGenerousCap covers the 118b no-truncate
// leg: a generous scanDuration ("2h") + a fast DB completes -> the truncation
// flag resets to false (never sticky) and the counter is absent.
func (s *Scenario118Suite) TestFunctional_Scenario118_NoTruncateGenerousCap() {
	cluster := scenario118ScanCluster("s118-notrunc", cases.Scenario118NearFutureSchedule, "2h")
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario118Rec("bloat", "public", "t1", 25)}, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario118Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.False(s.T(), updated.Status.RecommendationScanTruncated,
		"118b no-truncate: a completed scan must reset the truncation flag to false (never sticky)")
	_, found := scenario118CounterValue(s.T(), reg, cases.Scenario118TruncatedMetricName,
		map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace})
	assert.False(s.T(), found,
		"118b no-truncate: a non-truncated scan must NOT increment the truncation counter")
}

// TestFunctional_Scenario118_W5Validation covers 118-VALIDATE-duration-F (W.5):
// ValidateCreate (the same chain the admission webhook uses) REJECTS an enabled
// scan with an invalid scanDuration and ADMITs valid / empty / disabled+invalid.
func (s *Scenario118Suite) TestFunctional_Scenario118_W5Validation() {
	tests := []struct {
		name         string
		enabled      bool
		scanDuration string
		expectErr    bool
	}{
		{name: "enabled + invalid -> deny", enabled: true, scanDuration: "banana", expectErr: true},
		{name: "enabled + bare number -> deny", enabled: true, scanDuration: "30", expectErr: true},
		{name: "enabled + 30s -> admit", enabled: true, scanDuration: "30s", expectErr: false},
		{name: "enabled + 2h -> admit", enabled: true, scanDuration: "2h", expectErr: false},
		{name: "enabled + empty -> admit", enabled: true, scanDuration: "", expectErr: false},
		{name: "disabled + invalid -> admit", enabled: false, scanDuration: "banana", expectErr: false},
	}
	validator := webhookpkg.NewCloudberryClusterValidator(nil)
	for _, tt := range tests {
		tt := tt
		s.Run(tt.name, func() {
			cluster := scenario118ScanCluster("s118-w5", cases.Scenario118NearFutureSchedule, tt.scanDuration)
			cluster.Spec.Storage.RecommendationScan.Enabled = tt.enabled

			_, err := validator.ValidateCreate(s.ctx, cluster)
			if tt.expectErr {
				require.Error(s.T(), err, "118-VALIDATE-duration-F: %s must DENY", tt.name)
				assert.Contains(s.T(), err.Error(), "storage.recommendationScan.scanDuration")
				assert.Contains(s.T(), err.Error(), "valid Go duration")
				assert.Contains(s.T(), err.Error(), tt.scanDuration)
			} else {
				require.NoError(s.T(), err, "118-VALIDATE-duration-F: %s must ADMIT", tt.name)
			}
		})
	}
}

// TestFunctional_Scenario118_DisabledNoOp covers 118-DISABLED-noop:
// recommendationScan nil / enabled:false -> recordRecommendations is NOT run: no
// cap, no duration observe, no truncation flag/counter, and the DB factory is
// never reached for the scan. Per the C.4 clear-on-disable contract, any stale
// count is reset to 0 so an enabled->disabled scan does not leave a frozen count.
func (s *Scenario118Suite) TestFunctional_Scenario118_DisabledNoOp() {
	tests := []struct {
		name string
		scan *cbv1alpha1.RecommendationScanSpec
	}{
		{name: "scan nil", scan: nil},
		{name: "scan disabled", scan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false, ScanDuration: "10ms",
		}},
	}
	for _, tt := range tests {
		tt := tt
		s.Run(tt.name, func() {
			cluster := testutil.NewClusterBuilder("s118-disabled", "default").
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				Build()
			cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring:     true,
				RecommendationScan: tt.scan,
			}
			cluster.Status.RecommendationCount = 7 // prior value must survive.

			// scanCalls counts ONLY the recommendation-scan Get* invocations
			// (the disk-monitoring path may construct a DB client, but the scan
			// Get* must never be reached when the scan is disabled).
			var scanCalls int32
			dbFactory := &scanCountingDBFactory118{scanCalls: &scanCalls}
			reg := prometheus.NewRegistry()
			reconciler := s.scenario118Reconciler(cluster, dbFactory, reg)

			_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
			require.NoError(s.T(), err)

			updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
			require.NoError(s.T(), err)
			assert.Equal(s.T(), int32(0), updated.Status.RecommendationCount,
				"118-DISABLED-noop: stale count must be reset to 0 (C.4 clear-on-disable)")
			assert.False(s.T(), updated.Status.RecommendationScanTruncated,
				"118-DISABLED-noop: a disabled scan must not set the truncation flag")
			assert.Nil(s.T(), updated.Status.LastRecommendationScanTime,
				"118-DISABLED-noop: a disabled scan must not set the scan timestamp")
			assert.Equal(s.T(), int32(0), atomic.LoadInt32(&scanCalls),
				"118-DISABLED-noop: the scan Get* path must never be reached")
			_, found := scenario118CounterValue(s.T(), reg, cases.Scenario118TruncatedMetricName,
				map[string]string{"cluster": cluster.Name, "namespace": cluster.Namespace})
			assert.False(s.T(), found, "118-DISABLED-noop: a disabled scan must not increment the counter")
		})
	}
}

// scanCountingDBFactory118 returns a client whose recommendation-scan Get* hooks
// atomically count their invocations so the disabled no-op case can assert the
// SCAN path is never reached (the disk-monitoring path may still construct a
// client for GetDiskUsagePercent etc.). The scan hooks would return non-zero
// recs if reached, so the disabled path cannot accidentally count.
type scanCountingDBFactory118 struct {
	scanCalls *int32
}

func (f *scanCountingDBFactory118) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	hook := func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
		atomic.AddInt32(f.scanCalls, 1)
		return []db.Recommendation{scenario118Rec("bloat", "public", "x", 99)}, nil
	}
	return &testutil.MockDBClient{
		GetBloatRecommendationsFunc:      hook,
		GetSkewRecommendationsFunc:       hook,
		GetAgeRecommendationsFunc:        hook,
		GetIndexBloatRecommendationsFunc: hook,
	}, nil
}

// TestFunctional_Scenario118_CatalogCoversFunctionalRows asserts every
// functional (-F) catalog row is honest: a known Req family and a non-empty
// Gate/Expected/Description — so the matrix cannot silently drop a rule.
func (s *Scenario118Suite) TestFunctional_Scenario118_CatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"C.5": true, "C.10": true, "M.3": true, "TRUNCATE": true,
		"W.5": true, "DISABLED": true, "CONTROL": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario118Cases() {
		if c.Layer != cases.Scenario118LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Expected, "%s must carry an Expected token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req],
			"functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario118 %s (%s, gate=%s): %s", c.ID, c.Req, c.Gate, c.Expected)
	}
	for _, req := range []string{"C.5", "C.10", "M.3", "TRUNCATE", "W.5"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover rule %s", req)
	}
}
