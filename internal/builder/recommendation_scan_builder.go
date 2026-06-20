// Package builder: recommendation_scan_builder.go constructs the scheduled
// storage recommendation-scan CronJob (spec 13 §Reconciliation C.5). It mirrors
// the established Build*CronJob lifecycle (BuildBackupCronJob /
// BuildDataLoadCronJob): owner-referenced to the cluster, ForbidConcurrent, and
// history limits of 3.
package builder

import (
	"fmt"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// recommendationScanContainerName is the container name for the scheduled
	// recommendation-scan CronJob pod.
	recommendationScanContainerName = "recommendation-scan"

	// recommendationScanJobsHistoryLimit caps how many successful/failed Jobs the
	// CronJob retains, matching the backup/data-loading CronJob lifecycle.
	recommendationScanJobsHistoryLimit int32 = 3

	// Env var NAMES carrying the scan thresholds + duration into the CronJob pod
	// so the scan is parameterized and inspectable (spec 13 §C.3). They are
	// informational/inspectable: the scaffold probe below does not yet act on
	// them (the real bloat/skew/age/index-bloat scan SQL is future work).
	envScanBloatThreshold      = "SCAN_BLOAT_THRESHOLD"
	envScanSkewThreshold       = "SCAN_SKEW_THRESHOLD"
	envScanAgeThreshold        = "SCAN_AGE_THRESHOLD"
	envScanIndexBloatThreshold = "SCAN_INDEX_BLOAT_THRESHOLD"
	envScanDuration            = "SCAN_DURATION"
)

// recommendationScanLabels returns the standard labels for the recommendation-scan
// CronJob (CommonLabels + component=recommendation-scan).
func recommendationScanLabels(cluster string) map[string]string {
	return util.CommonLabels(cluster, util.ComponentRecommendationScan)
}

// BuildRecommendationScanCronJob builds the scheduled storage recommendation-scan
// CronJob (spec 13 §Reconciliation C.5). It returns nil — the nil-means-delete
// contract shared with BuildBackupCronJob, so the ensure helper GCs any stale
// CronJob — when storage management is absent, disk monitoring is off, the
// recommendation scan is disabled, or no schedule is configured (the schedule is
// webhook-defaulted to "0 3 * * 0" when enabled — Scenario 114).
func (b *DefaultBuilder) BuildRecommendationScanCronJob(
	cluster *cbv1alpha1.CloudberryCluster,
) *batchv1.CronJob {
	s := cluster.Spec.Storage
	if s == nil || !s.DiskMonitoring ||
		s.RecommendationScan == nil || !s.RecommendationScan.Enabled ||
		s.RecommendationScan.Schedule == "" {
		return nil
	}

	labels := recommendationScanLabels(cluster.Name)
	historyLimit := recommendationScanJobsHistoryLimit
	concurrency := batchv1.ForbidConcurrent
	podSpec := buildRecommendationScanPodSpec(cluster)

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.RecommendationScanCronJobName(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   s.RecommendationScan.Schedule,
			ConcurrencyPolicy:          concurrency,
			SuccessfulJobsHistoryLimit: &historyLimit,
			FailedJobsHistoryLimit:     &historyLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec:       podSpec,
					},
				},
			},
		},
	}
}

// buildRecommendationScanPodSpec builds the HONEST scaffold pod spec the
// recommendation-scan CronJob runs. It connects to the coordinator over psql and
// runs a single read-only, side-effect-free probe (SELECT 1) using the admin
// password sourced from the cluster's admin Secret (never embedded as plaintext).
//
// SCAFFOLD NOTICE: this is a documented placeholder. The real recommendation-scan
// SQL (bloat/skew/age/index-bloat scans gated by the threshold env vars below) is
// future work. This CronJob proves the SCHEDULE is materialized (spec 13 §C.5) —
// it does NOT fake a production scan or fabricate scan output. The thresholds and
// scan duration are passed as env vars so the run is parameterized and
// inspectable for the eventual real scan body.
func buildRecommendationScanPodSpec(cluster *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
	coordinatorSvc := util.CoordinatorServiceName(cluster.Name)

	container := corev1.Container{
		Name:  recommendationScanContainerName,
		Image: cluster.Spec.Image,
		// Read-only, side-effect-free connectivity probe. NOT a real scan: see
		// the SCAFFOLD NOTICE on buildRecommendationScanPodSpec.
		Command: []string{
			"psql",
			psqlHostFlag, coordinatorSvc,
			psqlUserFlag, util.DefaultAdminUser,
			psqlDBFlag, databasePostgres,
			psqlCommandFlag, "SELECT 1",
		},
		Env: buildRecommendationScanEnv(cluster),
	}

	return corev1.PodSpec{
		// OnFailure so a transient connectivity failure retries within the Job
		// rather than spawning a fresh pod.
		RestartPolicy: corev1.RestartPolicyOnFailure,
		Containers:    []corev1.Container{container},
	}
}

// buildRecommendationScanEnv returns the env vars for the recommendation-scan
// container: PGPASSWORD sourced from the admin Secret (never plaintext) plus the
// scan thresholds and duration (spec 13 §C.3) so the scan is parameterized and
// inspectable.
func buildRecommendationScanEnv(cluster *cbv1alpha1.CloudberryCluster) []corev1.EnvVar {
	scan := cluster.Spec.Storage.RecommendationScan

	return []corev1.EnvVar{
		{
			Name: envPGPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.AdminPasswordSecretName(cluster.Name),
					},
					Key: secretKeyPassword,
				},
			},
		},
		{Name: envScanBloatThreshold, Value: strconv.Itoa(int(scan.BloatThreshold))},
		{Name: envScanSkewThreshold, Value: strconv.Itoa(int(scan.SkewThreshold))},
		{Name: envScanAgeThreshold, Value: fmt.Sprintf("%d", scan.AgeThreshold)},
		{Name: envScanIndexBloatThreshold, Value: strconv.Itoa(int(scan.IndexBloatThreshold))},
		{Name: envScanDuration, Value: scan.ScanDuration},
	}
}
