package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// boolPtr returns a pointer to b (test helper for the *bool Continuous field).
func boolPtr(b bool) *bool { return &b }

// kafkaCdcJob returns the canonical continuous kafka-cdc job (Scenario 102 §6.2):
// continuous, batchSize 10000, flushInterval 30s, profile kafka, resource the
// cloudberry-cdc topic, referencing the kafka-connector custom server.
func kafkaCdcJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "kafka-cdc",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:        "kafka-connector",
			Profile:       "kafka",
			Resource:      "cloudberry-cdc",
			TargetTable:   "public.kafka_events",
			Continuous:    boolPtr(true),
			BatchSize:     10000,
			FlushInterval: "30s",
		},
	}
}

// jobContainerEnv returns the single dataload container's env as a name→EnvVar
// map for the given built Job.
func jobContainerEnv(t *testing.T, env []corev1.EnvVar) map[string]corev1.EnvVar {
	t.Helper()
	m := map[string]corev1.EnvVar{}
	for _, e := range env {
		m[e.Name] = e
	}
	return m
}

// TestBuildDataLoadJob_CBKEnv_Continuous asserts the CBK_* streaming env on the
// dataload Job container for a continuous kafka pxf job (U5 / SC102-J43/J44/J45):
// CBK_CONTINUOUS=true, CBK_BATCH_SIZE=10000, CBK_FLUSH_INTERVAL=30s.
func TestBuildDataLoadJob_CBKEnv_Continuous(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	env := jobContainerEnv(t, out.Spec.Template.Spec.Containers[0].Env)

	require.Contains(t, env, "CBK_CONTINUOUS")
	assert.Equal(t, "true", env["CBK_CONTINUOUS"].Value)
	require.Contains(t, env, "CBK_BATCH_SIZE")
	assert.Equal(t, "10000", env["CBK_BATCH_SIZE"].Value)
	require.Contains(t, env, "CBK_FLUSH_INTERVAL")
	assert.Equal(t, "30s", env["CBK_FLUSH_INTERVAL"].Value)
}

// TestBuildDataLoadJob_CBKEnv_NonContinuous asserts a non-continuous pxf job with
// unset batchSize/flushInterval yields CBK_CONTINUOUS=false and NO
// CBK_BATCH_SIZE / CBK_FLUSH_INTERVAL (omitted when zero/empty) (U5).
func TestBuildDataLoadJob_CBKEnv_NonContinuous(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob()
	job.PxfJob.Continuous = boolPtr(false)
	job.PxfJob.BatchSize = 0
	job.PxfJob.FlushInterval = ""
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	env := jobContainerEnv(t, out.Spec.Template.Spec.Containers[0].Env)

	require.Contains(t, env, "CBK_CONTINUOUS")
	assert.Equal(t, "false", env["CBK_CONTINUOUS"].Value)
	assert.NotContains(t, env, "CBK_BATCH_SIZE", "unset batchSize must omit CBK_BATCH_SIZE")
	assert.NotContains(t, env, "CBK_FLUSH_INTERVAL", "unset flushInterval must omit CBK_FLUSH_INTERVAL")
}

// TestBuildDataLoadJob_CBKEnv_ContinuousNilDefaultFalse asserts a pxf job with a
// nil Continuous pointer still emits CBK_CONTINUOUS=false (default), and that a
// non-zero batchSize/non-empty flushInterval are still passed through (U5).
func TestBuildDataLoadJob_CBKEnv_ContinuousNilDefaultFalse(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob() // s3:parquet, no streaming knobs
	job.PxfJob.BatchSize = 500
	job.PxfJob.FlushInterval = "1m"
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	env := jobContainerEnv(t, out.Spec.Template.Spec.Containers[0].Env)

	require.Contains(t, env, "CBK_CONTINUOUS")
	assert.Equal(t, "false", env["CBK_CONTINUOUS"].Value)
	assert.Equal(t, "500", env["CBK_BATCH_SIZE"].Value)
	assert.Equal(t, "1m", env["CBK_FLUSH_INTERVAL"].Value)
}

// TestBuildDataLoadJob_CBKEnv_GploadNone asserts a gpload (non-pxf) job carries
// NO CBK_* env at all — the non-pxf container env is byte-unchanged (U5 /
// SC102-J43/J44/J45 negative path).
func TestBuildDataLoadJob_CBKEnv_GploadNone(t *testing.T) {
	b := NewBuilder()
	job := gploadTestJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	env := jobContainerEnv(t, out.Spec.Template.Spec.Containers[0].Env)

	for _, k := range []string{"CBK_CONTINUOUS", "CBK_BATCH_SIZE", "CBK_FLUSH_INTERVAL"} {
		assert.NotContains(t, env, k, "gpload job must carry no %s", k)
	}
}

// TestBuildPxfStreamingEnv_Matrix exercises the pure buildPxfStreamingEnv helper
// directly across the matrix (U5): the CBK_* values, omissions, and the nil-for-
// non-pxf behavior.
func TestBuildPxfStreamingEnv_Matrix(t *testing.T) {
	t.Run("non-pxf job returns nil", func(t *testing.T) {
		assert.Nil(t, buildPxfStreamingEnv(gploadTestJob()))
	})
	t.Run("pxf job with nil PxfJob returns nil", func(t *testing.T) {
		job := cbv1alpha1.DataLoadingJob{Name: "x", Type: "pxf"}
		assert.Nil(t, buildPxfStreamingEnv(job))
	})
	t.Run("continuous true + full knobs", func(t *testing.T) {
		env := buildPxfStreamingEnv(kafkaCdcJob())
		m := jobContainerEnv(t, env)
		assert.Equal(t, "true", m["CBK_CONTINUOUS"].Value)
		assert.Equal(t, "10000", m["CBK_BATCH_SIZE"].Value)
		assert.Equal(t, "30s", m["CBK_FLUSH_INTERVAL"].Value)
		// CBK_CONTINUOUS is always first (deterministic order).
		require.NotEmpty(t, env)
		assert.Equal(t, "CBK_CONTINUOUS", env[0].Name)
	})
	t.Run("continuous false + zero knobs omits batch/flush", func(t *testing.T) {
		job := kafkaCdcJob()
		job.PxfJob.Continuous = boolPtr(false)
		job.PxfJob.BatchSize = 0
		job.PxfJob.FlushInterval = ""
		env := buildPxfStreamingEnv(job)
		require.Len(t, env, 1)
		assert.Equal(t, "CBK_CONTINUOUS", env[0].Name)
		assert.Equal(t, "false", env[0].Value)
	})
}

// TestBuildDataLoadJob_Continuous_Shaping asserts the continuous-Job shaping
// (U6 / SC102-J43-CONTINUOUS-JOB): a Continuous=true pxf job yields a Job with
// nil ActiveDeadlineSeconds, BackoffLimit 6 and RestartPolicy=OnFailure (a long-
// running consumer that is never killed by a short deadline).
func TestBuildDataLoadJob_Continuous_Shaping(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)

	assert.Nil(t, out.Spec.ActiveDeadlineSeconds,
		"continuous Job must have NO activeDeadline (runs until deleted)")
	require.NotNil(t, out.Spec.BackoffLimit)
	assert.Equal(t, int32(6), *out.Spec.BackoffLimit)
	assert.Equal(t, corev1.RestartPolicyOnFailure, out.Spec.Template.Spec.RestartPolicy)
}

// TestBuildDataLoadJob_NonContinuous_Shaping asserts a non-continuous pxf job
// keeps the existing 7200s deadline + BackoffLimit 2 + RestartPolicy Never
// (byte-unchanged shaping) (U6).
func TestBuildDataLoadJob_NonContinuous_Shaping(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob()
	job.PxfJob.Continuous = boolPtr(false)
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)

	require.NotNil(t, out.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(7200), *out.Spec.ActiveDeadlineSeconds)
	require.NotNil(t, out.Spec.BackoffLimit)
	assert.Equal(t, int32(2), *out.Spec.BackoffLimit)
	assert.Equal(t, corev1.RestartPolicyNever, out.Spec.Template.Spec.RestartPolicy)
}

// TestBuildDataLoadCronJob_KafkaCdc_Nil asserts BuildDataLoadCronJob returns nil
// for a kafka-cdc job with no schedule (J.46 / SC102-J46-CRON-NIL): it must be a
// one-off Job, never a CronJob.
func TestBuildDataLoadCronJob_KafkaCdc_Nil(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob() // no Schedule
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	assert.Nil(t, b.BuildDataLoadCronJob(cluster, job),
		"a continuous kafka-cdc job with no schedule must not produce a CronJob")
	// The one-off Job IS produced.
	assert.NotNil(t, b.BuildDataLoadJob(cluster, job))
}

// TestBuildDataLoadJob_KafkaDDL asserts the kafka external-table DDL carries the
// pxf:// LOCATION with PROFILE=kafka&SERVER=kafka-connector and the topic resource
// (U6 / SC102-KAFKA-DDL).
func TestBuildDataLoadJob_KafkaDDL(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script,
		"pxf://cloudberry-cdc?PROFILE=kafka&SERVER=kafka-connector")
}

// TestBuildDataLoadJob_ContinuousScript asserts the continuous streaming consume
// loop is rendered for a continuous kafka job (U6 / SC102-J43): a `while true`
// loop with the per-flush INSERT, the DATALOAD_ROWS marker and a sleep cadence
// driven by CBK_FLUSH_INTERVAL.
func TestBuildDataLoadJob_ContinuousScript(t *testing.T) {
	b := NewBuilder()
	job := kafkaCdcJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// Streaming consume loop.
	assert.Contains(t, script, "while true; do")
	assert.Contains(t, script, "done")
	// Per-flush INSERT into the target table.
	assert.Contains(t, script, "INSERT INTO \"public\".\"kafka_events\" SELECT * FROM")
	// Best-effort rowcount marker per flush.
	assert.Contains(t, script, "DATALOAD_ROWS=")
	// Flush cadence honors CBK_FLUSH_INTERVAL and sleeps between flushes.
	assert.Contains(t, script, "CBK_FLUSH_INTERVAL")
	assert.Contains(t, script, "sleep ")
	// The external table is created once for the loop (kafka pxf:// DDL).
	assert.Contains(t, script, "pxf://cloudberry-cdc?PROFILE=kafka&SERVER=kafka-connector")
	// The one-off `set -euo pipefail` is NOT used (the loop tolerates per-flush
	// failures with `set -uo pipefail`).
	assert.True(t, strings.Contains(script, "set -uo pipefail"),
		"continuous script uses set -uo pipefail (per-flush failures tolerated)")
}
