package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// dataLoadJobCluster returns a cluster with a data-loading spec carrying the
// given jobs and an optional job template.
func dataLoadJobCluster(jobs []cbv1alpha1.DataLoadingJob, tmpl *cbv1alpha1.DataLoadingJobTemplate) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled:     true,
		Jobs:        jobs,
		JobTemplate: tmpl,
	}
	return c
}

func pxfTestJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "s3-parquet-loader",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3-datalake",
			Profile:     "s3:parquet",
			Resource:    "s3a://data-lake/events/",
			TargetTable: "public.events",
		},
	}
}

func gploadTestJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "csv-bulk-load",
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: "public.bulk_data",
			Format:      "csv",
			FilePaths:   []string{"/data/incoming/*.csv"},
		},
	}
}

// TestBuildDataLoadJob_Shape asserts the one-off Job: deterministic name,
// ownerRef, labels, image, command, env (incl. the password SecretKeyRef),
// RestartPolicy and the script structure (DDL + INSERT + DATALOAD_ROWS marker +
// DROP + ANALYZE).
func TestBuildDataLoadJob_Shape(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)

	// Deterministic name <cluster>-dataload-<job>.
	assert.Equal(t, util.DataLoadJobName(cluster.Name, job.Name), out.Name)
	assert.Equal(t, "test-cluster-dataload-s3-parquet-loader", out.Name)
	assert.Equal(t, cluster.Namespace, out.Namespace)

	// OwnerRef to the cluster.
	require.Len(t, out.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, out.OwnerReferences[0].Name)

	// Labels: cluster + component=dataload + per-job NAME label.
	assert.Equal(t, cluster.Name, out.Labels[util.LabelCluster])
	assert.Equal(t, util.ComponentDataLoad, out.Labels[util.LabelComponent])
	assert.Equal(t, util.SanitizeK8sName(job.Name), out.Labels[util.LabelDataLoadJob])

	// JobSpec defaults.
	require.NotNil(t, out.Spec.BackoffLimit)
	assert.Equal(t, int32(2), *out.Spec.BackoffLimit)
	require.NotNil(t, out.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int64(7200), *out.Spec.ActiveDeadlineSeconds)
	require.NotNil(t, out.Spec.TTLSecondsAfterFinished)
	assert.Equal(t, int32(86400), *out.Spec.TTLSecondsAfterFinished)

	// Container.
	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	c := out.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "dataload", c.Name)
	// Image is the cluster runtime image (the cloudberry-official data-loader).
	assert.Equal(t, cluster.Spec.Image, c.Image)
	assert.Equal(t, []string{"/bin/bash", "-c"}, c.Command)
	require.Len(t, c.Args, 1)
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, c.TerminationMessagePolicy)
	assert.Equal(t, corev1.RestartPolicyNever, out.Spec.Template.Spec.RestartPolicy)

	// Script structure.
	script := c.Args[0]
	assert.Contains(t, script, "set -euo pipefail")
	assert.Contains(t, script, "CREATE EXTERNAL TABLE")
	assert.Contains(t, script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw")
	assert.Contains(t, script, "INSERT INTO \"public\".\"events\" SELECT * FROM")
	assert.Contains(t, script, "DATALOAD_ROWS=")
	assert.Contains(t, script, "/dev/termination-log")
	assert.Contains(t, script, "DROP EXTERNAL TABLE IF EXISTS")
	assert.Contains(t, script, "ANALYZE \"public\".\"events\"")
	assert.Contains(t, script, "pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake")

	// Env: PG* with the password SecretKeyRef.
	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	assert.Equal(t, util.CoordinatorServiceName(cluster.Name), env["PGHOST"].Value)
	assert.Equal(t, util.DefaultAdminUser, env["PGUSER"].Value)
	assert.Equal(t, "postgres", env["PGDATABASE"].Value)
	assert.NotEmpty(t, env["PGPORT"].Value)
	require.NotNil(t, env["PGPASSWORD"].ValueFrom)
	require.NotNil(t, env["PGPASSWORD"].ValueFrom.SecretKeyRef)
	assert.Equal(t, util.AdminPasswordSecretName(cluster.Name),
		env["PGPASSWORD"].ValueFrom.SecretKeyRef.Name)
	assert.Empty(t, env["PGPASSWORD"].Value, "password must never be a plaintext value")
}

// pxfWritableExportTestJob returns a PXF WRITABLE EXPORT job: mode=writable with
// a write-capable profile (s3:text). The load script for this job must REVERSE
// the INSERT direction (target -> temp writable external table) and SKIP ANALYZE.
func pxfWritableExportTestJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "s3-text-export",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3-datalake",
			Profile:     "s3:text",
			Resource:    "s3a://data-lake/exports/",
			TargetTable: "public.events",
			Mode:        "writable",
		},
	}
}

// TestBuildDataLoadJob_WritableExportScript asserts the WRITABLE EXPORT job
// script (the FIX for the PXF writable export direction). For a mode=writable PXF
// job the script must:
//   - emit the WRITABLE external-table DDL (CREATE WRITABLE EXTERNAL TABLE ...
//     pxfwritable_export),
//   - REVERSE the INSERT direction: INSERT INTO <tmp writable external>
//     SELECT * FROM <target> (the cluster table is the SELECT source; the temp
//     writable external table is the INSERT target — a WRITABLE external table
//     can only be written TO),
//   - SKIP ANALYZE on the target (an export does not mutate the source table's
//     stats and ANALYZE on the external table is invalid),
//   - still carry the DATALOAD_ROWS= marker and the DROP EXTERNAL TABLE cleanup.
//
// It uses the same access pattern as the read-path TestBuildDataLoadJob_Shape:
// BuildDataLoadJob -> container Args[0].
func TestBuildDataLoadJob_WritableExportScript(t *testing.T) {
	b := NewBuilder()
	job := pxfWritableExportTestJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	require.Len(t, out.Spec.Template.Spec.Containers[0].Args, 1)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// WRITABLE external-table DDL (export formatter).
	assert.Contains(t, script, "CREATE WRITABLE EXTERNAL TABLE")
	assert.Contains(t, script, "FORMATTER='pxfwritable_export'")

	// REVERSED INSERT direction: the temp WRITABLE external table is the INSERT
	// target, the cluster target table is the SELECT source. Compute the exact
	// temp-table name via the SAME helper the production code uses so the
	// assertion is byte-exact.
	tmp := quoteSQLIdentifier(dataLoadTmpTable(job.Name))
	assert.Equal(t, "\"cbk_dataload_ext_s3_text_export\"", tmp,
		"temp table name must match the production helper exactly")
	wantInsert := "INSERT INTO " + tmp + " SELECT * FROM \"public\".\"events\""
	assert.Contains(t, script, wantInsert)
	assert.Contains(t, script,
		"INSERT INTO \"cbk_dataload_ext_s3_text_export\" SELECT * FROM \"public\".\"events\"")

	// The READ direction must NOT appear for an export (no INSERT INTO target
	// SELECT FROM temp).
	assert.NotContains(t, script, "INSERT INTO \"public\".\"events\" SELECT * FROM")

	// Export skips ANALYZE on the target.
	assert.NotContains(t, script, "ANALYZE \"public\".\"events\"")
	assert.NotContains(t, script, "ANALYZE")

	// Still carries the rowcount marker and the temp-table cleanup.
	assert.Contains(t, script, "DATALOAD_ROWS=")
	assert.Contains(t, script, "/dev/termination-log")
	assert.Contains(t, script, "DROP EXTERNAL TABLE IF EXISTS "+tmp)
}

// pxfWritableExportFilteredTestJob returns a PXF WRITABLE EXPORT job (s3:text)
// with a SourceFilter set. For mode=writable the export INSERT must carry the
// ` WHERE <sourceFilter>` predicate, routed through the quoted heredoc so an
// embedded single quote (region='us-east') cannot break shell quoting.
func pxfWritableExportFilteredTestJob() cbv1alpha1.DataLoadingJob {
	job := pxfWritableExportTestJob()
	job.PxfJob.SourceFilter = "region='us-east'"
	return job
}

// TestBuildDataLoadJob_WritableExportFilteredScript (Scenario 99, SF.1) asserts
// the FILTERED writable-export script: with PxfJob.SourceFilter set, the export
// INSERT becomes the reversed-direction `INSERT INTO <tmp writable ext>
// SELECT * FROM <target> WHERE <sourceFilter>` emitted via the quoted heredoc
// (so embedded single quotes survive), while still carrying the WRITABLE DDL
// (FORMATTER='pxfwritable_export'), the DATALOAD_ROWS marker, the DROP cleanup,
// and skipping ANALYZE. The temp/target identifiers are computed via the SAME
// production helpers used by the builder so the assertion is byte-exact.
func TestBuildDataLoadJob_WritableExportFilteredScript(t *testing.T) {
	b := NewBuilder()
	job := pxfWritableExportFilteredTestJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	require.Len(t, out.Spec.Template.Spec.Containers[0].Args, 1)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// WRITABLE external-table DDL (export formatter) is still emitted.
	assert.Contains(t, script, "CREATE WRITABLE EXTERNAL TABLE")
	assert.Contains(t, script, "FORMATTER='pxfwritable_export'")

	// Byte-exact temp/target identifiers via the production helpers.
	tmp := quoteSQLIdentifier(dataLoadTmpTable(job.Name))
	target := quoteSQLIdentifier(job.PxfJob.TargetTable)
	assert.Equal(t, "\"cbk_dataload_ext_s3_text_export\"", tmp)
	assert.Equal(t, "\"public\".\"events\"", target)

	// The filtered, reversed-direction INSERT body with the WHERE predicate. The
	// predicate (with its single quotes) appears VERBATIM — the heredoc protects
	// it from shell-quoting hazards.
	wantInsertLine := "INSERT INTO " + tmp + " SELECT * FROM " + target + " WHERE region='us-east'"
	assert.Contains(t, script, wantInsertLine)
	assert.Contains(t, script,
		"INSERT INTO \"cbk_dataload_ext_s3_text_export\" SELECT * FROM \"public\".\"events\" "+
			"WHERE region='us-east'")

	// The predicate appears verbatim (single quotes intact).
	assert.Contains(t, script, "region='us-east'")

	// The filtered INSERT is delivered via the quoted heredoc piped to psql -tA
	// (NOT the single-line psql -c form), and the rowcount capture is preserved
	// through the SAME awk extraction.
	assert.Contains(t, script, "rows=$(psql -v ON_ERROR_STOP=1 -tA <<'_CBK_INSERT_EOF_' | awk '{print $NF}'")
	assert.Contains(t, script, "_CBK_INSERT_EOF_")
	assert.NotContains(t, script,
		"psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO \"cbk_dataload_ext_s3_text_export\"")

	// Rowcount capture + cleanup preserved; ANALYZE skipped for an export.
	assert.Contains(t, script, "DATALOAD_ROWS=")
	assert.Contains(t, script, "/dev/termination-log")
	assert.Contains(t, script, "DROP EXTERNAL TABLE IF EXISTS "+tmp)
	assert.NotContains(t, script, "ANALYZE")
}

// TestBuildDataLoadJob_WritableExportNoFilterByteIdentical (Scenario 99) pins
// the NO-filter writable export to its prior byte-identical behavior: with an
// EMPTY SourceFilter the export INSERT is the unchanged single-line
// `psql -tA -c 'INSERT INTO <tmp> SELECT * FROM <target>'` (no WHERE, no
// heredoc). It asserts the exact INSERT line a SourceFilter-free writable job
// produced before the feature so the existing writable behavior is unchanged.
func TestBuildDataLoadJob_WritableExportNoFilterByteIdentical(t *testing.T) {
	b := NewBuilder()
	job := pxfWritableExportTestJob() // SourceFilter unset
	require.Empty(t, job.PxfJob.SourceFilter)
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	tmp := quoteSQLIdentifier(dataLoadTmpTable(job.Name))
	target := quoteSQLIdentifier(job.PxfJob.TargetTable)

	// The unchanged single-line INSERT (reversed direction, NO WHERE) via the
	// historical psql -c form — byte-identical to pre-SourceFilter behavior.
	wantLine := "rows=$(psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO " + tmp +
		" SELECT * FROM " + target + "' | awk '{print $NF}')"
	assert.Contains(t, script, wantLine)
	assert.Contains(t, script,
		"rows=$(psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO \"cbk_dataload_ext_s3_text_export\" "+
			"SELECT * FROM \"public\".\"events\"' | awk '{print $NF}')")

	// No WHERE and no heredoc INSERT delimiter when the filter is unset.
	assert.NotContains(t, script, " WHERE ")
	assert.NotContains(t, script, "_CBK_INSERT_EOF_")
}

// TestBuildDataLoadJob_ReadJobIgnoresSourceFilter (Scenario 99) asserts the
// BUILDER ignores SourceFilter on a READ/import job: even when SourceFilter is
// (defensively) set on a non-writable pxf job, the builder emits the read-path
// `INSERT INTO <target> SELECT * FROM <ext>` with NO WHERE (the webhook W.17
// rejects such a CR at admission; this guards the builder's defensive behavior
// that only consults SourceFilter under the writable branch).
func TestBuildDataLoadJob_ReadJobIgnoresSourceFilter(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob() // read/import: Mode unset
	job.PxfJob.SourceFilter = "region='us-east'"
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// Read direction: INSERT INTO <target> SELECT * FROM <tmp ext>, NO WHERE.
	assert.Contains(t, script, "INSERT INTO \"public\".\"events\" SELECT * FROM")
	assert.NotContains(t, script, " WHERE ")
	assert.NotContains(t, script, "region='us-east'")
	assert.NotContains(t, script, "_CBK_INSERT_EOF_")
	// Read path still ANALYZEs the target (export-only paths skip it).
	assert.Contains(t, script, "ANALYZE \"public\".\"events\"")
}

// TestBuildDataLoadJob_NativeScriptSkipsPXFExtension asserts the gpload job's
// script does NOT attempt the pxf_fdw extension (gpload needs no PXF).
//
// Scenario 101 reroute: a gpload-type job no longer renders native external-table
// DDL with an inline gpfdist:// URL. Instead BuildDataLoadJob delegates to
// BuildGploadJob, whose container runs `gpload -f /etc/gpload/<job>.yml` and
// mounts the control-file ConfigMap at /etc/gpload. The gpfdist:// URL now lives
// in the control file (asserted in TestBuildGploadControlFile_* /
// TestBuildGploadControlFileConfigMap), NOT in the Job script — so this test is
// re-pointed to the new reality: it asserts the `gpload -f` invocation + the
// ConfigMap mount, while keeping the still-valid asserts (no pxf_fdw extension;
// DATALOAD_ROWS= marker present).
func TestBuildDataLoadJob_NativeScriptSkipsPXFExtension(t *testing.T) {
	b := NewBuilder()
	job := gploadTestJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	container := out.Spec.Template.Spec.Containers[0]
	script := container.Args[0]

	// Still valid: gpload needs no PXF foreign-data wrapper.
	assert.NotContains(t, script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw")
	// Still valid: best-effort rowcount marker present.
	assert.Contains(t, script, "DATALOAD_ROWS=")

	// New reality: the gpload Job runs `gpload -f /etc/gpload/<job>.yml` (the
	// gpfdist:// URL is in the control-file ConfigMap, not the Job script).
	wantCtl := "/etc/gpload/" + job.Name + ".yml"
	assert.Contains(t, script, "gpload -f "+wantCtl)
	// The inline gpfdist:// URL is gone from the Job script (it moved to the CM).
	assert.NotContains(t, script, "gpfdist://test-cluster-gpfdist:8080/data/incoming/*.csv")

	// New reality: the control-file ConfigMap is mounted read-only at /etc/gpload.
	var mount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].MountPath == "/etc/gpload" {
			mount = &container.VolumeMounts[i]
			break
		}
	}
	require.NotNil(t, mount, "gpload Job must mount the control-file ConfigMap at /etc/gpload")
	assert.True(t, mount.ReadOnly)
	// And the volume references the per-job control-file ConfigMap.
	var vol *corev1.Volume
	for i := range out.Spec.Template.Spec.Volumes {
		if out.Spec.Template.Spec.Volumes[i].Name == mount.Name {
			vol = &out.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	require.NotNil(t, vol)
	require.NotNil(t, vol.ConfigMap)
	assert.Equal(t,
		util.GploadControlFileConfigMapName(cluster.Name, job.Name),
		vol.ConfigMap.Name)
}

// TestBuildDataLoadJob_TemplateOverrides asserts the DataLoadingJobTemplate
// overrides are honored (backoff/deadline/ttl/resources/SA/nodeSelector/tolerations).
func TestBuildDataLoadJob_TemplateOverrides(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	tmpl := &cbv1alpha1.DataLoadingJobTemplate{
		BackoffLimit:            util.Ptr(int32(5)),
		ActiveDeadlineSeconds:   util.Ptr(int64(600)),
		TTLSecondsAfterFinished: util.Ptr(int32(120)),
		ServiceAccountName:      "loader-sa",
		NodeSelector:            map[string]string{"disktype": "ssd"},
		Tolerations: []cbv1alpha1.Toleration{
			{Key: "dedicated", Operator: "Equal", Value: "load", Effect: "NoSchedule"},
		},
		Resources: &cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "250m", Memory: "256Mi"},
			Limits:   &cbv1alpha1.ResourceList{CPU: "1", Memory: "1Gi"},
		},
	}
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, tmpl)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	assert.Equal(t, int32(5), *out.Spec.BackoffLimit)
	assert.Equal(t, int64(600), *out.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int32(120), *out.Spec.TTLSecondsAfterFinished)

	podSpec := out.Spec.Template.Spec
	assert.Equal(t, "loader-sa", podSpec.ServiceAccountName)
	assert.Equal(t, "ssd", podSpec.NodeSelector["disktype"])
	require.Len(t, podSpec.Tolerations, 1)
	assert.Equal(t, "dedicated", podSpec.Tolerations[0].Key)

	c := podSpec.Containers[0]
	assert.Equal(t, "250m", c.Resources.Requests.Cpu().String())
	assert.Equal(t, "1Gi", c.Resources.Limits.Memory().String())
}

// TestBuildDataLoadJob_MisconfiguredReturnsNil asserts a broken job yields a nil
// Job (defensive: no broken workload).
func TestBuildDataLoadJob_MisconfiguredReturnsNil(t *testing.T) {
	b := NewBuilder()
	job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "pxf", Enabled: true} // nil PxfJob
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)
	assert.Nil(t, b.BuildDataLoadJob(cluster, job))
}

// TestBuildDataLoadCronJob_Schedule asserts the scheduled CronJob shape and that
// a one-off (no schedule) job yields a nil CronJob.
func TestBuildDataLoadCronJob_Schedule(t *testing.T) {
	b := NewBuilder()

	scheduled := pxfTestJob()
	scheduled.Schedule = "0 2 * * *"
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{scheduled}, nil)

	cron := b.BuildDataLoadCronJob(cluster, scheduled)
	require.NotNil(t, cron)
	assert.Equal(t, util.DataLoadJobName(cluster.Name, scheduled.Name), cron.Name)
	assert.Equal(t, "0 2 * * *", cron.Spec.Schedule)
	assert.Equal(t, batchv1.ForbidConcurrent, cron.Spec.ConcurrencyPolicy)
	require.NotNil(t, cron.Spec.SuccessfulJobsHistoryLimit)
	assert.Equal(t, int32(3), *cron.Spec.SuccessfulJobsHistoryLimit)
	require.Len(t, cron.OwnerReferences, 1)

	// The JobTemplate carries the load container + script.
	containers := cron.Spec.JobTemplate.Spec.Template.Spec.Containers
	require.Len(t, containers, 1)
	assert.Contains(t, containers[0].Args[0], "DATALOAD_ROWS=")

	// No-schedule job => nil CronJob.
	oneOff := pxfTestJob()
	assert.Nil(t, b.BuildDataLoadCronJob(cluster, oneOff))
}

// TestBuildDataLoadCronJob_MisconfiguredReturnsNil asserts a scheduled but broken
// job yields a nil CronJob.
func TestBuildDataLoadCronJob_MisconfiguredReturnsNil(t *testing.T) {
	b := NewBuilder()
	job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "gpload", Enabled: true, Schedule: "* * * * *"}
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)
	assert.Nil(t, b.BuildDataLoadCronJob(cluster, job))
}

// TestDataLoaderImage asserts the data-loader image selection (cluster image
// preferred, default fallback).
func TestDataLoaderImage(t *testing.T) {
	c := newTestCluster()
	assert.Equal(t, c.Spec.Image, dataLoaderImage(c))

	c.Spec.Image = ""
	assert.Equal(t, util.DefaultImage, dataLoaderImage(c))
}

// TestGpfdistServiceName asserts the deterministic gpfdist service name.
func TestGpfdistServiceName(t *testing.T) {
	assert.Equal(t, "mycluster-gpfdist", GpfdistServiceName("mycluster"))
}
