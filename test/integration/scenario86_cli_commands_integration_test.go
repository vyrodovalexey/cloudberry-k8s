//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 86: All CLI Commands (cloudberry-ctl backup ...) — integration
// ============================================================================
//
// This integration test wires the WHOLE backup CLI path end-to-end against a
// real operator API: it starts the REAL api.Server behind a live httptest.Server
// (real HTTP transport + router + auth/RBAC) over a fake Kubernetes client (plus
// a fake typed clientset for the logs endpoint), then drives the operator using
// the SAME internal/ctl.OperatorClient that cloudberry-ctl uses, issuing every
// backup command 86a-k. It asserts:
//
//   - 86a create (x3) materializes backup Jobs whose args carry the gpbackup
//     flags of each variant (full / single-data-file / incremental);
//   - 86d delete creates a cleanup Job; 86e restore creates a restore Job whose
//     args include --resize-cluster;
//   - 86g/h/i schedule set/suspend/resume have the documented CronJob effects
//     (spec.backup.schedule update + CronJob .spec.suspend patches);
//   - 86k jobs logs streams the pod logs (text/plain) from the fake clientset.
//
// This is the integration analogue of the live CLI script: the live script
// obtains an OIDC token, port-forwards the operator API and drives cloudberry-ctl
// against it; here an Admin Basic identity flows through the same router so the
// whole CLI->API->builder->k8s-client path is exercised over real HTTP.
// ============================================================================

const (
	scenario86IntNamespace = "cloudberry-test"
	scenario86IntCluster   = "scenario86-s3"
	scenario86IntPrefix    = "/api/v1alpha1"
	scenario86IntDB        = "mydb"
	scenario86IntTS        = "20260601020000"
	scenario86IntRateLimit = 1000
	scenario86IntAdminUser = "adminuser"
	scenario86IntAdminPass = "adminpass"
)

// Scenario86IntegrationSuite drives the CLI client against a live api.Server.
type Scenario86IntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	srv    *httptest.Server
	server *api.Server
	client client.Client
	cli    *ctl.OperatorClient
}

func TestIntegration_Scenario86(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario86IntegrationSuite))
}

func (s *Scenario86IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario86IntegrationSuite) TearDownTest() {
	if s.srv != nil {
		s.srv.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

// scenario86IntBuildCluster builds the scenario86-s3 backup-enabled cluster
// (S3 + schedule + incremental) with the given history.
func scenario86IntBuildCluster(history ...cbv1alpha1.BackupHistoryEntry) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario86IntCluster, scenario86IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Image:    "cloudberry-backup:2.1.0",
		Schedule: "0 2 * * *",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			LeafPartitionData: true,
			CompressionType:   "zstd",
			CompressionLevel:  6,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "scenario86",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	cluster.Status.BackupHistory = history
	return cluster
}

// boot seeds the cluster + extra objects into a fake client and starts the API
// server (with a fake clientset for log streaming) behind a live httptest.Server
// with an Admin credential. It also configures the CLI OperatorClient.
func (s *Scenario86IntegrationSuite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario86IntAdminUser, scenario86IntAdminPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, &metrics.NoopRecorder{}, env.Logger,
		scenario86IntRateLimit).
		WithClientset(k8sfake.NewSimpleClientset())
	s.srv = httptest.NewServer(s.server.Handler())

	// The CLI talks to the operator API via the OperatorClient. Basic auth here
	// stands in for the live OIDC bearer; the request->router->handler path is
	// identical.
	s.cli = ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    s.srv.URL,
		AuthMethod: "basic",
		Username:   scenario86IntAdminUser,
		Password:   scenario86IntAdminPass,
		Timeout:    30 * time.Second,
	})
}

// sub mirrors ctl.ClusterSubresourcePath used by the CLI commands.
func scenario86IntSub(subresource string) string {
	return ctl.ClusterSubresourcePath(scenario86IntCluster, subresource, scenario86IntNamespace)
}

func scenario86IntTSPath(suffix string) string {
	return fmt.Sprintf("/clusters/%s/backups/%s",
		url.PathEscape(scenario86IntCluster), url.PathEscape(scenario86IntTS)) + suffix +
		"?namespace=" + scenario86IntNamespace
}

func (s *Scenario86IntegrationSuite) getJob(name string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: scenario86IntNamespace}, job))
	return job
}

func (s *Scenario86IntegrationSuite) jobArgs(name string) string {
	job := s.getJob(name)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// deleteJob removes a created Job so a subsequent create (whose name is derived
// from the second-granular server timestamp) does not collide within one test.
func (s *Scenario86IntegrationSuite) deleteJob(name string) {
	job := &batchv1.Job{}
	if err := s.client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: scenario86IntNamespace}, job); err == nil {
		require.NoError(s.T(), s.client.Delete(s.ctx, job))
	}
}

func (s *Scenario86IntegrationSuite) createBody(full, single, incremental bool) map[string]interface{} {
	gp := map[string]interface{}{}
	body := map[string]interface{}{"databases": []string{scenario86IntDB}, "gpbackupOptions": gp}
	switch {
	case full:
		body["type"] = "full"
		gp["compressionLevel"] = 6
		gp["compressionType"] = "zstd"
		gp["jobs"] = 4
		gp["includeSchemas"] = []string{"public"}
		gp["excludeTables"] = []string{"public.temp"}
		gp["withStats"] = true
		gp["withoutGlobals"] = true
	case single:
		body["type"] = "full"
		gp["singleDataFile"] = true
		gp["copyQueueSize"] = 4
	case incremental:
		body["type"] = "incremental"
		gp["incremental"] = true
		gp["fromTimestamp"] = scenario86IntTS
		gp["leafPartitionData"] = true
	}
	return body
}

// TestIntegration_Scenario86_AllCommandsEndToEnd drives every backup CLI command
// through the OperatorClient against the live API server and asserts effects.
func (s *Scenario86IntegrationSuite) TestIntegration_Scenario86_AllCommandsEndToEnd() {
	cluster := scenario86IntBuildCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: scenario86IntTS, Type: "full", Status: "Success"},
	)
	cluster.Status.LastBackupTimestamp = scenario86IntTS

	// Seed a CronJob (so 86f/g/h/i operate on it) and a backup Job + its pod
	// (so 86j lists it and 86k streams its logs).
	suspend := false
	lastRun := metav1.NewTime(time.Now().Add(-time.Hour))
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(scenario86IntCluster),
			Namespace: scenario86IntNamespace,
		},
		Spec:   batchv1.CronJobSpec{Schedule: "0 2 * * *", Suspend: &suspend},
		Status: batchv1.CronJobStatus{LastScheduleTime: &lastRun},
	}
	seededJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scenario86IntCluster + "-backup-seed",
			Namespace: scenario86IntNamespace,
			Labels: map[string]string{
				util.LabelCluster:         scenario86IntCluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
		},
		Status: batchv1.JobStatus{Active: 1},
	}
	seededPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scenario86IntCluster + "-backup-seed-abcde",
			Namespace: scenario86IntNamespace,
			Labels:    map[string]string{"batch.kubernetes.io/job-name": seededJob.Name},
		},
	}

	s.boot(cluster, cronJob, seededJob, seededPod)

	// 86a-1: create full.
	s.Run("86a_create_full", func() {
		resp, err := s.cli.Post(s.ctx, scenario86IntSub("backups"), s.createBody(true, false, false))
		require.NoError(s.T(), err)
		jobName, ok := resp.Body["job"].(string)
		require.True(s.T(), ok)
		script := s.jobArgs(jobName)
		for _, want := range []string{
			"'--compression-level' '6'", "'--compression-type' 'zstd'", "'--jobs' '4'",
			"'--include-schema' 'public'", "'--exclude-table' 'public.temp'",
			"'--with-stats'", "'--without-globals'",
		} {
			assert.Containsf(s.T(), script, want, "86a-1: backup Job must contain %q", want)
		}
		assert.NotContains(s.T(), script, "'--single-data-file'")
		assert.Equal(s.T(), util.BackupOperationBackup,
			s.getJob(jobName).Labels[util.LabelBackupOperation])
		// The Job name is derived from the second-granular server timestamp;
		// remove it so the next create variant can reuse the same name.
		s.deleteJob(jobName)
	})

	// 86a-2: create single-data-file.
	s.Run("86a_create_single_data_file", func() {
		resp, err := s.cli.Post(s.ctx, scenario86IntSub("backups"), s.createBody(false, true, false))
		require.NoError(s.T(), err)
		jobName := resp.Body["job"].(string)
		script := s.jobArgs(jobName)
		assert.Contains(s.T(), script, "'--single-data-file'")
		assert.Contains(s.T(), script, "'--copy-queue-size' '4'")
		assert.NotContains(s.T(), script, "'--jobs'")
		s.deleteJob(jobName)
	})

	// 86a-3: create incremental.
	s.Run("86a_create_incremental", func() {
		resp, err := s.cli.Post(s.ctx, scenario86IntSub("backups"), s.createBody(false, false, true))
		require.NoError(s.T(), err)
		jobName := resp.Body["job"].(string)
		script := s.jobArgs(jobName)
		assert.Contains(s.T(), script, "'--incremental'")
		assert.Contains(s.T(), script, "'--leaf-partition-data'")
		s.deleteJob(jobName)
	})

	// 86b: list.
	s.Run("86b_list", func() {
		resp, err := s.cli.Get(s.ctx, scenario86IntSub("backups"))
		require.NoError(s.T(), err)
		assert.Equal(s.T(), float64(1), resp.Body["total"])
	})

	// 86c: status --timestamp.
	s.Run("86c_status", func() {
		resp, err := s.cli.Get(s.ctx, scenario86IntTSPath(""))
		require.NoError(s.T(), err)
		assert.Equal(s.T(), scenario86IntTS, resp.Body["timestamp"])
	})

	// 86e: restore (incl --resize-cluster).
	s.Run("86e_restore", func() {
		body := map[string]interface{}{
			"timestamp": scenario86IntTS,
			"gprestoreOptions": map[string]interface{}{
				"jobs": 4, "redirectDb": "mydb_restored", "redirectSchema": "restored",
				"createDb": true, "includeTables": []string{"public.users"},
				"runAnalyze": true, "onErrorContinue": true, "truncateTable": true,
				"resizeCluster": true,
			},
		}
		resp, err := s.cli.Post(s.ctx, scenario86IntTSPath("/restore"), body)
		require.NoError(s.T(), err)
		jobName := resp.Body["job"].(string)
		script := s.jobArgs(jobName)
		for _, want := range []string{
			"'--timestamp' '" + scenario86IntTS + "'", "'--jobs' '4'",
			"'--redirect-db' 'mydb_restored'", "'--redirect-schema' 'restored'",
			"'--create-db'", "'--run-analyze'", "'--on-error-continue'",
			"'--truncate-table'", "'--resize-cluster'", "'--include-table' 'public.users'",
		} {
			assert.Containsf(s.T(), script, want, "86e: restore Job must contain %q", want)
		}
		assert.Equal(s.T(), util.BackupOperationRestore,
			s.getJob(jobName).Labels[util.LabelBackupOperation])
	})

	// 86f: schedule show.
	s.Run("86f_schedule_show", func() {
		resp, err := s.cli.Get(s.ctx, scenario86IntSub("backups/schedule"))
		require.NoError(s.T(), err)
		assert.Equal(s.T(), true, resp.Body["scheduled"])
		assert.Equal(s.T(), "0 2 * * *", resp.Body["schedule"])
	})

	// 86g: schedule set --cron -> spec.backup.schedule updated.
	s.Run("86g_schedule_set", func() {
		_, err := s.cli.Patch(s.ctx, scenario86IntSub("backups/schedule"),
			map[string]interface{}{"schedule": "0 3 * * *"})
		require.NoError(s.T(), err)
		updated := &cbv1alpha1.CloudberryCluster{}
		require.NoError(s.T(), s.client.Get(s.ctx,
			types.NamespacedName{Name: scenario86IntCluster, Namespace: scenario86IntNamespace}, updated))
		assert.Equal(s.T(), "0 3 * * *", updated.Spec.Backup.Schedule,
			"86g: schedule set must update spec.backup.schedule")
	})

	// 86h: schedule suspend -> CronJob .spec.suspend == true.
	s.Run("86h_schedule_suspend", func() {
		_, err := s.cli.Patch(s.ctx, scenario86IntSub("backups/schedule"),
			map[string]interface{}{"suspend": true})
		require.NoError(s.T(), err)
		cj := &batchv1.CronJob{}
		require.NoError(s.T(), s.client.Get(s.ctx,
			types.NamespacedName{Name: util.BackupCronJobName(scenario86IntCluster),
				Namespace: scenario86IntNamespace}, cj))
		require.NotNil(s.T(), cj.Spec.Suspend)
		assert.True(s.T(), *cj.Spec.Suspend, "86h: suspend must set CronJob .spec.suspend=true")
	})

	// 86i: schedule resume -> CronJob .spec.suspend == false.
	s.Run("86i_schedule_resume", func() {
		_, err := s.cli.Patch(s.ctx, scenario86IntSub("backups/schedule"),
			map[string]interface{}{"suspend": false})
		require.NoError(s.T(), err)
		cj := &batchv1.CronJob{}
		require.NoError(s.T(), s.client.Get(s.ctx,
			types.NamespacedName{Name: util.BackupCronJobName(scenario86IntCluster),
				Namespace: scenario86IntNamespace}, cj))
		require.NotNil(s.T(), cj.Spec.Suspend)
		assert.False(s.T(), *cj.Spec.Suspend, "86i: resume must set CronJob .spec.suspend=false")
	})

	// 86j: jobs -> lists the seeded backup Job (+ any created ones).
	s.Run("86j_jobs", func() {
		resp, err := s.cli.Get(s.ctx, scenario86IntSub("backups/jobs"))
		require.NoError(s.T(), err)
		total, ok := resp.Body["total"].(float64)
		require.True(s.T(), ok)
		assert.GreaterOrEqual(s.T(), total, float64(1))
	})

	// 86d: delete --timestamp -> cleanup Job.
	s.Run("86d_delete", func() {
		resp, err := s.cli.Delete(s.ctx, scenario86IntTSPath(""))
		require.NoError(s.T(), err)
		jobName := resp.Body["job"].(string)
		assert.Equal(s.T(), util.BackupOperationCleanup,
			s.getJob(jobName).Labels[util.LabelBackupOperation])
		assert.Contains(s.T(), s.jobArgs(jobName), "backup-delete")
	})

	// 86k: jobs logs --job -> streams the pod logs (text/plain) via GetStream.
	s.Run("86k_jobs_logs_stream", func() {
		logsPath := fmt.Sprintf("/clusters/%s/backups/jobs/%s/logs?namespace=%s",
			url.PathEscape(scenario86IntCluster), url.PathEscape(seededJob.Name),
			scenario86IntNamespace)
		var out bytes.Buffer
		err := s.cli.GetStream(s.ctx, logsPath, &out)
		require.NoError(s.T(), err, "86k: logs must stream from the seeded backup Job's pod")
		// The fake clientset returns "fake logs" for any pod log stream.
		assert.NotEmpty(s.T(), out.String())
		assert.Equal(s.T(), "fake logs", out.String())
	})
}
