//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 85: All Backup REST API Endpoints (integration)
// ============================================================================
//
// These integration tests start the REAL api.Server over a live httptest.Server
// (real HTTP transport + the registered router + auth/RBAC middleware) with a
// fake Kubernetes client and an Admin credential. They call all 7 backup
// endpoints end-to-end over HTTP and assert:
//
//   - the created backup (85b) / cleanup (85d) / restore (85e) batchv1.Jobs are
//     PERSISTED in the (fake) API server with the correct operation label and the
//     full gpbackup/gprestore arg set, and
//   - the GET endpoints (85a/85c/85f/85g) return the documented response shapes
//     including nextScheduleTime (85g).
//
// This is the integration analogue of the live OIDC script: the live script
// obtains an OIDC bearer token from Keycloak and calls the same routes over TLS;
// here we use a Basic-auth Admin identity through the same router so the whole
// request->handler->builder->k8s-client path is exercised with real HTTP.
// ============================================================================

const (
	scenario85IntNamespace = "cloudberry-test"
	scenario85IntCluster   = "scenario85-s3"
	scenario85IntPrefix    = "/api/v1alpha1"
	scenario85IntDB        = "mydb"
	scenario85IntTS        = "20260101010101"
	scenario85IntRateLimit = 1000
	scenario85IntAdminUser = "adminuser"
	scenario85IntAdminPass = "adminpass"
)

// Scenario85IntegrationSuite drives all 7 endpoints over a live httptest server.
type Scenario85IntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	srv    *httptest.Server
	server *api.Server
	client client.Client
}

func TestIntegration_Scenario85(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario85IntegrationSuite))
}

func (s *Scenario85IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario85IntegrationSuite) TearDownTest() {
	if s.srv != nil {
		s.srv.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

// scenario85IntCluster builds the scenario85-s3 backup-enabled cluster (S3 +
// schedule) with the given backup history.
func scenario85IntBuildCluster(history ...cbv1alpha1.BackupHistoryEntry) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario85IntCluster, scenario85IntNamespace).
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
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "scenario85",
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
// server behind a live httptest.Server with an Admin credential.
func (s *Scenario85IntegrationSuite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario85IntAdminUser, scenario85IntAdminPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, &metrics.NoopRecorder{}, env.Logger, scenario85IntRateLimit)
	s.srv = httptest.NewServer(s.server.Handler())
}

// call performs an authenticated HTTP request against the live test server.
func (s *Scenario85IntegrationSuite) call(method, endpoint, body string) (int, map[string]interface{}) {
	url := s.srv.URL + scenario85IntPrefix + "/clusters/" + scenario85IntCluster + endpoint +
		"?namespace=" + scenario85IntNamespace
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	require.NoError(s.T(), err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(scenario85IntAdminUser, scenario85IntAdminPass)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer func() { _ = resp.Body.Close() }()

	var decoded map[string]interface{}
	raw, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &decoded)
	}
	return resp.StatusCode, decoded
}

// getJob fetches a persisted Job by name from the fake API server.
func (s *Scenario85IntegrationSuite) getJob(name string) *batchv1.Job {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: scenario85IntNamespace}, job))
	return job
}

// jobArgs returns the rendered container script of a persisted Job.
func (s *Scenario85IntegrationSuite) jobArgs(name string) string {
	job := s.getJob(name)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	return job.Spec.Template.Spec.Containers[0].Args[0]
}

// TestIntegration_Scenario85_AllEndpointsEndToEnd exercises all 7 endpoints in
// sequence over real HTTP and asserts the persisted Jobs + GET shapes.
func (s *Scenario85IntegrationSuite) TestIntegration_Scenario85_AllEndpointsEndToEnd() {
	cluster := scenario85IntBuildCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: scenario85IntTS, Type: "full", Status: "Success"},
	)
	cluster.Status.LastBackupTimestamp = scenario85IntTS
	cluster.Status.LastBackupStatus = "Success"

	// Seed backup-op Jobs so 85f lists them.
	running := scenario85IntJob("scenario85-s3-backup-seed", util.BackupOperationBackup)
	running.Status.Active = 1
	// Seed a CronJob so 85g returns scheduled:true + nextScheduleTime.
	suspend := false
	lastRun := metav1.NewTime(time.Now().Add(-time.Hour))
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(scenario85IntCluster),
			Namespace: scenario85IntNamespace,
		},
		Spec:   batchv1.CronJobSpec{Schedule: "0 2 * * *", Suspend: &suspend},
		Status: batchv1.CronJobStatus{LastScheduleTime: &lastRun},
	}

	s.boot(cluster, running, cronJob)

	// 85a: GET /backups -> list.
	s.Run("85a_list", func() {
		code, resp := s.call(http.MethodGet, "/backups", "")
		require.Equal(s.T(), http.StatusOK, code)
		assert.Equal(s.T(), float64(1), resp["total"])
		assert.Equal(s.T(), scenario85IntCluster, resp["cluster"])
	})

	// 85b: POST /backups -> persisted backup Job with full gpbackupOptions args.
	s.Run("85b_create", func() {
		body := `{
			"type": "full",
			"databases": ["` + scenario85IntDB + `"],
			"gpbackupOptions": {
				"singleDataFile": true,
				"copyQueueSize": 8,
				"includeSchemas": ["public"],
				"excludeTables": ["public.tmp"],
				"leafPartitionData": true,
				"withStats": true,
				"withoutGlobals": true
			}
		}`
		code, resp := s.call(http.MethodPost, "/backups", body)
		require.Equal(s.T(), http.StatusAccepted, code)
		assert.Equal(s.T(), "backup started", resp["status"])
		jobName, ok := resp["job"].(string)
		require.True(s.T(), ok)
		require.NotEmpty(s.T(), jobName)

		job := s.getJob(jobName)
		assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
		script := s.jobArgs(jobName)
		for _, want := range []string{
			"'--dbname' '" + scenario85IntDB + "'",
			"'--single-data-file'",
			"'--copy-queue-size' '8'",
			"'--include-schema' 'public'",
			"'--exclude-table' 'public.tmp'",
			"'--leaf-partition-data'",
			"'--with-stats'",
			"'--without-globals'",
		} {
			assert.Containsf(s.T(), script, want, "85b: persisted backup Job must contain %q", want)
		}
		assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
			"85b: --leaf-partition-data exactly once on a full backup")
		assert.NotContains(s.T(), script, "'--jobs'")
	})

	// 85c: GET /backups/{ts} -> details.
	s.Run("85c_get", func() {
		code, resp := s.call(http.MethodGet, "/backups/"+scenario85IntTS, "")
		require.Equal(s.T(), http.StatusOK, code)
		assert.Equal(s.T(), scenario85IntTS, resp["timestamp"])
	})

	// 85e: POST /backups/{ts}/restore -> persisted restore Job with gprestore args.
	s.Run("85e_restore", func() {
		body := `{
			"databases": ["` + scenario85IntDB + `"],
			"gprestoreOptions": {
				"jobs": 4,
				"redirectDb": "mydb_restored",
				"createDb": true,
				"withGlobals": true,
				"runAnalyze": true,
				"onErrorContinue": true,
				"truncateTable": true,
				"dataOnly": true,
				"resizeCluster": true
			}
		}`
		code, resp := s.call(http.MethodPost, "/backups/"+scenario85IntTS+"/restore", body)
		require.Equal(s.T(), http.StatusAccepted, code)
		assert.Equal(s.T(), "restore started", resp["status"])
		jobName, ok := resp["job"].(string)
		require.True(s.T(), ok)

		job := s.getJob(jobName)
		assert.Equal(s.T(), util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])
		script := s.jobArgs(jobName)
		for _, want := range []string{
			"'--timestamp' '" + scenario85IntTS + "'",
			"'--jobs' '4'",
			"'--redirect-db' 'mydb_restored'",
			"'--create-db'",
			"'--with-globals'",
			"'--run-analyze'",
			"'--on-error-continue'",
			"'--truncate-table'",
			"'--data-only'",
			"'--resize-cluster'",
		} {
			assert.Containsf(s.T(), script, want, "85e: persisted restore Job must contain %q", want)
		}
		assert.NotContains(s.T(), script, "'--metadata-only'")
	})

	// 85e negative: dataOnly+metadataOnly -> 400.
	s.Run("85e_conflict_400", func() {
		code, _ := s.call(http.MethodPost, "/backups/"+scenario85IntTS+"/restore",
			`{"gprestoreOptions":{"dataOnly":true,"metadataOnly":true}}`)
		assert.Equal(s.T(), http.StatusBadRequest, code)
	})

	// 85d: DELETE /backups/{ts} -> persisted cleanup Job.
	s.Run("85d_cleanup", func() {
		code, resp := s.call(http.MethodDelete, "/backups/"+scenario85IntTS, "")
		require.Equal(s.T(), http.StatusAccepted, code)
		assert.Equal(s.T(), "deleted", resp["status"])
		jobName, ok := resp["job"].(string)
		require.True(s.T(), ok)
		job := s.getJob(jobName)
		assert.Equal(s.T(), util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation],
			"85d: persisted cleanup Job must carry operation=cleanup")
		assert.Contains(s.T(), s.jobArgs(jobName), "backup-delete")
	})

	// 85f: GET /backups/jobs -> job statuses (the seeded backup Job listed).
	s.Run("85f_jobs", func() {
		code, resp := s.call(http.MethodGet, "/backups/jobs", "")
		require.Equal(s.T(), http.StatusOK, code)
		total, ok := resp["total"].(float64)
		require.True(s.T(), ok)
		assert.GreaterOrEqual(s.T(), total, float64(1), "85f: at least the seeded backup Job must be listed")
	})

	// 85g: GET /backups/schedule -> CronJob status + nextScheduleTime.
	s.Run("85g_schedule", func() {
		code, resp := s.call(http.MethodGet, "/backups/schedule", "")
		require.Equal(s.T(), http.StatusOK, code)
		assert.Equal(s.T(), true, resp["scheduled"])
		assert.Equal(s.T(), "0 2 * * *", resp["schedule"])
		require.NotNil(s.T(), resp["nextScheduleTime"],
			"85g: a scheduled cluster must surface nextScheduleTime")
	})
}

// scenario85IntJob builds a backup-operation Job fixture in the test namespace.
func scenario85IntJob(name, operation string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scenario85IntNamespace,
			Labels: map[string]string{
				util.LabelCluster:         scenario85IntCluster,
				util.LabelBackupOperation: operation,
			},
		},
	}
}
