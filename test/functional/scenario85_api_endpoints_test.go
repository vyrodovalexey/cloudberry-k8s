//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
// Scenario 85: All Backup REST API Endpoints (functional)
// ============================================================================
//
// Scenario 85 black-boxes the operator's 7 backup REST API endpoints through the
// REAL HTTP router + auth/RBAC middleware (api.Server.Handler) with a fake k8s
// client. The functional tests prove, deterministically and infra-free, that
// every endpoint returns the documented status + response shape, and that the
// two write endpoints (85b create, 85e restore) materialize a batchv1.Job whose
// container args contain EVERY gpbackup/gprestore flag mapped from the request:
//
//	85a GET    /backups               -> list (status.BackupHistory).
//	85b POST   /backups               -> Job with the full gpbackupOptions args
//	                                      (incl. --leaf-partition-data on a FULL
//	                                      backup — the GAP-B fix).
//	85c GET    /backups/{ts}          -> details / 404 / 400.
//	85d DELETE /backups/{ts}          -> cleanup Job (operation=cleanup).
//	85e POST   /backups/{ts}/restore  -> restore Job with the full gprestoreOptions
//	                                      args (--data-only/--resize-cluster/...);
//	                                      dataOnly+metadataOnly => 400.
//	85f GET    /backups/jobs          -> job statuses.
//	85g GET    /backups/schedule      -> CronJob status + nextScheduleTime.
//
// The render shell-quotes each token individually, so a flag/value pair appears
// as '--flag' 'value'. RBAC: Basic GETs, Operator POST backup, Admin DELETE +
// restore — exercised through the real withPermission middleware.
// ============================================================================

const (
	scenario85Namespace = "cloudberry-test"
	scenario85Cluster   = "scenario85-s3"
	scenario85Prefix    = "/api/v1alpha1"
	scenario85DB        = "mydb"
	scenario85TS        = "20260101010101"
	scenario85RateLimit = 1000

	scenario85AdminUser = "adminuser"
	scenario85AdminPass = "adminpass"
	scenario85BasicUser = "basicuser"
	scenario85BasicPass = "basicpass"
	scenario85OperUser  = "operuser"
	scenario85OperPass  = "operpass"
)

// Scenario85APISuite drives all 7 backup endpoints through the real router with
// a fake k8s client + a credential store providing Basic/Operator/Admin users.
type Scenario85APISuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	client  client.Client
	ctx     context.Context
}

func TestFunctional_Scenario85(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario85APISuite))
}

// scenario85BackupCluster builds the scenario85-s3 backup-enabled cluster (S3
// destination + a schedule so 85g returns a CronJob) mirroring the sample CR.
func scenario85BackupCluster(history ...cbv1alpha1.BackupHistoryEntry) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario85Cluster, scenario85Namespace).
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

func (s *Scenario85APISuite) SetupTest() {
	s.ctx = context.Background()
}

// boot builds the API server (real router + auth/RBAC) over a fake client seeded
// with the cluster + any extra objects, and a credential store with the three
// permission tiers.
func (s *Scenario85APISuite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario85AdminUser, scenario85AdminPass, auth.PermissionAdmin)
	store.SetCredentials(scenario85OperUser, scenario85OperPass, auth.PermissionOperator)
	store.SetCredentials(scenario85BasicUser, scenario85BasicPass, auth.PermissionBasic)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, &metrics.NoopRecorder{}, env.Logger, scenario85RateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario85APISuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// path builds the namespaced cluster sub-path for scenario85-s3.
func scenario85Path(endpoint string) string {
	return scenario85Prefix + "/clusters/" + scenario85Cluster + endpoint +
		"?namespace=" + scenario85Namespace
}

// do executes an authenticated request as the given user and returns the recorder.
func (s *Scenario85APISuite) do(user, pass, method, path, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decode decodes a JSON object body.
func (s *Scenario85APISuite) decode(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// jobScript fetches the named Job and returns the rendered container script.
func (s *Scenario85APISuite) jobScript(name string) (*batchv1.Job, string) {
	job := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: name, Namespace: scenario85Namespace}, job))
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	return job, job.Spec.Template.Spec.Containers[0].Args[0]
}

// --- 85a: GET /backups -> list from status.BackupHistory ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_ListBackups() {
	cluster := scenario85BackupCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: scenario85TS, Type: "full", Status: "Success"},
		cbv1alpha1.BackupHistoryEntry{Timestamp: "20260102010101", Type: "incremental", Status: "Success"},
	)
	cluster.Status.LastBackupTimestamp = "20260102010101"
	cluster.Status.LastBackupStatus = "Success"
	s.boot(cluster)

	rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodGet, scenario85Path("/backups"), "")
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decode(rec)
	assert.Equal(s.T(), float64(2), resp["total"], "85a: total must equal the history length")
	assert.Equal(s.T(), "20260102010101", resp["lastBackupTimestamp"])
	assert.Equal(s.T(), "Success", resp["lastBackupStatus"])
	backups, ok := resp["backups"].([]interface{})
	require.True(s.T(), ok)
	assert.Len(s.T(), backups, 2)
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_ListBackups_MissingCluster404() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodGet,
		scenario85Prefix+"/clusters/nope/backups?namespace="+scenario85Namespace, "")
	assert.Equal(s.T(), http.StatusNotFound, rec.Code)
}

// --- 85b: POST /backups -> Job args contain every gpbackupOption flag ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_CreateBackup_FullOptionMapping() {
	s.boot(scenario85BackupCluster())

	body := `{
		"type": "full",
		"databases": ["` + scenario85DB + `"],
		"gpbackupOptions": {
			"singleDataFile": true,
			"copyQueueSize": 8,
			"includeSchemas": ["public"],
			"excludeTables": ["public.tmp"],
			"leafPartitionData": true,
			"withStats": true,
			"withoutGlobals": true,
			"compressionLevel": 5,
			"compressionType": "zstd"
		}
	}`

	rec := s.do(scenario85OperUser, scenario85OperPass, http.MethodPost, scenario85Path("/backups"), body)
	require.Equal(s.T(), http.StatusAccepted, rec.Code, "85b: Operator POST must be accepted")
	resp := s.decode(rec)
	assert.Equal(s.T(), "backup started", resp["status"])
	assert.Equal(s.T(), "full", resp["type"])
	jobName, ok := resp["job"].(string)
	require.True(s.T(), ok)
	require.NotEmpty(s.T(), jobName)

	job, script := s.jobScript(jobName)
	for _, want := range []string{
		"'--dbname' '" + scenario85DB + "'",
		"'--single-data-file'",
		"'--copy-queue-size' '8'",
		"'--include-schema' 'public'",
		"'--exclude-table' 'public.tmp'",
		"'--leaf-partition-data'",
		"'--with-stats'",
		"'--without-globals'",
		"'--compression-level' '5'",
		"'--compression-type' 'zstd'",
	} {
		assert.Containsf(s.T(), script, want, "85b: backup script must contain %q", want)
	}
	// GAP-B: --leaf-partition-data exactly once on a FULL backup (NOT incremental).
	assert.Equal(s.T(), 1, strings.Count(script, "'--leaf-partition-data'"),
		"85b: --leaf-partition-data must be emitted exactly once on a full backup")
	assert.NotContains(s.T(), script, "'--incremental'")
	// single-data-file is mutually exclusive with --jobs.
	assert.NotContains(s.T(), script, "'--jobs'")
	// The created object is a Job with the backup-operation label.
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_CreateBackup_Negatives() {
	s.boot(scenario85BackupCluster())
	cases := []struct {
		name string
		body string
		want int
	}{
		{"invalid type", `{"type":"bogus"}`, http.StatusBadRequest},
		{"invalid db identifier", `{"databases":["bad name"]}`, http.StatusBadRequest},
		{"malformed json", `{not-json`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			rec := s.do(scenario85OperUser, scenario85OperPass, http.MethodPost,
				scenario85Path("/backups"), tc.body)
			assert.Equal(s.T(), tc.want, rec.Code)
		})
	}
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_CreateBackup_NotEnabled400() {
	cluster := scenario85BackupCluster()
	cluster.Spec.Backup.Enabled = false
	s.boot(cluster)
	rec := s.do(scenario85OperUser, scenario85OperPass, http.MethodPost,
		scenario85Path("/backups"), `{"type":"full","databases":["mydb"]}`)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_CreateBackup_BasicForbidden403() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodPost,
		scenario85Path("/backups"), `{"type":"full"}`)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code, "85b: Basic identity must be forbidden (Operator required)")
}

// --- 85c: GET /backups/{ts} -> details / not-found / invalid ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_GetBackup_Matrix() {
	cluster := scenario85BackupCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: scenario85TS, Type: "full", Status: "Success"},
	)
	s.boot(cluster)
	cases := []struct {
		name string
		ts   string
		want int
	}{
		{"existing ts -> 200", scenario85TS, http.StatusOK},
		{"unknown 14-digit ts -> 404", "20200101000000", http.StatusNotFound},
		{"invalid ts format -> 400", "bk-1", http.StatusBadRequest},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodGet,
				scenario85Path("/backups/"+tc.ts), "")
			assert.Equal(s.T(), tc.want, rec.Code)
		})
	}
}

// --- 85d: DELETE /backups/{ts} -> cleanup Job ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_DeleteBackup_CleanupJob() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85AdminUser, scenario85AdminPass, http.MethodDelete,
		scenario85Path("/backups/"+scenario85TS), "")
	require.Equal(s.T(), http.StatusAccepted, rec.Code)
	resp := s.decode(rec)
	assert.Equal(s.T(), "deleted", resp["status"])
	jobName, ok := resp["job"].(string)
	require.True(s.T(), ok)

	job, script := s.jobScript(jobName)
	assert.Equal(s.T(), util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation],
		"85d: the cleanup Job must carry operation=cleanup")
	assert.Contains(s.T(), script, "backup-delete", "85d: cleanup runs gpbackman backup-delete")
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_DeleteBackup_OperatorForbidden403() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85OperUser, scenario85OperPass, http.MethodDelete,
		scenario85Path("/backups/"+scenario85TS), "")
	assert.Equal(s.T(), http.StatusForbidden, rec.Code, "85d: Operator must be forbidden (Admin required)")
}

// --- 85e: POST /backups/{ts}/restore -> Job args contain every gprestoreOption ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_RestoreBackup_FullOptionMapping() {
	s.boot(scenario85BackupCluster())

	body := `{
		"databases": ["` + scenario85DB + `"],
		"gprestoreOptions": {
			"jobs": 4,
			"redirectDb": "mydb_restored",
			"redirectSchema": "restored",
			"createDb": true,
			"includeTables": ["public.users", "public.orders"],
			"includeSchemas": ["public"],
			"excludeTables": ["public.audit"],
			"withGlobals": true,
			"withStats": true,
			"runAnalyze": true,
			"onErrorContinue": true,
			"truncateTable": true,
			"dataOnly": true,
			"resizeCluster": true
		}
	}`

	rec := s.do(scenario85AdminUser, scenario85AdminPass, http.MethodPost,
		scenario85Path("/backups/"+scenario85TS+"/restore"), body)
	require.Equal(s.T(), http.StatusAccepted, rec.Code, "85e: Admin POST restore must be accepted")
	resp := s.decode(rec)
	assert.Equal(s.T(), "restore started", resp["status"])
	jobName, ok := resp["job"].(string)
	require.True(s.T(), ok)

	job, script := s.jobScript(jobName)
	for _, want := range []string{
		"'--timestamp' '" + scenario85TS + "'",
		"'--jobs' '4'",
		"'--redirect-db' 'mydb_restored'",
		"'--redirect-schema' 'restored'",
		"'--create-db'",
		"'--with-globals'",
		"'--run-analyze'",
		"'--on-error-continue'",
		"'--truncate-table'",
		"'--data-only'",
		"'--resize-cluster'",
		"'--include-table' 'public.users'",
		"'--include-table' 'public.orders'",
		"'--exclude-table' 'public.audit'",
	} {
		assert.Containsf(s.T(), script, want, "85e: restore script must contain %q", want)
	}
	// include-table wins over include-schema; run-analyze wins over with-stats;
	// dataOnly set (not metadataOnly) -> no --metadata-only.
	assert.NotContains(s.T(), script, "'--include-schema'")
	assert.NotContains(s.T(), script, "'--with-stats'")
	assert.NotContains(s.T(), script, "'--metadata-only'")
	assert.Equal(s.T(), util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_RestoreBackup_DataAndMetadataOnly400() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85AdminUser, scenario85AdminPass, http.MethodPost,
		scenario85Path("/backups/"+scenario85TS+"/restore"),
		`{"gprestoreOptions":{"dataOnly":true,"metadataOnly":true}}`)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"85e negative: dataOnly+metadataOnly must be rejected with 400")
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_RestoreBackup_MetadataOnly() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85AdminUser, scenario85AdminPass, http.MethodPost,
		scenario85Path("/backups/"+scenario85TS+"/restore"),
		`{"gprestoreOptions":{"metadataOnly":true}}`)
	require.Equal(s.T(), http.StatusAccepted, rec.Code)
	resp := s.decode(rec)
	jobName, ok := resp["job"].(string)
	require.True(s.T(), ok)
	_, script := s.jobScript(jobName)
	assert.Contains(s.T(), script, "'--metadata-only'")
	assert.NotContains(s.T(), script, "'--data-only'")
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_RestoreBackup_OperatorForbidden403() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85OperUser, scenario85OperPass, http.MethodPost,
		scenario85Path("/backups/"+scenario85TS+"/restore"), `{}`)
	assert.Equal(s.T(), http.StatusForbidden, rec.Code, "85e: Operator must be forbidden (Admin required)")
}

// --- 85f: GET /backups/jobs -> job statuses ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_ListBackupJobs() {
	cluster := scenario85BackupCluster()
	running := scenario85BackupOpJob("scenario85-s3-backup-1", util.BackupOperationBackup)
	running.Status.Active = 1
	succeeded := scenario85BackupOpJob("scenario85-s3-restore-1", util.BackupOperationRestore)
	succeeded.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
	cleanup := scenario85BackupOpJob("scenario85-s3-cleanup-1", util.BackupOperationCleanup)
	unrelated := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scenario85-s3-unrelated",
			Namespace: scenario85Namespace,
			Labels:    map[string]string{util.LabelCluster: scenario85Cluster},
		},
	}
	s.boot(cluster, running, succeeded, cleanup, unrelated)

	rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodGet, scenario85Path("/backups/jobs"), "")
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decode(rec)
	assert.Equal(s.T(), float64(3), resp["total"], "85f: only backup-op jobs returned; unrelated excluded")

	jobs, ok := resp["jobs"].([]interface{})
	require.True(s.T(), ok)
	statuses := map[string]string{}
	for _, j := range jobs {
		jm := j.(map[string]interface{})
		statuses[jm["name"].(string)] = jm["status"].(string)
	}
	assert.Equal(s.T(), "running", statuses["scenario85-s3-backup-1"])
	assert.Equal(s.T(), "succeeded", statuses["scenario85-s3-restore-1"])
}

// scenario85BackupOpJob builds a backup-operation Job fixture in the test namespace.
func scenario85BackupOpJob(name, operation string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scenario85Namespace,
			Labels: map[string]string{
				util.LabelCluster:         scenario85Cluster,
				util.LabelBackupOperation: operation,
			},
		},
	}
}

// --- 85g: GET /backups/schedule -> CronJob status + nextScheduleTime ---

func (s *Scenario85APISuite) TestFunctional_Scenario85_GetSchedule_Present() {
	cluster := scenario85BackupCluster()
	lastRun := metav1.NewTime(metav1.Now().Add(-1))
	suspend := false
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupCronJobName(scenario85Cluster),
			Namespace: scenario85Namespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 2 * * *",
			Suspend:  &suspend,
		},
		Status: batchv1.CronJobStatus{LastScheduleTime: &lastRun},
	}
	s.boot(cluster, cronJob)

	rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodGet, scenario85Path("/backups/schedule"), "")
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decode(rec)
	assert.Equal(s.T(), true, resp["scheduled"])
	assert.Equal(s.T(), "0 2 * * *", resp["schedule"])
	assert.Equal(s.T(), false, resp["suspend"])
	require.NotNil(s.T(), resp["nextScheduleTime"], "85g: a scheduled cluster must surface nextScheduleTime")
}

func (s *Scenario85APISuite) TestFunctional_Scenario85_GetSchedule_Absent() {
	s.boot(scenario85BackupCluster())
	rec := s.do(scenario85BasicUser, scenario85BasicPass, http.MethodGet, scenario85Path("/backups/schedule"), "")
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := s.decode(rec)
	assert.Equal(s.T(), false, resp["scheduled"])
}
