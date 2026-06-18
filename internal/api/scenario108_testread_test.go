package api

// Scenario 108 — test-read REST handler (L.15 backing endpoint).
//
// handleTestReadPXFSource reads up to ?limit rows from a PXF source selected by
// ?job=<job> (primary) or by explicit ?server=&profile=&resource=. It is
// PermissionBasic, read-only, records NO metric, and upholds the honesty
// invariant: when the DB/source is unreachable it responds 200
// {available:false, rows:null} — NEVER a 500, NEVER fabricated rows. 400 on
// missing/invalid params; 404 on an unknown ?job=.
//
// Catalog IDs covered (see task-breackdown_claude_2026-06-16_14-22-36.out):
//   108-L15-api      ?job= happy → 200 available:true with real rows/columns
//   108-L15 explicit ?server=&profile=&resource= → 200
//   108-L15 404      unknown ?job=
//   108-L15 400      missing profile/resource (no job)
//   108-L15-limit    default 10, clamp >1000 → 1000, parse error → 400
//   108-L15-absent   db error / nil factory → 200 available:false rows:null
//   108-RBAC         Basic allowed (gate passes)

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// testReadDBClient embeds the base mock DB client and overrides only
// ReadPXFSourceSample so the success/error paths are configurable. It also
// records the exact (server, profile, resource, limit) the handler passed
// through so the limit-clamping behavior can be asserted.
type testReadDBClient struct {
	*mockDBClient
	sample *db.PXFSourceSample
	err    error

	gotServer   string
	gotProfile  string
	gotResource string
	gotLimit    int
}

func (c *testReadDBClient) ReadPXFSourceSample(
	_ context.Context, server, profile, resource string, limit int,
) (*db.PXFSourceSample, error) {
	c.gotServer = server
	c.gotProfile = profile
	c.gotResource = resource
	c.gotLimit = limit
	return c.sample, c.err
}

// newTestReadServer wires the cluster with a db factory yielding the given
// test-read fake client.
func newTestReadServer(
	cluster *cbv1alpha1.CloudberryCluster, client *testReadDBClient,
) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{client: client}
	return trackServer(NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0))
}

// testReadRequest builds a GET test-read request with the given query string
// (already URL-encoded; namespace is appended).
func testReadRequest(cluster, query string) *http.Request {
	url := apiPrefix + "/clusters/" + cluster + "/data-loading/test-read?namespace=default"
	if query != "" {
		url += "&" + query
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.SetPathValue("name", cluster)
	return req
}

// --- 108-L15-api: ?job= happy path -----------------------------------------

// A ?job= test-read resolves the source from the job's pxfJob and returns the
// REAL sampled rows/columns with available:true and rowCount == len(rows).
func TestScenario108_TestRead_ByJob_Happy(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{
		mockDBClient: &mockDBClient{},
		sample: &db.PXFSourceSample{
			Columns: []string{"line"},
			Rows:    [][]string{{"a,1"}, {"b,2"}, {"c,3"}},
		},
	}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw&limit=10"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp TestReadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	assert.True(t, resp.Available)
	assert.Equal(t, 3, resp.RowCount)
	assert.Equal(t, []string{"line"}, resp.Columns)
	require.Len(t, resp.Rows, 3)
	assert.Equal(t, []string{"a,1"}, resp.Rows[0])

	// The source was resolved from the job's pxfJob (s3srv / s3:parquet).
	assert.Equal(t, "s3srv", resp.Source.Server)
	assert.Equal(t, "s3:parquet", resp.Source.Profile)
	assert.Equal(t, 10, resp.Limit)

	// The handler passed the resolved source + limit through to the db client.
	assert.Equal(t, "s3srv", fakeClient.gotServer)
	assert.Equal(t, "s3:parquet", fakeClient.gotProfile)
	assert.Equal(t, 10, fakeClient.gotLimit)
}

// --- explicit ?server=&profile=&resource= ----------------------------------

func TestScenario108_TestRead_ByExplicitSource(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{
		mockDBClient: &mockDBClient{},
		sample: &db.PXFSourceSample{
			Columns: []string{"line"},
			Rows:    [][]string{{"x"}},
		},
	}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec,
		testReadRequest("test-cluster", "server=s3srv&profile=s3:text&resource=a/b.csv"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp TestReadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.True(t, resp.Available)
	assert.Equal(t, "s3srv", resp.Source.Server)
	assert.Equal(t, "s3:text", resp.Source.Profile)
	assert.Equal(t, "a/b.csv", resp.Source.Resource)
	assert.Equal(t, 1, resp.RowCount)

	assert.Equal(t, "a/b.csv", fakeClient.gotResource)
}

// --- 404 unknown job -------------------------------------------------------

func TestScenario108_TestRead_UnknownJob404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{mockDBClient: &mockDBClient{}}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=ghost"))

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobNotFound)
}

// A ?job= pointing at a non-PXF job is a 400 (test-read requires a pxf source).
func TestScenario108_TestRead_NonPXFJob400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	cluster.Spec.DataLoading.Jobs = append(cluster.Spec.DataLoading.Jobs,
		cbv1alpha1.DataLoadingJob{Name: "gp1", Type: "gpload"})
	fakeClient := &testReadDBClient{mockDBClient: &mockDBClient{}}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=gp1"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInvalidRequest)
}

// --- 400 missing profile/resource (no job) ---------------------------------

func TestScenario108_TestRead_MissingProfileResource400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{mockDBClient: &mockDBClient{}}
	s := newTestReadServer(cluster, fakeClient)

	// Only a server, no profile/resource and no job.
	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "server=s3srv"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInvalidRequest)
}

// --- limit handling: default 10, clamp >1000 → 1000, parse error → 400 ------

func TestScenario108_TestRead_DefaultLimit(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{
		mockDBClient: &mockDBClient{},
		sample:       &db.PXFSourceSample{Columns: []string{"line"}},
	}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	// No ?limit= → default 10.
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp TestReadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, testReadDefaultLimit, resp.Limit)
	assert.Equal(t, testReadDefaultLimit, fakeClient.gotLimit)
}

func TestScenario108_TestRead_LimitClampedToMax(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{
		mockDBClient: &mockDBClient{},
		sample:       &db.PXFSourceSample{Columns: []string{"line"}},
	}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw&limit=5000"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp TestReadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	// Clamped (rounded down) to the hard cap.
	assert.Equal(t, testReadMaxLimit, resp.Limit)
	assert.Equal(t, testReadMaxLimit, fakeClient.gotLimit)
}

func TestScenario108_TestRead_LimitParseError400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{mockDBClient: &mockDBClient{}}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw&limit=abc"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInvalidRequest)
}

func TestScenario108_TestRead_LimitZero400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{mockDBClient: &mockDBClient{}}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw&limit=0"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- 108-L15-absent (HONESTY): db error → 200 available:false rows:null -----

func TestScenario108_TestRead_DBError_HonestAbsent(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	fakeClient := &testReadDBClient{
		mockDBClient: &mockDBClient{},
		err:          assertErr("pxf source unreachable"),
	}
	s := newTestReadServer(cluster, fakeClient)

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw&limit=10"))

	// NOT a 500: an unreachable source is reported honestly as available:false.
	require.Equal(t, http.StatusOK, rec.Code)
	rawBody := rec.Body.String()
	var resp TestReadResponse
	require.NoError(t, json.Unmarshal([]byte(rawBody), &resp))
	assert.False(t, resp.Available)
	assert.Nil(t, resp.Rows)
	assert.Equal(t, 0, resp.RowCount)
	assert.Empty(t, resp.Columns)
	// The resolved source is still echoed so the caller knows what was attempted.
	assert.Equal(t, "s3srv", resp.Source.Server)

	// Raw body really carries rows:null (never fabricated).
	assert.Contains(t, rawBody, `"rows":null`)
}

// 108-L15-absent (connect error): a factory NewClient failure is also ABSENT,
// not a 500.
func TestScenario108_TestRead_ConnectError_HonestAbsent(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{clientErr: assertErr("cannot connect")}
	s := trackServer(NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0))

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp TestReadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.False(t, resp.Available)
	assert.Nil(t, resp.Rows)
}

// 108-L15-absent (nil factory): no db factory at all → available:false, rows:null.
func TestScenario108_TestRead_NilFactory_HonestAbsent(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster) // no db factory

	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp TestReadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.False(t, resp.Available)
	assert.Nil(t, resp.Rows)
}

// --- cluster / PXF gating --------------------------------------------------

func TestScenario108_TestRead_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("missing", "job=loadfdw"))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeClusterNotFound)
}

func TestScenario108_TestRead_PXFDisabled400(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default") // no DataLoading/PXF
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handleTestReadPXFSource(rec, testReadRequest("test-cluster", "job=loadfdw"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- 108-RBAC: test-read requires only PermissionBasic ---------------------

// Driven through the full router so withPermission actually runs: an identity at
// PermissionBasic passes the gate (the response is not 403).
func TestScenario108_TestRead_RBAC_BasicAllowed(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	url := apiPrefix + "/clusters/test-cluster/data-loading/test-read?namespace=default&job=loadfdw"
	req := httptest.NewRequest(http.MethodGet, url, strings.NewReader(""))
	rec := serveWithIdentity(s, req, auth.PermissionBasic)

	assert.NotEqual(t, http.StatusForbidden, rec.Code,
		"test-read must pass the permission gate at PermissionBasic")
}
