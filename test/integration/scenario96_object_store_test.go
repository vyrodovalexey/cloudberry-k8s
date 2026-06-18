//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 96: Object Store sample data against the REAL MinIO — integration
// ============================================================================
//
// This suite gates on MinIO reachability (skips cleanly when MinIO/compose is
// down). It assumes the gen-objstore-samples.sh generator has run (or runs it
// implicitly is NOT required here — the test only ASSERTS the synthesizable
// samples are present and that the operator-built DDL would target the right
// pxf:// resource). For the natively-synthesizable formats (text/CSV + JSON) the
// sample objects MUST exist in the MinIO buckets; parquet/avro are asserted when
// present and otherwise reported [CONFIG-ONLY]; ORC is always [CONFIG-ONLY].
//
// The live row-landing happens at e2e (KUBECONFIG-gated). Here we prove:
//   1. the MinIO buckets cloudberry-data / cloudberry-warehouse exist,
//   2. the synthesizable sample objects exist (text/json — required),
//   3. the operator's BUILT load Job DDL targets the byte-correct pxf:// resource
//      for the corresponding OS.* catalog rows.
//
// Isolation: read-only against shared sample buckets; no mutation, so parallel
// runs and CI re-runs never collide.
// ============================================================================

const (
	// scenario96DataBucket is the s3-datalake-backed bucket.
	scenario96DataBucket = "cloudberry-data"
	// scenario96WarehouseBucket is the minio-warehouse-backed bucket.
	scenario96WarehouseBucket = "cloudberry-warehouse"
	// scenario96Timeout bounds each object-store operation.
	scenario96Timeout = 60 * time.Second
)

// scenario96SampleObject describes an expected sample object and whether it is
// natively synthesizable (required to exist) or config-only (best-effort).
type scenario96SampleObject struct {
	key      string
	format   string
	required bool // true => text/json (native); false => parquet/avro (tooling)
}

// scenario96SampleObjects returns the expected sample object set per bucket. text
// and json are natively synthesizable (required); parquet/avro are tooling-gated
// (best-effort). ORC is never generated ([CONFIG-ONLY]) so it is not listed.
func scenario96SampleObjects() []scenario96SampleObject {
	return []scenario96SampleObject{
		{key: "text/data.csv", format: "text", required: true},
		{key: "json/data.json", format: "json", required: true},
		{key: "parquet/data.parquet", format: "parquet", required: false},
		{key: "avro/data.avro", format: "avro", required: false},
	}
}

// Scenario96ObjectStoreSuite drives the real MinIO object store for Scenario 96.
type Scenario96ObjectStoreSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	s3      *testutil.S3TestClient
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario96(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario96ObjectStoreSuite))
}

func (s *Scenario96ObjectStoreSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	s.s3 = testutil.NewS3TestClientFromEnv()
	s.builder = builder.NewBuilder()

	probeCtx, probeCancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer probeCancel()
	if !s.s3.IsAvailable(probeCtx) {
		s.T().Skip("MinIO is not available, skipping Scenario 96 object-store integration")
	}
}

func (s *Scenario96ObjectStoreSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// TestIntegration_Scenario96_BucketsExist asserts the two Scenario 96 sample
// buckets are provisioned. If the warehouse bucket is missing the gen script has
// not been run; the test skips cleanly with a clear message.
func (s *Scenario96ObjectStoreSuite) TestIntegration_Scenario96_BucketsExist() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario96Timeout)
	defer cancel()

	dataExists, err := s.s3.BucketExists(ctx, scenario96DataBucket)
	require.NoError(s.T(), err, "HEAD %s must succeed with MinIO credentials", scenario96DataBucket)
	require.True(s.T(), dataExists, "bucket %q must be provisioned", scenario96DataBucket)

	whExists, err := s.s3.BucketExists(ctx, scenario96WarehouseBucket)
	require.NoError(s.T(), err, "HEAD %s must succeed", scenario96WarehouseBucket)
	if !whExists {
		s.T().Skipf("bucket %q not present — run gen-objstore-samples.sh first",
			scenario96WarehouseBucket)
	}
	s.T().Logf("scenario96: buckets %s + %s present",
		scenario96DataBucket, scenario96WarehouseBucket)
}

// TestIntegration_Scenario96_SampleObjectsExist asserts, per bucket, that the
// natively-synthesizable sample objects (text/json) exist and reports the
// tooling-gated formats (parquet/avro) as present or [CONFIG-ONLY]. ORC is always
// [CONFIG-ONLY]. Skips cleanly if the gen script has not staged the samples.
func (s *Scenario96ObjectStoreSuite) TestIntegration_Scenario96_SampleObjectsExist() {
	for _, bucket := range []string{scenario96DataBucket, scenario96WarehouseBucket} {
		bucket := bucket
		s.Run(bucket, func() {
			ctx, cancel := context.WithTimeout(s.ctx, scenario96Timeout)
			defer cancel()

			exists, err := s.s3.BucketExists(ctx, bucket)
			require.NoError(s.T(), err)
			if !exists {
				s.T().Skipf("bucket %q not present — run gen-objstore-samples.sh first", bucket)
			}

			// If no sample objects are present at all, the gen script has not run.
			keys, err := s.s3.ListObjects(ctx, bucket, "")
			require.NoError(s.T(), err, "list %s must succeed", bucket)
			present := map[string]bool{}
			for _, k := range keys {
				present[k] = true
			}
			haveAnySample := false
			for _, o := range scenario96SampleObjects() {
				if present[o.key] {
					haveAnySample = true
					break
				}
			}
			if !haveAnySample {
				s.T().Skipf("no Scenario 96 samples in %q — run gen-objstore-samples.sh first", bucket)
			}

			for _, o := range scenario96SampleObjects() {
				o := o
				if o.required {
					require.Truef(s.T(), present[o.key],
						"%s sample %q (native) must exist in %s", o.format, o.key, bucket)
					// Verify the object is genuinely readable (non-empty).
					body, getErr := s.s3.GetObject(ctx, bucket, o.key)
					require.NoErrorf(s.T(), getErr, "GET %s/%s", bucket, o.key)
					assert.NotEmptyf(s.T(), body, "%s/%s must be non-empty", bucket, o.key)
					s.T().Logf("scenario96: %s/%s present (%d bytes, native %s)",
						bucket, o.key, len(body), o.format)
				} else if present[o.key] {
					s.T().Logf("scenario96: %s/%s present (tooling-generated %s)",
						bucket, o.key, o.format)
				} else {
					s.T().Logf("scenario96: %s/%s [CONFIG-ONLY] in %s (tooling absent)",
						o.format, o.key, bucket)
				}
			}
			s.T().Logf("scenario96: s3:orc is [CONFIG-ONLY] (never synthesized) in %s", bucket)
		})
	}
}

// TestIntegration_Scenario96_BuiltDDLTargetsSampleResource binds the OS.* catalog
// to the staged samples: for the live-read OS rows on the MinIO-backed servers,
// the operator-built load Job DDL must target a pxf:// LOCATION whose resource
// matches the bucket/prefix where the sample lives. This proves the operator's
// generated SQL would read the exact object the integration env stages.
func (s *Scenario96ObjectStoreSuite) TestIntegration_Scenario96_BuiltDDLTargetsSampleResource() {
	cluster := scenario96IntegrationCluster()

	for _, tc := range cases.Scenario96Cases() {
		tc := tc
		// Only the live-read OS rows on the MinIO-backed servers are relevant.
		if !strings.HasPrefix(tc.ID, "OS.") || !strings.Contains(tc.Layer, "live-read") {
			continue
		}
		bucket := scenario96WarehouseBucket
		if tc.Server == cases.Scenario96ServerS3Datalake {
			bucket = scenario96DataBucket
		}
		s.Run(tc.ID, func() {
			_, format, _ := strings.Cut(tc.Profile, ":")
			resource := bucket + "/" + format + "/data." + scenario96FormatExt(format)
			job := cbv1alpha1.DataLoadingJob{
				Name:    "s96-int-" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")),
				Type:    "pxf",
				Enabled: true,
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      tc.Server,
					Profile:     tc.Profile,
					Resource:    resource,
					TargetTable: "public.s96_int_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")),
				},
			}
			out := s.builder.BuildDataLoadJob(cluster, job)
			require.NotNil(s.T(), out)
			script := out.Spec.Template.Spec.Containers[0].Args[0]

			wantLoc := "pxf://" + resource + "?PROFILE=" + tc.Profile + "&SERVER=" + tc.Server
			assert.Containsf(s.T(), script, wantLoc,
				"%s built DDL must target the sample resource %q", tc.ID, resource)
		})
	}
}

// scenario96FormatExt maps a profile format to the sample file extension.
func scenario96FormatExt(format string) string {
	switch format {
	case "text":
		return "csv"
	case "json":
		return "json"
	case "parquet":
		return "parquet"
	case "avro":
		return "avro"
	default:
		return format
	}
}

// scenario96IntegrationCluster builds a cluster carrying the two MinIO-backed
// object-store servers so the builder synthesizes the pxf:// LOCATION.
func scenario96IntegrationCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("s96-integration", "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name:   cases.Scenario96ServerS3Datalake,
					Type:   "s3",
					Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
				},
				{
					Name: cases.Scenario96ServerMinioWarehouse,
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint":          "http://minio:9000",
						"fs.s3a.path.style.access": "true",
					},
				},
			},
		},
	}
	return cluster
}
