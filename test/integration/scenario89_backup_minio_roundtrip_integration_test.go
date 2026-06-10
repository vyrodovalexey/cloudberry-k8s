//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 89: Backup artifact round-trip against the REAL MinIO — integration
// ============================================================================
//
// See test/cases/scenario89_backup_minio_roundtrip_cases.go for the catalog.
// This suite talks to the live docker-compose MinIO using the same bucket and
// folder layout the backup/restore/retention Jobs use, validating the real S3
// dependency end-to-end: reachability + credentials (89-1), upload/list (89-2),
// download integrity (89-3) and retention delete (89-4).
//
// Isolation: every run writes under a unique timestamp prefix and cleans up
// after itself, so parallel runs and CI re-runs never collide.
// ============================================================================

const (
	// scenario89Bucket matches the bucket provisioned by docker-compose and
	// referenced by spec.backup.destination.s3.bucket in the other scenarios.
	scenario89Bucket = "cloudberry-backups"
	// scenario89Folder mirrors spec.backup.destination.s3.folder.
	scenario89Folder = "backups"
	// scenario89Cluster is the synthetic cluster name in the artifact layout.
	scenario89Cluster = "s89-roundtrip"
	// scenario89Timeout bounds every object-store operation.
	scenario89Timeout = 60 * time.Second
)

// Scenario89BackupMinIORoundTripSuite drives the real MinIO object store.
type Scenario89BackupMinIORoundTripSuite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
	s3     *testutil.S3TestClient
	prefix string
	// artifacts maps object key -> uploaded content.
	artifacts map[string][]byte
}

func TestIntegration_Scenario89(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario89BackupMinIORoundTripSuite))
}

func (s *Scenario89BackupMinIORoundTripSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	s.s3 = testutil.NewS3TestClientFromEnv()

	probeCtx, probeCancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer probeCancel()
	if !s.s3.IsAvailable(probeCtx) {
		s.T().Skip("MinIO is not available, skipping scenario 89 round-trip")
	}

	// Unique per-run timestamp prefix for isolation (gpbackup-style layout:
	// <folder>/<cluster>/<timestamp>/...).
	timestamp := time.Now().UTC().Format("20060102150405")
	s.prefix = fmt.Sprintf("%s/%s/%s", scenario89Folder, scenario89Cluster, timestamp)

	// Synthetic gpbackup artifact set: metadata + a data segment + the report.
	s.artifacts = map[string][]byte{
		s.prefix + "/gpbackup_" + timestamp + "_metadata.sql": []byte(
			"-- gpbackup metadata\nCREATE TABLE public.events (id int, payload text) DISTRIBUTED BY (id);\n"),
		s.prefix + "/gpbackup_0_" + timestamp + ".gz": []byte(
			"synthetic-compressed-segment-data-0\x00\x01\x02"),
		s.prefix + "/gpbackup_" + timestamp + "_report": []byte(
			"timestamp: " + timestamp + "\nstatus: Success\n"),
	}
}

func (s *Scenario89BackupMinIORoundTripSuite) TearDownSuite() {
	// Best-effort cleanup so re-runs never see stale artifacts.
	if s.s3 != nil && s.prefix != "" {
		ctx, cancel := context.WithTimeout(context.Background(), scenario89Timeout)
		defer cancel()
		if keys, err := s.s3.ListObjects(ctx, scenario89Bucket, s.prefix); err == nil {
			for _, key := range keys {
				_ = s.s3.DeleteObject(ctx, scenario89Bucket, key)
			}
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// TestIntegration_Scenario89_RoundTrip runs the full backup→restore→retention
// object-store journey in order (89-1 .. 89-4). A single ordered journey, not
// independent subtests, because each phase consumes the previous one's state —
// exactly like the real backup pipeline.
func (s *Scenario89BackupMinIORoundTripSuite) TestIntegration_Scenario89_RoundTrip() {
	t := s.T()

	// Log the catalog so a CI failure is traceable to the contract.
	for _, c := range cases.Scenario89BackupMinIORoundTripCases {
		t.Logf("case %s: %s", c.ID, c.Description)
	}

	// --- 89-1: bucket exists and credentials work ---
	ctx, cancel := context.WithTimeout(s.ctx, scenario89Timeout)
	defer cancel()

	exists, err := s.s3.BucketExists(ctx, scenario89Bucket)
	require.NoError(t, err, "89-1: HEAD bucket with backup credentials should succeed")
	require.True(t, exists,
		"89-1: bucket %q must be provisioned by the test environment", scenario89Bucket)
	t.Logf("89-1: bucket %s exists and credentials are valid", scenario89Bucket)

	// --- 89-2: upload the artifact set and list it back ---
	for key, content := range s.artifacts {
		require.NoError(t, s.s3.PutObject(ctx, scenario89Bucket, key, content),
			"89-2: uploading artifact %s", key)
	}

	keys, err := s.s3.ListObjects(ctx, scenario89Bucket, s.prefix)
	require.NoError(t, err, "89-2: listing the backup prefix should succeed")
	assert.Len(t, keys, len(s.artifacts),
		"89-2: listing must return exactly the uploaded artifacts")
	for _, key := range keys {
		_, known := s.artifacts[key]
		assert.True(t, known, "89-2: unexpected key in listing: %s", key)
	}
	t.Logf("89-2: uploaded and listed %d artifacts under %s", len(keys), s.prefix)

	// --- 89-3: download every artifact back and verify integrity ---
	for key, want := range s.artifacts {
		got, err := s.s3.GetObject(ctx, scenario89Bucket, key)
		require.NoError(t, err, "89-3: downloading artifact %s", key)
		assert.Equal(t, sha256.Sum256(want), sha256.Sum256(got),
			"89-3: artifact %s must match byte-for-byte after the round-trip", key)
	}
	t.Logf("89-3: all %d artifacts verified byte-for-byte", len(s.artifacts))

	// --- 89-4: retention delete removes the whole timestamp prefix ---
	for key := range s.artifacts {
		require.NoError(t, s.s3.DeleteObject(ctx, scenario89Bucket, key),
			"89-4: deleting artifact %s", key)
	}

	keys, err = s.s3.ListObjects(ctx, scenario89Bucket, s.prefix)
	require.NoError(t, err, "89-4: listing after retention delete should succeed")
	assert.Empty(t, keys, "89-4: the backup prefix must be empty after retention delete")
	t.Logf("89-4: retention delete removed prefix %s", s.prefix)
}
