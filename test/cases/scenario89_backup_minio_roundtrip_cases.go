package cases

// ============================================================================
// Scenario 89 — Backup artifact round-trip against the REAL MinIO object store
// ============================================================================
//
// Acceptance scenario "backup to S3 (MinIO) + restore" requires that the
// object-store side of the backup pipeline actually works against the live
// MinIO from test/docker-compose: reachability, credentials, the provisioned
// bucket, the upload (backup) path, the download (restore) path with
// byte-for-byte integrity, and the delete (retention) path.
//
// The gpbackup/gprestore binaries themselves run inside the database pods and
// are exercised by the live e2e scripts once the operator is deployed; THIS
// scenario validates the real S3 dependency the backup Jobs talk to, using the
// same bucket ("cloudberry-backups"), the same folder layout
// ("<folder>/<cluster>/<timestamp>/...") and the same credentials that the
// Jobs receive from the backup-s3-credentials Secret
// (keys aws_access_key_id / aws_secret_access_key).
//
// Configuration comes ONLY from the environment (MINIO_ADDR, MINIO_ACCESS_KEY,
// MINIO_SECRET_KEY) with docker-compose defaults — no hardcoded endpoints.
// ============================================================================

// BackupMinIORoundTripCase describes one Scenario 89 sub-case.
type BackupMinIORoundTripCase struct {
	// ID is the scenario sub-id (89-1 .. 89-4).
	ID string
	// Description documents the sub-case's behavior.
	Description string
}

// Scenario89BackupMinIORoundTripCases is the catalog of all Scenario 89 sub-cases.
var Scenario89BackupMinIORoundTripCases = []BackupMinIORoundTripCase{
	{
		ID: "89-1",
		Description: "The provisioned backup bucket (cloudberry-backups) exists and is " +
			"reachable with the credentials the backup Jobs use.",
	},
	{
		ID: "89-2",
		Description: "Backup upload path: a synthetic gpbackup artifact set (metadata + " +
			"data segment) is uploaded under <folder>/<cluster>/<timestamp>/ and a " +
			"prefix listing returns exactly the uploaded keys.",
	},
	{
		ID: "89-3",
		Description: "Restore download path: every uploaded artifact is downloaded back " +
			"and matches the original byte-for-byte (sha256), proving integrity of " +
			"the store the restore Job reads from.",
	},
	{
		ID: "89-4",
		Description: "Retention delete path: deleting the backup timestamp prefix " +
			"removes all artifacts and a subsequent listing is empty (the retention " +
			"cleanup Job's delete semantics).",
	},
}
