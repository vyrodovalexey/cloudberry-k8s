// Package api: migrate.go implements cross-cluster database migration (spec 11
// §Cross-Cluster Migration). A migration creates a coordinated pair of Jobs — a
// gpbackup Job on the source cluster and a gprestore Job on the target cluster —
// that share a single timestamp and the same S3 destination bucket.
package api

import (
	"context"
	"fmt"
	"net/http"

	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// migrateDestinationTypeS3 is the S3 backup-destination discriminator.
const migrateDestinationTypeS3 = "s3"

// MigrateRequest is the POST /clusters/{name}/migrate request body. The path
// {name} identifies the source cluster; TargetCluster names the destination.
type MigrateRequest struct {
	SourceCluster  string   `json:"sourceCluster,omitempty"`
	TargetCluster  string   `json:"targetCluster,omitempty"`
	Database       string   `json:"database,omitempty"`
	Tables         []string `json:"tables,omitempty"`
	Truncate       bool     `json:"truncate,omitempty"`
	RedirectDb     string   `json:"redirectDb,omitempty"`
	RedirectSchema string   `json:"redirectSchema,omitempty"`
	Jobs           int32    `json:"jobs,omitempty"`
}

// migrateClusters bundles the validated source and target clusters.
type migrateClusters struct {
	source *cbv1alpha1.CloudberryCluster
	target *cbv1alpha1.CloudberryCluster
}

// handleMigrate coordinates a cross-cluster migration by creating a source
// backup Job and a target restore Job that share a timestamp and S3 bucket, plus
// an optional post-migration validation Job on the target.
func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	defer r.Body.Close()

	req, ok := s.decodeMigrateRequest(w, r)
	if !ok {
		return
	}

	clusters, ok := s.resolveMigrateClusters(w, r.Context(), req)
	if !ok {
		return
	}

	timestamp := newBackupTimestamp()
	backupJob := s.builder.BuildBackupJob(clusters.source, migrateBackupOptions(req, timestamp))
	restoreJob := s.builder.BuildRestoreJob(clusters.target, migrateRestoreOptions(req, timestamp))

	if !s.createMigrateJob(w, r.Context(), backupJob, "migration backup") {
		return
	}
	if !s.createMigrateJob(w, r.Context(), restoreJob, "migration restore") {
		return
	}

	validationJob := s.builder.BuildPostRestoreValidationJob(clusters.target,
		&builder.ValidationJobOptions{Timestamp: timestamp, Database: migrateTargetDatabase(req)})
	// Validation is best-effort: a failure to create it must not fail the migration.
	if createErr := s.k8sClient.Create(r.Context(), validationJob); createErr != nil {
		s.logger.Warn("failed to create migration validation job",
			"target", clusters.target.Name, "error", createErr)
	}

	s.writeMigrateAccepted(w, clusters, timestamp, backupJob, restoreJob, validationJob)
}

// decodeMigrateRequest decodes and structurally validates the migrate request.
func (s *Server) decodeMigrateRequest(w http.ResponseWriter, r *http.Request) (*MigrateRequest, bool) {
	var req MigrateRequest
	if decErr := decodeOptionalJSON(r, &req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return nil, false
	}
	if req.SourceCluster == "" {
		req.SourceCluster = r.PathValue("name")
	}
	if req.SourceCluster == "" || req.TargetCluster == "" {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST",
			"sourceCluster and targetCluster are required")
		return nil, false
	}
	if req.SourceCluster == req.TargetCluster {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST",
			"sourceCluster and targetCluster must differ")
		return nil, false
	}
	if req.Database != "" && !isValidIdentifier(req.Database) {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST",
			"invalid database identifier: "+req.Database)
		return nil, false
	}
	return &req, true
}

// resolveMigrateClusters loads both clusters and validates that they are
// backup-enabled and share the same S3 destination bucket.
func (s *Server) resolveMigrateClusters(
	w http.ResponseWriter,
	ctx context.Context,
	req *MigrateRequest,
) (*migrateClusters, bool) {
	namespace := req.namespaceOrEmpty()

	source, err := s.getCluster(ctx, req.SourceCluster, namespace)
	if err != nil {
		writeClusterNotFound(w, req.SourceCluster)
		return nil, false
	}
	target, err := s.getCluster(ctx, req.TargetCluster, namespace)
	if err != nil {
		writeClusterNotFound(w, req.TargetCluster)
		return nil, false
	}

	if msg := validateMigrateDestinations(source, target); msg != "" {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", msg)
		return nil, false
	}
	return &migrateClusters{source: source, target: target}, true
}

// namespaceOrEmpty returns the namespace hint for cluster lookups. Migration
// always resolves clusters by name across namespaces, so this is empty.
func (*MigrateRequest) namespaceOrEmpty() string { return "" }

// validateMigrateDestinations ensures both clusters have backup enabled with an
// S3 destination pointing at the same bucket. Returns a non-empty message on
// failure suitable for a 400 response.
func validateMigrateDestinations(source, target *cbv1alpha1.CloudberryCluster) string {
	srcBucket, ok := s3Bucket(source)
	if !ok {
		return "source cluster must have backup enabled with an S3 destination"
	}
	dstBucket, ok := s3Bucket(target)
	if !ok {
		return "target cluster must have backup enabled with an S3 destination"
	}
	if srcBucket != dstBucket {
		return fmt.Sprintf(
			"source and target clusters must share the same S3 bucket (source=%q, target=%q)",
			srcBucket, dstBucket)
	}
	return ""
}

// s3Bucket returns the cluster's configured S3 bucket and whether the cluster has
// backup enabled with an S3 destination.
func s3Bucket(cluster *cbv1alpha1.CloudberryCluster) (string, bool) {
	b := cluster.Spec.Backup
	if b == nil || !b.Enabled || b.Destination.Type != migrateDestinationTypeS3 {
		return "", false
	}
	if b.Destination.S3 == nil || b.Destination.S3.Bucket == "" {
		return "", false
	}
	return b.Destination.S3.Bucket, true
}

// migrateBackupOptions builds the source-side gpbackup options for a migration.
func migrateBackupOptions(req *MigrateRequest, timestamp string) *builder.BackupJobOptions {
	opts := &builder.BackupJobOptions{
		Timestamp:     timestamp,
		Type:          util.BackupTypeFull,
		IncludeTables: req.Tables,
		Gpbackup:      &cbv1alpha1.GpbackupOptions{SingleDataFile: true},
	}
	if req.Database != "" {
		opts.Databases = []string{req.Database}
	}
	return opts
}

// migrateRestoreOptions builds the target-side gprestore options for a migration.
func migrateRestoreOptions(req *MigrateRequest, timestamp string) *builder.RestoreJobOptions {
	redirectDb := req.RedirectDb
	if redirectDb == "" {
		redirectDb = req.Database
	}
	return &builder.RestoreJobOptions{
		Timestamp:      timestamp,
		RedirectDb:     redirectDb,
		RedirectSchema: req.RedirectSchema,
		IncludeTables:  req.Tables,
		Gprestore:      &cbv1alpha1.GprestoreOptions{Jobs: req.Jobs, TruncateTable: req.Truncate},
	}
}

// migrateTargetDatabase returns the database the validation Job should target.
func migrateTargetDatabase(req *MigrateRequest) string {
	if req.RedirectDb != "" {
		return req.RedirectDb
	}
	return req.Database
}

// createMigrateJob creates a single migration Job, writing an error response and
// returning false on failure.
func (s *Server) createMigrateJob(
	w http.ResponseWriter,
	ctx context.Context,
	job *batchv1.Job,
	what string,
) bool {
	if createErr := s.k8sClient.Create(ctx, job); createErr != nil {
		s.logger.Error("failed to create "+what+" job", "job", job.Name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR",
			"failed to create "+what+" job")
		return false
	}
	return true
}

// writeMigrateAccepted writes the 202 response describing the created Jobs.
func (s *Server) writeMigrateAccepted(
	w http.ResponseWriter,
	clusters *migrateClusters,
	timestamp string,
	backupJob, restoreJob, validationJob *batchv1.Job,
) {
	if s.metrics != nil {
		s.metrics.RecordBackup(clusters.source.Name, clusters.source.Namespace,
			util.BackupTypeFull, "started")
		s.metrics.RecordRestore(clusters.target.Name, clusters.target.Namespace, "started")
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		responseKeyStatus:    "migration started",
		"sourceCluster":      clusters.source.Name,
		"targetCluster":      clusters.target.Name,
		responseKeyTimestamp: timestamp,
		"backupJob":          backupJob.Name,
		"restoreJob":         restoreJob.Name,
		"validationJob":      validationJob.Name,
	})
}
