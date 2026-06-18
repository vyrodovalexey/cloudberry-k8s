// Package api: migrate.go implements cross-cluster database migration (spec 11
// §Cross-Cluster Migration). A migration creates a coordinated pair of Jobs — a
// gpbackup Job on the source cluster and a gprestore Job on the target cluster —
// that share a single timestamp and the same S3 destination bucket.
package api

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
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

	// Root span for the migration orchestration. It nests under the per-request
	// span created by the tracing middleware so the multi-phase flow (validate,
	// build, create) is visible as child spans. No-op when telemetry is disabled.
	ctx, span := telemetry.StartSpan(r.Context(), apiTracerName, "handleMigrate")
	defer span.End()

	req, ok := s.decodeMigrateRequest(w, r)
	if !ok {
		return
	}
	span.SetAttributes(
		attribute.String("migrate.source", req.SourceCluster),
		attribute.String("migrate.target", req.TargetCluster),
	)

	// Phase: resolve + validate both clusters.
	validateCtx, validateSpan := telemetry.StartSpan(ctx, apiTracerName, "migrate.validate")
	clusters, ok := s.resolveMigrateClusters(w, validateCtx, req)
	if !ok {
		validateSpan.End()
		return
	}
	validateSpan.End()

	// timestamp NAMES the migration Job (and its per-run scratch files). It is NOT
	// used as the gpbackup/gprestore timestamp: gpbackup generates its OWN
	// timestamp at runtime and exposes no flag to pin it, so the single migration
	// Job CAPTURES gpbackup's real "Backup Timestamp" and feeds it to gprestore
	// (spec 11 §Cross-Cluster Migration). This is the fix for the prior two-Job
	// topology, whose restore used the operator timestamp and failed with a
	// NotFound because the backup landed under gpbackup's own timestamp.
	timestamp := newBackupTimestamp()
	migrationJob := s.builder.BuildMigrationJob(migrateJobOptions(req, timestamp, clusters))
	span.SetAttributes(attribute.String("migrate.timestamp", timestamp))

	// Phase: create the coordinated migration Job.
	createCtx, createSpan := telemetry.StartSpan(ctx, apiTracerName, "migrate.create")
	if createErr := s.createMigrateJob(w, createCtx, migrationJob, "migration"); createErr != nil {
		// Record the SAME underlying error on both spans so the parent and child
		// agree and the root cause is preserved (not a fabricated placeholder).
		jobErr := fmt.Errorf("creating migration job %q: %w", migrationJob.Name, createErr)
		telemetry.SetSpanError(createSpan, jobErr)
		createSpan.End()
		telemetry.SetSpanError(span, jobErr)
		s.recordMigrate("error")
		return
	}
	createSpan.End()

	s.recordMigrate("started")
	s.writeMigrateAccepted(w, clusters, timestamp, migrationJob)
}

// recordMigrate records a migrate operation outcome on the dedicated
// cloudberry_migrate_operations_total counter (in addition to the proxied
// backup/restore records emitted by writeMigrateAccepted). Nil-safe.
func (s *Server) recordMigrate(result string) {
	if s.metrics != nil {
		s.metrics.RecordMigrateOperation(result)
	}
}

// migrateJobOptions builds the single coordinated migration Job options. The
// backup writes under the SOURCE cluster's S3 folder and the (target) restore +
// validation read from that same folder (spec 11 §Cross-Cluster Migration: both
// reference the same S3 bucket/folder); BuildMigrationJob applies the source
// folder to the rendered plugin config for both phases.
func migrateJobOptions(
	req *MigrateRequest,
	timestamp string,
	clusters *migrateClusters,
) *builder.MigrationJobOptions {
	return &builder.MigrationJobOptions{
		Timestamp:          timestamp,
		Source:             clusters.source,
		Target:             clusters.target,
		Database:           req.Database,
		RedirectDb:         req.RedirectDb,
		RedirectSchema:     req.RedirectSchema,
		IncludeTables:      req.Tables,
		SingleDataFile:     true,
		Truncate:           req.Truncate,
		Jobs:               req.Jobs,
		ValidationDatabase: migrateTargetDatabase(req),
	}
}

// decodeMigrateRequest decodes and structurally validates the migrate request.
func (s *Server) decodeMigrateRequest(w http.ResponseWriter, r *http.Request) (*MigrateRequest, bool) {
	var req MigrateRequest
	if decErr := decodeOptionalJSON(r, &req); decErr != nil {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return nil, false
	}
	if req.SourceCluster == "" {
		req.SourceCluster = r.PathValue("name")
	}
	if req.SourceCluster == "" || req.TargetCluster == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"sourceCluster and targetCluster are required")
		return nil, false
	}
	if req.SourceCluster == req.TargetCluster {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"sourceCluster and targetCluster must differ")
		return nil, false
	}
	// The migration backup phase runs gpbackup, which hard-requires --dbname
	// (`required flag(s) "dbname" not set`): a database-less migration could
	// only produce a Job that fails at runtime, so reject it up front.
	if req.Database == "" {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
			"database is required: the migration backup phase runs gpbackup, "+
				"which requires a target database (--dbname)")
		return nil, false
	}
	if !isValidIdentifier(req.Database) {
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
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
		writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest, msg)
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

// migrateTargetDatabase returns the database the validation Job should target.
func migrateTargetDatabase(req *MigrateRequest) string {
	if req.RedirectDb != "" {
		return req.RedirectDb
	}
	return req.Database
}

// createMigrateJob creates a single migration Job, writing an error response and
// returning the underlying error on failure (nil on success). The error is
// surfaced so callers can record the real root cause on their telemetry spans
// instead of fabricating a placeholder.
func (s *Server) createMigrateJob(
	w http.ResponseWriter,
	ctx context.Context,
	job *batchv1.Job,
	what string,
) error {
	if createErr := s.k8sClient.Create(ctx, job); createErr != nil {
		s.logger.Error("failed to create "+what+" job", "job", job.Name, "error", createErr)
		writeErrorJSON(w, http.StatusInternalServerError, errCodeInternal,
			"failed to create "+what+" job")
		return fmt.Errorf("creating %s job %q: %w", what, job.Name, createErr)
	}
	return nil
}

// writeMigrateAccepted writes the 202 response describing the created migration
// Job. The migration runs as a SINGLE coordinated Job (it captures the real
// gpbackup timestamp and feeds it to gprestore, then validates), so the
// backupJob/restoreJob/validationJob envelope fields all reference that one Job —
// it performs all three phases. The explicit migrationJob field names it
// unambiguously for clients that understand the single-Job topology.
func (s *Server) writeMigrateAccepted(
	w http.ResponseWriter,
	clusters *migrateClusters,
	timestamp string,
	migrationJob *batchv1.Job,
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
		"migrationJob":       migrationJob.Name,
		// The single migration Job performs the backup, restore and validation
		// phases; these fields reference it so existing clients still resolve a
		// meaningful Job name for each phase.
		"backupJob":     migrationJob.Name,
		"restoreJob":    migrationJob.Name,
		"validationJob": migrationJob.Name,
	})
}
