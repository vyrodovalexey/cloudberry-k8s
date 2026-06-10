// Package api: backup.go holds the backup/restore REST request types and the
// helpers that translate API requests into builder job options and responses.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/cron"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// responseKeyTimestamp is the JSON response key for a backup timestamp.
const responseKeyTimestamp = "timestamp"

// GpbackupOptionsRequest carries per-request gpbackup option overrides.
type GpbackupOptionsRequest struct {
	CompressionLevel  int32    `json:"compressionLevel,omitempty"`
	CompressionType   string   `json:"compressionType,omitempty"`
	Jobs              int32    `json:"jobs,omitempty"`
	SingleDataFile    bool     `json:"singleDataFile,omitempty"`
	CopyQueueSize     int32    `json:"copyQueueSize,omitempty"`
	Incremental       bool     `json:"incremental,omitempty"`
	FromTimestamp     string   `json:"fromTimestamp,omitempty"`
	IncludeSchemas    []string `json:"includeSchemas,omitempty"`
	ExcludeTables     []string `json:"excludeTables,omitempty"`
	LeafPartitionData bool     `json:"leafPartitionData,omitempty"`
	WithStats         *bool    `json:"withStats,omitempty"`
	WithoutGlobals    bool     `json:"withoutGlobals,omitempty"`
	NoCompression     bool     `json:"noCompression,omitempty"`
}

// CreateBackupRequest is the POST /backups request body.
type CreateBackupRequest struct {
	Type            string                  `json:"type,omitempty"`
	Databases       []string                `json:"databases,omitempty"`
	GpbackupOptions *GpbackupOptionsRequest `json:"gpbackupOptions,omitempty"`
}

// GprestoreOptionsRequest carries per-request gprestore option overrides.
type GprestoreOptionsRequest struct {
	Jobs            int32    `json:"jobs,omitempty"`
	RedirectDb      string   `json:"redirectDb,omitempty"`
	RedirectSchema  string   `json:"redirectSchema,omitempty"`
	CreateDb        bool     `json:"createDb,omitempty"`
	IncludeSchemas  []string `json:"includeSchemas,omitempty"`
	IncludeTables   []string `json:"includeTables,omitempty"`
	ExcludeTables   []string `json:"excludeTables,omitempty"`
	WithGlobals     bool     `json:"withGlobals,omitempty"`
	WithStats       *bool    `json:"withStats,omitempty"`
	RunAnalyze      bool     `json:"runAnalyze,omitempty"`
	OnErrorContinue bool     `json:"onErrorContinue,omitempty"`
	DataOnly        bool     `json:"dataOnly,omitempty"`
	MetadataOnly    bool     `json:"metadataOnly,omitempty"`
	TruncateTable   bool     `json:"truncateTable,omitempty"`
	ResizeCluster   bool     `json:"resizeCluster,omitempty"`
}

// RestoreRequest is the POST /backups/{timestamp}/restore request body.
type RestoreRequest struct {
	Timestamp        string                   `json:"timestamp,omitempty"`
	Databases        []string                 `json:"databases,omitempty"`
	GprestoreOptions *GprestoreOptionsRequest `json:"gprestoreOptions,omitempty"`
}

// UpdateBackupScheduleRequest is the PATCH /backups/schedule request body. A
// non-nil Schedule updates spec.backup.schedule; a non-nil Suspend toggles the
// CronJob's .spec.suspend in place.
type UpdateBackupScheduleRequest struct {
	Schedule *string `json:"schedule,omitempty"`
	Suspend  *bool   `json:"suspend,omitempty"`
}

// backupJobInfo is the API representation of a backup/restore/cleanup Job.
type backupJobInfo struct {
	Name           string       `json:"name"`
	Operation      string       `json:"operation"`
	Status         string       `json:"status"`
	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

// decodeOptionalJSON decodes the request body into v, treating an empty body
// (io.EOF) as a valid empty request so optional bodies are supported.
func decodeOptionalJSON(r *http.Request, v interface{}) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

// isValidBackupTimestamp reports whether ts is a valid gpbackup timestamp.
func isValidBackupTimestamp(ts string) bool {
	return util.IsGpbackupTimestamp(ts)
}

// newBackupTimestamp returns the current UTC time as a gpbackup-style
// YYYYMMDDHHMMSS timestamp used to name backup/restore/migration Jobs.
func newBackupTimestamp() string {
	return time.Now().UTC().Format(util.GpbackupTimestampLayout)
}

// isValidCronSchedule reports whether schedule is a parseable 5-field cron
// expr (shared internal/cron engine).
func isValidCronSchedule(schedule string) bool {
	return cron.Validate(schedule) == nil
}

// backupTypeOrDefault returns the requested type, defaulting to full.
func backupTypeOrDefault(t string) string {
	if t == "" {
		return util.BackupTypeFull
	}
	return t
}

// validateBackupDatabases validates database identifiers, writing an error
// response and returning false when any identifier is invalid.
func validateBackupDatabases(w http.ResponseWriter, databases []string) bool {
	for _, dbName := range databases {
		if !isValidIdentifier(dbName) {
			writeErrorJSON(w, http.StatusBadRequest, errCodeInvalidRequest,
				"invalid database identifier: "+dbName)
			return false
		}
	}
	return true
}

// isBackupOperation reports whether operation is a recognized backup operation.
func isBackupOperation(operation string) bool {
	switch operation {
	case util.BackupOperationBackup, util.BackupOperationRestore, util.BackupOperationCleanup:
		return true
	default:
		return false
	}
}

// buildBackupJobOptions overlays the request onto the cluster's backup defaults.
func buildBackupJobOptions(
	cluster *cbv1alpha1.CloudberryCluster,
	req *CreateBackupRequest,
	backupType, timestamp string,
) *builder.BackupJobOptions {
	opts := &builder.BackupJobOptions{
		Timestamp: timestamp,
		Type:      backupType,
		Databases: req.Databases,
	}
	if gp := req.GpbackupOptions; gp != nil {
		opts.Gpbackup = mergeGpbackupOptions(cluster, gp)
		opts.FromTimestamp = gp.FromTimestamp
		opts.IncludeSchemas = gp.IncludeSchemas
		opts.ExcludeTables = gp.ExcludeTables
	}
	return opts
}

// mergeGpbackupOptions overlays the request gpbackup options onto the cluster's
// configured defaults, returning a new options value.
func mergeGpbackupOptions(
	cluster *cbv1alpha1.CloudberryCluster,
	gp *GpbackupOptionsRequest,
) *cbv1alpha1.GpbackupOptions {
	out := &cbv1alpha1.GpbackupOptions{}
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.Gpbackup != nil {
		*out = *cluster.Spec.Backup.Gpbackup
	}
	out.CompressionLevel = orInt32(gp.CompressionLevel, out.CompressionLevel)
	out.CompressionType = orString(gp.CompressionType, out.CompressionType)
	out.Jobs = orInt32(gp.Jobs, out.Jobs)
	out.CopyQueueSize = orInt32(gp.CopyQueueSize, out.CopyQueueSize)
	out.SingleDataFile = gp.SingleDataFile
	out.Incremental = gp.Incremental
	out.LeafPartitionData = gp.LeafPartitionData
	// WithStats is a *bool: overlay only when the request explicitly set it so an
	// omitted withStats preserves the cluster's configured (webhook-defaulted) value.
	if gp.WithStats != nil {
		out.WithStats = gp.WithStats
	}
	out.WithoutGlobals = gp.WithoutGlobals
	out.NoCompression = gp.NoCompression
	return out
}

// buildRestoreJobOptions overlays the request onto the cluster's restore defaults.
func buildRestoreJobOptions(
	cluster *cbv1alpha1.CloudberryCluster,
	req *RestoreRequest,
	timestamp string,
) *builder.RestoreJobOptions {
	opts := &builder.RestoreJobOptions{
		Timestamp: timestamp,
		Databases: req.Databases,
	}
	if gr := req.GprestoreOptions; gr != nil {
		opts.Gprestore = mergeGprestoreOptions(cluster, gr)
		opts.RedirectDb = gr.RedirectDb
		opts.RedirectSchema = gr.RedirectSchema
		opts.IncludeSchemas = gr.IncludeSchemas
		opts.IncludeTables = gr.IncludeTables
		opts.ExcludeTables = gr.ExcludeTables
	}
	return opts
}

// mergeGprestoreOptions overlays the request gprestore options onto the cluster's
// configured defaults, returning a new options value.
func mergeGprestoreOptions(
	cluster *cbv1alpha1.CloudberryCluster,
	gr *GprestoreOptionsRequest,
) *cbv1alpha1.GprestoreOptions {
	out := &cbv1alpha1.GprestoreOptions{}
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.Gprestore != nil {
		*out = *cluster.Spec.Backup.Gprestore
	}
	out.Jobs = orInt32(gr.Jobs, out.Jobs)
	out.CreateDb = gr.CreateDb
	out.WithGlobals = gr.WithGlobals
	// WithStats is a *bool: overlay only when the request explicitly set it so an
	// omitted withStats preserves the cluster's configured (webhook-defaulted) value.
	if gr.WithStats != nil {
		out.WithStats = gr.WithStats
	}
	out.RunAnalyze = gr.RunAnalyze
	out.OnErrorContinue = gr.OnErrorContinue
	out.TruncateTable = gr.TruncateTable
	out.DataOnly = gr.DataOnly
	out.MetadataOnly = gr.MetadataOnly
	out.ResizeCluster = gr.ResizeCluster
	return out
}

// restoreOptionsConflict reports whether the restore request sets mutually
// exclusive options. gprestore --data-only and --metadata-only cannot be
// combined; returning a non-empty message indicates a 400-worthy conflict.
func restoreOptionsConflict(gr *GprestoreOptionsRequest) string {
	if gr == nil {
		return ""
	}
	if gr.DataOnly && gr.MetadataOnly {
		return "gprestore dataOnly and metadataOnly are mutually exclusive"
	}
	return ""
}

// orInt32 returns v when non-zero, otherwise fallback.
func orInt32(v, fallback int32) int32 {
	if v != 0 {
		return v
	}
	return fallback
}

// orString returns v when non-empty, otherwise fallback.
func orString(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

// jobStatus derives a coarse status string from a Job's conditions/counters.
func jobStatus(job *batchv1.Job) string {
	for i := range job.Status.Conditions {
		cond := &job.Status.Conditions[i]
		if cond.Status != "True" {
			continue
		}
		switch cond.Type {
		case batchv1.JobComplete:
			return "succeeded"
		case batchv1.JobFailed:
			return "failed"
		}
	}
	if job.Status.Active > 0 {
		return "running"
	}
	return statusPending
}

// newBackupJobInfo builds the API representation of a backup Job.
func newBackupJobInfo(job *batchv1.Job, operation string) backupJobInfo {
	return backupJobInfo{
		Name:           job.Name,
		Operation:      operation,
		Status:         jobStatus(job),
		StartTime:      job.Status.StartTime,
		CompletionTime: job.Status.CompletionTime,
	}
}

// backupScheduleResponse builds the backup schedule status response from a CronJob.
func backupScheduleResponse(cluster string, cronJob *batchv1.CronJob) map[string]interface{} {
	suspend := cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend
	resp := map[string]interface{}{
		responseKeyCluster: cluster,
		"scheduled":        true,
		"schedule":         cronJob.Spec.Schedule,
		"suspend":          suspend,
		"activeJobs":       len(cronJob.Status.Active),
	}
	if cronJob.Status.LastScheduleTime != nil {
		resp["lastScheduleTime"] = cronJob.Status.LastScheduleTime
	}
	if next := nextScheduleTime(cronJob); next != nil {
		resp["nextScheduleTime"] = next
	}
	return resp
}

// nextScheduleTime computes the next run time from the CronJob schedule. When the
// schedule cannot be parsed it falls back to the last schedule time, allowing the
// caller to surface the raw schedule string alongside it.
func nextScheduleTime(cronJob *batchv1.CronJob) *time.Time {
	from := time.Now().UTC()
	if cronJob.Status.LastScheduleTime != nil {
		from = cronJob.Status.LastScheduleTime.Time.UTC()
	}
	next, ok := computeNextCron(cronJob.Spec.Schedule, from)
	if !ok {
		return nil
	}
	return &next
}
