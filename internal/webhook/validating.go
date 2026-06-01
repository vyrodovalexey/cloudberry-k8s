// Package webhook provides admission webhooks for the cloudberry operator.
package webhook

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// Webhook admission metric label values.
const (
	// webhookValidating and webhookMutating are the webhook label values.
	webhookValidating = "validating"
	webhookMutating   = "mutating"

	// admissionOpCreate, admissionOpUpdate, and admissionOpDelete are the
	// operation label values for admission metrics.
	admissionOpCreate = "create"
	admissionOpUpdate = "update"
	admissionOpDelete = "delete"

	// admissionAllowed, admissionDenied, and admissionError are the result
	// label values for admission metrics.
	admissionAllowed = "allowed"
	admissionDenied  = "denied"
	admissionError   = "error"
)

// CloudberryClusterValidator validates CloudberryCluster resources.
type CloudberryClusterValidator struct {
	reader client.Reader
	// recorder records admission metrics. It is optional and may be nil;
	// all metric recording is guarded with a nil check.
	recorder metrics.Recorder
}

// NewCloudberryClusterValidator creates a new CloudberryClusterValidator.
// An optional metrics recorder may be supplied to record admission metrics;
// when omitted (or nil), metric recording is a no-op.
func NewCloudberryClusterValidator(
	reader client.Reader,
	recorder ...metrics.Recorder,
) *CloudberryClusterValidator {
	v := &CloudberryClusterValidator{reader: reader}
	if len(recorder) > 0 {
		v.recorder = recorder[0]
	}
	return v
}

// recordAdmission records a validating-webhook admission decision when a
// recorder is configured. It is nil-safe and derives the result from the error
// and warnings returned by the validation.
func (v *CloudberryClusterValidator) recordAdmission(operation string, err error) {
	if v.recorder == nil {
		return
	}
	result := admissionAllowed
	if err != nil {
		result = admissionDenied
	}
	v.recorder.RecordWebhookAdmission(webhookValidating, operation, result)
}

// ValidateCreate validates a CloudberryCluster on creation.
func (v *CloudberryClusterValidator) ValidateCreate(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	warnings, err := v.validateCreate(ctx, cluster)
	v.recordAdmission(admissionOpCreate, err)
	return warnings, err
}

// validateCreate performs the create-time validation logic.
func (v *CloudberryClusterValidator) validateCreate(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	// Check for duplicate cluster names across all namespaces.
	if v.reader != nil {
		if err := v.checkDuplicateName(ctx, cluster); err != nil {
			return nil, err
		}
	}
	return validateCluster(cluster)
}

// checkDuplicateName checks if a CloudberryCluster with the same name already exists
// in any namespace.
func (v *CloudberryClusterValidator) checkDuplicateName(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) error {
	clusterList := &cbv1alpha1.CloudberryClusterList{}
	if err := v.reader.List(ctx, clusterList); err != nil {
		return fmt.Errorf("listing clusters for duplicate check: %w", err)
	}

	for i := range clusterList.Items {
		existing := &clusterList.Items[i]
		if existing.Name == cluster.Name && existing.Namespace != cluster.Namespace {
			return fmt.Errorf(
				"CloudberryCluster with name %q already exists in namespace %q",
				existing.Name, existing.Namespace,
			)
		}
	}

	return nil
}

// ValidateUpdate validates a CloudberryCluster on update.
func (v *CloudberryClusterValidator) ValidateUpdate(
	_ context.Context,
	oldCluster *cbv1alpha1.CloudberryCluster,
	newCluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	warnings, err := validateUpdate(oldCluster, newCluster)
	v.recordAdmission(admissionOpUpdate, err)
	return warnings, err
}

// validateUpdate performs the update-time validation logic.
func validateUpdate(
	oldCluster, newCluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	warnings, err := validateCluster(newCluster)
	if err != nil {
		return warnings, err
	}
	// Validate mirroring state transitions.
	mirrorWarnings, mirrorErr := validateMirroringTransition(oldCluster, newCluster)
	warnings = append(warnings, mirrorWarnings...)
	if mirrorErr != nil {
		return warnings, mirrorErr
	}
	return warnings, nil
}

// ValidateDelete validates a CloudberryCluster on deletion.
func (v *CloudberryClusterValidator) ValidateDelete(
	_ context.Context,
	_ *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	// No validation needed on delete.
	v.recordAdmission(admissionOpDelete, nil)
	return nil, nil
}

// validateCluster performs validation on a CloudberryCluster.
func validateCluster(cluster *cbv1alpha1.CloudberryCluster) (admission.Warnings, error) {
	var warnings admission.Warnings

	if err := validateSegments(cluster, &warnings); err != nil {
		return warnings, err
	}
	if err := validateOIDC(cluster); err != nil {
		return warnings, err
	}
	if err := validateVault(cluster); err != nil {
		return warnings, err
	}
	if err := validateDeletionPolicy(cluster); err != nil {
		return warnings, err
	}
	if err := validateStorage(cluster); err != nil {
		return warnings, err
	}
	if err := validateWorkload(cluster); err != nil {
		return warnings, err
	}
	if err := validateQueryMonitoring(cluster); err != nil {
		return warnings, err
	}
	if err := validateBackup(cluster); err != nil {
		return warnings, err
	}
	if err := validateDataLoading(cluster); err != nil {
		return warnings, err
	}
	if err := validateStorageManagement(cluster); err != nil {
		return warnings, err
	}

	return warnings, nil
}

// validateSegments validates segment configuration.
func validateSegments(cluster *cbv1alpha1.CloudberryCluster, warnings *admission.Warnings) error {
	if cluster.Spec.Segments.Count < 1 {
		return fmt.Errorf("segments.count must be >= 1, got %d", cluster.Spec.Segments.Count)
	}

	mirroring := cluster.Spec.Segments.Mirroring
	if mirroring != nil && mirroring.Enabled && mirroring.Layout == cbv1alpha1.MirroringLayoutSpread {
		primariesPerHost := cluster.Spec.Segments.PrimariesPerHost
		if primariesPerHost == 0 {
			primariesPerHost = 2
		}
		if cluster.Spec.Segments.Count <= primariesPerHost {
			*warnings = append(*warnings,
				"spread mirroring layout may require more segment hosts than primariesPerHost")
		}
	}
	return nil
}

// validateOIDC validates OIDC configuration.
func validateOIDC(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.Auth == nil || cluster.Spec.Auth.OIDC == nil || !cluster.Spec.Auth.OIDC.Enabled {
		return nil
	}
	if cluster.Spec.Auth.OIDC.IssuerURL == "" {
		return fmt.Errorf("auth.oidc.issuerURL is required when OIDC is enabled")
	}
	if cluster.Spec.Auth.OIDC.ClientID == "" {
		return fmt.Errorf("auth.oidc.clientID is required when OIDC is enabled")
	}
	return nil
}

// validateVault validates Vault configuration.
func validateVault(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.Vault != nil && cluster.Spec.Vault.Enabled && cluster.Spec.Vault.Address == "" {
		return fmt.Errorf("vault.address is required when Vault is enabled")
	}
	return nil
}

// validateDeletionPolicy validates the deletion policy.
func validateDeletionPolicy(cluster *cbv1alpha1.CloudberryCluster) error {
	dp := cluster.Spec.DeletionPolicy
	if dp != "" && dp != cbv1alpha1.DeletionPolicyRetain && dp != cbv1alpha1.DeletionPolicyDelete {
		return fmt.Errorf("deletionPolicy must be Retain or Delete, got %s", dp)
	}
	return nil
}

// validateStorage validates storage configuration.
func validateStorage(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.Coordinator.Storage.Size == "" {
		return fmt.Errorf("coordinator.storage.size is required")
	}
	if cluster.Spec.Segments.Storage.Size == "" {
		return fmt.Errorf("segments.storage.size is required")
	}
	return nil
}

// validateWorkload validates workload management configuration.
func validateWorkload(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.Workload == nil || !cluster.Spec.Workload.Enabled {
		return nil
	}

	for i, rg := range cluster.Spec.Workload.ResourceGroups {
		if rg.Name == "" {
			return fmt.Errorf("workload.resourceGroups[%d].name is required", i)
		}
	}

	for i, rule := range cluster.Spec.Workload.Rules {
		if rule.Name == "" {
			return fmt.Errorf("workload.rules[%d].name is required", i)
		}
		if rule.Action == "" {
			return fmt.Errorf("workload.rules[%d].action is required", i)
		}
	}

	for i, rule := range cluster.Spec.Workload.IdleRules {
		if rule.Name == "" {
			return fmt.Errorf("workload.idleRules[%d].name is required", i)
		}
		if rule.ResourceGroup == "" {
			return fmt.Errorf("workload.idleRules[%d].resourceGroup is required", i)
		}
		if rule.IdleTimeout == "" {
			return fmt.Errorf("workload.idleRules[%d].idleTimeout is required", i)
		}
	}

	return nil
}

// validateQueryMonitoring validates query monitoring configuration.
func validateQueryMonitoring(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.QueryMonitoring == nil || !cluster.Spec.QueryMonitoring.Enabled {
		return nil
	}

	if cluster.Spec.QueryMonitoring.SamplingInterval < 0 {
		return fmt.Errorf("queryMonitoring.samplingInterval must be non-negative")
	}

	return nil
}

// backupDestinationTypeS3 is the s3 destination type discriminator.
const backupDestinationTypeS3 = "s3"

// backupDestinationTypeLocal is the local destination type discriminator.
const backupDestinationTypeLocal = "local"

// validateBackup validates backup configuration per spec 11 §Webhook Validation.
// It enforces the full rule set when backup is enabled.
func validateBackup(cluster *cbv1alpha1.CloudberryCluster) error {
	backup := cluster.Spec.Backup
	if backup == nil || !backup.Enabled {
		return nil
	}

	if err := validateBackupDestination(&backup.Destination); err != nil {
		return err
	}
	if err := validateBackupGpbackup(backup.Gpbackup); err != nil {
		return err
	}
	if err := validateBackupGprestore(backup.Gprestore); err != nil {
		return err
	}
	if backup.Schedule != "" {
		if err := validateCron(backup.Schedule); err != nil {
			return fmt.Errorf("backup.schedule is not a valid cron expression: %w", err)
		}
	}
	if backup.Image == "" {
		return fmt.Errorf("backup.image is required when backup is enabled")
	}
	return nil
}

// validateBackupDestination validates the destination type and, for s3, the
// required bucket and credential secret fields (rules 1-3).
func validateBackupDestination(dest *cbv1alpha1.BackupDestination) error {
	switch dest.Type {
	case "":
		return fmt.Errorf("backup.destination.type is required when backup is enabled")
	case backupDestinationTypeS3, backupDestinationTypeLocal:
		// Valid types.
	default:
		return fmt.Errorf("backup.destination.type must be %q or %q, got %q",
			backupDestinationTypeS3, backupDestinationTypeLocal, dest.Type)
	}

	if dest.Type != backupDestinationTypeS3 {
		return nil
	}
	s3 := dest.S3
	if s3 == nil || s3.Bucket == "" {
		return fmt.Errorf("backup.destination.s3.bucket is required for S3 destinations")
	}
	if s3.CredentialSecret == nil || s3.CredentialSecret.Name == "" {
		return fmt.Errorf(
			"backup.destination.s3.credentialSecret.name is required for S3 destinations")
	}
	return nil
}

// validateBackupGpbackup validates gpbackup option constraints (rules 4-8).
func validateBackupGpbackup(gp *cbv1alpha1.GpbackupOptions) error {
	if gp == nil {
		return nil
	}
	if gp.CompressionLevel != 0 && (gp.CompressionLevel < 1 || gp.CompressionLevel > 9) {
		return fmt.Errorf("backup.gpbackup.compressionLevel must be between 1 and 9, got %d",
			gp.CompressionLevel)
	}
	if gp.CompressionType != "" && gp.CompressionType != "gzip" && gp.CompressionType != "zstd" {
		return fmt.Errorf("backup.gpbackup.compressionType must be gzip or zstd, got %q",
			gp.CompressionType)
	}
	if gp.CopyQueueSize > 0 && !gp.SingleDataFile {
		return fmt.Errorf(
			"backup.gpbackup.copyQueueSize requires backup.gpbackup.singleDataFile to be true")
	}
	if gp.Jobs > 1 && gp.SingleDataFile {
		return fmt.Errorf(
			"backup.gpbackup.jobs cannot be combined with backup.gpbackup.singleDataFile")
	}
	if gp.Incremental && !gp.LeafPartitionData {
		return fmt.Errorf(
			"backup.gpbackup.incremental requires backup.gpbackup.leafPartitionData to be true")
	}
	return nil
}

// validateBackupGprestore validates gprestore option constraints. gprestore
// --data-only and --metadata-only are mutually exclusive and cannot both be set.
func validateBackupGprestore(gr *cbv1alpha1.GprestoreOptions) error {
	if gr == nil {
		return nil
	}
	if gr.DataOnly && gr.MetadataOnly {
		return fmt.Errorf(
			"backup.gprestore.dataOnly and backup.gprestore.metadataOnly cannot both be true")
	}
	return nil
}

// validateStorageManagement validates storage management configuration.
func validateStorageManagement(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.Storage == nil {
		return nil
	}

	scan := cluster.Spec.Storage.RecommendationScan
	if scan == nil || !scan.Enabled {
		return nil
	}

	if scan.BloatThreshold < 0 || scan.BloatThreshold > 100 {
		return fmt.Errorf(
			"storage.recommendationScan.bloatThreshold must be between 0 and 100, got %d",
			scan.BloatThreshold,
		)
	}
	if scan.SkewThreshold < 0 || scan.SkewThreshold > 100 {
		return fmt.Errorf(
			"storage.recommendationScan.skewThreshold must be between 0 and 100, got %d",
			scan.SkewThreshold,
		)
	}
	if scan.IndexBloatThreshold < 0 || scan.IndexBloatThreshold > 100 {
		return fmt.Errorf(
			"storage.recommendationScan.indexBloatThreshold must be between 0 and 100, got %d",
			scan.IndexBloatThreshold,
		)
	}
	if scan.AgeThreshold < 0 {
		return fmt.Errorf(
			"storage.recommendationScan.ageThreshold must be non-negative, got %d",
			scan.AgeThreshold,
		)
	}

	return nil
}

// validateMirroringTransition validates mirroring state transitions during updates.
// It ensures that mirroring can only be enabled on a Running cluster with sufficient
// segments, and that the layout cannot be changed while mirroring is enabled.
func validateMirroringTransition(
	oldCluster, newCluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	oldEnabled := isMirroringEnabled(oldCluster)
	newEnabled := isMirroringEnabled(newCluster)

	// No mirroring change — check for layout change only.
	if oldEnabled == newEnabled {
		return validateMirroringLayoutChange(oldCluster, newCluster, oldEnabled)
	}

	// Enabling mirroring (disabled -> enabled).
	if !oldEnabled && newEnabled {
		return validateMirroringEnable(oldCluster, newCluster)
	}

	// Disabling mirroring (enabled -> disabled) is allowed from any Running state.
	return nil, nil
}

// isMirroringEnabled returns true if mirroring is enabled in the cluster spec.
func isMirroringEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	return cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled
}

// validateMirroringLayoutChange rejects layout changes while mirroring is enabled.
func validateMirroringLayoutChange(
	oldCluster, newCluster *cbv1alpha1.CloudberryCluster,
	bothEnabled bool,
) (admission.Warnings, error) {
	if !bothEnabled {
		return nil, nil
	}
	oldLayout := oldCluster.Spec.Segments.Mirroring.Layout
	newLayout := newCluster.Spec.Segments.Mirroring.Layout
	if oldLayout != newLayout {
		return nil, fmt.Errorf(
			"cannot change mirroring layout from %s to %s while mirroring is enabled; "+
				"disable mirroring first", oldLayout, newLayout)
	}
	return nil, nil
}

// validateMirroringEnable validates that mirroring can be enabled on the cluster.
func validateMirroringEnable(
	oldCluster, newCluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	var warnings admission.Warnings

	// If the cluster has never reached Running phase (empty phase or Initializing),
	// allow the update — this covers operator adding finalizers or initial setup
	// where CRD schema defaults may set mirroring.enabled=true.
	if oldCluster.Status.Phase == "" || oldCluster.Status.Phase == cbv1alpha1.ClusterPhaseInitializing {
		return warnings, nil
	}

	// Cluster must be in Running phase for intentional mirroring enable.
	if oldCluster.Status.Phase != cbv1alpha1.ClusterPhaseRunning {
		return warnings, fmt.Errorf(
			"cannot enable mirroring: cluster must be in Running phase, current phase is %s",
			oldCluster.Status.Phase)
	}

	layout := newCluster.Spec.Segments.Mirroring.Layout
	if layout == "" {
		layout = cbv1alpha1.MirroringLayoutGroup
	}
	primariesPerHost := newCluster.Spec.Segments.PrimariesPerHost
	if primariesPerHost == 0 {
		primariesPerHost = 2
	}

	if err := validateNodeCountForMirroring(
		layout, newCluster.Spec.Segments.Count, primariesPerHost,
	); err != nil {
		return warnings, err
	}

	// Warn for spread layout with marginal segment count.
	if layout == cbv1alpha1.MirroringLayoutSpread &&
		newCluster.Spec.Segments.Count <= primariesPerHost+1 {
		warnings = append(warnings,
			"spread mirroring layout with marginal segment count may limit fault tolerance")
	}

	return warnings, nil
}

// validateNodeCountForMirroring checks whether the segment count is sufficient
// for the chosen mirroring layout.
func validateNodeCountForMirroring(
	layout cbv1alpha1.MirroringLayout,
	segmentCount, primariesPerHost int32,
) error {
	switch layout {
	case cbv1alpha1.MirroringLayoutGroup:
		// Group layout requires at least 2 * primariesPerHost segments
		// to place all mirrors on a different host.
		minCount := 2 * primariesPerHost
		if segmentCount < minCount {
			return fmt.Errorf(
				"cannot enable group mirroring: need at least %d segments (2 * primariesPerHost=%d), got %d",
				minCount, primariesPerHost, segmentCount)
		}
	case cbv1alpha1.MirroringLayoutSpread:
		// Spread layout requires more segments than primariesPerHost.
		if segmentCount <= primariesPerHost {
			return fmt.Errorf(
				"cannot enable spread mirroring: need more than %d segments (primariesPerHost), got %d",
				primariesPerHost, segmentCount)
		}
	}
	return nil
}

// validateDataLoading validates data loading configuration.
func validateDataLoading(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.DataLoading == nil || !cluster.Spec.DataLoading.Enabled {
		return nil
	}

	for i, job := range cluster.Spec.DataLoading.Jobs {
		if job.Name == "" {
			return fmt.Errorf("dataLoading.jobs[%d].name is required", i)
		}
		if job.Type == "" {
			return fmt.Errorf("dataLoading.jobs[%d].type is required", i)
		}
		if job.TargetTable == "" {
			return fmt.Errorf("dataLoading.jobs[%d].targetTable is required", i)
		}
	}

	return nil
}
