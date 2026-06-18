// Package webhook provides admission webhooks for the cloudberry operator.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/pxfpolicy"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// errAdmissionInternal classifies an internal (non-validation) admission
// failure — e.g. the cluster List in checkDuplicateName failing — so the
// webhook can record the distinct "error" result instead of bucketing it as a
// legitimate validation "denied". Validation rejections are NOT wrapped with
// this sentinel and therefore continue to record "denied".
var errAdmissionInternal = errors.New("admission internal error")

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
// returned by the validation: a nil error records "allowed", an internal
// (non-validation) failure flagged with errAdmissionInternal records "error",
// and any other error records "denied" (a legitimate validation rejection).
func (v *CloudberryClusterValidator) recordAdmission(operation string, err error) {
	if v.recorder == nil {
		return
	}
	var result string
	switch {
	case err == nil:
		result = admissionAllowed
	case errors.Is(err, errAdmissionInternal):
		result = admissionError
	default:
		result = admissionDenied
	}
	v.recorder.RecordWebhookAdmission(webhookValidating, operation, result)
}

// ValidateCreate validates a CloudberryCluster on creation.
func (v *CloudberryClusterValidator) ValidateCreate(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	ctx, end := startAdmissionSpan(ctx, "webhook.validate", admissionOpCreate)
	warnings, err := v.validateCreate(ctx, cluster)
	v.recordAdmission(admissionOpCreate, err)
	end(err)
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
		// A List failure is an internal/infrastructure error, not a validation
		// rejection — flag it with errAdmissionInternal so recordAdmission emits
		// the distinct "error" result instead of "denied".
		return fmt.Errorf("%w: listing clusters for duplicate check: %w", errAdmissionInternal, err)
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
	ctx context.Context,
	oldCluster *cbv1alpha1.CloudberryCluster,
	newCluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	_, end := startAdmissionSpan(ctx, "webhook.validate", admissionOpUpdate)
	warnings, err := validateUpdate(oldCluster, newCluster)
	v.recordAdmission(admissionOpUpdate, err)
	end(err)
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
	ctx context.Context,
	_ *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	// No validation needed on delete.
	_, end := startAdmissionSpan(ctx, "webhook.validate", admissionOpDelete)
	v.recordAdmission(admissionOpDelete, nil)
	end(nil)
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
	if err := validateBackup(cluster, &warnings); err != nil {
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
func validateBackup(cluster *cbv1alpha1.CloudberryCluster, warnings *admission.Warnings) error {
	backup := cluster.Spec.Backup
	if backup == nil || !backup.Enabled {
		return nil
	}

	if err := validateBackupDestination(&backup.Destination, warnings); err != nil {
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
	// The mutating webhook defaults backup.image to the official backup image,
	// so an empty value here means defaulting was bypassed. The image must be
	// backup-capable: it must contain kubectl (the Jobs `kubectl exec`
	// gpbackup/gprestore into the coordinator pod) and the gpbackup toolchain.
	if backup.Image == "" {
		return fmt.Errorf("backup.image is required when backup is enabled; "+
			"the image must contain kubectl and gpbackup (e.g. %s)", util.DefaultBackupImage)
	}
	return nil
}

// validateBackupDestination validates the destination type and, for s3, the
// required bucket and credential secret fields (rules 1-3), including the
// shape of an optional vaultSecret.path (H-1b).
func validateBackupDestination(dest *cbv1alpha1.BackupDestination, warnings *admission.Warnings) error {
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
	// S3 credentials may come from a Kubernetes Secret (credentialSecret.name)
	// OR from a Vault path (vaultSecret.path). Validation here only enforces the
	// spec shape: at least one credential source must be provided. Whether Vault
	// is enabled (spec.vault.enabled) is a runtime concern handled by the
	// operator, not by this admission check.
	hasSecret := s3.CredentialSecret != nil && s3.CredentialSecret.Name != ""
	hasVault := s3.VaultSecret != nil && s3.VaultSecret.Path != ""
	if !hasSecret && !hasVault {
		return fmt.Errorf(
			"backup.destination.s3 requires either credentialSecret.name or " +
				"vaultSecret.path for S3 credentials")
	}
	if s3.VaultSecret != nil {
		return validateS3VaultSecretPath(s3.VaultSecret.Path, warnings)
	}
	return nil
}

// validateS3VaultSecretPath validates the shape of the backup S3
// vaultSecret.path (H-1b). The documented form is the LOGICAL KV path
// ("secret/cloudberry/backup-s3" — both KV-v1 and KV-v2); the explicit KV-v2
// request path ("secret/data/cloudberry/backup-s3") is accepted for backward
// compatibility with a warning, since the operator injects the "data/"
// segment automatically when needed. Empty paths and leading slashes are
// rejected: Vault logical paths are mount-relative and never start with "/".
func validateS3VaultSecretPath(path string, warnings *admission.Warnings) error {
	if path == "" {
		return fmt.Errorf("backup.destination.s3.vaultSecret.path must not be empty")
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf(
			"backup.destination.s3.vaultSecret.path must not start with %q "+
				"(vault paths are mount-relative), got %q", "/", path)
	}
	if segments := strings.SplitN(path, "/", 3); len(segments) >= 2 && segments[1] == "data" {
		*warnings = append(*warnings, fmt.Sprintf(
			"backup.destination.s3.vaultSecret.path %q uses the explicit KV-v2 request "+
				"path; prefer the logical path %q — the operator injects the data/ "+
				"segment automatically for KV-v2 mounts",
			path, segments[0]+"/"+segments[len(segments)-1]))
	}
	return nil
}

// validateBackupGpbackup validates gpbackup option constraints (rules 4-8).
func validateBackupGpbackup(gp *cbv1alpha1.GpbackupOptions) error {
	if gp == nil {
		return nil
	}
	// compressionLevel must be 1-9. We reject 0 as well: in the real admission
	// chain the mutating defaulter runs first and sets compressionLevel=1 when a
	// user omits it (zero value), so a value of 0 reaching this validator means
	// the user explicitly set an invalid level (or defaulting was bypassed, as in
	// direct validator unit/functional tests). Either way 0 is not a valid
	// gpbackup compression level, so it is rejected here (Scenario 69d).
	if gp.CompressionLevel < 1 || gp.CompressionLevel > 9 {
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

// Data loading PXF discriminator constants.
const (
	// pxfServerTypeS3, pxfServerTypeHDFS, pxfServerTypeJDBC, pxfServerTypeHBase,
	// pxfServerTypeHive, and the object-store variants pxfServerTypeGS/Abfss/Wasbs
	// are the allowed pxf.servers[].type values (W.3). The gs/abfss/wasbs object
	// stores share the s3 (object-store) config model: PXF renders them all into
	// s3-site.xml and they require fs.s3a.endpoint (Scenario 96). They are kept in
	// lockstep with the CRD type enum (api/v1alpha1 PxfServerSpec.Type).
	pxfServerTypeS3    = "s3"
	pxfServerTypeHDFS  = "hdfs"
	pxfServerTypeJDBC  = "jdbc"
	pxfServerTypeHBase = "hbase"
	pxfServerTypeHive  = "hive"
	pxfServerTypeGS    = "gs"
	pxfServerTypeAbfss = "abfss"
	pxfServerTypeWasbs = "wasbs"
	// pxfServerTypeCustom is a generic connector server type (W.3). A custom
	// server has NO forced type-specific config keys; its profile
	// implementation is supplied by a matching customConnectors[] JAR. It MUST
	// be backed by a customConnectors[] entry of the same name (W.24).
	pxfServerTypeCustom = "custom"

	// dataLoadingJobTypePxf and dataLoadingJobTypeGpload are the allowed
	// jobs[].type values (W.8).
	dataLoadingJobTypePxf    = "pxf"
	dataLoadingJobTypeGpload = "gpload"

	// segmentRejectLimitTypeRows and segmentRejectLimitTypePercent are the
	// allowed errorHandling.segmentRejectLimitType values (W.15).
	segmentRejectLimitTypeRows    = "rows"
	segmentRejectLimitTypePercent = "percent"

	// gploadFileScheme is the bare local-file scheme that is REJECTED for
	// gpload jobs (W.16). A file:// external table in Cloudberry/Greenplum
	// requires a per-segment-host URI ("file://<seghost>/path"): each segment
	// reads its OWN local copy and the file must physically exist on every
	// segment host. The operator does not enumerate segment hostnames at
	// DDL-generation time and cannot synthesize a correct per-host file://
	// LOCATION, so a bare file:///path is invalid on a multi-segment cluster.
	gploadFileScheme = "file://"

	// PXF server config keys required by W.4/W.5/W.6.
	pxfConfigKeyS3Endpoint = "fs.s3a.endpoint"
	pxfConfigKeyJDBCDriver = "jdbc.driver"
	pxfConfigKeyJDBCURL    = "jdbc.url"
	pxfConfigKeyDefaultFS  = "fs.defaultFS"
)

// pxfServerTypes is the set of allowed PXF server types (W.3). It MUST stay in
// lockstep with the CRD's PxfServerSpec.Type enum.
var pxfServerTypes = map[string]struct{}{
	pxfServerTypeS3:     {},
	pxfServerTypeHDFS:   {},
	pxfServerTypeJDBC:   {},
	pxfServerTypeHBase:  {},
	pxfServerTypeHive:   {},
	pxfServerTypeGS:     {},
	pxfServerTypeAbfss:  {},
	pxfServerTypeWasbs:  {},
	pxfServerTypeCustom: {},
}

// pxfObjectStoreServerTypes is the subset of server types that use the
// object-store (s3-site.xml) config model (Scenario 96). They share the s3
// type-specific admission checks (W.4: fs.s3a.endpoint required).
var pxfObjectStoreServerTypes = map[string]struct{}{
	pxfServerTypeS3:    {},
	pxfServerTypeGS:    {},
	pxfServerTypeAbfss: {},
	pxfServerTypeWasbs: {},
}

// pxfKerberosServerTypes is the subset of server types for which Kerberos
// authentication is meaningful and supported (SE.4): the Hadoop family
// (hdfs/hive/hbase). Kerberos on any other type (object stores, jdbc, custom) is
// rejected by the admission webhook.
var pxfKerberosServerTypes = map[string]struct{}{
	pxfServerTypeHDFS:  {},
	pxfServerTypeHive:  {},
	pxfServerTypeHBase: {},
}

// dataLoadingJobTypes is the set of allowed data loading job types (W.8).
var dataLoadingJobTypes = map[string]struct{}{
	dataLoadingJobTypePxf:    {},
	dataLoadingJobTypeGpload: {},
}

// pxfProfileSchemes maps the recognized PXF profile scheme (the part before
// the ":" separator, or a bare profile with no separator) to the set of
// allowed format suffixes for that scheme. This encodes the W.10 "valid PXF
// profile" policy directly from the spec's PXF Profile Reference table.
//
// Policy: a profile is "<scheme>" or "<scheme>:<format>". The scheme must be
// known; if a format suffix is present it must be allowed for that scheme.
// A nil/empty format set means the scheme is only valid as a bare profile
// (no suffix). The "*" sentinel means any non-empty suffix is accepted
// (used for the gs/abfss/wasbs object-store variants per the spec, which
// reuse the s3 format set but are documented as "same format variants").
//
//   - Object stores: s3, gs, abfss, wasbs  -> text, parquet, avro, json, orc
//   - Hadoop:        hdfs                   -> text, parquet, avro, json, orc, SequenceFile
//   - Hive:          hive                   -> (bare) | text, orc, rc
//   - JDBC:          jdbc                   -> (bare only)
//   - HBase:         hbase                  -> (bare only)
//
// Profile matching is case-insensitive for the scheme and format so that
// "HBase" and "hbase" both pass (the spec table mixes casing).
var pxfProfileSchemes = map[string]map[string]struct{}{
	"s3":    objectStoreFormats(),
	"gs":    objectStoreFormats(),
	"abfss": objectStoreFormats(),
	"wasbs": objectStoreFormats(),
	"hdfs":  hdfsFormats(),
	"hive":  {"": {}, pxfFormatText: {}, pxfFormatORC: {}, "rc": {}},
	"jdbc":  {"": {}},
	"hbase": {"": {}},
}

// PXF profile format suffix constants (W.10). These alias the canonical
// constants in internal/pxfpolicy (the single source of truth) so the webhook's
// W.10 allowlist and the write-capability policy can never use diverging
// literals while the local names keep the existing W.10 code (and its tests)
// unchanged.
const (
	pxfFormatText         = pxfpolicy.FormatText
	pxfFormatParquet      = pxfpolicy.FormatParquet
	pxfFormatAvro         = pxfpolicy.FormatAvro
	pxfFormatJSON         = pxfpolicy.FormatJSON
	pxfFormatORC          = pxfpolicy.FormatORC
	pxfFormatSequenceFile = pxfpolicy.FormatSequenceFile
)

// objectStoreFormats returns the allowed format suffixes shared by the object
// store profiles (s3, gs, abfss, wasbs).
func objectStoreFormats() map[string]struct{} {
	return map[string]struct{}{
		pxfFormatText: {}, pxfFormatParquet: {}, pxfFormatAvro: {},
		pxfFormatJSON: {}, pxfFormatORC: {},
	}
}

// hdfsFormats returns the allowed format suffixes for the hdfs profile: the
// object-store set plus SequenceFile.
func hdfsFormats() map[string]struct{} {
	formats := objectStoreFormats()
	formats[pxfFormatSequenceFile] = struct{}{}
	return formats
}

// isValidPxfProfile reports whether profile is a recognized PXF profile per the
// pxfProfileSchemes policy (W.10). Matching is case-insensitive.
func isValidPxfProfile(profile string) bool {
	if profile == "" {
		return false
	}
	lower := strings.ToLower(profile)
	scheme, format, hasSep := strings.Cut(lower, ":")
	formats, ok := pxfProfileSchemes[scheme]
	if !ok {
		return false
	}
	if !hasSep {
		// Bare profile (no ":"): allowed only when the scheme accepts an empty
		// format suffix (e.g. jdbc, hbase, hive).
		_, allowed := formats[""]
		return allowed
	}
	_, allowed := formats[format]
	return allowed
}

// pxfCustomConnectorSchemes is the set of streaming profile schemes that are
// NOT built-in PXF profiles but are re-enabled ONLY via a custom-connector JAR
// (W.23). A profile whose scheme is in this set is "recognized" by W.10 so it
// is not rejected with the bare "not a valid PXF profile" error, but its
// validity is FINISHED by the W.23 gate (the referenced server must be
// type=custom with a matching customConnectors[] entry). This set is kept
// SEPARATE from pxfProfileSchemes so the built-in W.10 policy (and
// TestIsValidPxfProfile) is undisturbed.
var pxfCustomConnectorSchemes = map[string]struct{}{
	"kafka":    {},
	"rabbitmq": {},
}

// isCustomConnectorProfile reports whether profile's scheme is a custom-connector
// (streaming) scheme (W.23). It mirrors isValidPxfProfile's scheme parsing:
// case-insensitive, scheme is the part before the first ":" (or the bare
// profile). Format suffixes are connector-defined and not constrained here.
func isCustomConnectorProfile(profile string) bool {
	if profile == "" {
		return false
	}
	scheme, _, _ := strings.Cut(strings.ToLower(profile), ":")
	_, ok := pxfCustomConnectorSchemes[scheme]
	return ok
}

// validateDataLoading validates data loading configuration per spec 12
// §Webhook Validation (rules W.1–W.16). It enforces the full rule set only
// when data loading is enabled.
func validateDataLoading(cluster *cbv1alpha1.CloudberryCluster) error {
	dl := cluster.Spec.DataLoading
	if dl == nil || !dl.Enabled {
		return nil
	}
	serverNames, connectorBackedServers, err := validatePxf(dl.Pxf)
	if err != nil {
		return err
	}
	return validateDataLoadingJobs(dl.Jobs, serverNames, connectorBackedServers)
}

// validatePxf validates the PXF service configuration (W.1–W.6, W.24) and
// returns the set of defined server names for cross-referencing by pxf jobs
// (W.9) plus the subset that is connector-backed (type==custom AND has a
// matching customConnectors[] entry) for the W.23 streaming-profile gate.
func validatePxf(pxf *cbv1alpha1.PxfSpec) (serverNames, connectorBacked map[string]struct{}, err error) {
	serverNames = map[string]struct{}{}
	connectorBacked = map[string]struct{}{}
	if pxf == nil {
		return serverNames, connectorBacked, nil
	}
	// W.1: image is required when PXF is enabled.
	if pxf.Enabled && pxf.Image == "" {
		return nil, nil, fmt.Errorf("dataLoading.pxf.image is required when pxf.enabled is true")
	}
	// customConnectorNames is the set of declared custom-connector names; it is
	// the link target for both W.24 (server -> connector) and W.23 (the
	// connector-backed server set used by streaming jobs).
	customConnectorNames := pxfCustomConnectorNames(pxf.CustomConnectors)
	if err := validatePxfServers(pxf.Servers, serverNames, customConnectorNames, connectorBacked); err != nil {
		return nil, nil, err
	}
	return serverNames, connectorBacked, nil
}

// pxfCustomConnectorNames returns the set of declared customConnectors[].name.
func pxfCustomConnectorNames(connectors []cbv1alpha1.PxfCustomConnector) map[string]struct{} {
	names := make(map[string]struct{}, len(connectors))
	for i := range connectors {
		names[connectors[i].Name] = struct{}{}
	}
	return names
}

// validatePxfServers validates pxf.servers[] (W.2–W.6, W.24), records each valid
// server name into serverNames, and records each connector-backed custom server
// (type==custom with a matching customConnectors[] entry) into connectorBacked.
func validatePxfServers(
	servers []cbv1alpha1.PxfServerSpec,
	serverNames, customConnectorNames, connectorBacked map[string]struct{},
) error {
	for i := range servers {
		srv := &servers[i]
		// W.2: name must be non-empty and unique.
		if srv.Name == "" {
			return fmt.Errorf("dataLoading.pxf.servers[%d].name is required and must be non-empty", i)
		}
		if _, dup := serverNames[srv.Name]; dup {
			return fmt.Errorf("dataLoading.pxf.servers[%d].name %q is a duplicate; "+
				"server names must be unique", i, srv.Name)
		}
		serverNames[srv.Name] = struct{}{}
		if err := validatePxfServerType(srv, i); err != nil {
			return err
		}
		// SE.4: validate Kerberos config strictly, but ONLY when it is set, so
		// existing CRs without Kerberos are entirely unaffected.
		if err := validatePxfServerKerberos(srv, i); err != nil {
			return err
		}
		// W.24 (custom-server-requires-connector): a server of type=custom MUST
		// have a customConnectors[] entry of the same name — otherwise the JAR
		// that implements its profile is missing.
		if srv.Type == pxfServerTypeCustom {
			if _, ok := customConnectorNames[srv.Name]; !ok {
				return fmt.Errorf("dataLoading.pxf.servers[%d] of type custom requires a "+
					"matching customConnectors[].name %q", i, srv.Name)
			}
			connectorBacked[srv.Name] = struct{}{}
		}
	}
	return nil
}

// validatePxfServerType validates a single server's type (W.3) and its
// type-specific required config keys (W.4–W.6).
func validatePxfServerType(srv *cbv1alpha1.PxfServerSpec, i int) error {
	if _, ok := pxfServerTypes[srv.Type]; !ok {
		return fmt.Errorf("dataLoading.pxf.servers[%d].type must be one of "+
			"%q, %q, %q, %q, %q, %q, %q, %q, %q, got %q", i,
			pxfServerTypeS3, pxfServerTypeHDFS, pxfServerTypeJDBC,
			pxfServerTypeHBase, pxfServerTypeHive,
			pxfServerTypeGS, pxfServerTypeAbfss, pxfServerTypeWasbs,
			pxfServerTypeCustom, srv.Type)
	}
	// type=custom has NO forced type-specific config keys: its profile
	// implementation comes from a matching customConnectors[] JAR (the
	// server -> connector link is enforced by W.24 in validatePxfServers).
	if srv.Type == pxfServerTypeCustom {
		return nil
	}
	if _, isObjStore := pxfObjectStoreServerTypes[srv.Type]; isObjStore {
		return validatePxfObjectStoreServer(srv, i)
	}
	switch srv.Type {
	case pxfServerTypeJDBC:
		return validatePxfJDBCServer(srv, i)
	case pxfServerTypeHDFS:
		return validatePxfHDFSServer(srv, i)
	default:
		// hbase/hive have no extra required config keys at admission time.
		return nil
	}
}

// validatePxfServerKerberos enforces the SE.4 Kerberos admission rules. It is a
// no-op when srv.Kerberos is nil so non-Kerberos servers (and all existing CRs)
// are unaffected. When Kerberos IS set it (a) rejects Kerberos on server types
// that do not support it (only hdfs/hive/hbase are allowed) and (b) requires the
// service principal and the keytab Secret reference (Name + Key) to be non-empty.
func validatePxfServerKerberos(srv *cbv1alpha1.PxfServerSpec, i int) error {
	if srv.Kerberos == nil {
		return nil
	}
	if _, ok := pxfKerberosServerTypes[srv.Type]; !ok {
		return fmt.Errorf("dataLoading.pxf.servers[%d] (type %s) does not support kerberos; "+
			"kerberos is only valid for server types %q, %q, %q",
			i, srv.Type, pxfServerTypeHDFS, pxfServerTypeHive, pxfServerTypeHBase)
	}
	if srv.Kerberos.Principal == "" {
		return fmt.Errorf("dataLoading.pxf.servers[%d].kerberos.principal is required and "+
			"must be non-empty", i)
	}
	if srv.Kerberos.KeytabSecret.Name == "" {
		return fmt.Errorf("dataLoading.pxf.servers[%d].kerberos.keytabSecret.name is required "+
			"and must be non-empty", i)
	}
	if srv.Kerberos.KeytabSecret.Key == "" {
		return fmt.Errorf("dataLoading.pxf.servers[%d].kerberos.keytabSecret.key is required "+
			"and must be non-empty", i)
	}
	return nil
}

// validatePxfObjectStoreServer enforces W.4 for the object-store server types
// (s3, gs, abfss, wasbs — Scenario 96). All must declare fs.s3a.endpoint (PXF
// renders every object store into s3-site.xml). Only the s3 type additionally
// REQUIRES credentialSecrets: the cloud-native object stores (GCS/Azure) may
// authenticate via other means (workload identity, account keys in config), so
// their credentialSecrets are optional.
func validatePxfObjectStoreServer(srv *cbv1alpha1.PxfServerSpec, i int) error {
	if _, ok := srv.Config[pxfConfigKeyS3Endpoint]; !ok {
		return fmt.Errorf("dataLoading.pxf.servers[%d] (type %s) must include "+
			"config %q", i, srv.Type, pxfConfigKeyS3Endpoint)
	}
	if srv.Type == pxfServerTypeS3 && len(srv.CredentialSecrets) == 0 {
		return fmt.Errorf("dataLoading.pxf.servers[%d] (type s3) must include "+
			"credentialSecrets (credential references)", i)
	}
	return nil
}

// validatePxfJDBCServer enforces W.5: a jdbc server must declare jdbc.driver
// and jdbc.url.
func validatePxfJDBCServer(srv *cbv1alpha1.PxfServerSpec, i int) error {
	if _, ok := srv.Config[pxfConfigKeyJDBCDriver]; !ok {
		return fmt.Errorf("dataLoading.pxf.servers[%d] (type jdbc) must include "+
			"config %q", i, pxfConfigKeyJDBCDriver)
	}
	if _, ok := srv.Config[pxfConfigKeyJDBCURL]; !ok {
		return fmt.Errorf("dataLoading.pxf.servers[%d] (type jdbc) must include "+
			"config %q", i, pxfConfigKeyJDBCURL)
	}
	return nil
}

// validatePxfHDFSServer enforces W.6: an hdfs server must declare fs.defaultFS.
func validatePxfHDFSServer(srv *cbv1alpha1.PxfServerSpec, i int) error {
	if _, ok := srv.Config[pxfConfigKeyDefaultFS]; !ok {
		return fmt.Errorf("dataLoading.pxf.servers[%d] (type hdfs) must include "+
			"config %q", i, pxfConfigKeyDefaultFS)
	}
	return nil
}

// validateDataLoadingJobs validates jobs[] (W.7–W.15, W.23/W.23c).
func validateDataLoadingJobs(
	jobs []cbv1alpha1.DataLoadingJob, serverNames, connectorBackedServers map[string]struct{},
) error {
	seen := map[string]struct{}{}
	for i := range jobs {
		job := &jobs[i]
		// W.7: name must be non-empty and unique.
		if job.Name == "" {
			return fmt.Errorf("dataLoading.jobs[%d].name is required and must be non-empty", i)
		}
		if _, dup := seen[job.Name]; dup {
			return fmt.Errorf("dataLoading.jobs[%d].name %q is a duplicate; "+
				"job names must be unique", i, job.Name)
		}
		seen[job.Name] = struct{}{}
		// W.8: type must be pxf or gpload.
		if _, ok := dataLoadingJobTypes[job.Type]; !ok {
			return fmt.Errorf("dataLoading.jobs[%d].type must be %q or %q, got %q",
				i, dataLoadingJobTypePxf, dataLoadingJobTypeGpload, job.Type)
		}
		if err := validateDataLoadingJobBody(job, serverNames, connectorBackedServers, i); err != nil {
			return err
		}
		// W.13: schedule must be a valid cron expression when provided.
		if job.Schedule != "" {
			if err := validateCron(job.Schedule); err != nil {
				return fmt.Errorf("dataLoading.jobs[%d].schedule is not a valid cron "+
					"expression: %w", i, err)
			}
		}
	}
	return nil
}

// validateDataLoadingJobBody dispatches to the type-specific job validator.
// It also evaluates the W.23c continuous/schedule cross-check here, where both
// the job's Schedule and the PxfJob fields are visible.
func validateDataLoadingJobBody(
	job *cbv1alpha1.DataLoadingJob, serverNames, connectorBackedServers map[string]struct{}, i int,
) error {
	if job.Type != dataLoadingJobTypePxf {
		return validateGploadJob(job.GploadJob, i)
	}
	if err := validatePxfJob(job.PxfJob, serverNames, connectorBackedServers, i); err != nil {
		return err
	}
	// W.23c: a continuous streaming job must not also be scheduled — it runs as
	// a one-off long-running Job, never a CronJob (J.46). Evaluated here because
	// Schedule lives on the DataLoadingJob, not the PxfJobSpec.
	if job.PxfJob != nil && job.PxfJob.Continuous != nil && *job.PxfJob.Continuous &&
		job.Schedule != "" {
		return fmt.Errorf("dataLoading.jobs[%d]: continuous streaming jobs must not set a "+
			"schedule; they run as a one-off long-running Job, not a CronJob", i)
	}
	return nil
}

// validatePxfJob validates a pxf job body (W.9–W.11, W.14, W.15, W.17, W.23, W.23c).
func validatePxfJob(
	pxfJob *cbv1alpha1.PxfJobSpec, serverNames, connectorBackedServers map[string]struct{}, i int,
) error {
	if pxfJob == nil {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob is required for type pxf", i)
	}
	// W.9: server must reference a defined PXF server name.
	if _, ok := serverNames[pxfJob.Server]; !ok {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.server %q does not reference a "+
			"defined pxf.servers[].name", i, pxfJob.Server)
	}
	// W.10: profile must be a valid built-in PXF profile OR a recognized
	// custom-connector (streaming) profile. A custom-connector profile passes
	// this "recognized" check but is then gated by W.23 below.
	if !isValidPxfProfile(pxfJob.Profile) && !isCustomConnectorProfile(pxfJob.Profile) {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.profile %q is not a valid PXF "+
			"profile", i, pxfJob.Profile)
	}
	// W.23 (kafka-profile-requires-custom-connector): a custom-connector
	// (streaming) profile is admitted ONLY when the referenced server is
	// connector-backed (type=custom with a matching customConnectors[] entry).
	// This preserves the "no built-in streaming" guarantee: a bare kafka
	// profile, or one on a non-custom server, is still REJECTED.
	if isCustomConnectorProfile(pxfJob.Profile) {
		if _, ok := connectorBackedServers[pxfJob.Server]; !ok {
			return fmt.Errorf("dataLoading.jobs[%d].pxfJob.profile %q is a custom-connector "+
				"profile and requires the referenced server %q to be type=custom with a "+
				"matching customConnectors[] entry", i, pxfJob.Profile, pxfJob.Server)
		}
	}
	// W.23c: streaming buffering knobs sanity (continuous/schedule is checked in
	// validateDataLoadingJobBody where Schedule is visible).
	if err := validateStreamingParams(pxfJob, i); err != nil {
		return err
	}
	// W.10b (Scenario 96): a WRITABLE external table (mode=writable) requires a
	// profile whose format is writable per the spec Read/Write matrix. This is
	// driven by pxfpolicy.IsProfileWritable on the format, so it applies
	// uniformly to ALL object-store schemes (s3/gs/abfss/wasbs): e.g.
	// gs:json/s3:orc writable → DENY, gs:parquet/s3:text writable → PASS.
	if strings.EqualFold(pxfJob.Mode, pxfpolicy.ModeWritable) &&
		!pxfpolicy.IsProfileWritable(pxfJob.Profile) {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob: profile %q is write-unsupported "+
			"for a writable external table (mode=writable); only text/parquet/avro "+
			"object-store formats are writable", i, pxfJob.Profile)
	}
	// W.25 (load-method): loadMethod enum + the fdw read-only constraints. Called
	// BEFORE W.17 so an fdw+writable job is rejected here (not by W.17) and so
	// W.17 can safely consult loadMethod for its fdw-read allowance.
	if err := validateLoadMethod(pxfJob, i); err != nil {
		return err
	}
	// W.17: sourceFilter (the optional WHERE predicate) is valid on a writable
	// export OR an fdw read job and must not smuggle in a stacked query/comment.
	if err := validateSourceFilter(pxfJob, i); err != nil {
		return err
	}
	// W.11: targetTable is required.
	if pxfJob.TargetTable == "" {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.targetTable is required", i)
	}
	// W.14: partitioning requires column, range, and interval together.
	if err := validatePartitioning(pxfJob.Partitioning, i); err != nil {
		return err
	}
	// W.15: errorHandling.segmentRejectLimitType must be rows or percent.
	return validateErrorHandling(pxfJob.ErrorHandling, i)
}

// validateStreamingParams enforces W.23c for the streaming buffering knobs:
//   - BatchSize, when set, must be >= 1 (also enforced by kubebuilder Minimum=1).
//   - FlushInterval, when set, must parse as a Go duration (time.ParseDuration).
//
// The Continuous/Schedule mutual-exclusion is enforced in
// validateDataLoadingJobBody, where the job's Schedule is visible.
func validateStreamingParams(pxfJob *cbv1alpha1.PxfJobSpec, i int) error {
	if pxfJob.BatchSize != 0 && pxfJob.BatchSize < 1 {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.batchSize %d must be >= 1",
			i, pxfJob.BatchSize)
	}
	if pxfJob.FlushInterval != "" {
		if _, err := time.ParseDuration(pxfJob.FlushInterval); err != nil {
			return fmt.Errorf("dataLoading.jobs[%d].pxfJob.flushInterval %q must be a "+
				"valid duration", i, pxfJob.FlushInterval)
		}
	}
	return nil
}

// sqlPredicateForbidden lists the substrings rejected by the W.17 sourceFilter
// sanity check: a statement terminator (stacked query) and the two SQL comment
// openers. This is a CHEAP substring scan, NOT a SQL parser — the predicate is
// author-trusted CR content (same trust boundary as targetTable); the check
// merely reduces obvious footguns, it does not make a malicious predicate safe.
var sqlPredicateForbidden = []string{";", "--", "/*"}

// containsUnsafeSQLFragment reports the first W.17-forbidden substring present
// in s, or "" if none. It is a deliberately simple substring scan (see
// sqlPredicateForbidden) and is NOT a substitute for SQL parsing.
func containsUnsafeSQLFragment(s string) string {
	for _, bad := range sqlPredicateForbidden {
		if strings.Contains(s, bad) {
			return bad
		}
	}
	return ""
}

// loadMethodFDW is the pxfJob.loadMethod value selecting the persistent
// foreign-data-wrapper loading path (Scenario 103). The empty string and
// "external-table" both select the default transient external-table path.
const loadMethodFDW = "fdw"

// validateLoadMethod enforces W.25 for pxfJob.loadMethod:
//
//	W.25(a) ENUM — loadMethod, when set, must be "external-table" or "fdw" (also
//	  enforced by the CRD kubebuilder Enum; re-checked here for defense in depth).
//	W.25(b) FDW IS READ-ONLY — loadMethod=fdw is a READ/import path: it is
//	  REJECTED with mode=writable (a writable FDW export is out of scope).
//	W.25(c) FDW IS ONE-OFF — loadMethod=fdw builds a PERSISTENT one-off load and
//	  is REJECTED with continuous=true (it is not a streaming consume loop).
func validateLoadMethod(pxfJob *cbv1alpha1.PxfJobSpec, i int) error {
	switch pxfJob.LoadMethod {
	case "", "external-table", loadMethodFDW:
	default:
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.loadMethod %q must be "+
			"external-table or fdw", i, pxfJob.LoadMethod)
	}
	if !strings.EqualFold(pxfJob.LoadMethod, loadMethodFDW) {
		return nil
	}
	if strings.EqualFold(pxfJob.Mode, pxfpolicy.ModeWritable) {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob: loadMethod=fdw is a read/import "+
			"path and is not valid with mode=writable (a writable FDW export is out of "+
			"scope)", i)
	}
	if pxfJob.Continuous != nil && *pxfJob.Continuous {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob: loadMethod=fdw is a one-off "+
			"persistent load and is not valid with continuous=true", i)
	}
	return nil
}

// validateSourceFilter enforces W.17 for pxfJob.sourceFilter (the optional
// WHERE predicate):
//
//	W.17(a) MODE/METHOD GATE — sourceFilter is meaningful for a writable export
//	  (mode=writable) OR an fdw read job (loadMethod=fdw). On a PLAIN
//	  external-table read/import the INSERT direction is
//	  `INSERT INTO <target> SELECT * FROM <ext>`, which has no source-table
//	  predicate to apply, so a set sourceFilter is rejected rather than silently
//	  ignored (surfacing the misuse early, matching the W.* reject posture).
//	W.17(b) SANITY CHECK — reject an obvious statement terminator / SQL comment
//	  (';', '--', '/*'). This is a cheap substring scan, not a full SQL parser;
//	  the predicate is author-trusted CR content (same trust boundary as
//	  targetTable), the check only reduces stacked-query/comment footguns.
func validateSourceFilter(pxfJob *cbv1alpha1.PxfJobSpec, i int) error {
	if pxfJob.SourceFilter == "" {
		return nil
	}
	// W.17(a): valid on a writable export OR an fdw read job (the fdw read's
	// INSERT INTO <target> SELECT * FROM <foreign> WHERE <filter> applies the
	// predicate to the foreign-table SELECT). Still rejected on a plain
	// external-table read import (loadMethod unset/external-table, not writable).
	isWritableExport := strings.EqualFold(pxfJob.Mode, pxfpolicy.ModeWritable)
	isFDWRead := strings.EqualFold(pxfJob.LoadMethod, loadMethodFDW) && !isWritableExport
	if !isWritableExport && !isFDWRead {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.sourceFilter is only valid for a "+
			"writable export job (mode=writable) or an fdw read job (loadMethod=fdw); it "+
			"is not allowed on a plain external-table read/import job", i)
	}
	// W.17(b): cheap sanity check against stacked queries / SQL comments.
	if bad := containsUnsafeSQLFragment(pxfJob.SourceFilter); bad != "" {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.sourceFilter must not contain statement "+
			"terminators or SQL comments (%q)", i, bad)
	}
	return nil
}

// validatePartitioning enforces W.14: when partitioning specifies a column, the
// range and interval must also be present (all three together).
func validatePartitioning(part *cbv1alpha1.PartitioningSpec, i int) error {
	if part == nil || part.Column == "" {
		return nil
	}
	if part.Range == "" || part.Interval == "" {
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.partitioning requires column, "+
			"range, and interval together", i)
	}
	return nil
}

// validateErrorHandling enforces W.15: segmentRejectLimitType must be rows or
// percent when error handling is configured with a non-empty type.
func validateErrorHandling(eh *cbv1alpha1.ErrorHandlingSpec, i int) error {
	if eh == nil || eh.SegmentRejectLimitType == "" {
		return nil
	}
	switch eh.SegmentRejectLimitType {
	case segmentRejectLimitTypeRows, segmentRejectLimitTypePercent:
		return nil
	default:
		return fmt.Errorf("dataLoading.jobs[%d].pxfJob.errorHandling."+
			"segmentRejectLimitType must be %q or %q, got %q", i,
			segmentRejectLimitTypeRows, segmentRejectLimitTypePercent,
			eh.SegmentRejectLimitType)
	}
}

// gploadInputTypeGpfdist / gploadInputTypeLocal are the gploadJob.inputSource.type
// values (W.18 / W.22). They mirror the builder discriminators.
const (
	gploadInputTypeGpfdist = "gpfdist"
	gploadInputTypeLocal   = "local"
)

// gploadModeUpdate / gploadModeMerge are the gploadJob.mode values that REQUIRE
// matchColumns (W.20).
const (
	gploadModeUpdate = "update"
	gploadModeMerge  = "merge"
)

// validateGploadJob validates a gpload job body (W.12, W.16, W.18-W.22).
func validateGploadJob(gploadJob *cbv1alpha1.GploadJobSpec, i int) error {
	if gploadJob == nil || gploadJob.TargetTable == "" {
		return fmt.Errorf("dataLoading.jobs[%d].gploadJob.targetTable is required", i)
	}
	// W.16: filePaths must not use the bare file:// scheme.
	if err := validateGploadFilePaths(gploadJob.FilePaths, i); err != nil {
		return err
	}
	// W.18 / W.22: inputSource.type enum + host/port only valid for gpfdist.
	if err := validateGploadInputSource(gploadJob.InputSource, i); err != nil {
		return err
	}
	// W.19: delimiter, when set, must be exactly one character.
	if err := validateGploadDelimiter(gploadJob.Delimiter, i); err != nil {
		return err
	}
	// W.20: mode update/merge requires non-empty matchColumns.
	if err := validateGploadMode(gploadJob, i); err != nil {
		return err
	}
	// W.21: each postActions[] element must pass the SQL sanity check.
	return validateGploadPostActions(gploadJob.PostActions, i)
}

// validateGploadInputSource enforces W.18 (inputSource.type enum gpfdist|local
// when set) and W.22 (host/port are only valid for type gpfdist; on type local
// they must be empty/zero). The CRD enum also constrains type, but the webhook
// rejects with a clear message for defense in depth.
func validateGploadInputSource(src *cbv1alpha1.GploadInputSourceSpec, i int) error {
	if src == nil {
		return nil
	}
	// W.18: type, when set, must be gpfdist or local.
	if src.Type != "" &&
		!strings.EqualFold(src.Type, gploadInputTypeGpfdist) &&
		!strings.EqualFold(src.Type, gploadInputTypeLocal) {
		return fmt.Errorf("dataLoading.jobs[%d].gploadJob.inputSource.type must be "+
			"%q or %q", i, gploadInputTypeGpfdist, gploadInputTypeLocal)
	}
	// W.22: host/port are only meaningful for a gpfdist source; reject them on a
	// local source where they have no effect.
	if strings.EqualFold(src.Type, gploadInputTypeLocal) &&
		(src.Host != "" || src.Port != 0) {
		return fmt.Errorf("dataLoading.jobs[%d].gploadJob.inputSource.host/port are "+
			"only valid for type gpfdist", i)
	}
	return nil
}

// validateGploadDelimiter enforces W.19: a set delimiter must be exactly one
// character (the CRD MaxLength=1 also bounds it; the webhook rejects the empty
// vs. multi distinction with a clear message).
func validateGploadDelimiter(delimiter string, i int) error {
	if delimiter == "" {
		return nil
	}
	if len([]rune(delimiter)) != 1 {
		return fmt.Errorf("dataLoading.jobs[%d].gploadJob.delimiter must be a single "+
			"character", i)
	}
	return nil
}

// validateGploadMode enforces W.20: when mode is update or merge, gpload requires
// matchColumns (the MATCH_COLUMNS block); an empty matchColumns is rejected.
func validateGploadMode(gploadJob *cbv1alpha1.GploadJobSpec, i int) error {
	if strings.EqualFold(gploadJob.Mode, gploadModeUpdate) ||
		strings.EqualFold(gploadJob.Mode, gploadModeMerge) {
		if len(gploadJob.MatchColumns) == 0 {
			return fmt.Errorf("dataLoading.jobs[%d].gploadJob: mode %q requires "+
				"gploadJob.matchColumns", i, gploadJob.Mode)
		}
	}
	return nil
}

// validateGploadPostActions enforces W.21: each postActions[] element must pass
// the cheap SQL sanity check (no statement terminators / comment sequences),
// reusing the W.17 helper. The actions are author-trusted CR content (same trust
// boundary as targetTable); the check only reduces obvious footguns.
func validateGploadPostActions(postActions []string, i int) error {
	for j, action := range postActions {
		if bad := containsUnsafeSQLFragment(action); bad != "" {
			return fmt.Errorf("dataLoading.jobs[%d].gploadJob.postActions[%d] contains a "+
				"forbidden SQL fragment (%q)", i, j, bad)
		}
	}
	return nil
}

// validateGploadFilePaths enforces W.16: a gpload job's filePaths[] must not use
// the bare file:// scheme. In Cloudberry/Greenplum a file:// external table
// requires a per-segment-host URI ("file://<seghost>/path") — each segment reads
// its OWN local copy and the file must physically exist on every segment host.
// The operator cannot synthesize the correct per-segment-host LOCATION from the
// CRD (segment hostnames are not enumerated at DDL-generation time), so a bare
// file:///path is invalid on a multi-segment cluster and is rejected at
// admission. Bare paths (served by the cluster gpfdist Service), gpfdist://, and
// s3:// remain valid native sources.
func validateGploadFilePaths(filePaths []string, i int) error {
	for j, p := range filePaths {
		if strings.HasPrefix(strings.TrimSpace(p), gploadFileScheme) {
			return fmt.Errorf(
				"dataLoading.jobs[%d].gploadJob.filePaths[%d]: file:// scheme is not "+
					"supported for multi-segment loads; use gpfdist:// or s3:// (or a "+
					"bare path served by the cluster gpfdist service)", i, j)
		}
	}
	return nil
}
