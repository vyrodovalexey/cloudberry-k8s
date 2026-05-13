// Package webhook provides admission webhooks for the cloudberry operator.
package webhook

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// CloudberryClusterValidator validates CloudberryCluster resources.
type CloudberryClusterValidator struct {
	reader client.Reader
}

// NewCloudberryClusterValidator creates a new CloudberryClusterValidator.
func NewCloudberryClusterValidator(reader client.Reader) *CloudberryClusterValidator {
	return &CloudberryClusterValidator{reader: reader}
}

// ValidateCreate validates a CloudberryCluster on creation.
func (v *CloudberryClusterValidator) ValidateCreate(
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
	_ *cbv1alpha1.CloudberryCluster,
	newCluster *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	return validateCluster(newCluster)
}

// ValidateDelete validates a CloudberryCluster on deletion.
func (v *CloudberryClusterValidator) ValidateDelete(
	_ context.Context,
	_ *cbv1alpha1.CloudberryCluster,
) (admission.Warnings, error) {
	// No validation needed on delete.
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

// validateBackup validates backup configuration.
func validateBackup(cluster *cbv1alpha1.CloudberryCluster) error {
	if cluster.Spec.Backup == nil || !cluster.Spec.Backup.Enabled {
		return nil
	}

	if cluster.Spec.Backup.Destination.Type == "" {
		return fmt.Errorf("backup.destination.type is required when backup is enabled")
	}

	if cluster.Spec.Backup.Destination.Type == "s3" && cluster.Spec.Backup.Destination.Bucket == "" {
		return fmt.Errorf("backup.destination.bucket is required for S3 destinations")
	}

	if cluster.Spec.Backup.Compression < 0 || cluster.Spec.Backup.Compression > 9 {
		return fmt.Errorf("backup.compression must be between 0 and 9, got %d",
			cluster.Spec.Backup.Compression)
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
