// Package webhook provides admission webhooks for the cloudberry operator.
package webhook

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// CloudberryClusterValidator validates CloudberryCluster resources.
type CloudberryClusterValidator struct{}

// NewCloudberryClusterValidator creates a new CloudberryClusterValidator.
func NewCloudberryClusterValidator() *CloudberryClusterValidator {
	return &CloudberryClusterValidator{}
}

// ValidateCreate validates a CloudberryCluster on creation.
func (v *CloudberryClusterValidator) ValidateCreate(
	_ context.Context,
	obj runtime.Object,
) (admission.Warnings, error) {
	cluster, ok := obj.(*cbv1alpha1.CloudberryCluster)
	if !ok {
		return nil, fmt.Errorf("expected CloudberryCluster, got %T", obj)
	}
	return validateCluster(cluster)
}

// ValidateUpdate validates a CloudberryCluster on update.
func (v *CloudberryClusterValidator) ValidateUpdate(
	_ context.Context,
	_ runtime.Object,
	newObj runtime.Object,
) (admission.Warnings, error) {
	cluster, ok := newObj.(*cbv1alpha1.CloudberryCluster)
	if !ok {
		return nil, fmt.Errorf("expected CloudberryCluster, got %T", newObj)
	}
	return validateCluster(cluster)
}

// ValidateDelete validates a CloudberryCluster on deletion.
func (v *CloudberryClusterValidator) ValidateDelete(
	_ context.Context,
	_ runtime.Object,
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
