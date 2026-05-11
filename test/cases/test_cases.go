// Package cases defines test case data structures and test case catalogs for the cloudberry-k8s project.
package cases

import (
	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// TestCase represents a generic test case.
type TestCase struct {
	Name        string
	Description string
	Input       interface{}
	Expected    interface{}
	ShouldFail  bool
	ErrorMsg    string
}

// WebhookValidationCase represents a webhook validation test case.
type WebhookValidationCase struct {
	Name           string
	Cluster        *cbv1alpha1.CloudberryCluster
	ExpectError    bool
	ErrorSubstring string
	ExpectWarnings bool
}

// ClusterLifecycleCase represents a cluster lifecycle test case.
type ClusterLifecycleCase struct {
	Name          string
	Action        string
	InitialPhase  cbv1alpha1.ClusterPhase
	ExpectedPhase cbv1alpha1.ClusterPhase
	Annotations   map[string]string
	ExpectError   bool
}

// ConfigManagementCase represents a configuration management test case.
type ConfigManagementCase struct {
	Name           string
	Parameters     map[string]string
	ExpectReload   bool
	ExpectError    bool
	ErrorSubstring string
}

// HBATestCase represents an HBA rule test case.
type HBATestCase struct {
	Name        string
	Rules       []cbv1alpha1.HBARule
	ExpectError bool
}

// HAOperationCase represents an HA operation test case.
type HAOperationCase struct {
	Name             string
	Action           string
	MirroringEnabled bool
	StandbyEnabled   bool
	SegmentsHealthy  bool
	ExpectEvent      string
}

// MaintenanceCase represents a maintenance operation test case.
type MaintenanceCase struct {
	Name        string
	Operation   string
	ExpectEvent string
	ExpectError bool
}

// AuthFlowCase represents an authentication flow test case.
type AuthFlowCase struct {
	Name           string
	AuthMethod     string
	Username       string
	Password       string
	Token          string
	ExpectSuccess  bool
	ExpectedStatus int
}

// VaultOperationCase represents a Vault operation test case.
type VaultOperationCase struct {
	Name        string
	Operation   string
	Path        string
	Data        map[string]interface{}
	ExpectError bool
}
