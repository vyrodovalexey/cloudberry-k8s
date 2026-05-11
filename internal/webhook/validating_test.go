package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

func newValidCluster() *cbv1alpha1.CloudberryCluster {
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version: "7.7",
			Image:   "cloudberrydb/cloudberry:7.7",
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Storage: cbv1alpha1.StorageSpec{Size: "10Gi"},
				Port:    5432,
			},
			Segments: cbv1alpha1.SegmentsSpec{
				Count:   4,
				Storage: cbv1alpha1.StorageSpec{Size: "20Gi"},
			},
			DeletionPolicy: cbv1alpha1.DeletionPolicyRetain,
		},
	}
}

func TestNewCloudberryClusterValidator(t *testing.T) {
	v := NewCloudberryClusterValidator()
	require.NotNil(t, v)
}

func TestValidateCreate(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "valid cluster",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "invalid segment count",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Segments.Count = 0
				return c
			}(),
			expectErr:   true,
			errContains: "segments.count",
		},
		{
			name: "missing coordinator storage size",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Coordinator.Storage.Size = ""
				return c
			}(),
			expectErr:   true,
			errContains: "coordinator.storage.size",
		},
		{
			name: "missing segment storage size",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Segments.Storage.Size = ""
				return c
			}(),
			expectErr:   true,
			errContains: "segments.storage.size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewCloudberryClusterValidator()
			warnings, err := v.ValidateCreate(context.Background(), tt.cluster)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
			_ = warnings
		})
	}
}

func TestValidateCreate_WrongType(t *testing.T) {
	v := NewCloudberryClusterValidator()
	// Use a different runtime.Object that is not CloudberryCluster
	wrongObj := &cbv1alpha1.CloudberryClusterList{}
	_, err := v.ValidateCreate(context.Background(), wrongObj)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected CloudberryCluster")
}

func TestValidateUpdate(t *testing.T) {
	v := NewCloudberryClusterValidator()
	oldCluster := newValidCluster()
	newCluster := newValidCluster()

	warnings, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	require.NoError(t, err)
	_ = warnings
}

func TestValidateUpdate_WrongType(t *testing.T) {
	v := NewCloudberryClusterValidator()
	wrongObj := &cbv1alpha1.CloudberryClusterList{}
	_, err := v.ValidateUpdate(context.Background(), newValidCluster(), wrongObj)
	require.Error(t, err)
}

func TestValidateDelete(t *testing.T) {
	v := NewCloudberryClusterValidator()
	warnings, err := v.ValidateDelete(context.Background(), newValidCluster())
	require.NoError(t, err)
	assert.Nil(t, warnings)
}

func TestValidateOIDC(t *testing.T) {
	tests := []struct {
		name        string
		cluster     *cbv1alpha1.CloudberryCluster
		expectErr   bool
		errContains string
	}{
		{
			name:      "no auth spec",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "oidc disabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{Enabled: false},
				}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "oidc enabled without issuer url",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:  true,
						ClientID: "client-id",
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "issuerURL",
		},
		{
			name: "oidc enabled without client id",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:   true,
						IssuerURL: "https://issuer.example.com",
					},
				}
				return c
			}(),
			expectErr:   true,
			errContains: "clientID",
		},
		{
			name: "oidc enabled with all required fields",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{
						Enabled:   true,
						IssuerURL: "https://issuer.example.com",
						ClientID:  "client-id",
					},
				}
				return c
			}(),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOIDC(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateVault(t *testing.T) {
	tests := []struct {
		name      string
		cluster   *cbv1alpha1.CloudberryCluster
		expectErr bool
	}{
		{
			name:      "no vault spec",
			cluster:   newValidCluster(),
			expectErr: false,
		},
		{
			name: "vault disabled",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: false}
				return c
			}(),
			expectErr: false,
		},
		{
			name: "vault enabled without address",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: true, Address: ""}
				return c
			}(),
			expectErr: true,
		},
		{
			name: "vault enabled with address",
			cluster: func() *cbv1alpha1.CloudberryCluster {
				c := newValidCluster()
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: true, Address: "https://vault.example.com"}
				return c
			}(),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVault(tt.cluster)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateDeletionPolicy(t *testing.T) {
	tests := []struct {
		name      string
		policy    cbv1alpha1.DeletionPolicy
		expectErr bool
	}{
		{"empty policy", "", false},
		{"retain policy", cbv1alpha1.DeletionPolicyRetain, false},
		{"delete policy", cbv1alpha1.DeletionPolicyDelete, false},
		{"invalid policy", cbv1alpha1.DeletionPolicy("Invalid"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newValidCluster()
			c.Spec.DeletionPolicy = tt.policy
			err := validateDeletionPolicy(c)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateSegments_SpreadWarning(t *testing.T) {
	c := newValidCluster()
	c.Spec.Segments.Count = 2
	c.Spec.Segments.PrimariesPerHost = 2
	c.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutSpread,
	}

	var warnings admission.Warnings
	err := validateSegments(c, &warnings)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "spread mirroring")
}

func TestValidateStorage(t *testing.T) {
	tests := []struct {
		name        string
		coordSize   string
		segSize     string
		expectErr   bool
		errContains string
	}{
		{"valid sizes", "10Gi", "20Gi", false, ""},
		{"missing coordinator size", "", "20Gi", true, "coordinator.storage.size"},
		{"missing segment size", "10Gi", "", true, "segments.storage.size"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newValidCluster()
			c.Spec.Coordinator.Storage.Size = tt.coordSize
			c.Spec.Segments.Storage.Size = tt.segSize
			err := validateStorage(c)
			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
