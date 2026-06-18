package webhook

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// capturingRecorder captures webhook admission metric invocations. It embeds
// metrics.NoopRecorder so only the relevant method is overridden.
type capturingRecorder struct {
	*metrics.NoopRecorder

	mu sync.Mutex

	admissions    int
	lastWebhook   string
	lastOperation string
	lastResult    string
}

func newCapturingRecorder() *capturingRecorder {
	return &capturingRecorder{NoopRecorder: &metrics.NoopRecorder{}}
}

func (c *capturingRecorder) RecordWebhookAdmission(webhook, operation, result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.admissions++
	c.lastWebhook = webhook
	c.lastOperation = operation
	c.lastResult = result
}

// ============================================================================
// recordAdmission
// ============================================================================

func TestRecordAdmission_NilRecorder(t *testing.T) {
	v := &CloudberryClusterValidator{}
	// No recorder: must be a no-op and not panic.
	v.recordAdmission(admissionOpCreate, nil)
}

func TestRecordAdmission_Allowed(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	_, err := v.ValidateCreate(context.Background(), newValidCluster())
	require.NoError(t, err)

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, webhookValidating, rec.lastWebhook)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionAllowed, rec.lastResult)
}

func TestRecordAdmission_Denied_OnCreate(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	c := newValidCluster()
	c.Spec.Segments.Count = 0 // invalid -> denied

	_, err := v.ValidateCreate(context.Background(), c)
	require.Error(t, err)

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionDenied, rec.lastResult)
}

func TestRecordAdmission_Allowed_OnUpdate(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	_, err := v.ValidateUpdate(context.Background(), newValidCluster(), newValidCluster())
	require.NoError(t, err)

	assert.Equal(t, admissionOpUpdate, rec.lastOperation)
	assert.Equal(t, admissionAllowed, rec.lastResult)
}

func TestRecordAdmission_Denied_OnUpdate(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	oldCluster := newValidCluster()
	newCluster := newValidCluster()
	newCluster.Spec.Segments.Count = 0 // invalid -> denied

	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	require.Error(t, err)

	assert.Equal(t, admissionOpUpdate, rec.lastOperation)
	assert.Equal(t, admissionDenied, rec.lastResult)
}

func TestRecordAdmission_OnDelete(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	_, err := v.ValidateDelete(context.Background(), newValidCluster())
	require.NoError(t, err)

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, admissionOpDelete, rec.lastOperation)
	assert.Equal(t, admissionAllowed, rec.lastResult)
}

func TestRecordAdmission_Error_OnInternalFailure(t *testing.T) {
	// A List failure during the duplicate-name check is an internal error, not a
	// validation rejection, so it must record the distinct "error" result.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				_ context.Context, _ client.WithWatch, _ client.ObjectList,
				_ ...client.ListOption,
			) error {
				return fmt.Errorf("api server unavailable")
			},
		}).
		Build()

	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(k8sClient, rec)

	_, err := v.ValidateCreate(context.Background(), newValidCluster())
	require.Error(t, err)

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionError, rec.lastResult)
}

// ============================================================================
// validateCluster — exercise each validator's error path through the
// top-level entry point so every early-return branch is covered.
// ============================================================================

func TestValidateCluster_AllErrorBranches(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(c *cbv1alpha1.CloudberryCluster)
		errContains string
	}{
		{
			name:        "valid cluster",
			mutate:      func(_ *cbv1alpha1.CloudberryCluster) {},
			errContains: "",
		},
		{
			name:        "segments error",
			mutate:      func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Segments.Count = 0 },
			errContains: "segments.count",
		},
		{
			name: "oidc error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Auth = &cbv1alpha1.AuthSpec{
					OIDC: &cbv1alpha1.OIDCSpec{Enabled: true, ClientID: "id"},
				}
			},
			errContains: "issuerURL",
		},
		{
			name: "vault error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: true}
			},
			errContains: "vault.address",
		},
		{
			name: "deletion policy error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicy("Bogus")
			},
			errContains: "deletionPolicy",
		},
		{
			name: "storage error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Coordinator.Storage.Size = ""
			},
			errContains: "coordinator.storage.size",
		},
		{
			name: "workload error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Workload = &cbv1alpha1.WorkloadSpec{
					Enabled:        true,
					ResourceGroups: []cbv1alpha1.ResourceGroupSpec{{Concurrency: 1}},
				}
			},
			errContains: "workload.resourceGroups[0].name",
		},
		{
			name: "query monitoring error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
					Enabled:          true,
					SamplingInterval: -1,
				}
			},
			errContains: "samplingInterval",
		},
		{
			name: "backup error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{
					Enabled:     true,
					Destination: cbv1alpha1.BackupDestination{Type: ""},
				}
			},
			errContains: "backup.destination.type",
		},
		{
			name: "data loading error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
					Enabled: true,
					Jobs:    []cbv1alpha1.DataLoadingJob{{Type: "pxf"}},
				}
			},
			errContains: "dataLoading.jobs[0].name",
		},
		{
			name: "storage management error",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
					RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
						Enabled:        true,
						BloatThreshold: 200,
					},
				}
			},
			errContains: "bloatThreshold",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newValidCluster()
			tt.mutate(c)
			warnings, err := validateCluster(c)
			if tt.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			}
			_ = warnings
		})
	}
}
