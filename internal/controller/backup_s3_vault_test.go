package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// fakeVaultClient is a minimal vault.Client used to exercise the backup S3
// vault-credential materialization path without a real Vault server.
type fakeVaultClient struct {
	enabled  bool
	readData map[string]interface{}
	readErr  error
	readPath string
}

func (f *fakeVaultClient) ReadSecret(_ context.Context, path string) (map[string]interface{}, error) {
	f.readPath = path
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.readData, nil
}

func (f *fakeVaultClient) WriteSecret(_ context.Context, _ string, _ map[string]interface{}) error {
	return nil
}

func (f *fakeVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

func (f *fakeVaultClient) IsEnabled() bool { return f.enabled }

// vaultBackupCluster returns a Running cluster with an S3 destination using a
// Vault secret for credentials (no Kubernetes credential secret).
func vaultBackupCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket: "my-bucket",
				VaultSecret: &cbv1alpha1.S3VaultSecret{
					Path: "secret/data/cloudberry/backup-s3",
				},
			},
		},
		Image: "cloudberry-backup:2.1.0",
	}
	return cluster
}

func TestEnsureBackupS3VaultCredentials_Materializes(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{
		enabled: true,
		readData: map[string]interface{}{
			"aws_access_key_id":     "AKIAEXAMPLE",
			"aws_secret_access_key": "secretvalue",
		},
	}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))
	assert.Equal(t, "secret/data/cloudberry/backup-s3", vc.readPath)

	secret := &corev1.Secret{}
	name := util.BackupS3VaultCredentialsSecretName(cluster.Name)
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret))
	assert.Equal(t, "AKIAEXAMPLE", string(secret.Data["aws_access_key_id"]))
	assert.Equal(t, "secretvalue", string(secret.Data["aws_secret_access_key"]))
	require.Len(t, secret.OwnerReferences, 1)
}

func TestEnsureBackupS3VaultCredentials_CustomFields(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	cluster.Spec.Backup.Destination.S3.VaultSecret.AccessKeyField = "ak"
	cluster.Spec.Backup.Destination.S3.VaultSecret.SecretKeyField = "sk"
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{
		enabled:  true,
		readData: map[string]interface{}{"ak": "id1", "sk": "secret1"},
	}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))

	secret := &corev1.Secret{}
	name := util.BackupS3VaultCredentialsSecretName(cluster.Name)
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret))
	// Materialized under canonical default keys regardless of vault field names.
	assert.Equal(t, "id1", string(secret.Data["aws_access_key_id"]))
	assert.Equal(t, "secret1", string(secret.Data["aws_secret_access_key"]))
}

func TestEnsureBackupS3VaultCredentials_UpdatesExisting(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupS3VaultCredentialsSecretName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Data: map[string][]byte{
			"aws_access_key_id":     []byte("old"),
			"aws_secret_access_key": []byte("old"),
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, existing).Build()

	vc := &fakeVaultClient{
		enabled: true,
		readData: map[string]interface{}{
			"aws_access_key_id":     "new-id",
			"aws_secret_access_key": "new-secret",
		},
	}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))

	secret := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: existing.Name, Namespace: cluster.Namespace,
	}, secret))
	assert.Equal(t, "new-id", string(secret.Data["aws_access_key_id"]))
}

func TestEnsureBackupS3VaultCredentials_NilVaultSkips(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	// No vault client wired (nil): must skip without panic and create no secret.
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))

	secret := &corev1.Secret{}
	name := util.BackupS3VaultCredentialsSecretName(cluster.Name)
	err := k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: cluster.Namespace}, secret)
	assert.True(t, apierrors.IsNotFound(err), "no secret should be created when vault is nil")
}

func TestEnsureBackupS3VaultCredentials_DisabledVaultSkips(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{enabled: false}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))
	assert.Empty(t, vc.readPath, "disabled vault must not be read")
}

func TestEnsureBackupS3VaultCredentials_NoVaultSecretNoop(t *testing.T) {
	scheme := newTestScheme()
	cluster := backupTestCluster() // uses CredentialSecret, not VaultSecret
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{enabled: true}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	require.NoError(t, r.ensureBackupS3VaultCredentials(context.Background(), cluster))
	assert.Empty(t, vc.readPath, "non-vault destination must not read vault")
}

func TestEnsureBackupS3VaultCredentials_ReadError(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{enabled: true, readErr: errors.New("vault down")}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	err := r.ensureBackupS3VaultCredentials(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault down")
}

func TestEnsureBackupS3VaultCredentials_MissingField(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{
		enabled:  true,
		readData: map[string]interface{}{"aws_access_key_id": "only-access"},
	}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	err := r.ensureBackupS3VaultCredentials(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret key field")
}

func TestEnsureBackupS3VaultCredentials_NilDataError(t *testing.T) {
	scheme := newTestScheme()
	cluster := vaultBackupCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	vc := &fakeVaultClient{enabled: true, readData: nil}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc)

	err := r.ensureBackupS3VaultCredentials(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no data found")
}

func TestVaultS3CredsField_Stringifies(t *testing.T) {
	data := map[string]interface{}{"num": 42, "str": "x"}
	v, ok := vaultS3CredsField(data, "num")
	assert.True(t, ok)
	assert.Equal(t, "42", v)

	v, ok = vaultS3CredsField(data, "str")
	assert.True(t, ok)
	assert.Equal(t, "x", v)

	_, ok = vaultS3CredsField(data, "missing")
	assert.False(t, ok)
}
