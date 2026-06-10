package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clientpkg "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// clusterTLSSecretName is the certSecret name used across the cluster TLS tests.
const testClusterTLSSecretName = "test-cluster-server-tls"

// tlsTestCluster returns a cluster requesting TLS auto-issuance: vault enabled,
// auth.ssl enabled with a named certSecret.
func tlsTestCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Spec.Vault = &cbv1alpha1.VaultSpec{Enabled: true, Address: "https://vault:8200"}
	cluster.Spec.Auth = &cbv1alpha1.AuthSpec{
		SSL: &cbv1alpha1.SSLSpec{
			Enabled:    true,
			CertSecret: &cbv1alpha1.CertSecretRef{Name: testClusterTLSSecretName},
		},
	}
	return cluster
}

// pkiVaultClient returns an enabled fake Vault client whose
// WriteSecretWithResponse answers like the PKI issue endpoint.
func pkiVaultClient() *fakeVaultClient {
	return &fakeVaultClient{
		enabled: true,
		writeResp: map[string]interface{}{
			"certificate": "CERT-PEM",
			"private_key": "KEY-PEM",
			"issuing_ca":  "CA-PEM",
		},
	}
}

// tlsTestReconciler builds a ClusterReconciler with a fake client, a fake
// event recorder and the supplied vault client (nil leaves SetClusterTLS
// unwired so the no-Vault path is exercised).
func tlsTestReconciler(vc *fakeVaultClient) (*ClusterReconciler, *record.FakeRecorder) {
	scheme := newTestScheme()
	recorder := record.NewFakeRecorder(20)
	r := NewClusterReconciler(fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme, recorder, builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	if vc != nil {
		r.SetClusterTLS(vc, "pki", "cloudberry")
	}
	return r, recorder
}

// selfSignedPEM generates a self-signed certificate PEM whose validity window
// is [notBefore, notAfter] so rotation-threshold behavior can be pinned.
func selfSignedPEM(t *testing.T, notBefore, notAfter time.Time) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-cluster.default.svc.cluster.local"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// operatorManagedTLSSecret returns an operator-labeled cluster TLS Secret
// holding the given certificate PEM.
func operatorManagedTLSSecret(certPEM []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterTLSSecretName,
			Namespace: "default",
			Labels:    util.CommonLabels("test-cluster", clusterTLSComponentLabel),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			secretKeyCACert:  []byte("CA-PEM"),
			secretKeyTLSCert: certPEM,
			secretKeyTLSKey:  []byte("KEY-PEM"),
		},
	}
}

func getTLSSecret(t *testing.T, r *ClusterReconciler) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	err := r.client.Get(context.Background(), types.NamespacedName{
		Name: testClusterTLSSecretName, Namespace: "default",
	}, secret)
	require.NoError(t, err)
	return secret
}

func TestClusterTLSAutoIssueEnabled(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*cbv1alpha1.CloudberryCluster)
		want   bool
	}{
		{name: "all enabled", mutate: nil, want: true},
		{name: "nil vault", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Vault = nil
		}, want: false},
		{name: "vault disabled", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Vault.Enabled = false
		}, want: false},
		{name: "nil auth", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Auth = nil
		}, want: false},
		{name: "nil ssl", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Auth.SSL = nil
		}, want: false},
		{name: "ssl disabled", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Auth.SSL.Enabled = false
		}, want: false},
		{name: "nil certSecret", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Auth.SSL.CertSecret = nil
		}, want: false},
		{name: "empty certSecret name", mutate: func(c *cbv1alpha1.CloudberryCluster) {
			c.Spec.Auth.SSL.CertSecret.Name = ""
		}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := tlsTestCluster()
			if tc.mutate != nil {
				tc.mutate(cluster)
			}
			assert.Equal(t, tc.want, clusterTLSAutoIssueEnabled(cluster))
		})
	}
}

func TestClusterTLSDNSNames(t *testing.T) {
	cluster := tlsTestCluster()
	names := clusterTLSDNSNames(cluster)

	// CN is the cluster-level FQDN and must come first.
	require.NotEmpty(t, names)
	assert.Equal(t, "test-cluster.default.svc.cluster.local", names[0])

	// Headless coordinator/standby/segment services get plain + wildcard SANs.
	for _, svc := range []string{
		util.CoordinatorServiceName("test-cluster"),
		util.StandbyServiceName("test-cluster"),
		util.SegmentServiceName("test-cluster"),
	} {
		assert.Contains(t, names, fmt.Sprintf("%s.default.svc", svc))
		assert.Contains(t, names, fmt.Sprintf("%s.default.svc.cluster.local", svc))
		assert.Contains(t, names, fmt.Sprintf("*.%s.default.svc.cluster.local", svc))
	}

	// Client service gets plain SANs.
	client := util.ClientServiceName("test-cluster")
	assert.Contains(t, names, fmt.Sprintf("%s.default.svc", client))
	assert.Contains(t, names, fmt.Sprintf("%s.default.svc.cluster.local", client))
}

func TestReconcileClusterTLSSecret_NotRequested_NoOp(t *testing.T) {
	cluster := newTestCluster() // no vault, no ssl
	r, _ := tlsTestReconciler(pkiVaultClient())

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := &corev1.Secret{}
	err := r.client.Get(context.Background(), types.NamespacedName{
		Name: testClusterTLSSecretName, Namespace: "default",
	}, secret)
	assert.Error(t, err, "no Secret may be created when auto-issuance is not requested")
}

func TestReconcileClusterTLSSecret_IssuesSecretWhenMissing(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	cluster := tlsTestCluster()
	vc := pkiVaultClient()
	r, recorder := tlsTestReconciler(vc)
	require.NoError(t, r.client.Create(context.Background(), cluster))

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	// Generic (Opaque) Secret with all THREE keys — the init-tls container
	// requires ca.crt, which a kubernetes.io/tls Secret would not carry.
	assert.Equal(t, corev1.SecretTypeOpaque, secret.Type)
	assert.Equal(t, []byte("CERT-PEM"), secret.Data[secretKeyTLSCert])
	assert.Equal(t, []byte("KEY-PEM"), secret.Data[secretKeyTLSKey])
	assert.Equal(t, []byte("CA-PEM"), secret.Data[secretKeyCACert])
	// Operator-managed labels mark the Secret as renewable by the operator.
	assert.Equal(t, util.LabelManagedByValue, secret.Labels[util.LabelManagedBy])
	assert.Equal(t, clusterTLSComponentLabel, secret.Labels[util.LabelComponent])
	// Owner reference ties the Secret lifecycle to the cluster.
	require.Len(t, secret.OwnerReferences, 1)
	assert.Equal(t, "test-cluster", secret.OwnerReferences[0].Name)

	// PKI issue path uses the wired mount/role.
	assert.Equal(t, "pki/issue/cloudberry", vc.writePath)

	// Issuance event emitted.
	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, cbv1alpha1.EventReasonClusterTLSIssued)
	default:
		t.Fatal("expected ClusterTLSIssued event")
	}

	// Span controller.clusterTLS recorded, with no PII in attributes.
	spans := sr.Ended()
	var found bool
	for _, span := range spans {
		if span.Name() == "controller."+clusterTLSSpanName {
			found = true
		}
	}
	assert.True(t, found, "controller.clusterTLS span must be recorded")
	telemetry.AssertNoPII(t, spans)
}

func TestReconcileClusterTLSSecret_UserProvidedSecretUntouched(t *testing.T) {
	cluster := tlsTestCluster()
	// User-provided Secret: NO operator labels.
	userSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterTLSSecretName,
			Namespace: "default",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			secretKeyTLSCert: []byte("USER-CERT"),
			secretKeyTLSKey:  []byte("USER-KEY"),
			secretKeyCACert:  []byte("USER-CA"),
		},
	}
	r, recorder := tlsTestReconciler(pkiVaultClient())
	require.NoError(t, r.client.Create(context.Background(), userSecret))

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	assert.Equal(t, []byte("USER-CERT"), secret.Data[secretKeyTLSCert],
		"user-provided Secret must never be modified")
	select {
	case ev := <-recorder.Events:
		t.Fatalf("no event expected for an untouched user-provided Secret, got %q", ev)
	default:
	}
}

func TestReconcileClusterTLSSecret_RenewsExpiredManagedCert(t *testing.T) {
	cluster := tlsTestCluster()
	// Certificate past 2/3 of its lifetime (issued 100d ago, 10d left).
	oldCert := selfSignedPEM(t,
		time.Now().Add(-100*24*time.Hour), time.Now().Add(10*24*time.Hour))
	managed := operatorManagedTLSSecret(oldCert)

	vc := pkiVaultClient()
	vc.writeResp["certificate"] = "RENEWED-CERT"
	r, recorder := tlsTestReconciler(vc)
	require.NoError(t, r.client.Create(context.Background(), managed))

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	assert.Equal(t, []byte("RENEWED-CERT"), secret.Data[secretKeyTLSCert])
	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, cbv1alpha1.EventReasonClusterTLSRenewed)
	default:
		t.Fatal("expected ClusterTLSRenewed event")
	}
}

func TestReconcileClusterTLSSecret_FreshManagedCertNotRenewed(t *testing.T) {
	cluster := tlsTestCluster()
	// Fresh certificate well within the first 2/3 of its lifetime.
	freshCert := selfSignedPEM(t,
		time.Now().Add(-time.Hour), time.Now().Add(365*24*time.Hour))
	managed := operatorManagedTLSSecret(freshCert)

	r, recorder := tlsTestReconciler(pkiVaultClient())
	require.NoError(t, r.client.Create(context.Background(), managed))

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	assert.Equal(t, freshCert, secret.Data[secretKeyTLSCert], "fresh cert must be kept")
	select {
	case ev := <-recorder.Events:
		t.Fatalf("no event expected for a fresh managed cert, got %q", ev)
	default:
	}
}

func TestReconcileClusterTLSSecret_UnparsableManagedCertReissued(t *testing.T) {
	cluster := tlsTestCluster()
	managed := operatorManagedTLSSecret([]byte("not-a-pem"))

	vc := pkiVaultClient()
	vc.writeResp["certificate"] = "REISSUED-CERT"
	r, _ := tlsTestReconciler(vc)
	require.NoError(t, r.client.Create(context.Background(), managed))

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))

	secret := getTLSSecret(t, r)
	assert.Equal(t, []byte("REISSUED-CERT"), secret.Data[secretKeyTLSCert])
}

func TestReconcileClusterTLSSecret_NoVaultClient_FailsWithEvent(t *testing.T) {
	cluster := tlsTestCluster()
	r, recorder := tlsTestReconciler(nil) // SetClusterTLS never called

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no enabled Vault client")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, cbv1alpha1.EventReasonClusterTLSFailed)
	default:
		t.Fatal("expected ClusterTLSFailed warning event")
	}
}

func TestReconcileClusterTLSSecret_DisabledVaultClient_Fails(t *testing.T) {
	cluster := tlsTestCluster()
	r, _ := tlsTestReconciler(&fakeVaultClient{enabled: false})

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no enabled Vault client")
}

func TestReconcileClusterTLSSecret_VaultIssueError_FailsWithEvent(t *testing.T) {
	cluster := tlsTestCluster()
	vc := &fakeVaultClient{enabled: true, writeErr: fmt.Errorf("pki backend sealed")}
	r, recorder := tlsTestReconciler(vc)

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuing cluster TLS certificate")

	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, cbv1alpha1.EventReasonClusterTLSFailed)
	default:
		t.Fatal("expected ClusterTLSFailed warning event")
	}
}

func TestReconcileClusterTLSSecret_MetricsRecorded(t *testing.T) {
	cluster := tlsTestCluster()
	rec := &clusterCertIssuanceRecorder{}
	scheme := newTestScheme()
	r := NewClusterReconciler(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme, record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)
	r.SetClusterTLS(pkiVaultClient(), "pki", "cloudberry")

	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))
	require.Len(t, rec.calls, 1)
	assert.Equal(t, [3]string{"test-cluster", "default", reconcileResultSuccess}, rec.calls[0])

	// Error path records the error result.
	r2 := NewClusterReconciler(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme, record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)
	r2.SetClusterTLS(&fakeVaultClient{enabled: true, writeErr: fmt.Errorf("boom")}, "", "")
	require.Error(t, r2.reconcileClusterTLSSecret(context.Background(), cluster))
	require.Len(t, rec.calls, 2)
	assert.Equal(t, reconcileResultError, rec.calls[1][2])
}

func TestReconcileClusterTLSSecret_CreateRaceAlreadyExistsIsTolerated(t *testing.T) {
	cluster := tlsTestCluster()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c clientpkg.WithWatch,
				obj clientpkg.Object, opts ...clientpkg.CreateOption) error {
				return apierrors.NewAlreadyExists(
					corev1.Resource("secrets"), obj.GetName())
			},
		}).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	r.SetClusterTLS(pkiVaultClient(), "pki", "cloudberry")

	// A concurrent reconcile won the create race: tolerated, no error.
	require.NoError(t, r.reconcileClusterTLSSecret(context.Background(), cluster))
}

func TestReconcileClusterTLSSecret_CreateErrorPropagates(t *testing.T) {
	cluster := tlsTestCluster()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c clientpkg.WithWatch,
				obj clientpkg.Object, opts ...clientpkg.CreateOption) error {
				return apierrors.NewInternalError(fmt.Errorf("etcd down"))
			},
		}).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	r.SetClusterTLS(pkiVaultClient(), "pki", "cloudberry")

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating cluster TLS secret")
}

func TestReconcileClusterTLSSecret_GetErrorPropagates(t *testing.T) {
	cluster := tlsTestCluster()
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c clientpkg.WithWatch,
				key types.NamespacedName, obj clientpkg.Object,
				opts ...clientpkg.GetOption) error {
				return apierrors.NewInternalError(fmt.Errorf("apiserver flake"))
			},
		}).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	r.SetClusterTLS(pkiVaultClient(), "pki", "cloudberry")

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting cluster TLS secret")
}

func TestRenewClusterTLSSecret_UpdateErrorPropagates(t *testing.T) {
	cluster := tlsTestCluster()
	oldCert := selfSignedPEM(t,
		time.Now().Add(-100*24*time.Hour), time.Now().Add(10*24*time.Hour))
	managed := operatorManagedTLSSecret(oldCert)

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(managed).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c clientpkg.WithWatch,
				obj clientpkg.Object, opts ...clientpkg.UpdateOption) error {
				return apierrors.NewInternalError(fmt.Errorf("conflict storm"))
			},
		}).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)
	r.SetClusterTLS(pkiVaultClient(), "pki", "cloudberry")

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating cluster TLS secret")
}

func TestRenewClusterTLSSecret_VaultErrorDuringRenewal(t *testing.T) {
	cluster := tlsTestCluster()
	oldCert := selfSignedPEM(t,
		time.Now().Add(-100*24*time.Hour), time.Now().Add(10*24*time.Hour))
	managed := operatorManagedTLSSecret(oldCert)

	r, recorder := tlsTestReconciler(&fakeVaultClient{
		enabled: true, writeErr: fmt.Errorf("pki sealed"),
	})
	require.NoError(t, r.client.Create(context.Background(), managed))

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	select {
	case ev := <-recorder.Events:
		assert.Contains(t, ev, cbv1alpha1.EventReasonClusterTLSFailed)
	default:
		t.Fatal("expected ClusterTLSFailed warning event")
	}
}

func TestIssueClusterTLSSecret_OwnerRefErrorPropagates(t *testing.T) {
	cluster := tlsTestCluster()
	// A scheme WITHOUT the CloudberryCluster type makes SetControllerReference
	// fail (unregistered owner GVK).
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	r := NewClusterReconciler(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme, record.NewFakeRecorder(20), builder.NewBuilder(),
		&metrics.NoopRecorder{}, nil)
	r.SetClusterTLS(pkiVaultClient(), "pki", "cloudberry")

	err := r.reconcileClusterTLSSecret(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "owner reference")
}

func TestRecordClusterCertIssuance_NilMetricsSafe(t *testing.T) {
	r := &ClusterReconciler{}
	assert.NotPanics(t, func() {
		r.recordClusterCertIssuance(tlsTestCluster(), reconcileResultSuccess)
	})
}

// clusterCertIssuanceRecorder captures RecordClusterCertIssuance calls while
// no-oping the rest of the Recorder interface.
type clusterCertIssuanceRecorder struct {
	metrics.NoopRecorder
	calls [][3]string
}

func (c *clusterCertIssuanceRecorder) RecordClusterCertIssuance(cluster, namespace, result string) {
	c.calls = append(c.calls, [3]string{cluster, namespace, result})
}
