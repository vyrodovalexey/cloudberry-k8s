// Package certmanager provides webhook TLS certificate management for the cloudberry operator.
// It supports two strategies: Vault PKI (preferred) and self-signed (fallback).
package certmanager

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

const (
	// CertSourceVaultPKI uses Vault PKI to issue certificates.
	CertSourceVaultPKI = "vault-pki"
	// CertSourceSelfSigned uses self-signed certificates.
	CertSourceSelfSigned = "self-signed"

	// organizationName is the organization name used in generated certificates.
	organizationName = "cloudberry-operator"

	// secretKeyCACert is the key for the CA certificate in the Secret.
	secretKeyCACert = "ca.crt"
	// secretKeyTLSCert is the key for the TLS certificate in the Secret.
	secretKeyTLSCert = "tls.crt"
	// secretKeyTLSKey is the key for the TLS private key in the Secret.
	secretKeyTLSKey = "tls.key"

	// rotationThresholdFraction is the fraction of certificate lifetime at which rotation is triggered.
	// Certificates are rotated when 2/3 of their lifetime has elapsed.
	rotationThresholdFraction = 2.0 / 3.0

	// certComponent is the component label value used for certificate metrics.
	certComponent = "webhook"
	// resultSuccess and resultError are the result label values for cert metrics.
	resultSuccess = "success"
	resultError   = "error"
)

// CertManager manages webhook TLS certificates.
type CertManager interface {
	// EnsureCertificates ensures webhook TLS certificates exist and are valid.
	// Returns the CA bundle (PEM-encoded) for webhook configuration injection.
	EnsureCertificates(ctx context.Context) (caBundle []byte, err error)
	// NeedsRotation checks if certificates need rotation.
	NeedsRotation(ctx context.Context) (bool, error)
}

// Config holds certificate manager configuration.
type Config struct {
	// ServiceName is the webhook service name.
	ServiceName string
	// ServiceNamespace is the webhook service namespace.
	ServiceNamespace string
	// SecretName is the name of the Secret to store certs in.
	SecretName string
	// SecretNamespace is the namespace of the cert Secret.
	SecretNamespace string
	// CertSource is "vault-pki" or "self-signed".
	CertSource string
	// VaultPKIMountPath is the Vault PKI mount path (for vault-pki source).
	VaultPKIMountPath string
	// VaultPKIRole is the Vault PKI role name (for vault-pki source).
	VaultPKIRole string
	// CertValidityDuration is the certificate validity period.
	CertValidityDuration time.Duration
}

// certManager implements CertManager.
type certManager struct {
	client      client.Client
	vaultClient vault.Client
	config      Config
	logger      *slog.Logger
	// recorder records certificate metrics. It is optional and may be nil;
	// all metric recording is guarded with a nil check.
	recorder metrics.Recorder
}

// New creates a new CertManager based on the provided configuration.
// An optional metrics recorder may be supplied to record certificate metrics;
// when omitted (or nil), metric recording is a no-op.
func New(
	k8sClient client.Client,
	vaultClient vault.Client,
	cfg Config,
	logger *slog.Logger,
	recorder ...metrics.Recorder,
) CertManager {
	if logger == nil {
		logger = slog.Default()
	}
	cm := &certManager{
		client:      k8sClient,
		vaultClient: vaultClient,
		config:      cfg,
		logger:      logger.With("component", "certmanager"),
	}
	if len(recorder) > 0 {
		cm.recorder = recorder[0]
	}
	return cm
}

// certSource returns the configured certificate source label value,
// defaulting to self-signed when unset.
func (m *certManager) certSource() string {
	if m.config.CertSource == CertSourceVaultPKI {
		return CertSourceVaultPKI
	}
	return CertSourceSelfSigned
}

// recordCertRotation records a certificate rotation metric when a recorder is
// configured. It is nil-safe.
func (m *certManager) recordCertRotation(result string) {
	if m.recorder == nil {
		return
	}
	m.recorder.RecordCertRotation(certComponent, m.certSource(), result)
}

// setCertExpiry parses the TLS certificate from the secret data and records the
// seconds until it expires. It is nil-safe and silently ignores parse errors.
func (m *certManager) setCertExpiry(certPEM []byte) {
	if m.recorder == nil || len(certPEM) == 0 {
		return
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return
	}
	m.recorder.SetCertExpirySeconds(certComponent, time.Until(cert.NotAfter).Seconds())
}

// EnsureCertificates ensures webhook TLS certificates exist and are valid.
func (m *certManager) EnsureCertificates(ctx context.Context) ([]byte, error) {
	m.logger.Info("ensuring webhook certificates",
		"certSource", m.config.CertSource,
		"secretName", m.config.SecretName,
		"secretNamespace", m.config.SecretNamespace,
	)

	// Check if the secret already exists with valid certificates.
	existing := &corev1.Secret{}
	err := m.client.Get(ctx, types.NamespacedName{
		Name:      m.config.SecretName,
		Namespace: m.config.SecretNamespace,
	}, existing)

	if err == nil {
		// Secret exists; check if certificates are still valid.
		needsRotation, rotErr := m.checkCertRotation(existing)
		if rotErr != nil {
			m.logger.Warn("failed to check certificate validity, regenerating", "error", rotErr)
		} else if !needsRotation {
			m.logger.Info("existing certificates are valid, no rotation needed")
			// Refresh the expiry gauge from the currently loaded certificate.
			m.setCertExpiry(existing.Data[secretKeyTLSCert])
			return existing.Data[secretKeyCACert], nil
		}
		m.logger.Info("certificates need rotation, regenerating")
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("getting cert secret: %w", err)
	}

	// Generate or issue new certificates.
	caBundle, err := m.generateCertificates(ctx, existing, apierrors.IsNotFound(err))
	if err != nil {
		m.recordCertRotation(resultError)
		return nil, fmt.Errorf("generating certificates: %w", err)
	}

	return caBundle, nil
}

// NeedsRotation checks if certificates need rotation.
func (m *certManager) NeedsRotation(ctx context.Context) (bool, error) {
	existing := &corev1.Secret{}
	err := m.client.Get(ctx, types.NamespacedName{
		Name:      m.config.SecretName,
		Namespace: m.config.SecretNamespace,
	}, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("getting cert secret: %w", err)
	}

	return m.checkCertRotation(existing)
}

// generateCertificates generates new certificates using the configured source.
func (m *certManager) generateCertificates(
	ctx context.Context,
	existing *corev1.Secret,
	isNew bool,
) ([]byte, error) {
	dnsNames := m.dnsNames()
	validity := m.config.CertValidityDuration
	if validity == 0 {
		validity = 365 * 24 * time.Hour // 1 year default
	}

	var caCert, tlsCert, tlsKey []byte
	var err error

	switch m.config.CertSource {
	case CertSourceVaultPKI:
		caCert, tlsCert, tlsKey, err = issueVaultPKICert(ctx, m.vaultClient, m.config, dnsNames, validity)
	case CertSourceSelfSigned, "":
		caCert, tlsCert, tlsKey, err = generateSelfSignedCert(dnsNames, validity)
	default:
		return nil, fmt.Errorf("unsupported cert source: %s", m.config.CertSource)
	}

	if err != nil {
		return nil, err
	}

	// Store certificates in the Kubernetes Secret.
	secretData := map[string][]byte{
		secretKeyCACert:  caCert,
		secretKeyTLSCert: tlsCert,
		secretKeyTLSKey:  tlsKey,
	}

	if isNew {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      m.config.SecretName,
				Namespace: m.config.SecretNamespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": organizationName,
					"app.kubernetes.io/component":  "webhook-certs",
				},
			},
			Type: corev1.SecretTypeTLS,
			Data: secretData,
		}
		if createErr := m.client.Create(ctx, secret); createErr != nil {
			return nil, fmt.Errorf("creating cert secret: %w", createErr)
		}
		m.logger.Info("created webhook certificate secret", "name", m.config.SecretName)
	} else {
		existing.Data = secretData
		if updateErr := m.client.Update(ctx, existing); updateErr != nil {
			return nil, fmt.Errorf("updating cert secret: %w", updateErr)
		}
		m.logger.Info("updated webhook certificate secret", "name", m.config.SecretName)
	}

	// Record successful (re)generation and refresh the expiry gauge.
	m.recordCertRotation(resultSuccess)
	m.setCertExpiry(tlsCert)

	return caCert, nil
}

// checkCertRotation checks if the certificate in the secret needs rotation.
// Returns true if rotation is needed.
func (m *certManager) checkCertRotation(secret *corev1.Secret) (bool, error) {
	certPEM, ok := secret.Data[secretKeyTLSCert]
	if !ok || len(certPEM) == 0 {
		return true, nil
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return true, fmt.Errorf("failed to decode certificate PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return true, fmt.Errorf("parsing certificate: %w", err)
	}

	now := time.Now()
	if now.After(cert.NotAfter) {
		return true, nil
	}

	// Rotate at 2/3 of the certificate lifetime.
	lifetime := cert.NotAfter.Sub(cert.NotBefore)
	rotationTime := cert.NotBefore.Add(
		time.Duration(float64(lifetime) * rotationThresholdFraction),
	)

	return now.After(rotationTime), nil
}

// dnsNames returns the DNS SANs for the webhook server certificate.
func (m *certManager) dnsNames() []string {
	svc := m.config.ServiceName
	ns := m.config.ServiceNamespace
	return []string{
		fmt.Sprintf("%s.%s.svc", svc, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", svc, ns),
	}
}
