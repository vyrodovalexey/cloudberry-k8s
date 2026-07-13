// Package certmanager provides webhook TLS certificate management for the cloudberry operator.
// It supports two strategies: Vault PKI (preferred) and self-signed (fallback).
package certmanager

import (
	"bytes"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

const (
	// CertSourceVaultPKI uses Vault PKI to issue certificates.
	CertSourceVaultPKI = "vault-pki"
	// CertSourceSelfSigned uses self-signed certificates.
	CertSourceSelfSigned = "self-signed"

	// organizationName is the organization name used in generated certificates.
	organizationName = "cloudberry-operator"

	// selfSignedCACommonName is the Common Name of the CA produced by the
	// self-signed generator (see generateSelfSignedCert). It is used as the
	// marker that identifies a leaf certificate as locally self-signed.
	selfSignedCACommonName = organizationName + "-ca"

	// secretKeyCACert is the key for the CA certificate in the Secret.
	secretKeyCACert = "ca.crt"
	// secretKeyCAKey is the key for the CA private key in the Secret
	// (self-signed source only). SECURITY rationale for persisting it: the
	// same namespace-scoped Secret already holds the server private key
	// (tls.key), so the blast radius of a Secret compromise is unchanged;
	// persisting the CA key enables leaf-only renewals that keep ca.crt —
	// and therefore the injected webhook CA bundle — stable across rotations
	// (H-1 hardening). Legacy Secrets without this key keep working via the
	// full-regeneration fallback.
	secretKeyCAKey = "ca.key"
	// secretKeyTLSCert is the key for the TLS certificate in the Secret.
	secretKeyTLSCert = "tls.crt"
	// secretKeyTLSKey is the key for the TLS private key in the Secret.
	secretKeyTLSKey = "tls.key"

	// rotationThresholdFraction is the fraction of certificate lifetime at which rotation is triggered.
	// Certificates are rotated when 2/3 of their lifetime has elapsed.
	rotationThresholdFraction = 2.0 / 3.0

	// certComponent is the component label value used for certificate metrics.
	// The result label values are the shared metrics.ResultSuccess /
	// metrics.ResultError constants (L-7).
	certComponent = "webhook"

	// certTracerName is the tracer name used for certificate-management spans.
	certTracerName = "certmanager"
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
func (m *certManager) EnsureCertificates(ctx context.Context) (caBundle []byte, err error) {
	ctx, span := telemetry.StartSpan(ctx, certTracerName, "EnsureCertificates")
	defer span.End()
	// Mark the span on any error path (get/rotation/generation failures) exactly
	// once via the named return error. No-op when telemetry is disabled.
	defer func() { telemetry.SetSpanError(span, err) }()

	m.logger.Info("ensuring webhook certificates",
		"certSource", m.config.CertSource,
		"secretName", m.config.SecretName,
		"secretNamespace", m.config.SecretNamespace,
	)

	// Check if the secret already exists with valid certificates.
	existing := &corev1.Secret{}
	getErr := m.client.Get(ctx, types.NamespacedName{
		Name:      m.config.SecretName,
		Namespace: m.config.SecretNamespace,
	}, existing)

	if getErr == nil {
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
	} else if !apierrors.IsNotFound(getErr) {
		err = fmt.Errorf("getting cert secret: %w", getErr)
		return nil, err
	}

	// Generate or issue new certificates.
	caBundle, err = m.generateCertificates(ctx, existing, apierrors.IsNotFound(getErr))
	if err != nil {
		m.recordCertRotation(metrics.ResultError)
		err = fmt.Errorf("generating certificates: %w", err)
		return nil, err
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

// issuedCerts bundles the PEM materials produced for the webhook cert Secret.
// caKey is empty for the vault-pki source (the issuing CA key never leaves
// Vault) and non-empty for the self-signed source, enabling later leaf-only
// renewals.
type issuedCerts struct {
	caCert  []byte
	caKey   []byte
	tlsCert []byte
	tlsKey  []byte
}

// generateCertificates generates new certificates using the configured source
// and persists them, returning the CA bundle to inject into the webhook
// configurations (a union of the new and the still-valid previous CA when the
// CA changed — see buildCABundle).
func (m *certManager) generateCertificates(
	ctx context.Context,
	existing *corev1.Secret,
	isNew bool,
) ([]byte, error) {
	dnsNames := m.dnsNames()
	validity := m.config.CertValidityDuration
	if validity == 0 {
		validity = DefaultCertValidity
	}

	// Capture the pre-rotation CA BEFORE the Secret data is replaced: it is
	// needed for the union-bundle decision below.
	var previousCA []byte
	if !isNew {
		previousCA = existing.Data[secretKeyCACert]
	}

	certs, err := m.issueCertificates(ctx, existing, isNew, dnsNames, validity)
	if err != nil {
		return nil, err
	}

	// Store certificates in the Kubernetes Secret. ca.key is present only
	// for the self-signed source (see secretKeyCAKey rationale); replacing
	// Data wholesale also drops a stale ca.key on a switch to vault-pki.
	secretData := map[string][]byte{
		secretKeyCACert:  certs.caCert,
		secretKeyTLSCert: certs.tlsCert,
		secretKeyTLSKey:  certs.tlsKey,
	}
	if len(certs.caKey) > 0 {
		secretData[secretKeyCAKey] = certs.caKey
	}

	if writeErr := m.writeCertSecret(ctx, existing, isNew, secretData); writeErr != nil {
		return nil, writeErr
	}

	// Record successful (re)generation and refresh the expiry gauge.
	m.recordCertRotation(metrics.ResultSuccess)
	m.setCertExpiry(certs.tlsCert)

	return m.buildCABundle(certs.caCert, previousCA), nil
}

// issueCertificates produces the certificate materials from the configured
// source. The self-signed source attempts a leaf-only renewal against a CA
// persisted in the existing Secret before falling back to full regeneration.
func (m *certManager) issueCertificates(
	ctx context.Context,
	existing *corev1.Secret,
	isNew bool,
	dnsNames []string,
	validity time.Duration,
) (issuedCerts, error) {
	switch m.config.CertSource {
	case CertSourceVaultPKI:
		caCert, tlsCert, tlsKey, err := issueVaultPKICert(ctx, m.vaultClient, m.config, dnsNames, validity)
		if err != nil {
			return issuedCerts{}, err
		}
		return issuedCerts{caCert: caCert, tlsCert: tlsCert, tlsKey: tlsKey}, nil
	case CertSourceSelfSigned, "":
		return m.issueSelfSigned(existing, isNew, dnsNames, validity)
	default:
		return issuedCerts{}, fmt.Errorf("unsupported cert source: %s", m.config.CertSource)
	}
}

// issueSelfSigned issues self-signed certificate materials: a leaf-only
// renewal from the persisted CA when possible (keeping ca.crt byte-identical,
// H-1 hardening), otherwise a full CA + leaf regeneration. The regeneration
// path starts persisting ca.key for legacy Secrets that lack it.
func (m *certManager) issueSelfSigned(
	existing *corev1.Secret,
	isNew bool,
	dnsNames []string,
	validity time.Duration,
) (issuedCerts, error) {
	if !isNew {
		if certs, ok := m.reuseCAForLeaf(existing, dnsNames, validity); ok {
			return certs, nil
		}
	}
	caCert, caKey, tlsCert, tlsKey, err := generateSelfSignedCert(dnsNames, validity)
	if err != nil {
		return issuedCerts{}, err
	}
	return issuedCerts{caCert: caCert, caKey: caKey, tlsCert: tlsCert, tlsKey: tlsKey}, nil
}

// reuseCAForLeaf attempts a leaf-only renewal against the CA persisted in the
// existing Secret. Reuse requires ALL of: ca.crt and ca.key present, the CA
// parses and is the operator's own self-signed CA, it is not past its own
// 2/3-lifetime rotation threshold, and its remaining validity covers the full
// validity of the new leaf (a leaf must never outlive its issuer). Anything
// else (legacy Secret without ca.key, unparseable or aging CA) reports
// ok=false so the caller regenerates everything.
func (m *certManager) reuseCAForLeaf(
	existing *corev1.Secret,
	dnsNames []string,
	validity time.Duration,
) (issuedCerts, bool) {
	caCertPEM := existing.Data[secretKeyCACert]
	caKeyPEM := existing.Data[secretKeyCAKey]
	if len(caCertPEM) == 0 || len(caKeyPEM) == 0 {
		return issuedCerts{}, false
	}

	caCert, err := parseCertificatePEM(caCertPEM)
	if err != nil {
		m.logger.Warn("persisted CA certificate is not parseable; regenerating CA", "error", err)
		return issuedCerts{}, false
	}
	if !caUsableForReuse(caCert, validity) {
		return issuedCerts{}, false
	}

	tlsCert, tlsKey, err := issueLeafFromCA(caCertPEM, caKeyPEM, dnsNames, validity)
	if err != nil {
		m.logger.Warn("failed to issue leaf from persisted CA; regenerating CA", "error", err)
		return issuedCerts{}, false
	}

	m.logger.Info("reusing persisted self-signed CA for leaf-only renewal",
		"caNotAfter", caCert.NotAfter,
	)
	return issuedCerts{caCert: caCertPEM, caKey: caKeyPEM, tlsCert: tlsCert, tlsKey: tlsKey}, true
}

// caUsableForReuse reports whether the persisted self-signed CA may sign
// another leaf: it must be the operator's own CA, must not be past its own
// 2/3-lifetime rotation threshold, and its remaining validity must cover the
// full leaf validity.
func caUsableForReuse(caCert *x509.Certificate, leafValidity time.Duration) bool {
	// A self-signed CA's issuer equals its subject, so the shared issuer
	// check identifies the operator's own CA here as well.
	if !isSelfSignedIssuer(caCert) {
		return false
	}
	if certPastRotationThreshold(caCert) {
		return false
	}
	return time.Now().Add(leafValidity).Before(caCert.NotAfter)
}

// writeCertSecret persists the certificate Secret (create on first use,
// update on rotation).
func (m *certManager) writeCertSecret(
	ctx context.Context,
	existing *corev1.Secret,
	isNew bool,
	secretData map[string][]byte,
) error {
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
			return fmt.Errorf("creating cert secret: %w", createErr)
		}
		m.logger.Info("created webhook certificate secret", "name", m.config.SecretName)
		return nil
	}

	existing.Data = secretData
	if updateErr := m.client.Update(ctx, existing); updateErr != nil {
		return fmt.Errorf("updating cert secret: %w", updateErr)
	}
	m.logger.Info("updated webhook certificate secret", "name", m.config.SecretName)
	return nil
}

// buildCABundle returns the CA bundle to inject into the webhook
// configurations. When the rotation replaced the CA (any source, including a
// vault-pki issuing-CA change) and the previous CA is still time-valid, the
// bundle is the UNION of the new and old CA: during the kubelet Secret
// propagation window the webhook pod may still serve the OLD leaf while the
// API server already trusts only the freshly injected bundle — including the
// old CA keeps admission working through the cutover race (R-1). The CA-reuse
// path (previous CA byte-identical) returns the single, unchanged CA.
func (m *certManager) buildCABundle(newCA, previousCA []byte) []byte {
	if len(previousCA) == 0 || bytes.Equal(newCA, previousCA) {
		return newCA
	}
	prevCert, err := parseCertificatePEM(previousCA)
	if err != nil || time.Now().After(prevCert.NotAfter) {
		// Unparseable or expired previous CA: nothing valid to union.
		return newCA
	}
	m.logger.Info("CA changed on rotation; returning union CA bundle for the propagation window")
	bundle := make([]byte, 0, len(newCA)+len(previousCA)+1)
	bundle = append(bundle, newCA...)
	if len(newCA) > 0 && newCA[len(newCA)-1] != '\n' {
		// Keep the concatenated PEM blocks parseable (vault-pki issuing CAs
		// are not always newline-terminated).
		bundle = append(bundle, '\n')
	}
	bundle = append(bundle, previousCA...)
	return bundle
}

// parseCertificatePEM decodes the first PEM block of certPEM and parses it as
// an X.509 certificate.
func parseCertificatePEM(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing certificate: %w", err)
	}
	return cert, nil
}

// checkCertRotation checks if the certificate in the secret needs rotation.
// Returns true if rotation is needed.
//
// A missing ca.key is deliberately NOT a rotation trigger: legacy Secrets
// (created before ca.key persistence) stay valid until the leaf hits its
// normal 2/3-lifetime threshold; the regeneration path then starts persisting
// ca.key (DEV-2).
func (m *certManager) checkCertRotation(secret *corev1.Secret) (bool, error) {
	certPEM, ok := secret.Data[secretKeyTLSCert]
	if !ok || len(certPEM) == 0 {
		return true, nil
	}

	cert, err := parseCertificatePEM(certPEM)
	if err != nil {
		return true, err
	}

	// Force rotation when the existing certificate's issuer does not correspond
	// to the configured certificate source. This handles reconfiguration between
	// self-signed and vault-pki sources, where a still-time-valid cert from the
	// old source must be re-issued from the new source immediately.
	if m.certSourceMismatch(cert) {
		m.logger.Info("forcing certificate rotation due to source mismatch",
			"configuredSource", m.certSource(),
			"issuer", cert.Issuer.String(),
		)
		return true, nil
	}

	// Rotate when expired or past 2/3 of the certificate lifetime (shared with
	// NeedsRotationFromPEM, which the cluster controller uses for cluster TLS).
	return certPastRotationThreshold(cert), nil
}

// isSelfSignedIssuer reports whether the certificate was issued by the operator's
// own self-signed CA, identified by the Common Name and Organization that
// generateSelfSignedCert assigns to its CA certificate.
func isSelfSignedIssuer(cert *x509.Certificate) bool {
	if cert.Issuer.CommonName != selfSignedCACommonName {
		return false
	}
	for _, org := range cert.Issuer.Organization {
		if org == organizationName {
			return true
		}
	}
	return false
}

// certSourceMismatch reports whether the issuer of the existing certificate does
// not correspond to the configured certificate source. When the source is
// self-signed, the certificate must be issued by the operator's self-signed CA;
// when the source is vault-pki, the certificate must NOT be issued by that local
// self-signed CA.
func (m *certManager) certSourceMismatch(cert *x509.Certificate) bool {
	selfSigned := isSelfSignedIssuer(cert)
	if m.certSource() == CertSourceVaultPKI {
		return selfSigned
	}
	return !selfSigned
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
