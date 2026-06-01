package db

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// defaultDatabase is the default database name for admin connections.
	defaultDatabase = "postgres"
	// passwordSecretKey is the key in the admin password secret.
	passwordSecretKey = "password"
	// caCertSecretKey is the key holding the PEM-encoded CA certificate in the
	// SSL cert Secret. It mirrors the standard kubernetes.io/tls layout and the
	// key consumed by the coordinator pod (see internal/builder).
	caCertSecretKey = "ca.crt"

	// sslModeDisable disables TLS entirely.
	sslModeDisable = "disable"
	// sslModeRequire negotiates TLS without validating the server certificate.
	sslModeRequire = "require"
	// sslModeVerifyCA validates the server certificate chain against a trusted
	// CA but does NOT verify that the connection hostname matches a certificate
	// SAN. This is required when dialing the coordinator headless service
	// (<cluster>-coord-hl.<ns>.svc), whose name is intentionally absent from the
	// serving certificate SANs (which cover <cluster>-coordinator / <cluster>).
	// verify-ca still protects against MITM because the chain must validate
	// against the cluster CA (for example, Vault Root CA).
	sslModeVerifyCA = "verify-ca"
)

// DBClientFactory defines the interface for creating database clients for clusters.
// This interface is shared across the api and controller packages to avoid duplication.
type DBClientFactory interface {
	// NewClient creates a new database client for the given cluster.
	NewClient(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) (Client, error)
}

// ClientFactory creates database clients from cluster connection information.
// It reads the coordinator service name, port, and admin credentials from the
// cluster spec and the associated Kubernetes Secret.
type ClientFactory struct {
	k8sClient client.Client
	logger    *slog.Logger
	// recorder is an optional metrics recorder propagated to created clients so
	// that query-history metrics can be recorded. It may be nil (nil-safe).
	recorder metrics.Recorder
}

// NewClientFactory creates a new ClientFactory.
// An optional metrics recorder may be supplied; it is propagated to every client
// created by the factory so that query-history metrics can be recorded.
func NewClientFactory(
	k8sClient client.Client,
	logger *slog.Logger,
	recorder ...metrics.Recorder,
) *ClientFactory {
	if logger == nil {
		logger = slog.Default()
	}
	f := &ClientFactory{
		k8sClient: k8sClient,
		logger:    logger.With("component", "db-client-factory"),
	}
	if len(recorder) > 0 {
		f.recorder = recorder[0]
	}
	return f
}

// NewClient creates a new database client for the given cluster.
// It resolves the coordinator service endpoint and reads the admin password
// from the cluster's admin password Secret.
func (f *ClientFactory) NewClient(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) (Client, error) {
	if cluster == nil {
		return nil, fmt.Errorf("cluster must not be nil")
	}

	// Resolve coordinator service host.
	host := fmt.Sprintf(
		"%s.%s.svc",
		util.CoordinatorServiceName(cluster.Name),
		cluster.Namespace,
	)

	// Resolve coordinator port.
	port := cluster.Spec.Coordinator.Port
	if port == 0 {
		port = int32(util.DefaultCoordinatorPort)
	}

	// Read admin password from Secret.
	password, err := f.readAdminPassword(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("reading admin password for cluster %s/%s: %w",
			cluster.Namespace, cluster.Name, err)
	}

	// Determine admin username.
	username := util.DefaultAdminUser
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.Basic != nil &&
		cluster.Spec.Auth.Basic.AdminUser != "" {
		username = cluster.Spec.Auth.Basic.AdminUser
	}

	// Determine SSL mode from the cluster's auth configuration.
	sslMode := resolveSSLMode(cluster)

	// When verifying the certificate chain (verify-ca), load the cluster CA
	// from the SSL cert Secret so pgx can validate the Vault-issued serving
	// certificate against the private CA instead of the system trust store.
	var rootCA []byte
	if sslMode == sslModeVerifyCA {
		rootCA, err = f.readSSLRootCA(ctx, cluster)
		if err != nil {
			return nil, fmt.Errorf("reading SSL root CA for cluster %s/%s: %w",
				cluster.Namespace, cluster.Name, err)
		}
	}

	cfg := Config{
		Host:      host,
		Port:      port,
		Database:  defaultDatabase,
		Username:  username,
		Password:  password,
		SSLMode:   sslMode,
		SSLRootCA: rootCA,
		RetryOpts: util.RetryOptions{
			MaxRetries:     3,
			InitialBackoff: util.DefaultRetryOptions().InitialBackoff,
			MaxBackoff:     util.DefaultRetryOptions().MaxBackoff,
			Multiplier:     util.DefaultRetryOptions().Multiplier,
			JitterFraction: util.DefaultRetryOptions().JitterFraction,
		},
	}

	f.logger.Info("creating database client",
		"cluster", cluster.Name,
		"namespace", cluster.Namespace,
		"host", host,
		"port", port,
		"username", username,
	)

	dbClient, err := NewClient(ctx, cfg, f.logger)
	if err != nil {
		return nil, err
	}

	// Propagate the optional metrics recorder (with cluster/namespace labels) so
	// that query-history metrics are recorded by the created client.
	if f.recorder != nil {
		if pc, ok := dbClient.(*pgxClient); ok {
			pc.SetRecorder(f.recorder, cluster.Name, cluster.Namespace)
		}
	}

	return dbClient, nil
}

// readAdminPassword reads the admin password from the cluster's admin password Secret.
func (f *ClientFactory) readAdminPassword(ctx context.Context, cluster *cbv1alpha1.CloudberryCluster) (string, error) {
	secretName := util.AdminPasswordSecretName(cluster.Name)

	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}

	if err := f.k8sClient.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("getting secret %s/%s: %w", cluster.Namespace, secretName, err)
	}

	passwordBytes, ok := secret.Data[passwordSecretKey]
	if !ok {
		return "", fmt.Errorf("secret %s/%s does not contain key %q",
			cluster.Namespace, secretName, passwordSecretKey)
	}

	return string(passwordBytes), nil
}

// resolveSSLMode determines the libpq SSL mode for connecting to the cluster
// coordinator based on the cluster's auth configuration:
//
//   - SSL disabled (or unset)            => "disable"
//   - SSL enabled, no CertSecret         => "require"
//   - SSL enabled, with CertSecret       => "verify-ca"
//
// verify-ca (rather than verify-full) is used with a CertSecret because the
// factory dials the coordinator headless service (<cluster>-coord-hl.<ns>.svc),
// whose name is not present in the serving certificate SANs. verify-ca still
// validates the certificate chain against the cluster CA (protecting against
// MITM) but skips hostname verification, which is the standard approach for an
// internal headless service whose name differs from the certificate CN/SANs.
func resolveSSLMode(cluster *cbv1alpha1.CloudberryCluster) string {
	if cluster.Spec.Auth == nil || cluster.Spec.Auth.SSL == nil ||
		!cluster.Spec.Auth.SSL.Enabled {
		return sslModeDisable
	}
	if cluster.Spec.Auth.SSL.CertSecret == nil {
		return sslModeRequire
	}
	return sslModeVerifyCA
}

// readSSLRootCA reads the PEM-encoded CA certificate (ca.crt) from the cluster's
// SSL cert Secret. The CA is used to validate the coordinator's serving
// certificate chain when connecting with verify-ca. If the Secret does not carry
// a ca.crt entry, it returns nil without error so that verify-ca falls back to
// the host's system trust store (which is correct when the serving certificate
// is signed by a publicly trusted CA).
func (f *ClientFactory) readSSLRootCA(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) ([]byte, error) {
	secretName := cluster.Spec.Auth.SSL.CertSecret.Name

	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}

	if err := f.k8sClient.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("getting cert secret %s/%s: %w",
			cluster.Namespace, secretName, err)
	}

	caBytes, ok := secret.Data[caCertSecretKey]
	if !ok || len(caBytes) == 0 {
		// No CA bundle in the cert Secret; fall back to system roots.
		return nil, nil
	}

	return caBytes, nil
}
