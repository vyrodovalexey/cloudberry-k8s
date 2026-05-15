package db

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// defaultDatabase is the default database name for admin connections.
	defaultDatabase = "postgres"
	// passwordSecretKey is the key in the admin password secret.
	passwordSecretKey = "password"
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
}

// NewClientFactory creates a new ClientFactory.
func NewClientFactory(k8sClient client.Client, logger *slog.Logger) *ClientFactory {
	if logger == nil {
		logger = slog.Default()
	}
	return &ClientFactory{
		k8sClient: k8sClient,
		logger:    logger.With("component", "db-client-factory"),
	}
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

	cfg := Config{
		Host:     host,
		Port:     port,
		Database: defaultDatabase,
		Username: username,
		Password: password,
		SSLMode:  "disable",
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

	return NewClient(ctx, cfg, f.logger)
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
