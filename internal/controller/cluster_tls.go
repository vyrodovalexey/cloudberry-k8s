package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/certmanager"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/vault"
)

// Cluster TLS auto-issuance (spec.auth.ssl + spec.vault):
//
// When a CloudberryCluster enables BOTH Vault integration (spec.vault.enabled)
// and database SSL (spec.auth.ssl.enabled) and the referenced certSecret does
// not exist, the operator auto-issues a server certificate from the SAME Vault
// PKI mount/role used for the webhook certificates and materializes it as a
// generic (Opaque) Secret with tls.crt, tls.key AND ca.crt — the init-tls
// container requires ca.crt, which a kubernetes.io/tls Secret does not carry.
// Operator-issued certificates are renewed during reconcile once 2/3 of their
// lifetime has elapsed (the same rotation policy as webhook certs). A Secret
// that already exists but was NOT created by the operator (user-provided) is
// never touched.

const (
	// secretKeyCACert/TLSCert/TLSKey are the cluster TLS Secret data keys.
	secretKeyCACert  = "ca.crt"
	secretKeyTLSCert = "tls.crt"
	secretKeyTLSKey  = "tls.key"

	// clusterTLSComponentLabel is the component label value for the
	// operator-managed cluster TLS Secret; it marks the Secret as
	// operator-issued (vs user-provided) so renewal only touches our own.
	clusterTLSComponentLabel = "cluster-tls"

	// clusterTLSSpanName is the OTel span name suffix for the cluster TLS
	// reconciliation ("controller." prefix is added by startControllerSpan).
	clusterTLSSpanName = "clusterTLS"
)

// SetClusterTLS wires the optional Vault client and PKI settings used to
// auto-issue cluster server certificates (spec.auth.ssl) from Vault PKI. The
// mount path and role are the operator-level PKI settings (the same ones used
// for webhook certificates). A nil client disables auto-issuance: the
// reconcile logs a warning and skips when a cluster requests it.
func (r *ClusterReconciler) SetClusterTLS(vaultClient vault.Client, mountPath, role string) {
	r.vaultClient = vaultClient
	r.vaultPKIMountPath = mountPath
	r.vaultPKIRole = role
}

// clusterTLSAutoIssueEnabled reports whether the cluster requests cluster TLS
// auto-issuance: Vault integration enabled, SSL enabled and a certSecret name
// to materialize the certificate into.
func clusterTLSAutoIssueEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	if cluster.Spec.Vault == nil || !cluster.Spec.Vault.Enabled {
		return false
	}
	auth := cluster.Spec.Auth
	return auth != nil && auth.SSL != nil && auth.SSL.Enabled &&
		auth.SSL.CertSecret != nil && auth.SSL.CertSecret.Name != ""
}

// clusterTLSDNSNames returns the DNS SANs for the cluster server certificate.
// The first entry is the Common Name (<cluster>.<namespace>.svc.cluster.local).
// Wildcard SANs cover the per-pod FQDNs behind the headless coordinator,
// standby and segment Services (<pod>.<svc>.<ns>.svc.cluster.local), and plain
// SANs cover the Service names themselves plus the client Service.
func clusterTLSDNSNames(cluster *cbv1alpha1.CloudberryCluster) []string {
	ns := cluster.Namespace
	names := []string{
		fmt.Sprintf("%s.%s.svc.cluster.local", cluster.Name, ns),
	}
	for _, svc := range []string{
		util.CoordinatorServiceName(cluster.Name),
		util.StandbyServiceName(cluster.Name),
		util.SegmentServiceName(cluster.Name),
	} {
		names = append(names,
			fmt.Sprintf("%s.%s.svc", svc, ns),
			fmt.Sprintf("%s.%s.svc.cluster.local", svc, ns),
			fmt.Sprintf("*.%s.%s.svc.cluster.local", svc, ns),
		)
	}
	client := util.ClientServiceName(cluster.Name)
	return append(names,
		fmt.Sprintf("%s.%s.svc", client, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", client, ns),
	)
}

// isOperatorManagedTLSSecret reports whether the Secret was created by this
// operator's cluster TLS auto-issuance (managed-by + component labels). Only
// such Secrets are renewed; anything else is user-provided and left untouched.
func isOperatorManagedTLSSecret(secret *corev1.Secret) bool {
	return secret.Labels[util.LabelManagedBy] == util.LabelManagedByValue &&
		secret.Labels[util.LabelComponent] == clusterTLSComponentLabel
}

// reconcileClusterTLSSecret ensures the cluster server certificate Secret
// exists (auto-issued from Vault PKI when absent) and renews operator-managed
// certificates before expiry. It is a no-op unless the cluster enables both
// Vault and SSL with a named certSecret. User-provided Secrets are never
// modified.
func (r *ClusterReconciler) reconcileClusterTLSSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (err error) {
	if !clusterTLSAutoIssueEnabled(cluster) {
		return nil
	}

	ctx, end := startControllerSpan(ctx, clusterControllerName, clusterTLSSpanName)
	defer func() { end(err) }()

	secretName := cluster.Spec.Auth.SSL.CertSecret.Name

	existing := &corev1.Secret{}
	getErr := r.client.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, existing)

	switch {
	case getErr == nil:
		err = r.renewClusterTLSSecretIfNeeded(ctx, cluster, existing)
	case apierrors.IsNotFound(getErr):
		err = r.issueClusterTLSSecret(ctx, cluster, secretName)
	default:
		err = fmt.Errorf("getting cluster TLS secret %s: %w", secretName, getErr)
	}
	return err
}

// renewClusterTLSSecretIfNeeded renews an OPERATOR-MANAGED cluster TLS Secret
// when its certificate has passed the rotation threshold. User-provided
// Secrets (no operator labels) are left untouched.
func (r *ClusterReconciler) renewClusterTLSSecretIfNeeded(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	existing *corev1.Secret,
) error {
	logger := util.LoggerFromContext(ctx)
	if !isOperatorManagedTLSSecret(existing) {
		// User-provided certificate Secret: never modify it.
		return nil
	}

	needsRotation, rotErr := certmanager.NeedsRotationFromPEM(existing.Data[secretKeyTLSCert])
	if rotErr != nil {
		logger.Warn("failed to check cluster TLS certificate validity, re-issuing",
			"secret", existing.Name, "error", rotErr)
	} else if !needsRotation {
		return nil
	}

	caCert, tlsCert, tlsKey, issueErr := r.issueClusterCertFromVault(ctx, cluster)
	if issueErr != nil {
		return issueErr
	}

	existing.Data = clusterTLSSecretData(caCert, tlsCert, tlsKey)
	if updateErr := r.client.Update(ctx, existing); updateErr != nil {
		r.recordClusterCertIssuance(cluster, reconcileResultError)
		return fmt.Errorf("updating cluster TLS secret %s: %w", existing.Name, updateErr)
	}

	r.recordClusterCertIssuance(cluster, reconcileResultSuccess)
	logger.Info("renewed cluster TLS certificate from Vault PKI", "secret", existing.Name)
	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonClusterTLSRenewed,
		fmt.Sprintf("Cluster server certificate in secret %s renewed from Vault PKI", existing.Name))
	return nil
}

// issueClusterTLSSecret issues a fresh server certificate from Vault PKI and
// creates the cluster TLS Secret (generic/Opaque type so it can carry ca.crt
// alongside tls.crt and tls.key — the init-tls container requires ca.crt).
func (r *ClusterReconciler) issueClusterTLSSecret(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
	secretName string,
) error {
	logger := util.LoggerFromContext(ctx)

	caCert, tlsCert, tlsKey, issueErr := r.issueClusterCertFromVault(ctx, cluster)
	if issueErr != nil {
		return issueErr
	}

	labels := util.CommonLabels(cluster.Name, clusterTLSComponentLabel)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeOpaque,
		Data: clusterTLSSecretData(caCert, tlsCert, tlsKey),
	}
	if refErr := controllerutil.SetControllerReference(cluster, secret, r.scheme); refErr != nil {
		r.recordClusterCertIssuance(cluster, reconcileResultError)
		return fmt.Errorf("setting owner reference on cluster TLS secret %s: %w", secretName, refErr)
	}

	if createErr := r.client.Create(ctx, secret); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			// Lost a race with a concurrent reconcile — the Secret now exists.
			return nil
		}
		r.recordClusterCertIssuance(cluster, reconcileResultError)
		return fmt.Errorf("creating cluster TLS secret %s: %w", secretName, createErr)
	}

	r.recordClusterCertIssuance(cluster, reconcileResultSuccess)
	logger.Info("issued cluster TLS certificate from Vault PKI", "secret", secretName)
	r.recorder.Event(cluster, corev1.EventTypeNormal, cbv1alpha1.EventReasonClusterTLSIssued,
		fmt.Sprintf("Cluster server certificate issued from Vault PKI into secret %s", secretName))
	return nil
}

// issueClusterCertFromVault issues the server certificate from Vault PKI. On
// any failure it records the error metric and emits the Warning Event so
// issuance and renewal failures are observable in one place.
func (r *ClusterReconciler) issueClusterCertFromVault(
	ctx context.Context,
	cluster *cbv1alpha1.CloudberryCluster,
) (caCert, tlsCert, tlsKey []byte, err error) {
	if r.vaultClient == nil || !r.vaultClient.IsEnabled() {
		// The CR requests auto-issuance, but the operator has no Vault client
		// (operator-level Vault integration disabled). Surface this loudly:
		// without the Secret the cluster pods cannot start with SSL.
		err = fmt.Errorf("cluster %s requests TLS auto-issuance (vault.enabled + auth.ssl.enabled) "+
			"but the operator has no enabled Vault client; configure operator Vault settings "+
			"or create the certSecret manually", cluster.Name)
		r.recordClusterCertIssuance(cluster, reconcileResultError)
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonClusterTLSFailed,
			err.Error())
		return nil, nil, nil, err
	}

	caCert, tlsCert, tlsKey, err = certmanager.IssueServerCertificate(
		ctx,
		r.vaultClient,
		r.vaultPKIMountPath,
		r.vaultPKIRole,
		clusterTLSDNSNames(cluster),
		certmanager.DefaultCertValidity,
	)
	if err != nil {
		err = fmt.Errorf("issuing cluster TLS certificate from Vault PKI: %w", err)
		r.recordClusterCertIssuance(cluster, reconcileResultError)
		r.recorder.Event(cluster, corev1.EventTypeWarning, cbv1alpha1.EventReasonClusterTLSFailed,
			err.Error())
		return nil, nil, nil, err
	}
	return caCert, tlsCert, tlsKey, nil
}

// clusterTLSSecretData assembles the Secret data map with the three keys the
// cluster pods mount (tls.crt, tls.key, ca.crt).
func clusterTLSSecretData(caCert, tlsCert, tlsKey []byte) map[string][]byte {
	return map[string][]byte{
		secretKeyCACert:  caCert,
		secretKeyTLSCert: tlsCert,
		secretKeyTLSKey:  tlsKey,
	}
}

// recordClusterCertIssuance records the cluster cert issuance metric nil-safely.
func (r *ClusterReconciler) recordClusterCertIssuance(
	cluster *cbv1alpha1.CloudberryCluster,
	result string,
) {
	if r.metrics == nil {
		return
	}
	r.metrics.RecordClusterCertIssuance(cluster.Name, cluster.Namespace, result)
}
