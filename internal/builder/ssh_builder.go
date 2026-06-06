// Package builder: ssh_builder.go constructs the cluster-wide gpadmin SSH
// keypair Secret and the volume/mount wiring that installs it into every
// cluster pod and into the backup/restore Jobs.
//
// gpbackup/gprestore are MPP tools: the coordinator dispatches to EACH segment
// over SSH (port 22) to create per-segment backup directories and run
// gpbackup_helper, even when streaming data to S3 via gpbackup_s3_plugin. That
// requires passwordless SSH between ALL cluster pods using a SHARED identity —
// a per-pod keypair (the historical behavior) leaves the coordinator's key out
// of the segments' authorized_keys, so coordinator->segment SSH fails with
// "Permission denied (publickey)" and gpbackup aborts with "Unable to create
// backup directory ... Connection closed by ... port 22".
//
// The operator therefore generates ONE ed25519 keypair per cluster, stores it
// in the <cluster>-ssh-keys Secret, and mounts it (read-only) into a staging
// path on every pod; the entrypoint installs it into /home/gpadmin/.ssh with
// the strict permissions sshd requires (700 dir, 600 private key,
// 644 public key, 600 authorized_keys).
package builder

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"

	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// sshSecretVolumeName is the volume name for the shared SSH keypair Secret.
	sshSecretVolumeName = "cluster-ssh"
	// sshSecretMountPath is the staging path where the shared SSH keys are
	// mounted (read-only). The entrypoint copies them into /home/gpadmin/.ssh
	// with the strict ownership/permissions sshd requires (a Secret volume is
	// symlinked and 0644-ish, which sshd rejects).
	sshSecretMountPath = "/etc/cloudberry/ssh" //nolint:gosec // mount path, not a credential
	// sshSecretFileMode is the file mode applied to the mounted SSH key files.
	// Secret volumes are owned by root; the cluster pods run as the unprivileged
	// gpadmin user (UID 1000), so the staged copy must be world-readable (0444)
	// for the entrypoint's gpadmin process to read and install it into
	// ~/.ssh. The entrypoint re-secures the installed private key to 600 on copy,
	// so the relaxed mode only applies to the ephemeral staged copy inside the
	// pod's own (single-tenant) filesystem.
	sshSecretFileMode int32 = 0o444
)

// GenerateClusterSSHKeyPair generates a fresh ed25519 keypair for cluster-wide
// passwordless gpadmin SSH. It returns the OpenSSH-format private key, the
// authorized_keys-format public key line and an error. The private key is
// emitted in the OpenSSH PEM format that sshd/ssh expect for an id_ed25519 file.
//
// A unique key is generated per cluster (not a baked-in constant) so secrets are
// never shared across clusters or stored in image layers.
func GenerateClusterSSHKeyPair() (privateKeyPEM, authorizedKey []byte, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating ed25519 key: %w", err)
	}

	pemBlock, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling openssh private key: %w", err)
	}

	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil, nil, fmt.Errorf("building ssh public key: %w", err)
	}

	return pem.EncodeToMemory(pemBlock), ssh.MarshalAuthorizedKey(sshPub), nil
}

// BuildClusterSSHSecret builds the cluster-wide gpadmin SSH keypair Secret from
// a pre-generated private key and authorized_keys line. The caller (the cluster
// controller) generates the keypair once and persists it; the same Secret is
// then mounted into every cluster pod and the backup/restore Jobs so the whole
// cluster shares a single SSH identity.
func (b *DefaultBuilder) BuildClusterSSHSecret(
	cluster *cbv1alpha1.CloudberryCluster,
	privateKeyPEM, authorizedKey []byte,
) *corev1.Secret {
	labels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            util.ClusterSSHSecretName(cluster.Name),
			Namespace:       cluster.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			util.SSHPrivateKeyField:     privateKeyPEM,
			util.SSHPublicKeyField:      authorizedKey,
			util.SSHAuthorizedKeysField: authorizedKey,
		},
	}
}

// sshSecretVolume returns the read-only volume that mounts the shared SSH
// keypair Secret into a pod. It is optional (nil) at the call site when the
// Secret is not yet available.
func sshSecretVolume(cluster *cbv1alpha1.CloudberryCluster) corev1.Volume {
	mode := sshSecretFileMode
	return corev1.Volume{
		Name: sshSecretVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  util.ClusterSSHSecretName(cluster.Name),
				DefaultMode: &mode,
			},
		},
	}
}

// sshSecretVolumeMount returns the read-only mount for the shared SSH keypair
// Secret at the staging path consumed by the entrypoint.
func sshSecretVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      sshSecretVolumeName,
		MountPath: sshSecretMountPath,
		ReadOnly:  true,
	}
}

// addClusterSSHSecret appends the shared SSH keypair Secret volume to the pod
// spec and the corresponding read-only mount to the main container so the
// entrypoint can install the shared gpadmin identity. It is a no-op-safe helper
// used by every cluster StatefulSet (coordinator, standby, segments).
func addClusterSSHSecret(cluster *cbv1alpha1.CloudberryCluster, podSpec *corev1.PodSpec) {
	podSpec.Volumes = append(podSpec.Volumes, sshSecretVolume(cluster))
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == containerName {
			podSpec.Containers[i].VolumeMounts = append(
				podSpec.Containers[i].VolumeMounts, sshSecretVolumeMount())
		}
	}
}
