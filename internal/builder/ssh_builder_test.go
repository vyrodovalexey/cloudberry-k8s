package builder

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestGenerateClusterSSHKeyPair verifies that the generated private key parses as
// a valid ed25519 OpenSSH key and that the returned authorized_keys line matches
// the same identity.
func TestGenerateClusterSSHKeyPair(t *testing.T) {
	privPEM, authKey, err := GenerateClusterSSHKeyPair()
	require.NoError(t, err)
	require.NotEmpty(t, privPEM)
	require.NotEmpty(t, authKey)

	// The private key must parse as an OpenSSH-format ed25519 key.
	signer, err := ssh.ParsePrivateKey(privPEM)
	require.NoError(t, err, "private key must be a parseable OpenSSH private key")
	assert.Equal(t, ssh.KeyAlgoED25519, signer.PublicKey().Type())

	// The authorized_keys line must parse and match the private key's public key.
	parsedPub, _, _, _, err := ssh.ParseAuthorizedKey(authKey)
	require.NoError(t, err, "authorized_keys line must be parseable")
	assert.Equal(t, ssh.KeyAlgoED25519, parsedPub.Type())
	assert.True(t,
		bytes.Equal(parsedPub.Marshal(), signer.PublicKey().Marshal()),
		"authorized_keys public key must match the private key identity")
}

// TestGenerateClusterSSHKeyPairUnique verifies that each call yields a fresh,
// distinct keypair (no baked-in constant key shared across clusters).
func TestGenerateClusterSSHKeyPairUnique(t *testing.T) {
	priv1, auth1, err := GenerateClusterSSHKeyPair()
	require.NoError(t, err)
	priv2, auth2, err := GenerateClusterSSHKeyPair()
	require.NoError(t, err)

	assert.False(t, bytes.Equal(priv1, priv2), "private keys must be unique per call")
	assert.False(t, bytes.Equal(auth1, auth2), "authorized_keys must be unique per call")
}

// TestGenerateClusterSSHKeyPairUsablePublicKey is an end-to-end check that the
// derived public key is a real ed25519 public key of the right size.
func TestGenerateClusterSSHKeyPairUsablePublicKey(t *testing.T) {
	_, authKey, err := GenerateClusterSSHKeyPair()
	require.NoError(t, err)

	parsedPub, _, _, _, err := ssh.ParseAuthorizedKey(authKey)
	require.NoError(t, err)

	cryptoPub, ok := parsedPub.(ssh.CryptoPublicKey)
	require.True(t, ok)
	edPub, ok := cryptoPub.CryptoPublicKey().(ed25519.PublicKey)
	require.True(t, ok)
	assert.Len(t, edPub, ed25519.PublicKeySize)
}

// TestBuildClusterSSHSecret verifies the Secret name, the three data keys with
// the canonical field names, the values, and the owner reference.
func TestBuildClusterSSHSecret(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	privPEM := []byte("PRIVATE-KEY-PEM")
	authKey := []byte("ssh-ed25519 AAAA... gpadmin@cluster\n")

	secret := b.BuildClusterSSHSecret(cluster, privPEM, authKey)
	require.NotNil(t, secret)

	assert.Equal(t, util.ClusterSSHSecretName("test-cluster"), secret.Name)
	assert.Equal(t, "test-cluster-ssh-keys", secret.Name)
	assert.Equal(t, "default", secret.Namespace)
	assert.Equal(t, corev1.SecretTypeOpaque, secret.Type)

	require.Len(t, secret.OwnerReferences, 1)
	assert.Equal(t, "test-cluster", secret.OwnerReferences[0].Name)

	// All three canonical data keys must be present with the expected values.
	require.Contains(t, secret.Data, util.SSHPrivateKeyField)
	require.Contains(t, secret.Data, util.SSHPublicKeyField)
	require.Contains(t, secret.Data, util.SSHAuthorizedKeysField)
	assert.Len(t, secret.Data, 3)
	assert.Equal(t, privPEM, secret.Data[util.SSHPrivateKeyField])
	assert.Equal(t, authKey, secret.Data[util.SSHPublicKeyField])
	assert.Equal(t, authKey, secret.Data[util.SSHAuthorizedKeysField])

	// The data keys map to the documented OpenSSH file names.
	assert.Equal(t, "id_ed25519", util.SSHPrivateKeyField)
	assert.Equal(t, "id_ed25519.pub", util.SSHPublicKeyField)
	assert.Equal(t, "authorized_keys", util.SSHAuthorizedKeysField)
}

// TestSSHSecretVolume verifies the volume references the right Secret and uses
// the read-only-friendly 0444 DefaultMode.
func TestSSHSecretVolume(t *testing.T) {
	cluster := newTestCluster()
	vol := sshSecretVolume(cluster)

	assert.Equal(t, "cluster-ssh", vol.Name)
	require.NotNil(t, vol.Secret)
	assert.Equal(t, util.ClusterSSHSecretName("test-cluster"), vol.Secret.SecretName)
	require.NotNil(t, vol.Secret.DefaultMode)
	assert.Equal(t, int32(0o444), *vol.Secret.DefaultMode)
}

// TestSSHSecretVolumeMount verifies the mount path and read-only flag.
func TestSSHSecretVolumeMount(t *testing.T) {
	mount := sshSecretVolumeMount()
	assert.Equal(t, "cluster-ssh", mount.Name)
	assert.Equal(t, "/etc/cloudberry/ssh", mount.MountPath)
	assert.True(t, mount.ReadOnly)
}

// TestAddClusterSSHSecret verifies the helper appends the volume to the pod and
// the mount only to the main cloudberry container.
func TestAddClusterSSHSecret(t *testing.T) {
	cluster := newTestCluster()
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: containerName},
			{Name: "sidecar"},
		},
	}

	addClusterSSHSecret(cluster, podSpec)

	require.True(t, hasVolume(podSpec.Volumes, "cluster-ssh"))

	// The main container gets the mount.
	mainMounts := podSpec.Containers[0].VolumeMounts
	require.True(t, hasMount(mainMounts, "cluster-ssh", "/etc/cloudberry/ssh"))
	// The sidecar (non-main) container does NOT get the mount.
	assert.False(t, hasMount(podSpec.Containers[1].VolumeMounts, "cluster-ssh", "/etc/cloudberry/ssh"))
}

// hasMount reports whether a mount with the given name and path is present.
func hasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}
