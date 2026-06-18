package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// kafkaConnectorCluster returns a PXF-sidecar-enabled cluster whose custom
// connectors are exactly the given list, plus an S3 backup destination so the
// connector-init env (reused S3 credentials) is wired (Scenario 102 §6.2). The
// base PXF spec is the canonical newPXFTestCluster shape; the connectors are
// REPLACED with the test-supplied list.
func kafkaConnectorCluster(connectors []cbv1alpha1.PxfCustomConnector) *cbv1alpha1.CloudberryCluster {
	cluster := newPXFTestCluster()
	cluster.Spec.DataLoading.Pxf.CustomConnectors = connectors
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   "cloudberry-data",
				Endpoint: "https://minio.example.com",
				Region:   "us-east-1",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// TestBuildPXFConnectorInitContainers_S3 asserts the connector-download init
// container shape for an s3:// jarUrl (U4 / SC102-C18-*): a single
// "pxf-connector-init" container that mounts pxf-lib at /pxf/lib/custom and runs
// an `aws s3 cp` of the staged JAR followed by a non-empty `test -s` assertion,
// with the S3 credentials env (SecretKeyRef on backup-s3-credentials) + the MinIO
// endpoint env.
func TestBuildPXFConnectorInitContainers_S3(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "kafka-connector", JarURL: "s3://cloudberry-data/connectors/kafka-connector.jar"},
	})

	inits := b.BuildPXFConnectorInitContainers(cluster)
	require.Len(t, inits, 1, "exactly one connector-init container")
	c := inits[0]

	// Container identity mirrors the credential init container.
	assert.Equal(t, pxfConnectorInitContainerName, c.Name)
	assert.Equal(t, "pxf-connector-init", c.Name)
	assert.Equal(t, cluster.Spec.DataLoading.Pxf.Image, c.Image)
	assert.Equal(t, []string{shellCommand, shellFlag}, c.Command)
	assert.Equal(t, []string{"/bin/bash", "-c"}, c.Command)
	require.Len(t, c.Args, 1)

	// Mount: the shared pxf-lib emptyDir at /pxf/lib/custom (visible to sidecar).
	require.Len(t, c.VolumeMounts, 1)
	assert.Equal(t, pxfLibVolumeName, c.VolumeMounts[0].Name)
	assert.Equal(t, "pxf-lib", c.VolumeMounts[0].Name)
	assert.Equal(t, pxfLibMountPath, c.VolumeMounts[0].MountPath)
	assert.Equal(t, "/pxf/lib/custom", c.VolumeMounts[0].MountPath)

	// Download script: s3:// → aws s3 cp into /pxf/lib/custom/<name>.jar, then a
	// non-empty assertion.
	script := c.Args[0]
	assert.Contains(t, script, "set -euo pipefail")
	assert.Contains(t, script, "mkdir -p '/pxf/lib/custom'")
	assert.Contains(t, script,
		"aws --endpoint-url \"$AWS_S3_ENDPOINT\" s3 cp "+
			"'s3://cloudberry-data/connectors/kafka-connector.jar' "+
			"'/pxf/lib/custom/kafka-connector.jar'")
	assert.Contains(t, script, "test -s '/pxf/lib/custom/kafka-connector.jar'")
	// An unsupported scheme aborts the init.
	assert.Contains(t, script, "exit 1")

	// Env: the S3 credentials (SecretKeyRef on backup-s3-credentials) + endpoint.
	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	require.Contains(t, env, "AWS_ACCESS_KEY_ID")
	require.NotNil(t, env["AWS_ACCESS_KEY_ID"].ValueFrom)
	require.NotNil(t, env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef)
	assert.Equal(t, "backup-s3-credentials",
		env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, "aws_access_key_id",
		env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef.Key)
	assert.Empty(t, env["AWS_ACCESS_KEY_ID"].Value,
		"AWS_ACCESS_KEY_ID must be a SecretKeyRef, never a plaintext value")

	require.Contains(t, env, "AWS_SECRET_ACCESS_KEY")
	require.NotNil(t, env["AWS_SECRET_ACCESS_KEY"].ValueFrom)

	require.Contains(t, env, "AWS_S3_ENDPOINT")
	assert.Equal(t, "https://minio.example.com", env["AWS_S3_ENDPOINT"].Value)
	require.Contains(t, env, "AWS_DEFAULT_REGION")
	assert.Equal(t, "us-east-1", env["AWS_DEFAULT_REGION"].Value)
}

// TestBuildPXFConnectorInitContainers_HTTP asserts the http(s):// branch: the
// script fetches the JAR with curl into /pxf/lib/custom/<name>.jar.
func TestBuildPXFConnectorInitContainers_HTTP(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "http-connector", JarURL: "https://repo.example.com/http-connector.jar"},
	})

	inits := b.BuildPXFConnectorInitContainers(cluster)
	require.Len(t, inits, 1)
	script := inits[0].Args[0]

	assert.Contains(t, script,
		"curl -fsSL 'https://repo.example.com/http-connector.jar' "+
			"-o '/pxf/lib/custom/http-connector.jar'")
	assert.Contains(t, script, "test -s '/pxf/lib/custom/http-connector.jar'")
}

// TestBuildPXFConnectorInitContainers_NoConnectors asserts the gating: no custom
// connectors → no connector-init container (empty slice), even with the sidecar
// enabled.
func TestBuildPXFConnectorInitContainers_NoConnectors(t *testing.T) {
	b := NewBuilder()

	t.Run("nil connectors", func(t *testing.T) {
		cluster := kafkaConnectorCluster(nil)
		assert.Empty(t, b.BuildPXFConnectorInitContainers(cluster))
	})
	t.Run("empty connectors slice", func(t *testing.T) {
		cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{})
		assert.Empty(t, b.BuildPXFConnectorInitContainers(cluster))
	})
}

// TestBuildPXFConnectorInitContainers_SidecarDisabled asserts the sidecar gate:
// when the PXF sidecar is not enabled the connector-init is empty regardless of
// the connector list.
func TestBuildPXFConnectorInitContainers_SidecarDisabled(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "kafka-connector", JarURL: "s3://cloudberry-data/connectors/kafka-connector.jar"},
	})
	cluster.Spec.DataLoading.Pxf.Enabled = false

	assert.Empty(t, b.BuildPXFConnectorInitContainers(cluster))
}

// TestBuildPXFConnectorInitContainers_NoBackupS3 asserts the env path when the
// cluster has NO S3 backup destination: http(s):// connectors need no
// credentials, so the connector-init container still exists with its download
// command but carries no S3 credential env.
func TestBuildPXFConnectorInitContainers_NoBackupS3(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "http-connector", JarURL: "https://repo.example.com/http-connector.jar"},
	})
	cluster.Spec.Backup = nil

	inits := b.BuildPXFConnectorInitContainers(cluster)
	require.Len(t, inits, 1)
	assert.Empty(t, inits[0].Env, "no S3 backup destination => no credential env")
	assert.Contains(t, inits[0].Args[0],
		"curl -fsSL 'https://repo.example.com/http-connector.jar'")
}

// TestBuildPXFConnectorInitContainers_S3NilDestination asserts the defensive
// branch: backup type=s3 but a nil S3 destination => no credential env (the
// container still exists for the download command).
func TestBuildPXFConnectorInitContainers_S3NilDestination(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "kafka-connector", JarURL: "s3://cloudberry-data/connectors/kafka-connector.jar"},
	})
	cluster.Spec.Backup.Destination.S3 = nil

	inits := b.BuildPXFConnectorInitContainers(cluster)
	require.Len(t, inits, 1)
	assert.Empty(t, inits[0].Env, "type=s3 with nil S3 destination => no credential env")
}

// TestBuildPXFConnectorInitContainers_DefaultRegion asserts the AWS_DEFAULT_REGION
// default branch: an S3 destination with an empty Region falls back to the
// SigV4 default signing region.
func TestBuildPXFConnectorInitContainers_DefaultRegion(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "kafka-connector", JarURL: "s3://cloudberry-data/connectors/kafka-connector.jar"},
	})
	cluster.Spec.Backup.Destination.S3.Region = ""

	inits := b.BuildPXFConnectorInitContainers(cluster)
	require.Len(t, inits, 1)
	env := map[string]corev1.EnvVar{}
	for _, e := range inits[0].Env {
		env[e.Name] = e
	}
	require.Contains(t, env, "AWS_DEFAULT_REGION")
	assert.Equal(t, defaultS3SigningRegion, env["AWS_DEFAULT_REGION"].Value)
}

// TestBuildPXFConnectorInitContainers_MultiSorted asserts multiple connectors
// are rendered deterministically (sorted by Name) for byte-stability.
func TestBuildPXFConnectorInitContainers_MultiSorted(t *testing.T) {
	b := NewBuilder()
	cluster := kafkaConnectorCluster([]cbv1alpha1.PxfCustomConnector{
		{Name: "zeta", JarURL: "https://repo.example.com/zeta.jar"},
		{Name: "alpha", JarURL: "https://repo.example.com/alpha.jar"},
	})

	inits := b.BuildPXFConnectorInitContainers(cluster)
	require.Len(t, inits, 1)
	script := inits[0].Args[0]

	alphaIdx := indexOf(script, "/pxf/lib/custom/alpha.jar")
	zetaIdx := indexOf(script, "/pxf/lib/custom/zeta.jar")
	require.GreaterOrEqual(t, alphaIdx, 0)
	require.GreaterOrEqual(t, zetaIdx, 0)
	assert.Less(t, alphaIdx, zetaIdx, "connectors must be rendered sorted by name")
}

// indexOf returns the byte index of sub in s, or -1.
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
