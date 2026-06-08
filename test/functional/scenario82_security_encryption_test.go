//go:build functional

package functional

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 82: Security and Encryption (functional)
// ============================================================================
//
// Scenario 82 covers the backup security + encryption surface at the builder
// layer (deterministic, no live cluster). The operator must keep S3 credentials
// out of the rendered ConfigMap, inject them only as env from a Secret, render
// the resolved plugin config to an ephemeral in-pod path at runtime, flip the
// plugin encryption option from the CR, and propagate jobTemplate.imagePullSecrets
// to every backup Job pod spec:
//
//	82a credentials never on disk : BuildBackupS3ConfigMap emits ONLY ${...}
//	    placeholders for the AWS credentials (and tunables) — never literal
//	    credential material — and buildBackupEnv exposes the AWS creds via
//	    ValueFrom.SecretKeyRef (no literal Value).
//	82b ephemeral render          : renderToolScript for an S3 cluster renders
//	    /tmp/s3-config.yaml at runtime via envsubst (with a POSIX heredoc
//	    fallback) reading the read-only /etc/gpbackup template; a local cluster
//	    omits the S3 render entirely.
//	82d encryption flip           : buildS3Env maps s3.Encryption to S3_ENCRYPTION
//	    (on/off, default on) and the ConfigMap template carries the option line
//	    `encryption: ${S3_ENCRYPTION}` so the env-driven flip substitutes.
//	82e imagePullSecrets          : BuildBackupJob/Restore/Validation/Cleanup/
//	    CronJob all carry jobTemplate.imagePullSecrets in the rendered pod spec.
//
// These tests black-box the operator through the public builder (rendered
// ConfigMap / Job pod spec / tool script). They are deterministic and
// self-contained (no live infra). The live RBAC-deny (82c) and the in-pod
// ephemeral-render proof (82b live) are exercised by the e2e live script via
// kubectl auth can-i + a debug pod, since the builder cannot prove an SA's API
// authorization or the kubelet's runtime env injection.
// ============================================================================

const (
	scenario82BackupImage = "cloudberry-backup:2.1.0"
	// scenario82CredSecret is the user-named S3 credential Secret (creds live
	// ONLY here, never in the ConfigMap).
	scenario82CredSecret = "s3-credentials"
	// scenario82RegCred is the docker-registry imagePullSecret name.
	scenario82RegCred = "regcred"
	// scenario82TS is a pinned 14-digit gpbackup-style timestamp.
	scenario82TS = "20260608020000"

	// scenario82SampleLiteralCred is a fake literal credential value used to
	// assert it NEVER appears in the rendered ConfigMap (82a A2).
	scenario82SampleLiteralCred = "AKIAIOSFODNN7EXAMPLE"

	// Markers asserted across the suite.
	scenario82S3ConfigTemplateKey = "s3-plugin-config.yaml.tpl"
	scenario82S3ConfigMount       = "/etc/gpbackup"
	scenario82S3RenderedPath      = "/tmp/s3-config.yaml"
	scenario82BackupSA            = "cloudberry-backup-sa"
)

// Scenario82Suite exercises the security + encryption builder behaviour for the
// S3 backup destination.
type Scenario82Suite struct {
	suite.Suite
}

func TestFunctional_Scenario82(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario82Suite))
}

// scenario82S3BackupSpec returns an S3 (MinIO) destination BackupSpec mirroring
// the scenario82-s3 sample CR (encryption on, user credential Secret).
func scenario82S3BackupSpec(encryption string) *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario82BackupImage,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://host.docker.internal:9000",
				Region:         "us-east-1",
				Folder:         "scenario82",
				Encryption:     encryption,
				ForcePathStyle: true,
				Multipart: &cbv1alpha1.S3Multipart{
					BackupMaxConcurrentRequests: 4,
					BackupMultipartChunksize:    "10MB",
				},
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario82CredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
}

// scenario82LocalBackupSpec returns a LOCAL destination BackupSpec used by the
// 82b regression (the S3 render must be omitted for local).
func scenario82LocalBackupSpec() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario82BackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			Local: &cbv1alpha1.LocalDestination{
				Path:                  "/backups",
				PersistentVolumeClaim: "backup-pvc",
			},
		},
	}
}

// scenario82Cluster builds a Running cluster (pending generation) with the given
// backup spec, mirroring the functional harness used by scenario81.
func scenario82Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// scenario82SetImagePullSecrets configures the cluster's backup jobTemplate with
// the given imagePullSecret names (in order), creating the jobTemplate when nil.
func scenario82SetImagePullSecrets(c *cbv1alpha1.CloudberryCluster, names ...string) {
	if c.Spec.Backup.JobTemplate == nil {
		c.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{}
	}
	secrets := make([]cbv1alpha1.ImagePullSecret, 0, len(names))
	for _, n := range names {
		secrets = append(secrets, cbv1alpha1.ImagePullSecret{Name: n})
	}
	c.Spec.Backup.JobTemplate.ImagePullSecrets = secrets
}

// scenario82PullSecretNames extracts the ordered imagePullSecret names from a pod
// spec for concise assertions.
func scenario82PullSecretNames(podSpec corev1.PodSpec) []string {
	names := make([]string, 0, len(podSpec.ImagePullSecrets))
	for _, ref := range podSpec.ImagePullSecrets {
		names = append(names, ref.Name)
	}
	return names
}

// scenario82CredsFromSecret asserts the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
// env entries reference the named Secret via SecretKeyRef and carry an empty
// literal Value (no plaintext credentials in the pod spec).
func (s *Scenario82Suite) scenario82CredsFromSecret(env []corev1.EnvVar, secretName string) {
	var sawAccess, sawSecret bool
	for _, e := range env {
		switch e.Name {
		case "AWS_ACCESS_KEY_ID":
			sawAccess = true
			require.NotNil(s.T(), e.ValueFrom, "AWS_ACCESS_KEY_ID must use ValueFrom")
			require.NotNil(s.T(), e.ValueFrom.SecretKeyRef, "AWS_ACCESS_KEY_ID must use SecretKeyRef")
			assert.Equal(s.T(), secretName, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), e.Value, "AWS_ACCESS_KEY_ID must NOT carry a literal Value")
		case "AWS_SECRET_ACCESS_KEY":
			sawSecret = true
			require.NotNil(s.T(), e.ValueFrom, "AWS_SECRET_ACCESS_KEY must use ValueFrom")
			require.NotNil(s.T(), e.ValueFrom.SecretKeyRef, "AWS_SECRET_ACCESS_KEY must use SecretKeyRef")
			assert.Equal(s.T(), secretName, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), e.Value, "AWS_SECRET_ACCESS_KEY must NOT carry a literal Value")
		}
	}
	assert.True(s.T(), sawAccess, "env must contain AWS_ACCESS_KEY_ID")
	assert.True(s.T(), sawSecret, "env must contain AWS_SECRET_ACCESS_KEY")
}

// scenario82EnvMap collapses an env slice into a name->value map (ignores
// ValueFrom entries, whose Value is empty by definition).
func scenario82EnvMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

// --- 82a: ConfigMap placeholders-only + creds only via SecretKeyRef env ---

// TestFunctional_Scenario82_S3ConfigMapPlaceholdersOnly asserts the rendered S3
// ConfigMap carries ONLY ${...} placeholders for the AWS credentials (and the
// other tunables) and never any literal credential material.
func (s *Scenario82Suite) TestFunctional_Scenario82_S3ConfigMapPlaceholdersOnly() {
	cluster := scenario82Cluster("s82-cm", scenario82S3BackupSpec("on"))
	cm := builder.NewBuilder().BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm, "S3 destination must render an S3 ConfigMap")

	tpl := cm.Data[scenario82S3ConfigTemplateKey]
	require.NotEmpty(s.T(), tpl)

	// A1: every credential and tunable enters ONLY as a ${...} placeholder.
	for _, placeholder := range []string{
		"${AWS_ACCESS_KEY_ID}",
		"${AWS_SECRET_ACCESS_KEY}",
		"${S3_REGION}",
		"${S3_ENDPOINT}",
		"${S3_BUCKET}",
		"${S3_FOLDER}",
		"${S3_ENCRYPTION}",
	} {
		assert.Contains(s.T(), tpl, placeholder,
			"template must reference %s as an envsubst placeholder", placeholder)
	}
	assert.Contains(s.T(), tpl, "aws_access_key_id: ${AWS_ACCESS_KEY_ID}")
	assert.Contains(s.T(), tpl, "aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}")

	// A2: no literal secret material — neither the configured credential Secret
	// name's resolved value nor any sample literal key.
	assert.NotContains(s.T(), tpl, scenario82SampleLiteralCred,
		"template must NOT embed any literal credential value")
	assert.NotContains(s.T(), tpl, "minioadmin",
		"template must NOT embed the literal MinIO credential value")
}

// TestFunctional_Scenario82_BackupEnvCredentialsViaSecretKeyRef asserts the AWS
// credential env entries on the backup Job are sourced via SecretKeyRef from the
// user-named Secret (never a literal Value).
func (s *Scenario82Suite) TestFunctional_Scenario82_BackupEnvCredentialsViaSecretKeyRef() {
	cluster := scenario82Cluster("s82-env", scenario82S3BackupSpec("on"))
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario82TS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)

	env := job.Spec.Template.Spec.Containers[0].Env
	s.scenario82CredsFromSecret(env, scenario82CredSecret)

	// The rendered pod spec must never carry a literal credential value.
	for _, e := range env {
		assert.NotContains(s.T(), e.Value, "minioadmin",
			"no env entry may carry a literal credential value")
	}
}

// --- 82b: ephemeral render to /tmp/s3-config.yaml at runtime ---

// TestFunctional_Scenario82_BackupScriptRendersEphemeralS3Config asserts the
// rendered S3 backup tool script renders the resolved plugin config to the
// EPHEMERAL /tmp/s3-config.yaml at runtime (envsubst with the eval/heredoc
// fallback) reading the read-only /etc/gpbackup template.
func (s *Scenario82Suite) TestFunctional_Scenario82_BackupScriptRendersEphemeralS3Config() {
	cluster := scenario82Cluster("s82-render", scenario82S3BackupSpec("on"))
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario82TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.True(s.T(), strings.HasPrefix(script, "set -euo pipefail"),
		"backup script must start with set -euo pipefail")
	assert.Contains(s.T(), script, scenario82S3ConfigMount+"/"+scenario82S3ConfigTemplateKey,
		"script must read the read-only /etc/gpbackup template")
	assert.Contains(s.T(), script, "> "+scenario82S3RenderedPath,
		"script must write the rendered config to the ephemeral /tmp/s3-config.yaml")
	// Primary path: envsubst < template > /tmp/s3-config.yaml.
	assert.Contains(s.T(), script,
		"envsubst < "+scenario82S3ConfigMount+"/"+scenario82S3ConfigTemplateKey+" > "+scenario82S3RenderedPath)
	// POSIX fallback heredoc still writes the same ephemeral path.
	assert.Contains(s.T(), script, "_ENVSUBST_EOF_")
}

// TestFunctional_Scenario82_LocalScriptOmitsS3Render asserts a LOCAL-destination
// cluster omits the S3 ephemeral render entirely (regression guard from
// scenario81): no /etc/gpbackup read, no envsubst, no /tmp/s3-config.yaml.
func (s *Scenario82Suite) TestFunctional_Scenario82_LocalScriptOmitsS3Render() {
	cluster := scenario82Cluster("s82-render-local", scenario82LocalBackupSpec())
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario82TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.NotContains(s.T(), script, "> "+scenario82S3RenderedPath,
		"local script must NOT render /tmp/s3-config.yaml")
	assert.NotContains(s.T(), script, "envsubst <")
	assert.NotContains(s.T(), script, scenario82S3ConfigMount)
}

// --- 82d: encryption flip via S3_ENCRYPTION env + ConfigMap option line ---

// TestFunctional_Scenario82_EncryptionFlip asserts buildS3Env maps s3.Encryption
// to the S3_ENCRYPTION env var: "on" -> on, "off" -> off, unset -> on, and that
// the flip is visible end-to-end on the rendered backup Job pod env.
func (s *Scenario82Suite) TestFunctional_Scenario82_EncryptionFlip() {
	cases := []struct {
		name       string
		encryption string
		want       string
	}{
		{name: "explicit on", encryption: "on", want: "on"},
		{name: "explicit off flips option", encryption: "off", want: "off"},
		{name: "empty defaults to on", encryption: "", want: "on"},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			cluster := scenario82Cluster("s82-enc", scenario82S3BackupSpec(tc.encryption))
			job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
				Timestamp: scenario82TS,
				Databases: []string{"mydb"},
			})
			require.NotNil(s.T(), job)
			env := job.Spec.Template.Spec.Containers[0].Env
			assert.Equal(s.T(), tc.want, scenario82EnvMap(env)["S3_ENCRYPTION"],
				"S3_ENCRYPTION must reflect the configured encryption")
		})
	}
}

// TestFunctional_Scenario82_EncryptionConfigMapLine asserts the rendered ConfigMap
// template carries the env-driven `encryption: ${S3_ENCRYPTION}` option line so
// the flip substitutes into the rendered plugin config.
func (s *Scenario82Suite) TestFunctional_Scenario82_EncryptionConfigMapLine() {
	cluster := scenario82Cluster("s82-enc-cm", scenario82S3BackupSpec("on"))
	cm := builder.NewBuilder().BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm)
	assert.Contains(s.T(), cm.Data[scenario82S3ConfigTemplateKey], "encryption: ${S3_ENCRYPTION}",
		"template encryption option must be env-driven (plugin SSL option set, not literal HTTPS)")
}

// --- 82e: imagePullSecrets propagate to every Job pod spec ---

// TestFunctional_Scenario82_ImagePullSecretsAllBuilders asserts a jobTemplate with
// imagePullSecrets flows into the pod spec of EVERY Job the builder produces
// (backup / restore / post-restore-validate / cleanup / CronJob), covering single,
// multiple-in-order and empty cases and the LocalObjectReference conversion.
func (s *Scenario82Suite) TestFunctional_Scenario82_ImagePullSecretsAllBuilders() {
	b := builder.NewBuilder()

	builders := []struct {
		name    string
		podSpec func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec
	}{
		{
			name: "BuildBackupJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildBackupJob(c, &builder.BackupJobOptions{Timestamp: scenario82TS, Databases: []string{"mydb"}})
				require.NotNil(s.T(), job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildRestoreJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildRestoreJob(c, &builder.RestoreJobOptions{Timestamp: scenario82TS, Databases: []string{"mydb"}})
				require.NotNil(s.T(), job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildPostRestoreValidationJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildPostRestoreValidationJob(c, &builder.ValidationJobOptions{Timestamp: scenario82TS, Database: "mydb"})
				require.NotNil(s.T(), job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildRetentionCleanupJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildRetentionCleanupJob(c, scenario82TS)
				require.NotNil(s.T(), job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildBackupCronJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				c.Spec.Backup.Schedule = "0 2 * * *"
				cron := b.BuildBackupCronJob(c)
				require.NotNil(s.T(), cron)
				return cron.Spec.JobTemplate.Spec.Template.Spec
			},
		},
	}

	cases := []struct {
		name      string
		configure func(c *cbv1alpha1.CloudberryCluster)
		want      []string
	}{
		{
			name:      "single secret propagates",
			configure: func(c *cbv1alpha1.CloudberryCluster) { scenario82SetImagePullSecrets(c, scenario82RegCred) },
			want:      []string{scenario82RegCred},
		},
		{
			name:      "multiple secrets preserve order",
			configure: func(c *cbv1alpha1.CloudberryCluster) { scenario82SetImagePullSecrets(c, "reg-a", "reg-b", "reg-c") },
			want:      []string{"reg-a", "reg-b", "reg-c"},
		},
		{
			name:      "empty list yields no imagePullSecrets",
			configure: func(c *cbv1alpha1.CloudberryCluster) { scenario82SetImagePullSecrets(c) },
			want:      nil,
		},
		{
			name:      "nil jobTemplate yields no imagePullSecrets",
			configure: func(c *cbv1alpha1.CloudberryCluster) { c.Spec.Backup.JobTemplate = nil },
			want:      nil,
		},
	}

	for _, bld := range builders {
		for _, tc := range cases {
			s.Run(bld.name+"/"+tc.name, func() {
				cluster := scenario82Cluster("s82-ips", scenario82S3BackupSpec("on"))
				tc.configure(cluster)

				podSpec := bld.podSpec(cluster)

				if len(tc.want) == 0 {
					assert.Empty(s.T(), podSpec.ImagePullSecrets,
						"pod spec must carry no imagePullSecrets")
					return
				}
				// The conversion ImagePullSecret -> LocalObjectReference must
				// preserve names and order.
				assert.Equal(s.T(), tc.want, scenario82PullSecretNames(podSpec),
					"imagePullSecrets must match (in order)")
				for _, name := range tc.want {
					assert.Contains(s.T(), podSpec.ImagePullSecrets,
						corev1.LocalObjectReference{Name: name})
				}
				// The default backup SA co-exists with imagePullSecrets (82c/82e).
				assert.Equal(s.T(), scenario82BackupSA, podSpec.ServiceAccountName,
					"backup Job pods must run as the dedicated backup SA")
			})
		}
	}
}

// TestFunctional_Scenario82_ImagePullSecretsLocalDestination asserts the
// imagePullSecrets wiring is destination-agnostic: a LOCAL-destination backup Job
// (no S3 ConfigMap) still carries the configured imagePullSecrets.
func (s *Scenario82Suite) TestFunctional_Scenario82_ImagePullSecretsLocalDestination() {
	cluster := scenario82Cluster("s82-ips-local", scenario82LocalBackupSpec())
	scenario82SetImagePullSecrets(cluster, scenario82RegCred)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario82TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), []string{scenario82RegCred},
		scenario82PullSecretNames(job.Spec.Template.Spec))
}
