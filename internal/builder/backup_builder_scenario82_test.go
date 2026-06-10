package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// sampleLiteralSecretValue is a fake literal credential value used to assert the
// rendered S3 ConfigMap template NEVER embeds real credential material (82a). It
// must never appear in any ConfigMap data the builder produces.
const sampleLiteralSecretValue = "AKIAIOSFODNN7EXAMPLE"

// setBackupImagePullSecrets configures the cluster's backup jobTemplate with the
// given image-pull-secret names (in order), creating the jobTemplate when nil.
func setBackupImagePullSecrets(c *cbv1alpha1.CloudberryCluster, names ...string) {
	if c.Spec.Backup.JobTemplate == nil {
		c.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{}
	}
	secrets := make([]cbv1alpha1.ImagePullSecret, 0, len(names))
	for _, n := range names {
		secrets = append(secrets, cbv1alpha1.ImagePullSecret{Name: n})
	}
	c.Spec.Backup.JobTemplate.ImagePullSecrets = secrets
}

// pullSecretNames extracts the ordered list of imagePullSecret names from a pod
// spec for concise assertions.
func pullSecretNames(podSpec corev1.PodSpec) []string {
	names := make([]string, 0, len(podSpec.ImagePullSecrets))
	for _, ref := range podSpec.ImagePullSecrets {
		names = append(names, ref.Name)
	}
	return names
}

// ----------------------------------------------------------------------------
// 82e — imagePullSecrets propagate to every Job pod spec via applyJobTemplatePod
// ----------------------------------------------------------------------------

// TestBuildBackupJobsImagePullSecretsScenario82 (Scenario 82 §E / TC-82e) asserts
// that a jobTemplate with imagePullSecrets flows into the pod spec of EVERY Job
// the builder produces (backup / restore / post-restore-validate / cleanup /
// CronJob), since they all build their pod spec via buildBackupPodSpec ->
// applyJobTemplatePod. It covers single, multiple-in-order and empty cases and
// the corev1.LocalObjectReference conversion.
func TestBuildBackupJobsImagePullSecretsScenario82(t *testing.T) {
	b := NewBuilder()

	// builders maps a human name to a function that renders the Job pod spec for
	// a given cluster, so the same table of imagePullSecrets cases runs against
	// all five builders.
	builders := []struct {
		name    string
		podSpec func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec
	}{
		{
			name: "BuildBackupJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildBackupJob(c, &BackupJobOptions{
					Timestamp: "20260608020000",
					Databases: []string{"mydb"},
				})
				require.NotNil(t, job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildRestoreJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildRestoreJob(c, &RestoreJobOptions{
					Timestamp: "20260608020000",
					Databases: []string{"mydb"},
				})
				require.NotNil(t, job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildPostRestoreValidationJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildPostRestoreValidationJob(c, &ValidationJobOptions{
					Timestamp: "20260608020000",
					Database:  "mydb",
				})
				require.NotNil(t, job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildRetentionCleanupJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				job := b.BuildRetentionCleanupJob(c, "20260608020000")
				require.NotNil(t, job)
				return job.Spec.Template.Spec
			},
		},
		{
			name: "BuildBackupCronJob",
			podSpec: func(c *cbv1alpha1.CloudberryCluster) corev1.PodSpec {
				cron := b.BuildBackupCronJob(c)
				require.NotNil(t, cron)
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
			configure: func(c *cbv1alpha1.CloudberryCluster) { setBackupImagePullSecrets(c, "regcred") },
			want:      []string{"regcred"},
		},
		{
			name:      "multiple secrets preserve order",
			configure: func(c *cbv1alpha1.CloudberryCluster) { setBackupImagePullSecrets(c, "reg-a", "reg-b", "reg-c") },
			want:      []string{"reg-a", "reg-b", "reg-c"},
		},
		{
			name:      "empty list yields no imagePullSecrets",
			configure: func(c *cbv1alpha1.CloudberryCluster) { setBackupImagePullSecrets(c) },
			want:      nil,
		},
		{
			name:      "no jobTemplate yields no imagePullSecrets",
			configure: func(_ *cbv1alpha1.CloudberryCluster) {},
			want:      nil,
		},
		{
			name: "nil jobTemplate (explicit) yields no imagePullSecrets",
			configure: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.JobTemplate = nil
			},
			want: nil,
		},
	}

	for _, bld := range builders {
		for _, tc := range cases {
			t.Run(bld.name+"/"+tc.name, func(t *testing.T) {
				cluster := newBackupCluster()
				tc.configure(cluster)

				podSpec := bld.podSpec(cluster)

				if len(tc.want) == 0 {
					assert.Empty(t, podSpec.ImagePullSecrets,
						"pod spec must carry no imagePullSecrets")
					return
				}

				// The conversion ImagePullSecret -> LocalObjectReference must
				// preserve names and order.
				assert.Equal(t, tc.want, pullSecretNames(podSpec),
					"imagePullSecrets must match (in order)")
				for _, name := range tc.want {
					assert.Contains(t, podSpec.ImagePullSecrets,
						corev1.LocalObjectReference{Name: name})
				}
			})
		}
	}
}

// TestBuildBackupJobImagePullSecretsLocalDestinationScenario82 asserts the
// imagePullSecrets wiring is destination-agnostic: a LOCAL-destination backup Job
// (no S3 ConfigMap) still carries the configured imagePullSecrets.
func TestBuildBackupJobImagePullSecretsLocalDestinationScenario82(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	setBackupImagePullSecrets(cluster, "regcred")

	job := b.BuildBackupJob(cluster, &BackupJobOptions{
		Timestamp: "20260608020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)
	assert.Equal(t, []string{"regcred"}, pullSecretNames(job.Spec.Template.Spec))
}

// TestApplyJobTemplatePodImagePullSecretsScenario82 unit-tests applyJobTemplatePod
// directly for the imagePullSecrets branch, including the no-panic empty/nil
// paths and the default ServiceAccountName co-existence (82c/82e).
func TestApplyJobTemplatePodImagePullSecretsScenario82(t *testing.T) {
	t.Run("multiple secrets appended in order", func(t *testing.T) {
		cluster := newBackupCluster()
		setBackupImagePullSecrets(cluster, "reg-a", "reg-b")
		podSpec := &corev1.PodSpec{}

		applyJobTemplatePod(cluster, podSpec)

		assert.Equal(t, []string{"reg-a", "reg-b"}, pullSecretNames(*podSpec))
		// The default backup SA is still applied alongside imagePullSecrets.
		assert.Equal(t, "cloudberry-backup-sa", podSpec.ServiceAccountName)
	})

	t.Run("empty imagePullSecrets leaves pod spec nil (no panic)", func(t *testing.T) {
		cluster := newBackupCluster()
		setBackupImagePullSecrets(cluster) // empty list
		podSpec := &corev1.PodSpec{}

		assert.NotPanics(t, func() { applyJobTemplatePod(cluster, podSpec) })
		assert.Empty(t, podSpec.ImagePullSecrets)
	})

	t.Run("nil jobTemplate leaves pod spec nil (no panic)", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.JobTemplate = nil
		podSpec := &corev1.PodSpec{}

		assert.NotPanics(t, func() { applyJobTemplatePod(cluster, podSpec) })
		assert.Empty(t, podSpec.ImagePullSecrets)
	})
}

// ----------------------------------------------------------------------------
// 82a — credentials never on disk/ConfigMap; only as env from a Secret
// ----------------------------------------------------------------------------

// TestBuildBackupS3ConfigMapPlaceholdersOnlyScenario82 (Scenario 82 §A / TC-82a)
// asserts the rendered S3 ConfigMap template carries ONLY ${...} placeholders for
// the AWS credentials (and the other tunables) and never any literal credential
// material.
func TestBuildBackupS3ConfigMapPlaceholdersOnlyScenario82(t *testing.T) {
	b := NewBuilder()

	t.Run("s3 template contains only credential placeholders", func(t *testing.T) {
		cluster := newBackupCluster()
		// Even if a (sample) literal credential were configured somewhere, it
		// must never leak into the rendered ConfigMap template.
		cm := b.BuildBackupS3ConfigMap(cluster)
		require.NotNil(t, cm)
		tpl := cm.Data[s3ConfigTemplateKey]

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
			assert.Contains(t, tpl, placeholder,
				"template must reference %s as an envsubst placeholder", placeholder)
		}

		// A2: no literal secret material in the ConfigMap; the credential keys
		// only appear as the `<key>: ${PLACEHOLDER}` lines, never with a value.
		assert.NotContains(t, tpl, sampleLiteralSecretValue,
			"template must NOT embed any literal credential value")
		assert.Contains(t, tpl, "aws_access_key_id: ${AWS_ACCESS_KEY_ID}")
		assert.Contains(t, tpl, "aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}")
	})

	t.Run("local destination returns nil configmap", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		assert.Nil(t, b.BuildBackupS3ConfigMap(cluster),
			"BuildBackupS3ConfigMap must return nil for a local destination")
	})
}

// TestBuildBackupEnvCredentialsViaSecretKeyRefScenario82 (Scenario 82 §A / TC-82a
// A3) asserts the AWS credential env entries are sourced via SecretKeyRef (never
// a literal Value), so credentials are injected from a Secret at runtime.
func TestBuildBackupEnvCredentialsViaSecretKeyRefScenario82(t *testing.T) {
	cluster := newBackupCluster()
	cluster.Spec.Backup.Destination.S3.CredentialSecret = &cbv1alpha1.S3CredentialSecret{
		Name: "s3-credentials",
	}

	assertCredentialsFromSecret(t, buildBackupEnv(cluster), "s3-credentials")
}

// TestBuildS3EnvCredentialsViaSecretKeyRefScenario82 asserts buildS3Env (the
// lower-level S3 env builder) likewise sources AWS creds via SecretKeyRef.
func TestBuildS3EnvCredentialsViaSecretKeyRefScenario82(t *testing.T) {
	cluster := newBackupCluster()
	cluster.Spec.Backup.Destination.S3.CredentialSecret = &cbv1alpha1.S3CredentialSecret{
		Name:           "s3-credentials",
		AccessKeyField: "aws_access_key_id",
		SecretKeyField: "aws_secret_access_key",
	}

	assertCredentialsFromSecret(t,
		buildS3Env(cluster, cluster.Spec.Backup.Destination.S3), "s3-credentials")
}

// assertCredentialsFromSecret asserts the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
// env entries reference the named Secret via SecretKeyRef and carry an empty
// literal Value (no plaintext credentials in the pod spec).
func assertCredentialsFromSecret(t *testing.T, env []corev1.EnvVar, secretName string) {
	t.Helper()
	var sawAccess, sawSecret bool
	for _, e := range env {
		switch e.Name {
		case "AWS_ACCESS_KEY_ID":
			sawAccess = true
			require.NotNil(t, e.ValueFrom, "AWS_ACCESS_KEY_ID must use ValueFrom")
			require.NotNil(t, e.ValueFrom.SecretKeyRef, "AWS_ACCESS_KEY_ID must use SecretKeyRef")
			assert.Equal(t, secretName, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(t, e.Value, "AWS_ACCESS_KEY_ID must NOT carry a literal Value")
		case "AWS_SECRET_ACCESS_KEY":
			sawSecret = true
			require.NotNil(t, e.ValueFrom, "AWS_SECRET_ACCESS_KEY must use ValueFrom")
			require.NotNil(t, e.ValueFrom.SecretKeyRef, "AWS_SECRET_ACCESS_KEY must use SecretKeyRef")
			assert.Equal(t, secretName, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(t, e.Value, "AWS_SECRET_ACCESS_KEY must NOT carry a literal Value")
		}
	}
	assert.True(t, sawAccess, "env must contain AWS_ACCESS_KEY_ID")
	assert.True(t, sawSecret, "env must contain AWS_SECRET_ACCESS_KEY")
}

// ----------------------------------------------------------------------------
// 82b — ephemeral render: /tmp/s3-config.yaml rendered at runtime in the pod
// ----------------------------------------------------------------------------

// TestRenderToolScriptEphemeralS3ConfigScenario82 (Scenario 82 §B / TC-82b)
// asserts the rendered tool script for an S3 destination writes the resolved
// plugin config to the EPHEMERAL /tmp/s3-config.yaml at runtime (via envsubst,
// with the eval/heredoc fallback) reading the read-only /etc/gpbackup template;
// a LOCAL destination omits the S3 render entirely (regression from scenario81).
func TestRenderToolScriptEphemeralS3ConfigScenario82(t *testing.T) {
	t.Run("s3 destination renders to ephemeral /tmp path at runtime", func(t *testing.T) {
		cluster := newBackupCluster()
		args := mustGpbackupArgs(t, cluster, cluster.Spec.Backup.Gpbackup, &BackupJobOptions{
			Databases: []string{"mydb"},
		})
		script := renderToolScript(cluster, "gpbackup", args)

		// The render reads the read-only ConfigMap template at /etc/gpbackup and
		// writes the resolved file to the ephemeral /tmp/s3-config.yaml.
		assert.Equal(t, "/etc/gpbackup", s3ConfigMountPath)
		assert.Equal(t, "/tmp/s3-config.yaml", s3RenderedConfigPath)
		assert.Contains(t, script, s3ConfigMountPath+"/"+s3ConfigTemplateKey,
			"script must read the /etc/gpbackup template")
		assert.Contains(t, script, "> "+s3RenderedConfigPath,
			"script must write the rendered config to /tmp/s3-config.yaml")
		// Primary path: envsubst < template > /tmp/s3-config.yaml.
		assert.Contains(t, script,
			"envsubst < "+s3ConfigMountPath+"/"+s3ConfigTemplateKey+" > "+s3RenderedConfigPath)
		// POSIX fallback: eval/heredoc still writes to the same ephemeral path.
		assert.Contains(t, script, "_ENVSUBST_EOF_")
		assert.Contains(t, script,
			"_ENVSUBST_EOF_\" > "+s3RenderedConfigPath)
	})

	t.Run("local destination omits the s3 ephemeral render", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		args := mustGpbackupArgs(t, cluster, cluster.Spec.Backup.Gpbackup, &BackupJobOptions{
			Databases: []string{"mydb"},
		})
		script := renderToolScript(cluster, "gpbackup", args)

		assert.NotContains(t, script, "> "+s3RenderedConfigPath,
			"local script must NOT render /tmp/s3-config.yaml")
		assert.NotContains(t, script, "envsubst <")
		assert.NotContains(t, script, s3ConfigMountPath)
	})
}

// ----------------------------------------------------------------------------
// 82d — encryption flip via S3_ENCRYPTION env + ConfigMap option line
// ----------------------------------------------------------------------------

// TestBuildS3EnvEncryptionFlipScenario82 (Scenario 82 §D / TC-82d) asserts that
// buildS3Env maps s3.Encryption to the S3_ENCRYPTION env var: "on" -> on,
// "off" -> off, and unset ("") defaults to "on".
func TestBuildS3EnvEncryptionFlipScenario82(t *testing.T) {
	tests := []struct {
		name       string
		encryption string
		want       string
	}{
		{name: "explicit on", encryption: "on", want: "on"},
		{name: "explicit off flips option", encryption: "off", want: "off"},
		{name: "empty defaults to on", encryption: "", want: "on"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newBackupCluster()
			cluster.Spec.Backup.Destination.S3.Encryption = tc.encryption
			env := buildS3Env(cluster, cluster.Spec.Backup.Destination.S3)
			assert.Equal(t, tc.want, envMap(env)["S3_ENCRYPTION"],
				"S3_ENCRYPTION must reflect the configured encryption")
		})
	}
}

// TestBuildBackupEnvEncryptionFlipScenario82 asserts the encryption flip is also
// visible through the higher-level buildBackupEnv path used by the Job pods.
func TestBuildBackupEnvEncryptionFlipScenario82(t *testing.T) {
	for _, tc := range []struct {
		encryption string
		want       string
	}{
		{encryption: "on", want: "on"},
		{encryption: "off", want: "off"},
		{encryption: "", want: "on"},
	} {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.S3.Encryption = tc.encryption
		assert.Equal(t, tc.want, envMap(buildBackupEnv(cluster))["S3_ENCRYPTION"])
	}
}

// TestBuildBackupS3ConfigMapEncryptionLineScenario82 (Scenario 82 §D) asserts the
// rendered ConfigMap template carries the `encryption: ${S3_ENCRYPTION}` option
// line so the env-driven flip substitutes into the rendered plugin config.
func TestBuildBackupS3ConfigMapEncryptionLineScenario82(t *testing.T) {
	b := NewBuilder()
	cm := b.BuildBackupS3ConfigMap(newBackupCluster())
	require.NotNil(t, cm)
	assert.Contains(t, cm.Data[s3ConfigTemplateKey], "encryption: ${S3_ENCRYPTION}",
		"template encryption option must be env-driven")
}

// TestBuildBackupJobEncryptionEnvScenario82 ties 82d end-to-end at the Job level:
// the rendered backup Job pod container env carries the flipped S3_ENCRYPTION.
func TestBuildBackupJobEncryptionEnvScenario82(t *testing.T) {
	b := NewBuilder()
	for _, tc := range []struct {
		encryption string
		want       string
	}{
		{encryption: "off", want: "off"},
		{encryption: "on", want: "on"},
	} {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.S3.Encryption = tc.encryption
		job := b.BuildBackupJob(cluster, &BackupJobOptions{
			Timestamp: "20260608020000",
			Databases: []string{"mydb"},
		})
		require.NotNil(t, job)
		env := job.Spec.Template.Spec.Containers[0].Env
		assert.Equal(t, tc.want, envMap(env)["S3_ENCRYPTION"])
	}
}
