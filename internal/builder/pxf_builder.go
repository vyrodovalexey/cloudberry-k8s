package builder

import (
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

const (
	// defaultPxfPort is the default PXF service port used when pxf.port is unset.
	defaultPxfPort int32 = 5888
	// defaultPxfJvmOpts is the default JVM options string used when pxf.jvmOpts
	// is unset.
	defaultPxfJvmOpts = "-Xmx1g -Xms256m"
	// defaultPxfLogLevel is the default PXF log level used when pxf.logLevel is
	// unset. This is the CRITICAL fallback: an empty logLevel must always
	// resolve to INFO so the sidecar env is deterministic.
	defaultPxfLogLevel = "INFO"

	// pxfContainerName is the container name of the PXF sidecar.
	pxfContainerName = "pxf"
	// pxfPortName is the container port name of the PXF sidecar.
	pxfPortName = "pxf"

	// pxfHome is the PXF installation home directory inside the sidecar image.
	pxfHome = "/usr/local/cloudberry-pxf"
	// pxfBase is the PXF runtime base directory ($PXF_BASE).
	pxfBase = "/pxf-base"
	// pxfServersMountPath is where the rendered servers ConfigMap is mounted
	// (one subdirectory/file per server, under $PXF_BASE/servers).
	pxfServersMountPath = "/pxf-base/servers"
	// pxfLibMountPath is where custom connector JARs are made available.
	pxfLibMountPath = "/pxf/lib/custom"

	// pxfBaseVolumeName is the emptyDir volume backing $PXF_BASE.
	pxfBaseVolumeName = "pxf-base"
	// pxfServersVolumeName is the volume the sidecar reads its per-server
	// *-site.xml from at pxfServersMountPath. It is now an emptyDir (shared with
	// the credential init container) that holds the RESOLVED site files: the init
	// container renders the ConfigMap templates (with ${...} credential
	// placeholders) into it, so the sidecar never sees raw placeholders and the
	// credential values never live in a ConfigMap. (Previously this volume was
	// ConfigMap-backed and the sidecar saw unresolved placeholders.)
	pxfServersVolumeName = "pxf-servers"
	// pxfLibVolumeName is the emptyDir volume holding custom connector JARs.
	pxfLibVolumeName = "pxf-lib"
	// pxfTemplatesVolumeName is the ConfigMap-backed volume mounted ONLY on the
	// credential init container at pxfTemplatesMountPath. It carries the raw
	// per-server *-site.xml templates (with ${<NAME>_<KEY>} placeholders) that the
	// init container resolves into the shared pxf-servers emptyDir. It is marked
	// Optional so the segment pod can schedule before the ConfigMap is applied.
	pxfTemplatesVolumeName = "pxf-templates"
	// pxfTemplatesMountPath is where the raw templates ConfigMap is mounted on the
	// credential init container (the render SOURCE).
	pxfTemplatesMountPath = "/pxf-templates"

	// pxfCredInitContainerName is the name of the credential-resolution init
	// container that renders the per-server site templates with the live
	// credential-secret values into the shared pxf-servers emptyDir.
	pxfCredInitContainerName = "pxf-cred-init" //nolint:gosec // container NAME, not a credential

	// pxfConnectorInitContainerName is the name of the connector-download init
	// container that fetches each customConnectors[].jarUrl into the shared
	// pxf-lib emptyDir at pxfLibMountPath (/pxf/lib/custom), making the JARs
	// available on the sidecar's classpath (C.18).
	pxfConnectorInitContainerName = "pxf-connector-init"

	// pxfStatusPath is the PXF Spring Boot actuator health endpoint
	// (PXF 2.1.0) used as the single source of truth for both the liveness
	// and readiness probes. PXF 2.1.0 exposes its health via the Spring Boot
	// actuator at /actuator/health (returns {"status":"UP",...}); the legacy
	// /pxf/v15/Status path is a DB-client endpoint and returns 404, so it must
	// not be used for health checks.
	pxfStatusPath = "/actuator/health"

	// pxfLivenessInitialDelaySeconds is the liveness probe initial delay. PXF is
	// a JVM service with a non-trivial cold start, so the liveness probe is given
	// a generous delay to avoid premature restarts. Note: with a StartupProbe in
	// place (see pxfStartup* below) the liveness probe is HELD OFF until startup
	// succeeds, so this delay is now effectively a secondary guard.
	pxfLivenessInitialDelaySeconds int32 = 60
	// pxfLivenessPeriodSeconds is the liveness probe period.
	pxfLivenessPeriodSeconds int32 = 20
	// pxfLivenessTimeoutSeconds is the liveness probe per-check timeout. The
	// Kubernetes default is a pathologically tight 1s; PXF's Spring Boot actuator
	// /actuator/health can momentarily exceed 1s under GC/load, so a single slow
	// response would otherwise count as a liveness failure and risk a needless
	// SIGKILL. A slightly more tolerant 5s absorbs those transient spikes without
	// weakening the probe's intent (a truly hung JVM still fails every period).
	pxfLivenessTimeoutSeconds int32 = 5
	// pxfReadinessInitialDelaySeconds is the readiness probe initial delay.
	pxfReadinessInitialDelaySeconds int32 = 30
	// pxfReadinessPeriodSeconds is the readiness probe period.
	pxfReadinessPeriodSeconds int32 = 10

	// pxfStartup* configure the StartupProbe that protects PXF's slow Spring Boot
	// cold start. PXF (a JVM/Spring Boot service) takes ~50s to boot; during that
	// window /actuator/health is not yet UP. Without a StartupProbe, a restart
	// (e.g. to load a new connector jar, or under load) lets the LivenessProbe
	// fire mid-boot, fail, and have the kubelet SIGKILL the container -> a
	// CrashLoopBackOff that never lets PXF finish booting. The StartupProbe holds
	// the liveness/readiness probes OFF until it first succeeds, giving boot a
	// generous budget: failureThreshold * periodSeconds = 24 * 5s = 120s, which
	// comfortably covers the ~50s boot plus headroom. Once startup succeeds the
	// normal liveness/readiness cadence takes over. (This replaces the manual
	// StatefulSet startupProbe patch applied during acceptance testing.)
	pxfStartupInitialDelaySeconds int32 = 10
	pxfStartupPeriodSeconds       int32 = 5
	pxfStartupFailureThreshold    int32 = 24

	// pxfConnectorsDataKey is the ConfigMap data key listing custom connectors
	// as deterministic "name=jarUrl" lines.
	pxfConnectorsDataKey = "connectors.properties"

	// PXF server type discriminators (PxfServerSpec.Type).
	pxfServerTypeS3    = "s3"
	pxfServerTypeHDFS  = "hdfs"
	pxfServerTypeJDBC  = "jdbc"
	pxfServerTypeHive  = "hive"
	pxfServerTypeHBase = "hbase"

	// Object-store server-type discriminators (Scenario 96). PXF uses a single
	// s3-site.xml-style config file for ALL object-store connectors (the
	// connector is chosen by the profile scheme at query time, not by a distinct
	// per-cloud site file); the server merely carries the fs.* / endpoint /
	// credential keys. So gs/abfss/wasbs servers are rendered through the SAME
	// object-store renderer as s3 and emit an s3-site.xml. Dell ECS needs no
	// distinct type — it is an s3 server with a custom fs.s3a.endpoint. MinIO is
	// likewise an s3 server (path-style via fs.s3a.path.style.access=true).
	pxfServerTypeGS    = "gs"
	pxfServerTypeAbfss = "abfss"
	pxfServerTypeWasbs = "wasbs"

	// Site XML file names rendered per server.
	pxfFileS3Site     = "s3-site.xml"
	pxfFileCoreSite   = "core-site.xml"
	pxfFileHDFSSite   = "hdfs-site.xml"
	pxfFileHiveSite   = "hive-site.xml"
	pxfFileHBaseSite  = "hbase-site.xml"
	pxfFileJDBCSite   = "jdbc-site.xml"
	pxfFileMapredSite = "mapred-site.xml"
	pxfFileYarnSite   = "yarn-site.xml"

	// Hadoop site-file key prefixes used by splitHadoopSiteFiles to route a
	// server.Config entry to its canonical *-site.xml. The mapping mirrors the
	// Hadoop client convention PXF reads:
	//   - "fs."        → core-site.xml  (e.g. fs.defaultFS, fs.s3a.endpoint)
	//   - "dfs."       → hdfs-site.xml  (e.g. dfs.replication)
	//   - "mapred."/"mapreduce." → mapred-site.xml
	//   - "yarn."      → yarn-site.xml
	//   - "hive."      → hive-site.xml  (fallback when no dedicated Hive map)
	//   - "hbase."     → hbase-site.xml (fallback when no dedicated Hbase map)
	// Any key without a recognized prefix defaults to core-site.xml.
	pxfPrefixFS        = "fs."
	pxfPrefixDFS       = "dfs."
	pxfPrefixMapred    = "mapred."
	pxfPrefixMapreduce = "mapreduce."
	pxfPrefixYarn      = "yarn."
	pxfPrefixHive      = "hive."
	pxfPrefixHBase     = "hbase."

	// pxfTrue/pxfFalse are the string env values emitted for the *bool extension
	// toggles. A nil *bool defaults to true (extension enabled).
	pxfTrue  = "true"
	pxfFalse = "false"

	// pxfTemplatesWaitAttempts and pxfTemplatesWaitSleepSeconds bound the poll the
	// credential init container performs for the ConfigMap-backed templates
	// directory to be populated before iterating it. The kubelet projects a
	// ConfigMap volume via a symlink swap that can briefly race the init
	// container's start, so the init container must not iterate an empty (or
	// not-yet-populated) /pxf-templates dir and silently render nothing. We poll
	// up to pxfTemplatesWaitAttempts times, sleeping pxfTemplatesWaitSleepSeconds
	// between attempts (~10s total: 20 * 0.5s) for at least one *.xml to appear,
	// then proceed regardless (deterministic, never blocks forever).
	pxfTemplatesWaitAttempts     = 20
	pxfTemplatesWaitSleepSeconds = "0.5"

	// Standard PXF/Hadoop credential property names. PXF/Hadoop read these
	// well-known keys from the rendered site XML — NOT the non-standard
	// "pxf.credential.*" keys the operator used to emit (which PXF ignores).
	//
	//   - s3 servers   → fs.s3a.access.key / fs.s3a.secret.key
	//   - jdbc servers → jdbc.user        / jdbc.password
	//
	// Mapping a credentialSecrets[] entry to one of these is done by the
	// credentialProperties heuristic (see below): primarily by the secret KEY
	// name (e.g. "aws_access_key_id" => access, "password" => password) with a
	// deterministic ORDER-based fallback (first entry => access/user, second
	// entry => secret/password).
	pxfPropS3AccessKey = "fs.s3a.access.key"
	pxfPropS3SecretKey = "fs.s3a.secret.key" //nolint:gosec // Hadoop property NAME, not a credential value
	pxfPropJDBCUser    = "jdbc.user"
	// #nosec G101 -- Hadoop/JDBC property NAME, not a credential value
	pxfPropJDBCPassword = "jdbc.password"

	// Kerberos (SE.4) wiring constants. The keytab Secret is mounted read-only
	// per server under $PXF_BASE/keytabs/<server>/ on both the PXF sidecar and
	// the credential init container; the hadoop.security.* properties are folded
	// into the server's core-site.xml so PXF authenticates via the keytab. The
	// operator wires configuration only — it never performs a live kinit.
	pxfKeytabsMountBase = pxfBase + "/keytabs"
	// pxfKeytabVolumePrefix is the per-server keytab volume name prefix; the
	// server name is appended (sanitized) to form a unique volume name.
	pxfKeytabVolumePrefix = "pxf-keytab-"
	// pxfKrb5VolumeName is the volume name for the optional krb5.conf ConfigMap.
	pxfKrb5VolumeName = "pxf-krb5"
	// pxfKrb5MountPath is where the krb5.conf ConfigMap is mounted in the sidecar.
	pxfKrb5MountPath = "/etc/krb5.conf"
	// pxfKrb5ConfigKey is the ConfigMap key holding the krb5.conf contents.
	pxfKrb5ConfigKey = "krb5.conf"

	// Hadoop Kerberos property names rendered into core-site.xml (SE.4).
	pxfPropHadoopSecurityAuth  = "hadoop.security.authentication"
	pxfPropPxfServicePrincipal = "pxf.service.kerberos.principal"
	pxfPropPxfServiceKeytab    = "pxf.service.kerberos.keytab"
	// pxfKerberosAuthValue is the only authentication value we render for SE.4.
	pxfKerberosAuthValue = "kerberos"
)

// PxfServersConfigMapName returns the name of the rendered PXF servers ConfigMap
// for a cluster ("<cluster>-pxf-servers").
func PxfServersConfigMapName(cluster string) string {
	return util.SanitizeK8sName(fmt.Sprintf("%s-pxf-servers", cluster))
}

// pxfSidecarEnabled reports whether the PXF sidecar should be injected. It is
// the blast-radius firewall: the sidecar (and the servers ConfigMap) are only
// produced when data loading is enabled, the PXF block is present and enabled,
// AND a non-empty image is set. Any condition false => OFF, leaving pods
// byte-identical to a default cluster.
func pxfSidecarEnabled(cluster *cbv1alpha1.CloudberryCluster) bool {
	dl := cluster.Spec.DataLoading
	return dl != nil && dl.Enabled &&
		dl.Pxf != nil && dl.Pxf.Enabled &&
		dl.Pxf.Image != ""
}

// pxfPort resolves the PXF service port, falling back to the default.
func pxfPort(pxf *cbv1alpha1.PxfSpec) int32 {
	if pxf.Port > 0 {
		return pxf.Port
	}
	return defaultPxfPort
}

// pxfJvmOpts resolves the PXF JVM options, falling back to the default.
func pxfJvmOpts(pxf *cbv1alpha1.PxfSpec) string {
	if pxf.JvmOpts != "" {
		return pxf.JvmOpts
	}
	return defaultPxfJvmOpts
}

// pxfLogLevel resolves the PXF log level. An empty logLevel resolves to INFO —
// this is the critical fallback that underpins re-patch propagation: rebuilding
// from spec always yields the spec's level (or INFO when unset).
func pxfLogLevel(pxf *cbv1alpha1.PxfSpec) string {
	if pxf.LogLevel != "" {
		return pxf.LogLevel
	}
	return defaultPxfLogLevel
}

// pxfExtensionFlag converts a *bool extension toggle to its string env value. A
// nil pointer defaults to true (extension enabled).
func pxfExtensionFlag(v *bool) string {
	if v == nil || *v {
		return pxfTrue
	}
	return pxfFalse
}

// BuildPXFSidecarContainers returns the PXF sidecar container(s) for a cluster.
// It returns a single "pxf" container when the sidecar is enabled, otherwise an
// empty slice. All env is derived from the spec with deterministic fallbacks
// (PXF_LOG_LEVEL defaults to INFO), so a re-patch of pxf.logLevel rebuilds the
// container with the new value.
func (b *DefaultBuilder) BuildPXFSidecarContainers(
	cluster *cbv1alpha1.CloudberryCluster,
) []corev1.Container {
	if !pxfSidecarEnabled(cluster) {
		return []corev1.Container{}
	}

	pxf := cluster.Spec.DataLoading.Pxf
	port := pxfPort(pxf)

	var extPxf, extFdw *bool
	if pxf.Extensions != nil {
		extPxf = pxf.Extensions.Pxf
		extFdw = pxf.Extensions.PxfFdw
	}

	env := []corev1.EnvVar{
		{Name: "PXF_HOME", Value: pxfHome},
		{Name: "PXF_BASE", Value: pxfBase},
		{Name: "PXF_JVM_OPTS", Value: pxfJvmOpts(pxf)},
		{Name: "PXF_PORT", Value: strconv.Itoa(int(port))},
		{Name: "PXF_LOG_LEVEL", Value: pxfLogLevel(pxf)},
		{Name: "PXF_EXTENSION_PXF", Value: pxfExtensionFlag(extPxf)},
		{Name: "PXF_EXTENSION_PXF_FDW", Value: pxfExtensionFlag(extFdw)},
		// M.2/M.3: enable the PXF Spring Boot Actuator Prometheus endpoint so
		// /actuator/prometheus on the PXF port serves the REAL
		// http_server_requests_seconds_* metrics. These are scraped under their
		// native actuator names (a vmagent scrape job is added by DevOps — the
		// operator does NOT relabel/rename them or fabricate a synthetic
		// cloudberry_pxf_requests_total). The image honors env→Spring property,
		// so exposing the include list + enabling the prometheus endpoint here is
		// sufficient to make the endpoint scrapable.
		{
			Name:  "MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE",
			Value: "health,prometheus",
		},
		{
			Name:  "MANAGEMENT_ENDPOINT_PROMETHEUS_ENABLED",
			Value: "true",
		},
	}

	probeHandler := corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: pxfStatusPath,
			Port: intstr.FromInt32(port),
		},
	}

	container := corev1.Container{
		Name:  pxfContainerName,
		Image: pxf.Image,
		Env:   env,
		Ports: []corev1.ContainerPort{
			{
				Name:          pxfPortName,
				ContainerPort: port,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		// StartupProbe gates BOTH liveness and readiness until PXF's slow Spring
		// Boot cold start completes, so a mid-boot health check never trips
		// liveness into a SIGKILL/CrashLoopBackOff. It reuses the SAME health
		// handler (HTTP GET /actuator/health on the pxf port); only the timing is
		// startup-tolerant (see pxfStartup* constants: ~120s budget).
		StartupProbe: &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: pxfStartupInitialDelaySeconds,
			PeriodSeconds:       pxfStartupPeriodSeconds,
			FailureThreshold:    pxfStartupFailureThreshold,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: pxfLivenessInitialDelaySeconds,
			PeriodSeconds:       pxfLivenessPeriodSeconds,
			TimeoutSeconds:      pxfLivenessTimeoutSeconds,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: pxfReadinessInitialDelaySeconds,
			PeriodSeconds:       pxfReadinessPeriodSeconds,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: pxfBaseVolumeName, MountPath: pxfBase},
			{Name: pxfServersVolumeName, MountPath: pxfServersMountPath},
			{Name: pxfLibVolumeName, MountPath: pxfLibMountPath},
		},
	}

	// SE.4: mount each Kerberos-enabled server's keytab Secret (and any krb5.conf
	// ConfigMap) read-only into the sidecar so PXF can authenticate via keytab.
	container.VolumeMounts = append(container.VolumeMounts, pxfKerberosVolumeMounts(pxf.Servers)...)

	if pxf.Resources != nil {
		k8sRes, err := convertResources(pxf.Resources)
		if err == nil {
			container.Resources = k8sRes
		}
	}

	return []corev1.Container{container}
}

// BuildPXFSidecarVolumes returns the volumes required by the PXF sidecar and its
// credential init container:
//
//   - pxf-base    (emptyDir):   $PXF_BASE.
//   - pxf-servers (emptyDir):   the RESOLVED per-server *-site.xml the sidecar
//     reads; the init container renders the templates into it.
//   - pxf-lib     (emptyDir):   custom connector JARs.
//   - pxf-templates (ConfigMap, Optional): the RAW per-server site templates
//     (with ${...} placeholders), mounted only on the init container. Marked
//     Optional so the pod can schedule before the ConfigMap is applied (avoids
//     an ordering deadlock between the StatefulSet and the ConfigMap reconcile).
//
// The credential-secret values NEVER appear in the ConfigMap — they are injected
// into the init container's env (SecretKeyRef) and substituted into the resolved
// files at runtime, which live only in the emptyDir.
func (b *DefaultBuilder) BuildPXFSidecarVolumes(
	cluster *cbv1alpha1.CloudberryCluster,
) []corev1.Volume {
	if !pxfSidecarEnabled(cluster) {
		return []corev1.Volume{}
	}

	optional := true
	// SE.4 keytab/krb5 volumes (empty when no server uses Kerberos) are appended
	// after the four fixed volumes; pre-size to avoid a reallocation (prealloc).
	kerberos := pxfKerberosVolumes(cluster.Spec.DataLoading.Pxf.Servers)
	volumes := make([]corev1.Volume, 0, 4+len(kerberos))
	volumes = append(volumes,
		corev1.Volume{
			Name:         pxfBaseVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.Volume{
			Name:         pxfServersVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.Volume{
			Name:         pxfLibVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.Volume{
			Name: pxfTemplatesVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: PxfServersConfigMapName(cluster.Name),
					},
					Optional: &optional,
				},
			},
		},
	)
	volumes = append(volumes, kerberos...)
	return volumes
}

// pxfKerberosVolumeMounts returns the read-only keytab (and krb5.conf) volume
// mounts for every Kerberos-enabled server, in deterministic server-sorted order
// (SE.4). The krb5.conf ConfigMap is mounted once (by the first server that
// references it) at /etc/krb5.conf via subPath. Returns nil when no server uses
// Kerberos so non-Kerberos pods are byte-identical.
func pxfKerberosVolumeMounts(servers []cbv1alpha1.PxfServerSpec) []corev1.VolumeMount {
	sorted := kerberosServersSorted(servers)
	if len(sorted) == 0 {
		return nil
	}
	mounts := make([]corev1.VolumeMount, 0, len(sorted)+1)
	krb5Mounted := false
	for i := range sorted {
		srv := sorted[i]
		mounts = append(mounts, corev1.VolumeMount{
			Name:      pxfKeytabVolumeName(srv.Name),
			MountPath: fmt.Sprintf("%s/%s", pxfKeytabsMountBase, srv.Name),
			ReadOnly:  true,
		})
		if srv.Kerberos.Krb5ConfigMap != "" && !krb5Mounted {
			mounts = append(mounts, corev1.VolumeMount{
				Name:      pxfKrb5VolumeName,
				MountPath: pxfKrb5MountPath,
				SubPath:   pxfKrb5ConfigKey,
				ReadOnly:  true,
			})
			krb5Mounted = true
		}
	}
	return mounts
}

// pxfKerberosVolumes returns the keytab Secret volumes (one per Kerberos-enabled
// server) plus the optional shared krb5.conf ConfigMap volume, in deterministic
// server-sorted order (SE.4). Returns nil when no server uses Kerberos.
func pxfKerberosVolumes(servers []cbv1alpha1.PxfServerSpec) []corev1.Volume {
	sorted := kerberosServersSorted(servers)
	if len(sorted) == 0 {
		return nil
	}
	volumes := make([]corev1.Volume, 0, len(sorted)+1)
	krb5Added := false
	for i := range sorted {
		srv := sorted[i]
		volumes = append(volumes, corev1.Volume{
			Name: pxfKeytabVolumeName(srv.Name),
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: srv.Kerberos.KeytabSecret.Name,
				},
			},
		})
		if srv.Kerberos.Krb5ConfigMap != "" && !krb5Added {
			volumes = append(volumes, corev1.Volume{
				Name: pxfKrb5VolumeName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: srv.Kerberos.Krb5ConfigMap,
						},
					},
				},
			})
			krb5Added = true
		}
	}
	return volumes
}

// kerberosServersSorted returns the Kerberos-enabled servers sorted by Name for
// deterministic, byte-stable volume/mount ordering (SE.4).
func kerberosServersSorted(servers []cbv1alpha1.PxfServerSpec) []*cbv1alpha1.PxfServerSpec {
	out := make([]*cbv1alpha1.PxfServerSpec, 0, len(servers))
	for i := range servers {
		if servers[i].Kerberos != nil {
			out = append(out, &servers[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// BuildPXFCredentialInitContainers returns the credential-resolution init
// container(s) for the segment-primary pod: a single "pxf-cred-init" container
// when the sidecar is enabled, otherwise an empty slice. The init container
// mounts the raw templates ConfigMap (pxf-templates) at pxfTemplatesMountPath,
// receives every server's credentialSecrets as env (SecretKeyRef), and runs
// envsubst (with the POSIX eval/heredoc fallback used elsewhere) to write the
// resolved *-site.xml files into the shared pxf-servers emptyDir that the sidecar
// reads. This is what makes the live secret rendering Implemented (the ConfigMap
// keeps only ${...} placeholders; the resolved files exist only at runtime in the
// emptyDir).
func (b *DefaultBuilder) BuildPXFCredentialInitContainers(
	cluster *cbv1alpha1.CloudberryCluster,
) []corev1.Container {
	if !pxfSidecarEnabled(cluster) {
		return []corev1.Container{}
	}

	container := corev1.Container{
		Name:    pxfCredInitContainerName,
		Image:   cluster.Spec.DataLoading.Pxf.Image,
		Command: []string{shellCommand, shellFlag},
		Args:    []string{pxfCredentialInitScript()},
		Env:     buildPXFCredentialEnv(cluster.Spec.DataLoading.Pxf.Servers),
		VolumeMounts: []corev1.VolumeMount{
			{Name: pxfTemplatesVolumeName, MountPath: pxfTemplatesMountPath},
			{Name: pxfServersVolumeName, MountPath: pxfServersMountPath},
		},
	}
	// SE.4: the cred-init container also mounts the keytab/krb5 volumes so the
	// per-server keytab path it renders into core-site.xml is consistent with the
	// sidecar's mount layout.
	container.VolumeMounts = append(
		container.VolumeMounts, pxfKerberosVolumeMounts(cluster.Spec.DataLoading.Pxf.Servers)...)
	return []corev1.Container{container}
}

// BuildPXFConnectorInitContainers returns the connector-download init container
// for the segment-primary pod: a single "pxf-connector-init" container that
// fetches each customConnectors[].jarUrl into the shared pxf-lib emptyDir at
// pxfLibMountPath (/pxf/lib/custom) so the JARs land on the sidecar's classpath
// (C.18). It mirrors BuildPXFCredentialInitContainers: same image (Pxf.Image),
// shell command, and emptyDir sharing with the sidecar. It returns an empty
// slice unless the sidecar is enabled AND at least one custom connector is
// declared. For s3:// jarUrls the container receives the S3 credentials env
// (SecretKeyRef) reused from the cluster's backup S3 destination, plus the MinIO
// endpoint/region; http(s):// jarUrls are fetched directly with curl.
func (b *DefaultBuilder) BuildPXFConnectorInitContainers(
	cluster *cbv1alpha1.CloudberryCluster,
) []corev1.Container {
	if !pxfSidecarEnabled(cluster) {
		return []corev1.Container{}
	}
	connectors := cluster.Spec.DataLoading.Pxf.CustomConnectors
	if len(connectors) == 0 {
		return []corev1.Container{}
	}

	container := corev1.Container{
		Name:    pxfConnectorInitContainerName,
		Image:   cluster.Spec.DataLoading.Pxf.Image,
		Command: []string{shellCommand, shellFlag},
		Args:    []string{pxfConnectorInitScript(connectors)},
		Env:     buildPXFConnectorEnv(cluster),
		VolumeMounts: []corev1.VolumeMount{
			{Name: pxfLibVolumeName, MountPath: pxfLibMountPath},
		},
	}
	return []corev1.Container{container}
}

// buildPXFConnectorEnv returns the S3 credential + endpoint env for the
// connector-download init container. The credentials are reused from the
// cluster's backup S3 destination (the same MinIO-staged credentials the
// operator already wires for backup); when the cluster has no S3 backup
// destination the env is empty (http(s):// jarUrls need no credentials). The
// env is deterministic.
func buildPXFConnectorEnv(cluster *cbv1alpha1.CloudberryCluster) []corev1.EnvVar {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.Destination.Type != destinationTypeS3 {
		return nil
	}
	s3 := cluster.Spec.Backup.Destination.S3
	if s3 == nil {
		return nil
	}
	name, accessKeyField, secretKeyField := resolveS3CredentialSource(cluster, s3)
	env := buildS3CredentialEnv(name, accessKeyField, secretKeyField)
	endpoint := s3.Endpoint
	region := s3.Region
	if region == "" {
		region = defaultS3SigningRegion
	}
	env = append(env,
		corev1.EnvVar{Name: "AWS_S3_ENDPOINT", Value: endpoint},
		corev1.EnvVar{Name: "AWS_DEFAULT_REGION", Value: region},
	)
	return env
}

// pxfConnectorInitScript renders the bash script the connector-download init
// container runs. For each connector (sorted by Name for byte-stability) it
// downloads the jarUrl into /pxf/lib/custom/<name>.jar — s3:// via the AWS CLI
// (using AWS_S3_ENDPOINT for MinIO), http(s):// via curl — then asserts the
// downloaded file is non-empty. An unsupported scheme aborts the init (exit 1).
func pxfConnectorInitScript(connectors []cbv1alpha1.PxfCustomConnector) string {
	sorted := make([]cbv1alpha1.PxfCustomConnector, len(connectors))
	copy(sorted, connectors)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	fmt.Fprintf(&b, "mkdir -p %s\n", shellQuote(pxfLibMountPath))
	for i := range sorted {
		c := sorted[i]
		dst := fmt.Sprintf("%s/%s.jar", pxfLibMountPath, c.Name)
		jarURL := shellQuote(c.JarURL)
		quotedDst := shellQuote(dst)
		fmt.Fprintf(&b, "case %s in\n", jarURL)
		fmt.Fprintf(&b, "  s3://*) aws --endpoint-url \"$AWS_S3_ENDPOINT\" s3 cp %s %s ;;\n",
			jarURL, quotedDst)
		fmt.Fprintf(&b, "  http://*|https://*) curl -fsSL %s -o %s ;;\n", jarURL, quotedDst)
		b.WriteString("  *) echo 'unsupported jar scheme'; exit 1 ;;\n")
		b.WriteString("esac\n")
		fmt.Fprintf(&b, "test -s %s\n", quotedDst)
	}
	return b.String()
}

// pxfCredentialInitScript renders the bash script the credential init container
// runs. For every template in the mounted ConfigMap directory it substitutes the
// ${<SANITIZED_NAME_KEY>} credential placeholders from the container env using
// envsubst, falling back to the POSIX `eval "cat <<EOF"` idiom (the same fallback
// used by the backup coordinatorExecScript) when envsubst is absent.
//
// The resolved files are written into the shared pxf-servers emptyDir using the
// NATIVE nested layout PXF expects: a ConfigMap key "<server>__<file>.xml" is
// written to "<server>/<file>.xml" (a per-server subdirectory). This matches
// what `pxf prepare` / the sidecar reads natively under $PXF_BASE/servers, so the
// entrypoint reorg becomes a no-op. Non-server keys (e.g. connectors.properties,
// any key without the "__" separator) are written verbatim at the top level. The
// block is a guarded no-op when no templates are present.
func pxfCredentialInitScript() string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	fmt.Fprintf(&b, "SRC=%s\n", shellQuote(pxfTemplatesMountPath))
	fmt.Fprintf(&b, "DST=%s\n", shellQuote(pxfServersMountPath))
	b.WriteString("mkdir -p \"${DST}\"\n")
	b.WriteString("echo 'pxf-cred-init: resolving server site templates'\n")
	// Bounded wait for the ConfigMap volume to be projected before iterating.
	// The kubelet swaps the ConfigMap symlink atomically, but that swap can race
	// the init container's start; without this poll the loop below would iterate
	// an empty SRC and silently render nothing. Retry up to a fixed number of
	// attempts for at least one *.xml to appear, then proceed regardless so the
	// init container is deterministic and never blocks forever (e.g. a
	// credential-less cluster has no templates).
	fmt.Fprintf(&b, "i=0\n")
	fmt.Fprintf(&b, "while [ \"${i}\" -lt %d ]; do\n", pxfTemplatesWaitAttempts)
	b.WriteString("  if ls \"${SRC}\"/*.xml >/dev/null 2>&1; then\n")
	b.WriteString("    break\n")
	b.WriteString("  fi\n")
	fmt.Fprintf(&b, "  i=$((i + 1))\n")
	fmt.Fprintf(&b, "  sleep %s\n", pxfTemplatesWaitSleepSeconds)
	b.WriteString("done\n")
	// Iterate every regular file in the templates dir. ConfigMap mounts expose
	// each data key as a file; the *-site.xml keys carry the placeholders.
	b.WriteString("if [ -d \"${SRC}\" ]; then\n")
	b.WriteString("  for f in \"${SRC}\"/*; do\n")
	b.WriteString("    [ -e \"${f}\" ] || continue\n")
	b.WriteString("    base=$(basename \"${f}\")\n")
	// Map "<server>__<file>.xml" => "<server>/<file>.xml"; keys without the
	// "__" separator are written at the top level unchanged.
	b.WriteString("    case \"${base}\" in\n")
	b.WriteString("      *__*)\n")
	b.WriteString("        server=\"${base%%__*}\"\n")
	b.WriteString("        file=\"${base#*__}\"\n")
	b.WriteString("        out=\"${DST}/${server}/${file}\"\n")
	b.WriteString("        mkdir -p \"${DST}/${server}\"\n")
	b.WriteString("        ;;\n")
	b.WriteString("      *)\n")
	b.WriteString("        out=\"${DST}/${base}\"\n")
	b.WriteString("        ;;\n")
	b.WriteString("    esac\n")
	b.WriteString("    if command -v envsubst >/dev/null 2>&1; then\n")
	b.WriteString("      envsubst < \"${f}\" > \"${out}\"\n")
	b.WriteString("    else\n")
	b.WriteString("      eval \"cat <<_PXF_ENVSUBST_EOF_\n$(cat \"${f}\")\n_PXF_ENVSUBST_EOF_\" > \"${out}\"\n")
	b.WriteString("    fi\n")
	b.WriteString("  done\n")
	b.WriteString("fi\n")
	b.WriteString("echo 'pxf-cred-init: done'\n")
	return b.String()
}

// buildPXFCredentialEnv builds the env vars the credential init container uses to
// resolve the ${<SANITIZED_NAME_KEY>} placeholders. For each server's
// credentialSecrets reference it emits an env var whose Name is the SANITIZED
// "<NAME>_<KEY>" token (see pxfCredentialEnvName: hyphens/illegal chars become
// "_", uppercased, leading-digit guarded) sourced from the referenced Secret via
// SecretKeyRef. The Name therefore matches the placeholder emitted into the site
// XML byte-for-byte, so both envsubst and the POSIX fallback can resolve it. The
// names are emitted in a deterministic (sorted) order and de-duplicated so the
// container env is byte-stable. The secret VALUES never appear here (only
// references).
func buildPXFCredentialEnv(servers []cbv1alpha1.PxfServerSpec) []corev1.EnvVar {
	type ref struct {
		secret string
		key    string
	}
	seen := make(map[string]ref)
	for i := range servers {
		for _, cs := range servers[i].CredentialSecrets {
			envName := pxfCredentialEnvName(cs.Name, cs.Key)
			if envName == "" {
				continue
			}
			if _, ok := seen[envName]; !ok {
				seen[envName] = ref{secret: cs.Name, key: cs.Key}
			}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)

	env := make([]corev1.EnvVar, 0, len(names))
	for _, n := range names {
		r := seen[n]
		key := r.key
		if key == "" {
			// A ${<NAME>} placeholder with no key: source the secret's single
			// well-known value is not knowable here, so reference the secret name
			// as the key. Operators that use a key-less reference must store the
			// value under a key equal to the secret name (documented edge case).
			key = r.secret
		}
		env = append(env, corev1.EnvVar{
			Name: n,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: r.secret},
					Key:                  key,
				},
			},
		})
	}
	return env
}

// pxfCredentialEnvName returns the SANITIZED env var name for a credential
// secret reference, matching the ${<NAME>_<KEY>} / ${<NAME>} placeholder naming
// in credentialPlaceholderValue. The raw "<name>_<key>" token (e.g.
// "backup-s3-credentials_aws_access_key_id") is run through
// pxfSanitizeEnvName so the result is a VALID shell variable name (hyphens and
// other illegal characters become "_", a leading digit is prefixed with "_").
// This is critical: the POSIX eval/heredoc fallback in the init script cannot
// resolve placeholders containing hyphens, so the env var name and the
// placeholder token MUST share this exact sanitization (see
// credentialPlaceholderValue, which calls the same helper). Returns "" for an
// empty secret name.
func pxfCredentialEnvName(name, key string) string {
	if name == "" {
		return ""
	}
	raw := name
	if key != "" {
		raw = fmt.Sprintf("%s_%s", name, key)
	}
	return pxfSanitizeEnvName(raw)
}

// pxfSanitizeEnvName converts an arbitrary "<name>_<key>" token into a valid,
// uppercased shell/POSIX environment variable name: every character outside
// [A-Za-z0-9_] is replaced with "_", and a leading digit is prefixed with "_"
// (env var names may not start with a digit). Uppercasing yields the
// conventional ENV_VAR style and keeps the placeholder token and the
// SecretKeyRef env var Name byte-identical so they can never diverge. This is
// the SINGLE shared helper used by both pxfCredentialEnvName (the env var name)
// and credentialPlaceholderValue (the ${...} token emitted into the site XML).
func pxfSanitizeEnvName(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out[0] >= '0' && out[0] <= '9' {
		return "_" + out
	}
	return out
}

// BuildPXFServersConfigMap renders the "<cluster>-pxf-servers" ConfigMap holding
// each external server's *-site.xml plus the custom-connectors listing. Returns
// nil when the sidecar is disabled. The ownerRef + CommonLabels are set here so
// the controller can create-or-update it directly. Site XML is rendered with
// SORTED keys for byte-stable output (no needless StatefulSet rollouts from CM
// churn). Credential-secret values are emitted as ${<SANITIZED_NAME_KEY>}
// placeholders bound to the STANDARD PXF/Hadoop property names
// (fs.s3a.access.key/secret.key for s3, jdbc.user/password for jdbc); the live
// secret values are injected by the credential init container at runtime and
// never appear in the ConfigMap.
func (b *DefaultBuilder) BuildPXFServersConfigMap(
	cluster *cbv1alpha1.CloudberryCluster,
) *corev1.ConfigMap {
	if !pxfSidecarEnabled(cluster) {
		return nil
	}

	pxf := cluster.Spec.DataLoading.Pxf
	data := make(map[string]string)

	for i := range pxf.Servers {
		renderPXFServer(&pxf.Servers[i], data)
	}

	data[pxfConnectorsDataKey] = renderPXFConnectors(pxf.CustomConnectors)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PxfServersConfigMapName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    util.CommonLabels(cluster.Name, util.ComponentPxf),
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Data: data,
	}
}

// splitHadoopSiteFiles deterministically routes a server.Config map into the
// canonical Hadoop *-site.xml fragments by KEY PREFIX, so each Hadoop property
// lands in the file PXF actually reads it from:
//
//	fs.*                    → core-site.xml   (e.g. fs.defaultFS, fs.s3a.endpoint)
//	dfs.*                   → hdfs-site.xml   (e.g. dfs.replication)
//	mapred.* / mapreduce.*  → mapred-site.xml
//	yarn.*                  → yarn-site.xml
//	hive.*                  → hive-site.xml   (fallback when no dedicated Hive map)
//	hbase.*                 → hbase-site.xml  (fallback when no dedicated Hbase map)
//	<anything else>         → core-site.xml   (safe default for generic Hadoop)
//
// The return value maps a site-file name (e.g. pxfFileCoreSite) to its property
// fragment. A site-file key is present only when at least one property routed to
// it (so callers can decide which files are optional). The function is pure and
// allocation-light; renderSiteXML sorts keys, so the rendered output is
// byte-stable regardless of Go map iteration order. A nil/empty input returns an
// empty (non-nil) map.
func splitHadoopSiteFiles(config map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string)
	put := func(file, k, v string) {
		frag := out[file]
		if frag == nil {
			frag = make(map[string]string)
			out[file] = frag
		}
		frag[k] = v
	}
	for k, v := range config {
		switch {
		case strings.HasPrefix(k, pxfPrefixFS):
			put(pxfFileCoreSite, k, v)
		case strings.HasPrefix(k, pxfPrefixDFS):
			put(pxfFileHDFSSite, k, v)
		case strings.HasPrefix(k, pxfPrefixMapred), strings.HasPrefix(k, pxfPrefixMapreduce):
			put(pxfFileMapredSite, k, v)
		case strings.HasPrefix(k, pxfPrefixYarn):
			put(pxfFileYarnSite, k, v)
		case strings.HasPrefix(k, pxfPrefixHive):
			put(pxfFileHiveSite, k, v)
		case strings.HasPrefix(k, pxfPrefixHBase):
			put(pxfFileHBaseSite, k, v)
		default:
			put(pxfFileCoreSite, k, v)
		}
	}
	return out
}

// renderPXFServer renders one server's *-site.xml entries into the ConfigMap
// data map under deterministic "<name>__<file>.xml" keys. credentialSecrets are
// folded into the relevant site XML as STANDARD PXF/Hadoop properties carrying
// ${...} placeholders (see credentialProperties): s3 servers get
// fs.s3a.access.key/fs.s3a.secret.key, jdbc servers get jdbc.user/jdbc.password.
// hdfs/hive/hbase servers carry no inline credentials (they authenticate via
// Kerberos), so their credentialSecrets are intentionally NOT folded in.
//
// File-mapping per server type (SL.1–SL.5):
//   - s3/gs/abfss/wasbs (object stores): s3-site.xml. PXF reads object-store
//     connection settings from s3-site.xml for ALL object-store connectors; the
//     scheme in the profile (s3:/gs:/abfss:/wasbs:) selects the connector at
//     query time. So gs/abfss/wasbs are rendered identically to s3 (and Dell ECS
//     / MinIO are just s3 servers with a custom fs.s3a.endpoint and/or
//     fs.s3a.path.style.access=true in Config).
//   - hdfs:  core-site.xml AND hdfs-site.xml ALWAYS (even when empty → a valid
//     <configuration/>); mapred-site.xml / yarn-site.xml only when matching keys
//     exist; hive-site.xml / hbase-site.xml from the dedicated Hive/Hbase maps
//     (preferred) or the Config hive.*/hbase.* fragment.
//   - jdbc:  jdbc-site.xml.
//   - hive:  core-site.xml AND hive-site.xml ALWAYS.
//   - hbase: core-site.xml AND hbase-site.xml ALWAYS.
func renderPXFServer(server *cbv1alpha1.PxfServerSpec, data map[string]string) {
	creds := credentialProperties(server.Type, server.CredentialSecrets)

	switch server.Type {
	case pxfServerTypeS3, pxfServerTypeGS, pxfServerTypeAbfss, pxfServerTypeWasbs:
		// All object stores share the s3-site.xml-style config file (the profile
		// scheme drives the connector). The Config carries the fs.* keys
		// (endpoint, path-style, etc.) and the credential placeholders are
		// folded in as the standard fs.s3a.access.key/secret.key properties.
		data[pxfServerKey(server.Name, pxfFileS3Site)] = renderSiteXML(mergeMaps(server.Config, creds))
	case pxfServerTypeHDFS:
		renderPXFHDFSServer(server, data)
	case pxfServerTypeJDBC:
		// jdbc-site.xml merges the non-sensitive Config (e.g. jdbc.driver,
		// jdbc.url) with the dedicated Jdbc map and the jdbc.user/jdbc.password
		// credential placeholders.
		merged := mergeMaps(mergeMaps(server.Config, server.Jdbc), creds)
		data[pxfServerKey(server.Name, pxfFileJDBCSite)] = renderSiteXML(merged)
	case pxfServerTypeHive:
		renderPXFHiveServer(server, data)
	case pxfServerTypeHBase:
		renderPXFHBaseServer(server, data)
	default:
		// Unknown server type: render the Config as a core-site.xml so the value
		// is preserved deterministically rather than silently dropped. No inline
		// credentials are emitted for unknown types.
		data[pxfServerKey(server.Name, pxfFileCoreSite)] = renderSiteXML(server.Config)
	}
}

// renderPXFHDFSServer emits the hdfs server's site files (SL.2). core-site.xml
// and hdfs-site.xml are ALWAYS emitted (an empty fragment renders a valid
// <configuration/>) so the file set is deterministic; mapred-site.xml and
// yarn-site.xml are emitted only when their prefixed keys are present. Optional
// hive-site.xml / hbase-site.xml come from the dedicated Hive/Hbase maps when
// set, else from the Config hive.*/hbase.* fragment. hdfs authenticates via
// Kerberos, so no inline credentials are folded in.
func renderPXFHDFSServer(server *cbv1alpha1.PxfServerSpec, data map[string]string) {
	frags := splitHadoopSiteFiles(server.Config)

	// core-site.xml and hdfs-site.xml are always present for hdfs servers.
	coreSite := mergeMaps(frags[pxfFileCoreSite], kerberosCoreSiteProps(server))
	data[pxfServerKey(server.Name, pxfFileCoreSite)] = renderSiteXML(coreSite)
	data[pxfServerKey(server.Name, pxfFileHDFSSite)] = renderSiteXML(frags[pxfFileHDFSSite])

	// mapred-site.xml / yarn-site.xml are optional: only when keys routed to them.
	if frag := frags[pxfFileMapredSite]; len(frag) > 0 {
		data[pxfServerKey(server.Name, pxfFileMapredSite)] = renderSiteXML(frag)
	}
	if frag := frags[pxfFileYarnSite]; len(frag) > 0 {
		data[pxfServerKey(server.Name, pxfFileYarnSite)] = renderSiteXML(frag)
	}

	// hive-site.xml: dedicated Hive map wins; else Config hive.* fragment.
	if hive := resolveSiteFragment(server.Hive, frags[pxfFileHiveSite]); len(hive) > 0 {
		data[pxfServerKey(server.Name, pxfFileHiveSite)] = renderSiteXML(hive)
	}
	// hbase-site.xml: dedicated Hbase map wins; else Config hbase.* fragment.
	if hbase := resolveSiteFragment(server.Hbase, frags[pxfFileHBaseSite]); len(hbase) > 0 {
		data[pxfServerKey(server.Name, pxfFileHBaseSite)] = renderSiteXML(hbase)
	}
}

// renderPXFHiveServer emits a hive-typed server's site files (SL.4):
// core-site.xml AND hive-site.xml are both ALWAYS emitted. core-site.xml carries
// the non-hive Config fragment (fs.* + unprefixed keys); hive-site.xml is the
// dedicated server.Hive map when set, else the Config hive.* fragment.
func renderPXFHiveServer(server *cbv1alpha1.PxfServerSpec, data map[string]string) {
	frags := splitHadoopSiteFiles(server.Config)
	coreSite := mergeMaps(frags[pxfFileCoreSite], kerberosCoreSiteProps(server))
	data[pxfServerKey(server.Name, pxfFileCoreSite)] = renderSiteXML(coreSite)
	hive := resolveSiteFragment(server.Hive, frags[pxfFileHiveSite])
	data[pxfServerKey(server.Name, pxfFileHiveSite)] = renderSiteXML(hive)
}

// renderPXFHBaseServer emits an hbase-typed server's site files (SL.5):
// core-site.xml AND hbase-site.xml are both ALWAYS emitted. core-site.xml
// carries the non-hbase Config fragment; hbase-site.xml is the dedicated
// server.Hbase map when set, else the Config hbase.* fragment.
func renderPXFHBaseServer(server *cbv1alpha1.PxfServerSpec, data map[string]string) {
	frags := splitHadoopSiteFiles(server.Config)
	coreSite := mergeMaps(frags[pxfFileCoreSite], kerberosCoreSiteProps(server))
	data[pxfServerKey(server.Name, pxfFileCoreSite)] = renderSiteXML(coreSite)
	hbase := resolveSiteFragment(server.Hbase, frags[pxfFileHBaseSite])
	data[pxfServerKey(server.Name, pxfFileHBaseSite)] = renderSiteXML(hbase)
}

// kerberosCoreSiteProps returns the hadoop.security.* core-site.xml properties
// for a Kerberos-enabled Hadoop-family server (SE.4), or nil when Kerberos is
// not configured. It renders hadoop.security.authentication=kerberos, the PXF
// service principal, and the in-pod keytab path (where the keytab Secret is
// mounted). It is config-only: the operator never performs a live kinit. The
// returned map is folded into the server's core-site.xml fragment.
func kerberosCoreSiteProps(server *cbv1alpha1.PxfServerSpec) map[string]string {
	if server.Kerberos == nil {
		return nil
	}
	return map[string]string{
		pxfPropHadoopSecurityAuth:  pxfKerberosAuthValue,
		pxfPropPxfServicePrincipal: server.Kerberos.Principal,
		pxfPropPxfServiceKeytab:    pxfKeytabPath(server.Name, server.Kerberos.KeytabSecret.Key),
	}
}

// pxfKeytabPath returns the fixed in-pod path of a server's mounted keytab file
// under $PXF_BASE/keytabs/<server>/<key> (SE.4). When the Secret reference has no
// explicit key, the secret-projected filename defaults to "keytab".
func pxfKeytabPath(serverName, key string) string {
	if key == "" {
		key = "keytab"
	}
	return fmt.Sprintf("%s/%s/%s", pxfKeytabsMountBase, serverName, key)
}

// pxfKeytabVolumeName returns the unique, deterministic per-server keytab volume
// name (SE.4). The server name is sanitized to a valid Kubernetes volume name.
func pxfKeytabVolumeName(serverName string) string {
	return util.SanitizeK8sName(pxfKeytabVolumePrefix + serverName)
}

// resolveSiteFragment returns the dedicated map when it is non-empty (it wins),
// otherwise the Config-derived prefix fragment. Either may be nil; the result is
// nil only when both are empty (renderSiteXML maps a nil fragment to a valid
// empty <configuration/>).
func resolveSiteFragment(dedicated, fromConfig map[string]string) map[string]string {
	if len(dedicated) > 0 {
		return dedicated
	}
	return fromConfig
}

// pxfServerKey returns the ConfigMap data key for a server's site file.
func pxfServerKey(serverName, fileName string) string {
	return fmt.Sprintf("%s__%s", serverName, fileName)
}

// credentialProperties maps each credentialSecrets[] reference to the STANDARD
// PXF/Hadoop property name that PXF actually reads, carrying a ${...} placeholder
// the init container resolves at runtime. PXF does NOT read non-standard
// "pxf.credential.*" keys, so we emit the well-known properties instead:
//
//   - s3   => fs.s3a.access.key (the access-key secret) and
//     fs.s3a.secret.key (the secret-key secret)
//   - jdbc => jdbc.user (the user secret) and jdbc.password (the password secret)
//   - hdfs/hive/hbase/other => no inline credentials (Kerberos) → nil.
//
// Role detection uses a KEY-NAME heuristic first (robust against ordering): a
// key containing "access" → access/user role, "secret" → secret-key role,
// "user"/"username" → jdbc.user, "password"/"pass" → jdbc.password. When the
// key is ambiguous (or empty), it falls back to ORDER: the first credential
// entry takes the access/user property, the second takes the secret/password
// property. The resulting map is deterministic for byte-stable ConfigMaps.
func credentialProperties(
	serverType string,
	refs []cbv1alpha1.SecretReference,
) map[string]string {
	if len(refs) == 0 {
		return nil
	}
	accessProp, secretProp, ok := pxfCredentialPropNames(serverType)
	if !ok {
		// hdfs/hive/hbase/unknown: no inline credentials are emitted.
		return nil
	}

	out := make(map[string]string, len(refs))
	for i := range refs {
		prop := pxfCredentialRole(refs[i].Key, i, accessProp, secretProp)
		// First-writer wins so a deterministic, explicit mapping is preserved
		// even if two references resolve to the same role.
		if _, exists := out[prop]; !exists {
			out[prop] = credentialPlaceholderValue(refs[i].Name, refs[i].Key)
		}
	}
	return out
}

// pxfCredentialPropNames returns the (access, secret) standard property names for
// a server type, and whether the type supports inline credentials at all. All
// object-store types (s3/gs/abfss/wasbs) use the s3a credential properties since
// they share the s3-site.xml-style config file.
func pxfCredentialPropNames(serverType string) (access, secret string, ok bool) {
	switch serverType {
	case pxfServerTypeS3, pxfServerTypeGS, pxfServerTypeAbfss, pxfServerTypeWasbs:
		return pxfPropS3AccessKey, pxfPropS3SecretKey, true
	case pxfServerTypeJDBC:
		return pxfPropJDBCUser, pxfPropJDBCPassword, true
	default:
		return "", "", false
	}
}

// pxfCredentialRole resolves a single credential reference to its standard
// property name using the documented key-name heuristic with an order-based
// fallback. index is the reference's position in the credentialSecrets[] slice.
func pxfCredentialRole(key string, index int, accessProp, secretProp string) string {
	k := strings.ToLower(key)
	switch {
	case strings.Contains(k, "secret") || strings.Contains(k, "password") || strings.Contains(k, "pass"):
		return secretProp
	case strings.Contains(k, "access") || strings.Contains(k, "user"):
		return accessProp
	default:
		// Ambiguous/empty key: fall back to order — first => access/user,
		// any later => secret/password.
		if index == 0 {
			return accessProp
		}
		return secretProp
	}
}

// credentialPlaceholderValue returns the ${<SANITIZED_NAME_KEY>} placeholder
// string for a secret reference. The token is produced by pxfCredentialEnvName
// — the SAME helper that names the init-container SecretKeyRef env var — so the
// placeholder in the site XML and the env var resolving it are guaranteed to
// match (e.g. secret "backup-s3-credentials" + key "aws_access_key_id" =>
// "${BACKUP_S3_CREDENTIALS_AWS_ACCESS_KEY_ID}").
func credentialPlaceholderValue(name, key string) string {
	return fmt.Sprintf("${%s}", pxfCredentialEnvName(name, key))
}

// mergeMaps returns a new map with the entries of base overlaid by overlay.
// Either argument may be nil. The result is nil only when both are empty.
func mergeMaps(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}

// xmlConfiguration is the root element of a PXF *-site.xml document.
type xmlConfiguration struct {
	XMLName    xml.Name      `xml:"configuration"`
	Properties []xmlProperty `xml:"property"`
}

// xmlProperty is a single <property><name/><value/></property> entry.
type xmlProperty struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

// renderSiteXML emits a deterministic <configuration> document with properties
// sorted by key, so identical inputs always produce byte-identical output.
func renderSiteXML(kv map[string]string) string {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cfg := xmlConfiguration{Properties: make([]xmlProperty, 0, len(keys))}
	for _, k := range keys {
		cfg.Properties = append(cfg.Properties, xmlProperty{Name: k, Value: kv[k]})
	}

	body, err := xml.MarshalIndent(cfg, "", "  ")
	if err != nil {
		// xml.MarshalIndent over these plain string structs cannot fail; guard
		// defensively so a future struct change degrades to an empty document
		// rather than panicking during reconcile.
		return xml.Header + "<configuration></configuration>\n"
	}
	return xml.Header + string(body) + "\n"
}

// renderPXFConnectors emits a deterministic "name=jarUrl" listing of the custom
// connectors, sorted by name, so the ConfigMap value is byte-stable.
func renderPXFConnectors(connectors []cbv1alpha1.PxfCustomConnector) string {
	if len(connectors) == 0 {
		return ""
	}
	lines := make([]string, 0, len(connectors))
	for _, c := range connectors {
		lines = append(lines, fmt.Sprintf("%s=%s", c.Name, c.JarURL))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}
