// Package builder: gpexpand_builder.go constructs the coordinator-exec
// Kubernetes Job that runs the REAL Cloudberry `gpexpand` flow to add and
// initialize newly-desired segments during a scale-out.
//
// Root cause it fixes (see .opencode/output/scaleout-seeding-design_core):
// a newly-added segment pod is a fresh, empty stock PostgreSQL cluster
// (self-initialized via initdb in the entrypoint) that only gets a
// hand-written gp_segment_configuration row. A user database's segment set is
// FIXED at CREATE DATABASE time, so a coordinator-dispatched CREATE DATABASE /
// ALTER TABLE ... EXPAND TABLE can never retroactively provision the database
// onto a late-added segment — only gpexpand's segment-init phase (basebackup
// from the coordinator template) does that. This builder therefore replaces the
// hand-registration + SQL seeding with a gpexpand-driven Job.
//
// The Job mirrors the backup subsystem's coordinator-exec pattern
// (backup_builder.go coordinatorExecScript + addBackupSSHIdentity): it
// `kubectl exec`s into <cluster>-coordinator-0, sources greenplum_path.sh, and
// drives gpexpand -i (add+init) -> gpexpand -a (redistribute incl. EXPAND
// TABLE) -> gpexpand --clean. gpexpand.status / status_detail make the flow
// natively resumable, so the Job is idempotent under a deterministic name.
package builder

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// base64OfString returns the standard base64 encoding of s. The gpexpand input
// file is base64-piped into the coordinator pod so no quoting/here-doc hazard
// crosses the kubectl-exec boundary (the same technique the backup
// coordinator-exec wrapper uses for its payloads).
func base64OfString(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

const (
	// gpexpandContainerName is the container name for the gpexpand Job.
	gpexpandContainerName = "gpexpand"

	// gpexpandOperation is the backup-operation label value used to tag the
	// gpexpand Job so it reuses the backup RBAC/SA (coordinator-exec model).
	gpexpandOperation = "gpexpand"

	// coordinatorCatalogDataDir is the coordinator's catalog data directory
	// (segment -1) inside the coordinator pod. gpexpand reads/writes its
	// gpexpand.status table and initializes new segment datadirs relative to
	// this live coordinator template.
	coordinatorCatalogDataDir = "/data/pgdata/gpseg-1"

	// segmentDataDirPrefix is the per-segment data directory prefix; the content
	// id is appended (e.g. /data/pgdata/gpseg2). Coordinator is gpseg-1.
	segmentDataDirPrefix = "/data/pgdata/gpseg"

	// gpexpandStatusDatabase is the database whose gpexpand schema tracks the
	// expansion (the bootstrap maintenance database). gpexpand records its
	// status/status_detail here and redistributes the target database(s).
	gpexpandStatusDatabase = "postgres"

	// gpexpandInputFile is the path (inside the coordinator pod) where the
	// operator-rendered gpexpand input file is staged before `gpexpand -i`.
	gpexpandInputFile = "/tmp/cbk-gpexpand-input"

	// clusterSSHPort is the rootless sshd port the cluster pods listen on under
	// the scoped restricted-v2 SCC (no NET_BIND_SERVICE / no root => no
	// privileged :22 bind). gpexpand's segment-init phase shells out to
	// ssh/gpssh/gpsync, which are redirected here via the coordinator's gpadmin
	// ~/.ssh/config `Port` directive. The inner script re-asserts this override
	// and TCP-probes it before `gpexpand -i` so an unreachable segment fails fast
	// with a clear message instead of gpexpand's raw "Connection refused".
	clusterSSHPort = "2022"

	// segmentProbeRetries / segmentProbeSleepSeconds bound the coordinator->new
	// segment TCP-2022 reachability probe (60s total). The scaling-sts phase has
	// already waited for the segment StatefulSets Ready; this is a fail-closed
	// gate that the rootless sshd is actually accepting on 2022.
	segmentProbeRetries      = 30
	segmentProbeSleepSeconds = 2

	// gpexpandExistingSegWaitSeconds / gpexpandExistingSegPollSeconds bound the
	// EXISTING-segment (content < base) health RE-VERIFY wait loop (90s total,
	// polling every 5s => 18 attempts). When the operator scales up the segment
	// StatefulSets (adding primary-N/mirror-N), the EXISTING segments (content
	// 0..base-1) can experience a BRIEF connectivity/health blip (headless
	// service DNS update / StatefulSet controller churn) that momentarily flips
	// them to status<>'u'. On a stable cluster they recover within seconds, so
	// the health check polls until all existing segments are up (success) OR the
	// timeout elapses (then it is a GENUINE persistent fault and we fail). This
	// absorbs the transient scale-up blip without weakening the guard.
	gpexpandExistingSegWaitSeconds = 90
	gpexpandExistingSegPollSeconds = 5

	// gpexpandSettleSeconds is a short initial settle sleep at the very start of
	// the inner gpexpand flow so the StatefulSet scale-up churn (DNS / controller
	// rollout) has a moment to quiesce before the first existing-segment health
	// poll. The retry loop is the primary fix; this just trims the first blip.
	gpexpandSettleSeconds = 10

	// gpexpandRedistributeDuration bounds the redistribution phase so a wedged
	// gpexpand fails the Job (via activeDeadlineSeconds) instead of hanging
	// forever. Passed to `gpexpand -a -d <HH:MM:SS>`.
	gpexpandRedistributeDuration = "24:00:00"

	// gpexpandInitMaxAttempts / gpexpandInitRetrySleepSeconds bound the
	// `gpexpand -i` (add+init) RETRY loop. gpexpand rsync's/ssh's to the new
	// segment during segment-init; that connection can hit a transient
	// "Connection refused" when the new segment pod is (re)started between the
	// pre-flight 2022 probe and gpexpand's own rsync (StatefulSet churn / wedge
	// recovery). Rather than fail the whole Job on such a blip, `gpexpand -i` is
	// retried up to gpexpandInitMaxAttempts times; before EACH attempt the inner
	// flow re-establishes a clean pre-state (fresh cleanup + input regeneration +
	// dbid re-substitution), re-probes sshd:2022 on every new segment host, and
	// re-verifies the existing segments are healthy, so a mid-restart pod is
	// absorbed and gpexpand.status never wedges. A genuine persistent failure
	// after all attempts fails the Job as before.
	gpexpandInitMaxAttempts       = 3
	gpexpandInitRetrySleepSeconds = 10

	// expandResultMarker is the machine-readable marker the Job writes to
	// /dev/termination-log (and stdout) so the controller can distinguish a
	// completed expansion from an incomplete redistribution. Mirrors the
	// backupTimestampMarker convention.
	expandResultMarker = "EXPAND_RESULT="
	// expandResultCompleted is the marker value for a fully-completed expansion.
	expandResultCompleted = "completed"
	// expandResultAlreadyComplete is the marker value when a prior expansion had
	// already completed (idempotent resume, nothing to do).
	expandResultAlreadyComplete = "already-complete"
	// expandResultFinalizeFailed is the marker value when redistribution
	// completed but the gpexpand schema could not be finalized (dropped). The
	// Job fails so the operator does not declare scale-out complete over a
	// lingering gpexpand schema that would block a subsequent gpbackup (D9).
	expandResultFinalizeFailed = "finalize-failed"

	// gpexpandBackoffLimit is 0: gpexpand is natively resumable via
	// gpexpand.status, so the operator (not the Job's own backoff) owns retries
	// with a bounded attempt counter. A wedged Job should fail terminally and be
	// re-created by the controller, not silently retried by the Job.
	gpexpandBackoffLimit int32 = 0

	// gpexpandActiveDeadlineSeconds bounds a single gpexpand Job run (6h). The
	// controller's own timeout/attempt bound is the outer guard; this stops a
	// single wedged run from hanging.
	gpexpandActiveDeadlineSeconds int64 = 21600

	// gpexpandTTLSecondsAfterFinished retains a finished gpexpand Job for
	// inspection before GC.
	gpexpandTTLSecondsAfterFinished int32 = 86400
)

// BuildGpexpandJob builds the coordinator-exec Job that runs the real gpexpand
// flow to add+init the newly-desired segments (content ids [oldCount,newCount))
// and redistribute the target database(s). The Job:
//
//   - has a DETERMINISTIC name (<cluster>-gpexpand-<old>-<new>) so a
//     re-reconcile re-adopts the same Job (AlreadyExists-tolerant create);
//   - `kubectl exec`s into <cluster>-coordinator-0 (util.CoordinatorPodName)
//     with the shared cluster SSH identity mounted (addBackupSSHIdentity) so
//     gpexpand can basebackup+dispatch to the new segments over SSH;
//   - sources greenplum_path.sh, exports COORDINATOR_DATA_DIRECTORY / PGPORT /
//     PG* connection env, renders the gpexpand input file for the new segments,
//     then runs gpexpand -i -> gpexpand -a -> gpexpand --clean;
//   - is idempotent/resumable via gpexpand.status / status_detail and emits
//     EXPAND_RESULT=<completed|already-complete|incomplete:N> to
//     /dev/termination-log for the controller to read.
//
// timestamp is used only to label/stamp the Job (the name is derived from the
// segment counts so it stays deterministic across reconciles).
func (b *DefaultBuilder) BuildGpexpandJob(
	cluster *cbv1alpha1.CloudberryCluster,
	oldCount, newCount int32,
	timestamp string,
) *batchv1.Job {
	labels := backupLabels(cluster.Name, gpexpandOperation)
	labels[util.LabelScaleFrom] = strconv.Itoa(int(oldCount))
	labels[util.LabelScaleTo] = strconv.Itoa(int(newCount))

	// Hash the rendered coordinator-exec script so the controller can detect
	// script DRIFT across operator upgrades and force-recreate a stale Job
	// (which, having a deterministic name and an immutable pod template, would
	// otherwise be silently adopted by an AlreadyExists-tolerant create).
	script := renderGpexpandScript(cluster, oldCount, newCount)
	podSpec := b.buildGpexpandPodSpecFromScript(cluster, script)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GpexpandJobName(cluster.Name, oldCount, newCount),
			Namespace: cluster.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				util.AnnotationScaleStarted:       timestamp,
				util.AnnotationGpexpandScriptHash: util.ShortHash(util.ComputeStringHash(script)),
			},
			OwnerReferences: []metav1.OwnerReference{ownerRef(cluster)},
		},
		Spec: gpexpandJobSpec(labels, &podSpec),
	}
}

// gpexpandJobSpec builds the JobSpec for the gpexpand Job. Unlike the backup Job
// spec it pins backoffLimit=0 and a gpexpand-specific activeDeadlineSeconds (see
// the const docs): gpexpand is resumable, so the operator owns retries with its
// bounded attempt counter, and the Job must fail terminally (not silently retry)
// when it wedges.
func gpexpandJobSpec(
	labels map[string]string,
	podSpec *corev1.PodSpec,
) batchv1.JobSpec {
	backoff := gpexpandBackoffLimit
	deadline := gpexpandActiveDeadlineSeconds
	ttl := gpexpandTTLSecondsAfterFinished

	return batchv1.JobSpec{
		BackoffLimit:            &backoff,
		ActiveDeadlineSeconds:   &deadline,
		TTLSecondsAfterFinished: &ttl,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec:       *podSpec,
		},
	}
}

// buildGpexpandPodSpecFromScript builds the pod spec for the gpexpand Job from an
// already-rendered coordinator-exec script (the caller hashes the same script for
// drift detection, so it is rendered once). It reuses the backup env (PG*
// connection vars + admin password Secret) and the shared cluster SSH identity
// (addBackupSSHIdentity) — the same identity gpbackup uses to dispatch to
// segments, which gpexpand needs to basebackup+init the new ones.
func (b *DefaultBuilder) buildGpexpandPodSpecFromScript(
	cluster *cbv1alpha1.CloudberryCluster,
	script string,
) corev1.PodSpec {
	container := corev1.Container{
		Name:                     gpexpandContainerName,
		Image:                    backupImage(cluster),
		Command:                  []string{shellCommand, shellFlag},
		Args:                     []string{script},
		Env:                      buildBackupEnv(cluster),
		VolumeMounts:             buildBackupVolumeMounts(cluster),
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}
	applyJobTemplateContainer(cluster, &container)

	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    []corev1.Container{container},
		Volumes:       buildBackupVolumes(cluster),
	}
	// gpexpand dispatches over SSH to the new segments (basebackup + segment
	// init) exactly like gpbackup, so it needs the SHARED cluster SSH identity.
	addBackupSSHIdentity(cluster, &podSpec)
	applyJobTemplatePod(cluster, &podSpec)
	return podSpec
}

// gpexpandSegmentLine describes one new segment (primary or mirror) rendered
// into the gpexpand input file.
type gpexpandSegmentLine struct {
	// hostname is the segment pod FQDN (<pod>.<seg-hl>), used as both hostname
	// and address in the input file.
	hostname string
	// port is the segment listen port.
	port int32
	// dataDir is the segment data directory (/data/pgdata/gpseg<content>).
	dataDir string
	// content is the segment content id.
	content int32
	// role is 'p' (primary) or 'm' (mirror).
	role string
}

// newSegmentLines returns the gpexpand input-file lines for the newly-desired
// segments (content ids [oldCount,newCount)). Mirror lines are emitted only
// when segment mirroring is enabled. The lines are ordered primaries-first then
// their mirrors, content-ascending, so the rendered input file is deterministic.
func newSegmentLines(
	cluster *cbv1alpha1.CloudberryCluster,
	oldCount, newCount int32,
) []gpexpandSegmentLine {
	port := resolvePort(cluster)
	segSvc := util.SegmentServiceName(cluster.Name)
	mirrorEnabled := cluster.Spec.Segments.Mirroring != nil &&
		cluster.Spec.Segments.Mirroring.Enabled

	lines := make([]gpexpandSegmentLine, 0, int(newCount-oldCount)*2)
	for content := oldCount; content < newCount; content++ {
		lines = append(lines, gpexpandSegmentLine{
			hostname: fmt.Sprintf("%s-%d.%s", util.SegmentPrimaryName(cluster.Name), content, segSvc),
			port:     port,
			dataDir:  fmt.Sprintf("%s%d", segmentDataDirPrefix, content),
			content:  content,
			role:     "p",
		})
	}
	if !mirrorEnabled {
		return lines
	}
	for content := oldCount; content < newCount; content++ {
		lines = append(lines, gpexpandSegmentLine{
			hostname: fmt.Sprintf("%s-%d.%s", util.SegmentMirrorName(cluster.Name), content, segSvc),
			port:     port,
			dataDir:  fmt.Sprintf("%s%d", segmentDataDirPrefix, content),
			content:  content,
			role:     "m",
		})
	}
	return lines
}

// dbidPlaceholder is the token rendered in the dbid column of every gpexpand
// input line by the operator. The bundled Cloudberry 2.1.0 gpexpand REQUIRES a
// valid integer dbid for each new segment (an empty dbid fails with
// "Invalid dbid on line N"), and the correct next dbids can only be known from
// the LIVE cluster catalog (SELECT max(dbid) FROM gp_segment_configuration).
// The operator therefore renders this placeholder and the inner script
// (renderGpexpandInnerScript) substitutes consecutive real dbids (max+1, max+2,
// … in the file's deterministic primaries-then-mirrors order) at run time inside
// the coordinator pod. Computing the dbids in-script keeps the input correct
// even if the topology shifted between reconcile and Job execution (no
// TOCTOU window across the controller→Job boundary).
const dbidPlaceholder = "__CBK_DBID__"

// renderGpexpandInputFile renders the gpexpand input file body (one line per new
// segment). The bundled CBDB 2.1 gpexpand consumes the documented pipe-delimited
// column order:
//
//	<hostname>|<address>|<port>|<datadir>|<dbid>|<content>|<preferred_role>
//
// The dbid column carries dbidPlaceholder (NOT an empty field): gpexpand rejects
// an empty dbid, and the real next dbids are computed from the live catalog by
// the inner script (which replaces each placeholder with a consecutive dbid in
// the file's deterministic order). Lines are ordered primaries-first then
// mirrors (see newSegmentLines), matching the max+1, max+2, … assignment.
func renderGpexpandInputFile(lines []gpexpandSegmentLine) string {
	var b strings.Builder
	for _, l := range lines {
		// hostname|address|port|datadir|<placeholder-dbid>|content|role
		fmt.Fprintf(&b, "%s|%s|%d|%s|%s|%d|%s\n",
			l.hostname, l.hostname, l.port, l.dataDir, dbidPlaceholder, l.content, l.role)
	}
	return b.String()
}

// renderGpexpandScript builds the bash script the gpexpand Job container runs.
// It stages the operator-rendered input file into the coordinator pod (base64
// piped over kubectl exec — no quoting/here-doc hazard across the exec
// boundary, the same technique backup_builder uses), then executes the inner
// gpexpand flow INSIDE the coordinator pod where the catalog datadir
// (/data/pgdata/gpseg-1), a writable filesystem and the segment SSH topology all
// exist. The rendered script is validated by `bash -n` in the unit tests.
func renderGpexpandScript(
	cluster *cbv1alpha1.CloudberryCluster,
	oldCount, newCount int32,
) string {
	coordPod := util.CoordinatorPodName(cluster.Name)
	inputBody := renderGpexpandInputFile(newSegmentLines(cluster, oldCount, newCount))

	inner := renderGpexpandInnerScript()

	// The NEW-segment content-id base (== oldCount). The inner cleanup is bound
	// STRICTLY to content ids in [base, newCount): only rows for the segments
	// THIS expansion adds may ever be removed, so a stale/half-registered "down"
	// row left by a prior aborted `gpexpand -i` is cleaned while every existing
	// healthy segment (content < base) is provably untouched (no TOCTOU across
	// the controller->Job boundary: base is the reconcile-time old count).
	base := strconv.Itoa(int(oldCount))

	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString(gpEnvPreamble)
	// Resolve a kubectl binary (PATH, then common install dirs) — same probe as
	// the backup coordinator-exec wrapper.
	b.WriteString("KUBECTL=" + kubectlBin + "\n")
	b.WriteString("command -v \"${KUBECTL}\" >/dev/null 2>&1 || " +
		"{ for p in /usr/local/bin/kubectl /usr/bin/kubectl; do " +
		"[ -x \"$p\" ] && KUBECTL=\"$p\" && break; done; }\n")
	fmt.Fprintf(&b, "COORD_POD=%s\n", shellQuote(coordPod))
	fmt.Fprintf(&b, "GPEXPAND_INPUT=%s\n", shellQuote(gpexpandInputFile))

	// Stage the operator-rendered input file inside the coordinator pod (base64
	// piped so no quoting hazard crosses the exec boundary). The SAME single
	// coordinator exec ALSO performs the STALE-INPUT PURGE as its very first
	// action (fixes the iter22 wedge AND the iter23 exit-2 wedge):
	//
	//   - iter22: a PREVIOUS Job attempt may have left an input file on the
	//     coordinator's /tmp (or a persistent PVC) that still carries the
	//     UNRESOLVED __CBK_DBID__ placeholder; gpexpand -r / -i would then read
	//     that stale file and abort with "Invalid dbid on line 1".
	//   - iter23: the earlier fix used a SEPARATE `kubectl exec -- bash -c 'rm -f
	//     "$0" ...' "${GPEXPAND_INPUT}"` purge. Passing an extra positional arg to
	//     `kubectl exec ... -- bash -c '<cmd>' <arg>` is fragile across kubectl
	//     versions and here produced a shell usage error (exit code 2), killing
	//     the Job pod right after the settle. Folding the purge into the SAME
	//     inner staging command — with the input path written as a LITERAL string
	//     (NOT `$0`, which inside the remote shell is the shell's own name) —
	//     removes the extra exec, the argv-plumbing hazard, and the exit-2.
	//
	// So this single exec: (1) rm -f's ALL stale gpexpand input artifacts (the
	// primary input path, its `.orig` copy, and the transient `.dbid`
	// substitution temp) using the LITERAL path, then (2) writes the fresh
	// base64-decoded input. This guarantees a completely clean slate so no prior
	// placeholder-laden file is ever consumed. The rm -f is idempotent (never
	// fails on a missing file) and thus retry-safe.
	//
	// The literal path is shell-quoted once (single string) and reused for all
	// four references, so the remote shell never depends on argv plumbing.
	litInput := shellQuote(gpexpandInputFile)
	stageCmd := fmt.Sprintf(
		"rm -f %[1]s %[1]s.orig %[1]s.dbid 2>/dev/null || true; base64 -d > %[1]s",
		litInput)
	fmt.Fprintf(&b, "GPEXPAND_INPUT_B64=%s\n", shellQuote(base64OfString(inputBody)))
	fmt.Fprintf(&b, "printf '%%s' \"${GPEXPAND_INPUT_B64}\" | "+
		"\"${KUBECTL}\" exec -i \"${COORD_POD}\" -- bash -c %s\n",
		shellQuote(stageCmd))

	// Materialize the (readable) inner gpexpand script via a QUOTED heredoc
	// (delimiter quoted => NO expansion here; the env inside it expands in the
	// coordinator at run time), keeping the gpexpand flags visible in args[0].
	b.WriteString("INNER_GPEXPAND=$(cat <<'_CBK_GPEXPAND_EOF_'\n")
	b.WriteString(inner)
	b.WriteString("_CBK_GPEXPAND_EOF_\n)\n")

	// base64 this pod's live connection values (env-safe for any password) and
	// pass them as POSITIONAL args to the remote bash, which runs the inner
	// gpexpand script (piped on stdin as base64 so no expansion hazard crosses
	// the exec boundary). They land as $1..$5 inside the inner script
	// ($5 = the NEW-segment content-id base that bounds the stale-row cleanup).
	b.WriteString("PORT_B64=$(printf '%s' \"${PGPORT:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("USER_B64=$(printf '%s' \"${PGUSER:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("PASS_B64=$(printf '%s' \"${PGPASSWORD:-}\" | base64 | tr -d '\\n')\n")
	b.WriteString("INPUT_B64=$(printf '%s' \"${GPEXPAND_INPUT}\" | base64 | tr -d '\\n')\n")
	fmt.Fprintf(&b, "BASE_B64=$(printf '%%s' %s | base64 | tr -d '\\n')\n", shellQuote(base))

	remoteExec := "printf '%s' \"${INNER_GPEXPAND}\" | base64 | " +
		"\"${KUBECTL}\" exec -i \"${COORD_POD}\" -- bash -c '" +
		"INNER=$(base64 -d); " +
		"bash -c \"${INNER}\" _ \"$0\" \"$1\" \"$2\" \"$3\" \"$4\"' " +
		"\"${PORT_B64}\" \"${USER_B64}\" \"${PASS_B64}\" \"${INPUT_B64}\" \"${BASE_B64}\""

	// Capture the inner flow's combined output (tee'd to the Job log) so the
	// EXPAND_RESULT marker can be surfaced on /dev/termination-log for the
	// controller. pipefail (set at the top) re-raises the remote exit code
	// through the tee, so a genuine gpexpand failure still fails the Job.
	fmt.Fprintf(&b, "CBK_EXPAND_LOG=$(%s 2>&1 | tee /dev/stderr)\n", remoteExec)
	fmt.Fprintf(&b,
		"CBK_EXPAND_RESULT=$(printf '%%s' \"${CBK_EXPAND_LOG}\" | "+
			"grep -oE '%s[a-zA-Z0-9:_-]+' | tail -1 || true)\n",
		expandResultMarker)
	b.WriteString("if [ -n \"${CBK_EXPAND_RESULT:-}\" ]; then " +
		"printf '%s' \"${CBK_EXPAND_RESULT}\" > /dev/termination-log 2>/dev/null || true; " +
		"echo \"${CBK_EXPAND_RESULT}\"; fi\n")
	return b.String()
}

// renderGpexpandInnerScript renders the gpexpand flow that runs INSIDE the
// coordinator pod. Positional args (base64-decoded): $1=port, $2=user,
// $3=password, $4=input file path, $5=new-segment content-id base (== oldCount,
// bounds the stale-row cleanup). It:
//
//  1. sources the Cloudberry env so gpexpand/psql are on PATH and exports the
//     coordinator catalog datadir + connection env;
//  2. re-asserts the ~/.ssh/config `Port 2022` override + GPSSH_SSH_PORT so
//     gpexpand's segment-init SSHes to the rootless sshd (restricted-v2 SCC),
//     and TCP-probes 2022 on every new segment host (fail fast if unreachable);
//  3. gates on gpexpand.status for idempotent resume (skip when COMPLETED);
//  4. RESUME/CLEANUP: on empty/SETUP (or unexpected) state runs a FULL clean
//     pre-state — `gpexpand -r` (rollback stale setup) + `DROP SCHEMA IF EXISTS
//     gpexpand CASCADE` (remove any leftover gpexpand catalog schema `-r` missed)
//     + a GUARDED stale-segment cleanup (delete only down/unknown rows for the
//     NEW content ids [base, target) that a prior aborted `gpexpand -i` left) +
//     an existing-segment (content < base) health RE-VERIFY as a bounded
//     RETRY/WAIT loop (polls until all existing segments are up, tolerating the
//     transient scale-up blip; fails fast only on a PERSISTENT real cluster
//     fault) — then REGENERATES the input file; on SETUP DONE /
//     EXPANSION STARTED/STOPPED skips add-segment and resumes with `gpexpand -a`
//     WITHOUT wiping (a real expansion is in progress);
//  5. PHASE A (only when no expansion is set up): assigns REAL consecutive dbids
//     (max(dbid)+NR, DEFECT A fix) then gpexpand -i <input> -a — the DB is
//     selected via PGDATABASE (Cloudberry 2.1.0 gpexpand has no -D option);
//  6. PHASE B: gpexpand -a -d <duration>  (redistribution incl.
//     ALTER TABLE ... EXPAND TABLE) — resumable via status_detail;
//  7. verifies completion, runs gpexpand --clean, removes the input file, and
//     emits EXPAND_RESULT=... as row/segment evidence.
func renderGpexpandInnerScript() string {
	var b strings.Builder
	b.WriteString("set -euo pipefail\n")
	b.WriteString("export PGPORT=$(printf '%s' \"$1\" | base64 -d)\n")
	b.WriteString("export PGUSER=$(printf '%s' \"$2\" | base64 -d)\n")
	b.WriteString("export PGPASSWORD=$(printf '%s' \"$3\" | base64 -d)\n")
	b.WriteString("GPEXPAND_INPUT=$(printf '%s' \"$4\" | base64 -d)\n")
	// EXPAND_BASE is the NEW-segment content-id base (== oldCount). Every stale-
	// row cleanup DELETE is bound to content >= EXPAND_BASE, so existing healthy
	// segments (content < base) can NEVER be touched. Validate it is a run of
	// digits (fail closed) so it can be safely interpolated into SQL predicates.
	b.WriteString("EXPAND_BASE=$(printf '%s' \"$5\" | base64 -d)\n")
	b.WriteString("if ! printf '%s' \"${EXPAND_BASE:-}\" | grep -qE '^[0-9]+$'; then " +
		"echo 'gpexpand: invalid expansion base content id' >&2; exit 1; fi\n")
	fmt.Fprintf(&b, "export PGDATABASE=%s\n", gpexpandStatusDatabase)
	fmt.Fprintf(&b, "export COORDINATOR_DATA_DIRECTORY=%s\n", coordinatorCatalogDataDir)
	// MASTER_DATA_DIRECTORY is the legacy alias some gpexpand builds still read;
	// export both so the utility finds the catalog datadir regardless.
	fmt.Fprintf(&b, "export MASTER_DATA_DIRECTORY=%s\n", coordinatorCatalogDataDir)
	// Source the Cloudberry env so gpexpand/psql/gpstate are on PATH (guarded
	// no-op when absent; matches the backup coordinator-exec inner tool).
	b.WriteString("GPENV=$(ls \"${GPHOME:-/usr/local/cloudberry-db}\"/greenplum_path.sh " +
		"\"${GPHOME:-/usr/local/cloudberry-db}\"/cloudberry-env.sh 2>/dev/null | head -1 || true)\n")
	b.WriteString("if [ -n \"${GPENV}\" ]; then . \"${GPENV}\"; fi\n")
	b.WriteString("export PATH=\"${GPHOME:-/usr/local/cloudberry-db}/bin:${PATH}\"\n")

	// Initial SETTLE wait: give the StatefulSet scale-up churn (headless-service
	// DNS update / controller rollout) a moment to quiesce before the first
	// existing-segment health poll, so the EXISTING segments (content < base) have
	// begun recovering from the brief scale-up connectivity blip. This is a small
	// head start; the bounded retry loop in writeInnerExistingSegmentHealthCheck
	// is the primary fix that actually tolerates the blip.
	fmt.Fprintf(&b, "echo 'gpexpand: settling %ds for scale-up churn to quiesce' >&2\n",
		gpexpandSettleSeconds)
	fmt.Fprintf(&b, "sleep %d\n", gpexpandSettleSeconds)

	// SSH PORT REDIRECT (restricted-v2 SCC): the cluster pods run a ROOTLESS
	// sshd on 2022 (no NET_BIND_SERVICE / no root => no privileged :22). gpexpand
	// shells out to ssh/gpssh/gpsync, which honor the per-host `Port` directive
	// in ~/.ssh/config. Re-assert the override here (inside the coordinator pod)
	// so gpexpand's segment-init reaches 2022 even if the coordinator's baked
	// config drifted, and export GPSSH_SSH_PORT (honored by some gpssh builds,
	// harmless otherwise).
	writeInnerSSHConfig(&b)

	// TCP-2022 reachability probe for every NEW segment host (field 1 of each
	// input line). Fail fast with a clear message if the rootless sshd is not yet
	// accepting on 2022, instead of letting gpexpand emit a raw
	// "ssh: connect ... port 2022: Connection refused" mid-init.
	writeInnerSegmentProbe(&b)

	// EARLY DBID RESOLUTION (fixes the iter22 stale-placeholder wedge): resolve
	// the __CBK_DBID__ placeholder to REAL consecutive dbids IN PLACE in the
	// ACTUAL GPEXPAND_INPUT path RIGHT NOW — BEFORE any gpexpand -r / -i and
	// BEFORE the .orig snapshot is taken. Previously the substitution ran only
	// inside PHASE A (just before gpexpand -i), so gpexpand -r (in the fresh
	// cleanup) read the still-placeholder'd file and aborted with
	// "Invalid dbid on line 1". Doing it here guarantees every downstream
	// consumer (-r, -i) reads a fully-resolved file. The fail-closed grep guard
	// inside writeInnerDbidAssignment asserts NO placeholder remains before we
	// proceed. This is idempotent (a placeholder-free line is passed through
	// unchanged), so it is safe to re-run on the fresh-path regenerate below.
	writeInnerDbidAssignment(&b)

	// Idempotency / resume gate on gpexpand.status. Snapshot the ALREADY-RESOLVED
	// input file (dbids substituted above) into `.orig` so it can be regenerated
	// cleanly on a retry (see writeInnerResumeGuard) WITHOUT ever reintroducing
	// the placeholder — the snapshot is taken AFTER writeInnerDbidAssignment, so
	// the `.orig` is placeholder-free by construction.
	//
	// NOTE: the unconditional "refusing to expand with down/unknown segments"
	// pre-flight was REPLACED by the state-aware health checks inside the resume
	// guard. A prior aborted `gpexpand -i` can leave stale/half-registered
	// "down" rows for the NEW content ids; failing on those upfront wedged the
	// scale-out forever. Instead, on the FRESH path we CLEAN those stale rows
	// (bounded to content >= base) and then RE-VERIFY only the EXISTING segments
	// (content < base) are up; on the RESUME path we likewise verify only the
	// existing segments (a real in-progress expansion legitimately has
	// not-yet-up new-segment rows).
	b.WriteString("GPEXPAND_INPUT_ORIG=\"${GPEXPAND_INPUT}.orig\"\n")
	b.WriteString("cp -f \"${GPEXPAND_INPUT}\" \"${GPEXPAND_INPUT_ORIG}\" 2>/dev/null || true\n")
	writeInnerStatusRead(&b, "STATUS")
	fmt.Fprintf(&b, "if [ \"${STATUS}\" = \"COMPLETED\" ]; then "+
		"echo \"%s%s\"; exit 0; fi\n",
		expandResultMarker, expandResultAlreadyComplete)

	// RESUME / CLEANUP + PHASE A (add + init) wrapped in a BOUNDED RETRY loop.
	//
	// gpexpand's segment-init rsync's/ssh's to the new segment; that connection
	// can hit a transient "Connection refused" when the new segment pod is
	// (re)started between the pre-flight 2022 probe and gpexpand's own rsync
	// (StatefulSet churn / wedge recovery). To absorb such a mid-init blip,
	// `gpexpand -i` is retried up to gpexpandInitMaxAttempts times; before EACH
	// attempt the loop RE-RUNS the fresh cleanup + input regeneration + dbid
	// re-substitution (so gpexpand.status never wedges), RE-PROBES sshd:2022 on
	// every new segment host until stable, and RE-VERIFIES the existing segments
	// are healthy. A genuine persistent failure after all attempts fails the Job
	// as before. See writeInnerAddSegmentWithRetry.
	writeInnerAddSegmentWithRetry(&b)

	// PHASE B: redistribution (ALTER TABLE ... EXPAND TABLE), bounded duration,
	// resumable via status_detail. -a = non-interactive. DB selected via
	// PGDATABASE (no -D option in Cloudberry 2.1.0 gpexpand).
	fmt.Fprintf(&b, "gpexpand -a -d %s -v\n", gpexpandRedistributeDuration)

	// Verify completion via status_detail, then clean up the expansion schema.
	b.WriteString("REMAINING=$(psql -tA -d " + gpexpandStatusDatabase +
		" -c \"SELECT count(*) FROM gpexpand.status_detail WHERE status <> 'COMPLETED';\" " +
		"2>/dev/null | tr -d '[:space:]' || echo 1)\n")
	b.WriteString("if [ \"${REMAINING:-1}\" = \"0\" ]; then\n")
	// FINALIZATION (D9): verify-and-fall-back clean so the Job never reports
	// success while the gpexpand schema lingers (which blocks a subsequent
	// gpbackup with "expansion currently in process"). DB selected via
	// PGDATABASE (no -D option in Cloudberry 2.1.0 gpexpand). gpexpand -c can
	// fail if the schema is half-finalized, so drop it directly as a fallback.
	b.WriteString("  if ! gpexpand --clean -a; then\n")
	b.WriteString("    psql -d " + gpexpandStatusDatabase +
		" -c \"DROP SCHEMA IF EXISTS gpexpand CASCADE;\" || true\n")
	b.WriteString("  fi\n")
	// Drop the staged input file(s) so a stale format can never leak into a
	// subsequent expansion (idempotent cleanup).
	b.WriteString("  rm -f \"${GPEXPAND_INPUT}\" \"${GPEXPAND_INPUT_ORIG}\" 2>/dev/null || true\n")
	// Assert the gpexpand schema is actually gone BEFORE emitting
	// EXPAND_RESULT=completed; otherwise fail with a distinct finalize marker so
	// the operator does not declare scale-out complete over a lingering schema.
	b.WriteString("  LEFT=$(psql -tA -d " + gpexpandStatusDatabase +
		" -c \"SELECT count(*) FROM information_schema.schemata WHERE schema_name='gpexpand';\" " +
		"2>/dev/null | tr -d '[:space:]' || echo 1)\n")
	fmt.Fprintf(&b, "  if [ \"${LEFT:-1}\" != \"0\" ]; then "+
		"echo \"%s%s\" >&2; exit 1; fi\n",
		expandResultMarker, expandResultFinalizeFailed)
	// Row/segment evidence: report the number of segments now configured.
	b.WriteString("  SEGS=$(psql -tA -d " + gpexpandStatusDatabase +
		" -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 AND role='p'\" " +
		"2>/dev/null | tr -d '[:space:]' || echo '?')\n")
	fmt.Fprintf(&b, "  echo \"%s%s segments=${SEGS}\"\n",
		expandResultMarker, expandResultCompleted)
	b.WriteString("else\n")
	fmt.Fprintf(&b, "  echo \"%sincomplete:${REMAINING}\" >&2\n", expandResultMarker)
	b.WriteString("  exit 1\n")
	b.WriteString("fi\n")
	return b.String()
}

// writeInnerAddSegmentWithRetry emits the RESUME/CLEANUP + PHASE-A (add + init)
// block wrapped in a BOUNDED RETRY loop that absorbs a transient segment-restart
// blip (rsync/ssh "Connection refused") during gpexpand's segment-init.
//
// Each of the up-to-gpexpandInitMaxAttempts iterations:
//  1. RE-PROBES sshd:2022 on every new segment host until it is accepting
//     (writeInnerSegmentProbe), so a mid-restart pod is caught BEFORE gpexpand's
//     own rsync rather than surfacing as a raw refusal;
//  2. RE-RUNS the resume/cleanup branch (writeInnerResumeGuard): on the fresh
//     path it re-establishes a clean pre-state (gpexpand -r + DROP SCHEMA +
//     content>=base stale-row cleanup) and REGENERATES the input file, and on
//     BOTH paths re-verifies the existing segments are healthy — so gpexpand.status
//     never wedges across retries and each attempt starts from a converged state;
//  3. RE-READS status and, only when no expansion is set up yet (STATUS2 empty),
//     re-substitutes dbids (idempotent) and runs `gpexpand -i`. Its exit code is
//     captured (not allowed to abort under `set -e`); on a NON-zero exit it
//     backs off gpexpandInitRetrySleepSeconds and retries (unless this was the
//     last attempt, in which case it fails the Job as before).
//
// On success it breaks out; a genuine persistent failure after all attempts
// exits 1 (unchanged terminal-failure behavior). When STATUS2 is non-empty the
// add-segment phase is legitimately skipped (resume path) and the loop succeeds
// immediately so PHASE B (redistribution) resumes.
func writeInnerAddSegmentWithRetry(b *strings.Builder) {
	fmt.Fprintf(b, "echo 'gpexpand: add-segment phase (up to %d attempts, "+
		"re-probe + re-clean each retry to absorb a transient segment-restart blip)' >&2\n",
		gpexpandInitMaxAttempts)
	b.WriteString("CBK_GPEXPAND_I_OK=0\n")
	fmt.Fprintf(b, "for CBK_ATTEMPT in $(seq 1 %d); do\n", gpexpandInitMaxAttempts)
	fmt.Fprintf(b, "  echo \"gpexpand: add-segment attempt ${CBK_ATTEMPT}/%d\" >&2\n",
		gpexpandInitMaxAttempts)

	// (1) RE-PROBE sshd:2022 on every new segment host (catches a mid-restart
	// pod that came back between a prior attempt and now).
	writeInnerSegmentProbe(b)

	// (2) RE-READ gpexpand.status each attempt so the resume/cleanup branch makes
	// a FRESH decision (a prior attempt may have advanced/aborted the state). This
	// overwrites the pre-loop ${STATUS} read so retries never act on a stale value.
	writeInnerStatusRead(b, "STATUS")

	// (3) RE-RUN the resume/cleanup branch: fresh clean pre-state + input
	// regeneration (fresh path) or resume-echo (in-progress path), then the
	// existing-segment health re-verify. Idempotent across retries.
	writeInnerResumeGuard(b)

	// (4) RE-READ status; run gpexpand -i only when no expansion is set up yet.
	writeInnerStatusRead(b, "STATUS2")
	b.WriteString("  if [ -z \"${STATUS2}\" ]; then\n")
	// bash ignores leading whitespace, so the (unindented) dbid-assignment
	// emitter is valid inside this if-branch.
	writeInnerDbidAssignment(b)
	// Capture the exit code so a transient `gpexpand -i` failure does NOT abort
	// under `set -e`; retry on a non-zero exit, fail terminally on the last one.
	b.WriteString("    CBK_I_RC=0\n")
	b.WriteString("    gpexpand -i \"${GPEXPAND_INPUT}\" -a -v || CBK_I_RC=$?\n")
	b.WriteString("    if [ \"${CBK_I_RC}\" != \"0\" ]; then\n")
	b.WriteString("      echo \"gpexpand: -i attempt ${CBK_ATTEMPT} failed " +
		"(rc=${CBK_I_RC}; transient rsync/ssh blip?)\" >&2\n")
	fmt.Fprintf(b, "      if [ \"${CBK_ATTEMPT}\" -lt %d ]; then "+
		"echo 'gpexpand: backing off then re-probing + re-cleaning for retry' >&2; "+
		"sleep %d; continue; fi\n",
		gpexpandInitMaxAttempts, gpexpandInitRetrySleepSeconds)
	fmt.Fprintf(b, "      echo 'gpexpand: -i failed after %d attempts "+
		"(persistent failure)' >&2; exit 1\n", gpexpandInitMaxAttempts)
	b.WriteString("    fi\n")
	b.WriteString("  fi\n")
	// Reached here => gpexpand -i succeeded (or was legitimately skipped on the
	// resume path). Mark success and break out of the retry loop.
	b.WriteString("  CBK_GPEXPAND_I_OK=1\n")
	b.WriteString("  break\n")
	b.WriteString("done\n")
	b.WriteString("if [ \"${CBK_GPEXPAND_I_OK}\" != \"1\" ]; then " +
		"echo 'gpexpand: add-segment did not complete after retries' >&2; exit 1; fi\n")
}

// writeInnerSSHConfig emits the ~/.ssh/config `Port 2022` re-assert + the
// GPSSH_SSH_PORT export at the top of the inner gpexpand flow. Under the scoped
// restricted-v2 SCC the cluster pods run a rootless sshd on 2022, so gpexpand's
// ssh/gpssh/gpsync must target 2022 (OpenSSH client honors the per-host `Port`).
// The heredoc delimiter is quoted so nothing expands here; the file is written
// verbatim inside the coordinator pod.
func writeInnerSSHConfig(b *strings.Builder) {
	b.WriteString("mkdir -p ~/.ssh && chmod 700 ~/.ssh\n")
	fmt.Fprintf(b, "cat > ~/.ssh/config <<'EOF'\n"+
		"Host *\n"+
		"  Port %s\n"+
		"  StrictHostKeyChecking no\n"+
		"  UserKnownHostsFile /dev/null\n"+
		"  LogLevel ERROR\n"+
		"  BatchMode yes\n"+
		"EOF\n", clusterSSHPort)
	b.WriteString("chmod 600 ~/.ssh/config\n")
	fmt.Fprintf(b, "export GPSSH_SSH_PORT=%s\n", clusterSSHPort)
}

// writeInnerSegmentProbe emits a bounded TCP-2022 reachability probe for every
// NEW segment host (field 1 of each input-file line). It fails fast with a clear
// message if the rootless sshd is not yet accepting on 2022, instead of letting
// gpexpand -i emit a raw "Connection refused" mid-init. Uses bash's /dev/tcp so
// no nc dependency is required.
func writeInnerSegmentProbe(b *strings.Builder) {
	// Extract the unique segment hostnames from the input file (field 1).
	b.WriteString("SEG_HOSTS=$(awk -F'|' 'NF>0 {print $1}' \"${GPEXPAND_INPUT}\" | sort -u)\n")
	b.WriteString("for h in ${SEG_HOSTS}; do\n")
	b.WriteString("  ok=0\n")
	fmt.Fprintf(b, "  for _ in $(seq 1 %d); do\n", segmentProbeRetries)
	fmt.Fprintf(b, "    if (exec 3<>\"/dev/tcp/${h}/%s\") 2>/dev/null; then "+
		"exec 3>&- 3<&-; ok=1; break; fi\n", clusterSSHPort)
	fmt.Fprintf(b, "    sleep %d\n", segmentProbeSleepSeconds)
	b.WriteString("  done\n")
	fmt.Fprintf(b, "  if [ \"${ok}\" != \"1\" ]; then "+
		"echo \"gpexpand: segment ${h} not reachable on %s (rootless sshd)\" >&2; "+
		"exit 1; fi\n", clusterSSHPort)
	b.WriteString("done\n")
}

// writeInnerStatusRead emits the `<var>=$(psql ... gpexpand.status ...)` read of
// the most-recent gpexpand.status row (whitespace-trimmed; empty when the schema
// does not exist). Reused for the initial gate and the post-rollback re-read so
// the resume logic always reflects the CURRENT catalog state (no TOCTOU window).
func writeInnerStatusRead(b *strings.Builder, varName string) {
	fmt.Fprintf(b, "%s=$(psql -tA -d %s "+
		"-c \"SELECT status FROM gpexpand.status ORDER BY updated DESC LIMIT 1;\" "+
		"2>/dev/null | tr -d '[:space:]' || true)\n",
		varName, gpexpandStatusDatabase)
}

// writeInnerResumeGuard emits the resume/cleanup branch on the prior
// gpexpand.status (${STATUS}). Empty / SETUP / unexpected states establish a
// fully CLEAN pre-state (see writeInnerFreshCleanup) and regenerate the input
// file so a fresh `gpexpand -i` runs cleanly; redistribution states (SETUP
// DONE / EXPANSION STARTED / EXPANSION STOPPED) are left untouched (a REAL
// expansion is in progress) to resume with `gpexpand -a`. Both branches then
// RE-VERIFY that every EXISTING segment (content < base) is up. A `case` is
// avoided (some bash builds mis-parse `;;` inside a quoted heredoc under
// `bash -n`); an if/else chain is used instead.
func writeInnerResumeGuard(b *strings.Builder) {
	b.WriteString("if [ \"${STATUS}\" = \"SETUPDONE\" ] || " +
		"[ \"${STATUS}\" = \"EXPANSIONSTARTED\" ] || " +
		"[ \"${STATUS}\" = \"EXPANSIONSTOPPED\" ]; then\n")
	// RESUME PATH: a real expansion is set up or in progress. Do NOT wipe (that
	// would discard progress); resume with gpexpand -a below.
	b.WriteString("  echo 'gpexpand: expansion in progress; resuming with gpexpand -a (no cleanup)' >&2\n")
	b.WriteString("else\n")
	// FRESH PATH: empty / SETUP / any unexpected state -> the prior add-segment
	// setup did not finish. Establish a fully clean pre-state and regenerate the
	// input file.
	writeInnerFreshCleanup(b)
	b.WriteString("  rm -f \"${GPEXPAND_INPUT}\" 2>/dev/null || true\n")
	b.WriteString("  cp -f \"${GPEXPAND_INPUT_ORIG}\" \"${GPEXPAND_INPUT}\" 2>/dev/null || true\n")
	b.WriteString("fi\n")
	// BOTH paths: re-verify the EXISTING (pre-scale) segments are up. A genuinely
	// down existing segment is a real cluster fault -> fail fast (do not proceed).
	writeInnerExistingSegmentHealthCheck(b)
}

// writeInnerFreshCleanup emits the FRESH-expansion clean-pre-state block (runs
// ONLY when no real expansion is in progress). It guarantees gpexpand's preflight
// passes even after a prior aborted `gpexpand -i` OR a reused coordinator PVC that
// still carries stale gpexpand catalog state from a PREVIOUS cluster incarnation
// (the live-report wedge: `gpexpand -r` cannot roll back cleanly because the stale
// rows reference segments/dbids that no longer exist). It self-heals by:
//
//  1. `gpexpand -r -a` — the supported rollback: removes gpexpand's own added
//     segments/state. Its failure is TOLERATED (`|| true`): when the stale rows it
//     references point at segments that no longer exist, `-r` errors, and that must
//     NOT abort the fresh expansion. The manual cleanup below is the real remover.
//  2. `DROP SCHEMA IF EXISTS gpexpand CASCADE` — removes any leftover gpexpand
//     catalog schema that `-r` did not clean (residue that persists on the reused
//     coordinator PVC across cluster recreations). NOT gated on `-r` success.
//  3. UNGATED stale NEW-segment row cleanup, bounded STRICTLY to
//     `content >= ${EXPAND_BASE}` (regardless of status). This runs whether or not
//     `gpexpand -r` succeeded.
//
// FRESH-vs-RESUME INVARIANT (why deleting ALL content>=base rows on the fresh path
// is safe): a GENUINE in-progress expansion always sets a redistribution
// gpexpand.status (SETUP DONE / EXPANSION STARTED / EXPANSION STOPPED), which
// routes to the RESUME branch (writeInnerResumeGuard's then-arm) and NEVER reaches
// this fresh block. Therefore, when we DO reach here (empty/SETUP/unexpected
// status), ANY pre-existing `content >= base` row is stale by definition — it is a
// leftover from a prior aborted attempt or a previous incarnation on the reused
// PVC (wrong dbid/hostname, possibly even marked UP 'u' but orphaned). Removing all
// such rows lets `gpexpand -i` re-add the new segments cleanly. Existing HEALTHY
// segments are provably content < base, so this DELETE can never touch them.
//
// Every step is echoed (slog-style) for observability.
func writeInnerFreshCleanup(b *strings.Builder) {
	b.WriteString("  echo 'gpexpand: fresh expansion; establishing clean pre-state' >&2\n")
	// (1) supported rollback of any stale setup — failure TOLERATED (stale rows
	// may reference segments that no longer exist, which makes -r error; the
	// manual cleanup below is the authoritative remover so -r must not abort).
	b.WriteString("  echo 'gpexpand: rolling back stale setup (gpexpand -r; failure tolerated)' >&2\n")
	b.WriteString("  gpexpand -r -a 2>/dev/null || true\n")
	// (2) drop any leftover gpexpand catalog schema -r did not clean (NOT gated
	// on -r success).
	b.WriteString("  echo 'gpexpand: dropping leftover gpexpand schema (DROP SCHEMA IF EXISTS gpexpand CASCADE)' >&2\n")
	b.WriteString("  psql -tA -d " + gpexpandStatusDatabase +
		" -c \"DROP SCHEMA IF EXISTS gpexpand CASCADE;\" 2>/dev/null || true\n")
	// (3) UNGATED stale NEW-segment row cleanup, bounded STRICTLY to content >=
	// base (any status). This runs even if gpexpand -r failed above. Per the
	// fresh-vs-resume invariant (see the function doc): reaching this block means
	// NO real expansion is in progress (a genuine one would have a redistribution
	// status and route to the resume branch), so EVERY content>=base row here is
	// stale — a half-registered 'down' row from a prior aborted `gpexpand -i`, or
	// an orphaned row (wrong dbid/hostname) marked UP that survived on the reused
	// coordinator PVC. Deleting them all lets `gpexpand -i` re-add cleanly.
	// Existing HEALTHY segments are content < base and are provably untouched.
	b.WriteString("  echo 'gpexpand: fresh path invariant: no in-progress expansion, " +
		"so any content>=base row is stale' >&2\n")
	b.WriteString("  STALE=$(psql -tA -d " + gpexpandStatusDatabase +
		" -c \"SELECT count(*) FROM gp_segment_configuration " +
		"WHERE content >= ${EXPAND_BASE};\" " +
		"2>/dev/null | tr -d '[:space:]' || echo 0)\n")
	b.WriteString("  if [ \"${STALE:-0}\" != \"0\" ]; then\n")
	b.WriteString("    echo \"gpexpand: cleaning ${STALE} stale new-segment row(s) " +
		"(content >= ${EXPAND_BASE}, any status; fresh path => all are stale)\" >&2\n")
	b.WriteString("    psql -tA -d " + gpexpandStatusDatabase +
		" -c \"DELETE FROM gp_segment_configuration " +
		"WHERE content >= ${EXPAND_BASE};\" 2>/dev/null || true\n")
	b.WriteString("  fi\n")
}

// writeInnerExistingSegmentHealthCheck emits the post-cleanup RE-VERIFY that
// every EXISTING (pre-scale) segment — content < ${EXPAND_BASE} — is up
// (status='u'). The stale-row cleanup only ever touches content >= base, so if
// any content < base segment is still down it is either expansion residue (never,
// since cleanup is content>=base bounded) or a real fault — with one caveat: the
// StatefulSet scale-up (adding primary-N/mirror-N) causes a BRIEF headless-service
// DNS / controller-churn blip that momentarily flips the existing segments to
// status<>'u'. On a stable cluster they recover within seconds.
//
// This is therefore a bounded RETRY/WAIT loop (not a single shot): it polls
// `SELECT count(*) ... content < base AND status <> 'u'` every
// gpexpandExistingSegPollSeconds until it returns 0 (all existing segments up ->
// proceed) OR gpexpandExistingSegWaitSeconds elapses. Only a PERSISTENT down state
// (still down after the timeout) is a GENUINE cluster fault -> fail fast with the
// clear message and exit 1. The content<base guard is preserved: the loop never
// weakens the guard, it just tolerates the transient scale-up blip. The query goes
// through the coordinator psql exactly like the (unchanged) other catalog reads.
// The style mirrors the 2022 segment reachability probe (bounded seq loop with a
// progress echo on each retry).
func writeInnerExistingSegmentHealthCheck(b *strings.Builder) {
	attempts := gpexpandExistingSegWaitSeconds / gpexpandExistingSegPollSeconds
	b.WriteString("EXIST_HEALTHY=0\n")
	fmt.Fprintf(b, "for _ in $(seq 1 %d); do\n", attempts)
	b.WriteString("  EXIST_DOWN=$(psql -tA -d " + gpexpandStatusDatabase +
		" -c \"SELECT count(*) FROM gp_segment_configuration " +
		"WHERE content < ${EXPAND_BASE} AND status <> 'u';\" " +
		"2>/dev/null | tr -d '[:space:]' || echo 1)\n")
	b.WriteString("  if [ \"${EXIST_DOWN:-1}\" = \"0\" ]; then EXIST_HEALTHY=1; break; fi\n")
	b.WriteString("  echo \"gpexpand: waiting for existing segments to be up: " +
		"${EXIST_DOWN:-?} down, retrying...\" >&2\n")
	fmt.Fprintf(b, "  sleep %d\n", gpexpandExistingSegPollSeconds)
	b.WriteString("done\n")
	// Only a PERSISTENT down state (still down after the bounded wait) is a real
	// cluster fault. The transient scale-up blip recovers within the loop above.
	b.WriteString("if [ \"${EXIST_HEALTHY}\" != \"1\" ]; then " +
		"echo 'gpexpand: refusing to expand: existing segment(s) down (real cluster fault)' >&2; " +
		"exit 1; fi\n")
	b.WriteString("echo 'gpexpand: existing segments healthy; proceeding' >&2\n")
}

// writeInnerDbidAssignment emits the DEFECT A fix: assign a REAL, consecutive
// dbid to every new segment line, IN PLACE in the actual GPEXPAND_INPUT path
// that gpexpand reads. Cloudberry 2.1.0 gpexpand requires a valid integer dbid
// per line (an empty/placeholder dbid fails with "Invalid dbid on line N"). The
// correct next dbids are only knowable from the LIVE catalog, so compute
// max(dbid) here (inside the coordinator pod, at Job run time — no TOCTOU
// window) and substitute the operator-rendered placeholder token with
// max+1, max+2, … in the file's deterministic primaries-then-mirrors order.
//
// This is emitted at TOP LEVEL (no leading indentation) and runs EARLY —
// BEFORE any `gpexpand -r`/`-i` — so the file those utilities read never carries
// the __CBK_DBID__ placeholder. It is idempotent: awk `sub()` replaces only the
// first placeholder per line, and a line with no placeholder is passed through
// unchanged, so re-running it over an already-substituted file is a safe no-op
// (which makes the fresh-path regenerate → re-substitute sequence retry-safe).
// The trailing fail-closed grep guard asserts NO placeholder survives before the
// caller proceeds to gpexpand.
func writeInnerDbidAssignment(b *strings.Builder) {
	b.WriteString("MAXDBID=$(psql -tA -d " + gpexpandStatusDatabase +
		" -c \"SELECT COALESCE(max(dbid),0) FROM gp_segment_configuration;\" " +
		"2>/dev/null | tr -d '[:space:]')\n")
	// Validate MAXDBID is a non-empty run of digits.
	b.WriteString("if ! printf '%s' \"${MAXDBID:-}\" | grep -qE '^[0-9]+$'; then " +
		"echo 'gpexpand: could not determine max(dbid) from gp_segment_configuration' >&2; " +
		"exit 1; fi\n")
	// Rewrite the input file in place: replace the FIRST placeholder occurrence
	// on each line with the next consecutive dbid (max+1, max+2, …). Written to a
	// `.dbid` temp then atomically mv'd over the ACTUAL GPEXPAND_INPUT path so the
	// substituted output is exactly what gpexpand consumes (never a temp/.orig
	// that gets ignored).
	fmt.Fprintf(b, "awk -v base=\"${MAXDBID}\" -v ph=%s '"+
		"{ n=base+NR; sub(ph, n); print }' \"${GPEXPAND_INPUT}\" > \"${GPEXPAND_INPUT}.dbid\" "+
		"&& mv \"${GPEXPAND_INPUT}.dbid\" \"${GPEXPAND_INPUT}\"\n",
		shellQuote(dbidPlaceholder))
	// FAIL-CLOSED no-placeholder guard: if any __CBK_DBID__ survived (substitution
	// error) abort BEFORE gpexpand ever reads the file, so an unresolved
	// placeholder can never reach gpexpand -r/-i ("Invalid dbid on line N").
	fmt.Fprintf(b, "if grep -q %s \"${GPEXPAND_INPUT}\"; then "+
		"echo 'gpexpand: failed to assign dbids to input file (placeholder survived)' >&2; "+
		"exit 1; fi\n",
		shellQuote(dbidPlaceholder))
}
