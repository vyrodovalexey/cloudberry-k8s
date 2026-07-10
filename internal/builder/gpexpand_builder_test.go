package builder

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestBuildGpexpandJob_Shape asserts the coordinator-exec gpexpand Job:
//   - has the DETERMINISTIC name <cluster>-gpexpand-<old>-<new>,
//   - is owner-referenced to the cluster and carries the gpexpand operation label,
//   - runs as the backup SA (coordinator-exec model) with backoffLimit=0,
//   - mounts the shared cluster SSH identity, and
//   - renders a script that targets <cluster>-coordinator-0 and runs gpexpand.
func TestBuildGpexpandJob_Shape(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()

	job := b.BuildGpexpandJob(cluster, 2, 3, "20260706120000")
	require.NotNil(t, job)

	// Deterministic name derived from the segment counts (NOT the timestamp).
	assert.Equal(t, util.GpexpandJobName(cluster.Name, 2, 3), job.Name)
	assert.Equal(t, "test-cluster-gpexpand-2-3", job.Name)
	assert.Equal(t, cluster.Namespace, job.Namespace)

	// Labels record the operation + the scale bounds.
	assert.Equal(t, gpexpandOperation, job.Labels[util.LabelBackupOperation])
	assert.Equal(t, "2", job.Labels[util.LabelScaleFrom])
	assert.Equal(t, "3", job.Labels[util.LabelScaleTo])

	// Owner reference to the cluster.
	require.Len(t, job.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, job.OwnerReferences[0].Name)

	// gpexpand is resumable -> the operator owns retries, so backoffLimit=0.
	require.NotNil(t, job.Spec.BackoffLimit)
	assert.Equal(t, int32(0), *job.Spec.BackoffLimit)
	require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
	assert.Positive(t, *job.Spec.ActiveDeadlineSeconds)

	podSpec := job.Spec.Template.Spec
	// Coordinator-exec model reuses the backup SA.
	assert.Equal(t, util.BackupServiceAccountName(cluster.Name), podSpec.ServiceAccountName)
	require.Len(t, podSpec.Containers, 1)
	c := podSpec.Containers[0]
	assert.Equal(t, gpexpandContainerName, c.Name)

	script := c.Args[0]
	assert.Contains(t, script, "\"${KUBECTL}\" exec")
	assert.Contains(t, script, util.CoordinatorPodName(cluster.Name))
	assert.Contains(t, script, "gpexpand -i")
	assert.Contains(t, script, "gpexpand -a")
	assert.Contains(t, script, "gpexpand --clean")

	// Cloudberry 2.1.0 gpexpand does NOT support the -D <db> option; the target
	// DB is selected via the exported PGDATABASE env instead. Assert no -D flag
	// is passed to any gpexpand invocation and the PGDATABASE export is present.
	assert.NotContains(t, script, "-D "+gpexpandStatusDatabase,
		"gpexpand must not be passed the unsupported -D <db> option")
	assert.Contains(t, script, "export PGDATABASE="+gpexpandStatusDatabase,
		"the target DB must be selected via the PGDATABASE env")
}

// TestBuildGpexpandJob_MountsSSHIdentity proves the gpexpand Job mounts the
// shared cluster SSH keypair Secret (gpexpand basebackups+dispatches to the new
// segments over SSH exactly like gpbackup).
func TestBuildGpexpandJob_MountsSSHIdentity(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()

	job := b.BuildGpexpandJob(cluster, 2, 3, "20260706120000")
	podSpec := job.Spec.Template.Spec

	require.NotNil(t, findVolume(podSpec.Volumes, sshSecretVolumeName),
		"the shared cluster SSH identity volume must be mounted")
	require.Len(t, podSpec.Containers, 1)
	assert.True(t, hasMount(podSpec.Containers[0].VolumeMounts, sshSecretVolumeName, sshSecretMountPath),
		"the gpexpand container must mount cluster-ssh at /etc/cloudberry/ssh")
}

// TestGpexpandInputFile_PrimaryLines asserts the input file rendered for the new
// segments (content ids [old,new)) carries the correct primary FQDNs, port and
// data dirs, one line per new content id, and NO mirror lines when mirroring is
// disabled.
func TestGpexpandInputFile_PrimaryLines(t *testing.T) {
	cluster := newBackupCluster() // mirroring not enabled
	lines := newSegmentLines(cluster, 2, 4)
	require.Len(t, lines, 2, "two new primaries for content ids 2,3")

	body := renderGpexpandInputFile(lines)
	segSvc := util.SegmentServiceName(cluster.Name)
	for _, content := range []int32{2, 3} {
		wantHost := fmt.Sprintf("%s-%d.%s", util.SegmentPrimaryName(cluster.Name), content, segSvc)
		wantDir := fmt.Sprintf("%s%d", segmentDataDirPrefix, content)
		assert.Contains(t, body, wantHost)
		assert.Contains(t, body, wantDir)
	}
	// port present, primary role marker, no mirror role.
	assert.Contains(t, body, fmt.Sprintf("|%d|", resolvePort(cluster)))
	assert.Contains(t, body, "|2|p\n")
	assert.NotContains(t, body, "|m\n")

	// DEFECT A: every input line carries a NON-EMPTY dbid column (the
	// placeholder the inner script substitutes with a real dbid). The old bug
	// rendered an empty dbid field (|| between datadir and content) which
	// gpexpand rejects ("Invalid dbid on line N").
	assert.NotContains(t, body, "||", "dbid column must never be empty")
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		fields := strings.Split(line, "|")
		require.Len(t, fields, 7, "line must have 7 pipe-delimited fields")
		assert.Equal(t, dbidPlaceholder, fields[4],
			"the dbid column (field 5) must carry the dbid placeholder")
	}
}

// TestGpexpandInputFile_DbidOrdering asserts the deterministic primaries-then-
// mirrors line ordering so the inner script's consecutive dbid assignment
// (max+1 to the first primary … then the mirrors) matches gpexpand's expected
// ordering. dbids are assigned by line number, so ordering IS the dbid order.
func TestGpexpandInputFile_DbidOrdering(t *testing.T) {
	cluster := newBackupCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	lines := newSegmentLines(cluster, 2, 4)
	require.Len(t, lines, 4, "two new primaries + two mirrors for content ids 2,3")
	// Primaries first (content-ascending), then mirrors (content-ascending):
	// dbids max+1..max+2 -> primaries content 2,3; max+3..max+4 -> mirrors 2,3.
	assert.Equal(t, "p", lines[0].role)
	assert.Equal(t, int32(2), lines[0].content)
	assert.Equal(t, "p", lines[1].role)
	assert.Equal(t, int32(3), lines[1].content)
	assert.Equal(t, "m", lines[2].role)
	assert.Equal(t, int32(2), lines[2].content)
	assert.Equal(t, "m", lines[3].role)
	assert.Equal(t, int32(3), lines[3].content)
}

// TestGpexpandScript_ComputesDbidFromMax asserts DEFECT A's in-script fix: the
// inner script computes max(dbid) from gp_segment_configuration and substitutes
// consecutive real dbids (max+NR) into the input file, replacing the placeholder,
// before running gpexpand -i. This keeps the dbids correct even if the topology
// shifted between reconcile and Job execution (no TOCTOU window).
func TestGpexpandScript_ComputesDbidFromMax(t *testing.T) {
	inner := renderGpexpandInnerScript()

	assert.Contains(t, inner, "SELECT COALESCE(max(dbid),0) FROM gp_segment_configuration",
		"the inner script must compute max(dbid) from the live catalog")
	// Consecutive dbids assigned per line (base+NR), placeholder substituted.
	assert.Contains(t, inner, "base=\"${MAXDBID}\"")
	assert.Contains(t, inner, "base+NR")
	assert.Contains(t, inner, dbidPlaceholder)
	// The dbid computation must precede the gpexpand -i add-segment phase.
	assert.Less(t, strings.Index(inner, "MAXDBID="),
		strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`),
		"dbids must be assigned before gpexpand -i consumes the input file")
	// Fail-closed if any placeholder survived substitution.
	assert.Contains(t, inner, "failed to assign dbids to input file")
}

// TestGpexpandInputFile_MirrorLinesWhenEnabled asserts mirror lines are rendered
// (after the primaries) only when segment mirroring is enabled.
func TestGpexpandInputFile_MirrorLinesWhenEnabled(t *testing.T) {
	cluster := newBackupCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	lines := newSegmentLines(cluster, 2, 3)
	require.Len(t, lines, 2, "one new primary + its mirror for content id 2")
	assert.Equal(t, "p", lines[0].role)
	assert.Equal(t, "m", lines[1].role)

	body := renderGpexpandInputFile(lines)
	mirrorHost := fmt.Sprintf("%s-2.%s",
		util.SegmentMirrorName(cluster.Name), util.SegmentServiceName(cluster.Name))
	assert.Contains(t, body, mirrorHost)
	assert.Contains(t, body, "|2|m\n")
}

// TestGpexpandInputFile_MirrorUsesResolvableFQDN is the regression guard for the
// hostname-resolution DEFECT (iter11 smoke report): gpexpand SSHes/rsyncs to the
// new MIRROR segment during segment-init, so the mirror's `hostname` AND
// `address` fields in the input file MUST be the resolvable pod FQDN
// (<pod>.<seg-hl>), exactly like the primary — NOT the short pod name (which
// does not resolve in Kubernetes DNS: "Could not resolve hostname
// okd4-cluster-segment-mirror-2"). It also pins the mirror line's
// content/role/port/datadir so only the host/address fields are ever at issue.
func TestGpexpandInputFile_MirrorUsesResolvableFQDN(t *testing.T) {
	cluster := newBackupCluster()
	// Match the smoke-report cluster name so the asserted strings are exactly
	// the FQDN/short-name pair from the live failure.
	cluster.Name = "okd4-cluster"
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	const content int32 = 2
	lines := newSegmentLines(cluster, content, content+1)
	require.Len(t, lines, 2, "one new primary + its mirror for content id 2")

	segSvc := util.SegmentServiceName(cluster.Name)
	require.Equal(t, "okd4-cluster-seg-hl", segSvc,
		"the segment headless service is the shared *-seg-hl suffix for both roles")

	primary, mirror := lines[0], lines[1]

	// Both the primary and the mirror must carry the SAME headless-service
	// suffix so both pod FQDNs resolve identically in Kubernetes DNS.
	wantPrimaryHost := fmt.Sprintf("%s-%d.%s",
		util.SegmentPrimaryName(cluster.Name), content, segSvc)
	wantMirrorHost := fmt.Sprintf("%s-%d.%s",
		util.SegmentMirrorName(cluster.Name), content, segSvc)
	assert.Equal(t, "okd4-cluster-segment-primary-2.okd4-cluster-seg-hl", wantPrimaryHost)
	assert.Equal(t, "okd4-cluster-segment-mirror-2.okd4-cluster-seg-hl", wantMirrorHost)

	// struct-level: hostname is the FQDN for BOTH roles.
	assert.Equal(t, wantPrimaryHost, primary.hostname)
	assert.Equal(t, wantMirrorHost, mirror.hostname)

	// Only the host/address fields were wrong: content/role/port/datadir for the
	// mirror line must remain correct (same content id + datadir as its primary,
	// role 'm', segment postgres port).
	assert.Equal(t, content, mirror.content, "mirror shares the content id of its primary")
	assert.Equal(t, "m", mirror.role)
	assert.Equal(t, resolvePort(cluster), mirror.port, "mirror uses the segment postgres port")
	assert.Equal(t, fmt.Sprintf("%s%d", segmentDataDirPrefix, content), mirror.dataDir)
	// Mirror datadir mirrors the primary's (same content id).
	assert.Equal(t, primary.dataDir, mirror.dataDir)

	// Rendered-file level: the mirror line's hostname AND address (fields 1 and
	// 2) are BOTH the FQDN. The pipe-delimited order is
	// hostname|address|port|datadir|dbid|content|role.
	body := renderGpexpandInputFile(lines)
	var mirrorLine string
	for _, l := range strings.Split(strings.TrimSpace(body), "\n") {
		if strings.HasSuffix(l, "|m") {
			mirrorLine = l
		}
	}
	require.NotEmpty(t, mirrorLine, "a mirror line must be rendered")
	fields := strings.Split(mirrorLine, "|")
	require.Len(t, fields, 7, "line must have 7 pipe-delimited fields")
	assert.Equal(t, wantMirrorHost, fields[0], "mirror hostname field must be the FQDN")
	assert.Equal(t, wantMirrorHost, fields[1], "mirror address field must be the FQDN")
	assert.Equal(t, strconv.Itoa(int(resolvePort(cluster))), fields[2], "mirror port")
	assert.Equal(t, fmt.Sprintf("%s%d", segmentDataDirPrefix, content), fields[3], "mirror datadir")
	assert.Equal(t, strconv.Itoa(int(content)), fields[5], "mirror content id")
	assert.Equal(t, "m", fields[6], "mirror role")

	// The short pod name (no headless-service suffix) must NEVER appear as a
	// standalone host/address token: it does not resolve in K8s DNS and is the
	// exact iter11 failure. Assert neither field 1 nor field 2 is the bare pod
	// name for ANY line (primary or mirror).
	for _, l := range strings.Split(strings.TrimSpace(body), "\n") {
		f := strings.Split(l, "|")
		require.GreaterOrEqual(t, len(f), 2)
		for _, hostField := range []string{f[0], f[1]} {
			assert.Contains(t, hostField, "."+segSvc,
				"every host/address field must be a <pod>.<seg-hl> FQDN, got short name %q", hostField)
		}
	}
	shortMirror := fmt.Sprintf("%s-%d", util.SegmentMirrorName(cluster.Name), content)
	assert.NotContains(t, body, shortMirror+"|",
		"the short mirror name must never be used as a host/address field (K8s DNS cannot resolve it)")
}

// TestGpexpandScript_ResumeAndEnv asserts the rendered inner gpexpand flow sets
// the coordinator catalog datadir, gates on gpexpand.status for resume, sources
// the Cloudberry env, and emits the EXPAND_RESULT marker.
func TestGpexpandScript_ResumeAndEnv(t *testing.T) {
	cluster := newBackupCluster()
	script := renderGpexpandScript(cluster, 2, 3)

	assert.Contains(t, script, "COORDINATOR_DATA_DIRECTORY="+coordinatorCatalogDataDir)
	assert.Contains(t, script, "gpexpand.status")
	assert.Contains(t, script, "greenplum_path.sh")
	assert.Contains(t, script, expandResultMarker)
	// idempotent short-circuit when a prior expansion already completed.
	assert.Contains(t, script, expandResultAlreadyComplete)
	// the input file is staged into the coordinator pod via base64.
	assert.Contains(t, script, "base64 -d >")

	// Cloudberry 2.1.0 gpexpand selects the DB via PGDATABASE, NOT -D <db>.
	inner := renderGpexpandInnerScript()
	assert.Contains(t, inner, "export PGDATABASE="+gpexpandStatusDatabase,
		"the DB must be selected via PGDATABASE, not the unsupported -D option")
	assert.NotContains(t, inner, "-D "+gpexpandStatusDatabase,
		"no gpexpand invocation may pass the unsupported -D <db> option")
	// The three gpexpand invocations keep all their supported flags intact.
	assert.Contains(t, inner, `gpexpand -i "${GPEXPAND_INPUT}" -a -v`)
	assert.Contains(t, inner,
		fmt.Sprintf("gpexpand -a -d %s -v", gpexpandRedistributeDuration))
	// D9: the final clean is the hardened verify-and-fall-back form.
	assert.Contains(t, inner, "if ! gpexpand --clean -a; then")
	assert.Contains(t, inner, "DROP SCHEMA IF EXISTS gpexpand CASCADE;")
}

// TestGpexpandScript_HardenedCleanFinalizeGuard is the D9 finalize-verification
// guard for the hardened `--clean` render: after `gpexpand --clean` (with its
// DROP SCHEMA fallback) the inner script must (a) probe whether the gpexpand
// schema is still present, and (b) if a residual schema remains, emit the
// distinct `EXPAND_RESULT=finalize-failed` marker and `exit 1` BEFORE the
// `EXPAND_RESULT=completed` marker — so the operator never declares scale-out
// complete over a lingering schema that would block a subsequent gpbackup.
func TestGpexpandScript_HardenedCleanFinalizeGuard(t *testing.T) {
	inner := renderGpexpandInnerScript()

	finalizeFailed := expandResultMarker + expandResultFinalizeFailed
	completed := expandResultMarker + expandResultCompleted

	tests := []struct {
		name   string
		needle string
		desc   string
	}{
		{
			name:   "residual-schema presence probe",
			needle: "schema_name='gpexpand'",
			desc:   "the guard must re-probe information_schema for a residual gpexpand schema",
		},
		{
			name:   "residual guard emits finalize-failed marker",
			needle: finalizeFailed,
			desc:   "a lingering schema must surface EXPAND_RESULT=finalize-failed",
		},
		{
			name:   "residual guard fails the job",
			needle: `echo "` + finalizeFailed + `" >&2; exit 1;`,
			desc:   "the finalize-failed path must exit 1 so the Job fails",
		},
		{
			name:   "residual guard gated on non-zero LEFT count",
			needle: `if [ "${LEFT:-1}" != "0" ]; then`,
			desc:   "the guard fires only when the residual-schema count is non-zero",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Contains(t, inner, tt.needle, tt.desc)
		})
	}

	// ORDERING: the finalize-failed guard must sit AFTER the hardened --clean and
	// strictly BEFORE the completed marker, so a residual schema aborts the Job
	// before it can ever report completion.
	idxClean := strings.Index(inner, "if ! gpexpand --clean -a; then")
	idxFinalizeFailed := strings.Index(inner, finalizeFailed)
	idxCompleted := strings.Index(inner, completed)
	require.NotEqual(t, -1, idxClean)
	require.NotEqual(t, -1, idxFinalizeFailed)
	require.NotEqual(t, -1, idxCompleted)
	assert.Less(t, idxClean, idxFinalizeFailed,
		"the finalize-failed guard must follow the hardened --clean")
	assert.Less(t, idxFinalizeFailed, idxCompleted,
		"the finalize-failed guard must precede the completed marker")
}

// TestGpexpandScript_SSHPortRedirect asserts the inner gpexpand flow redirects
// all segment SSH to the rootless sshd on port 2022 (restricted-v2 SCC): it
// re-asserts a ~/.ssh/config `Port 2022` block and exports GPSSH_SSH_PORT=2022
// BEFORE any gpexpand invocation. Without this, gpexpand's segment-init SSHes to
// the privileged port 22 (no listener under the SCC) and fails with
// "Connection refused".
func TestGpexpandScript_SSHPortRedirect(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// ~/.ssh/config Port 2022 heredoc re-assert.
	assert.Contains(t, inner, "cat > ~/.ssh/config",
		"the inner script must (re)write ~/.ssh/config")
	assert.Contains(t, inner, "Port "+clusterSSHPort,
		"the ssh client config must pin Port 2022 for the rootless sshd")
	// GPSSH_SSH_PORT export (honored by some gpssh builds; harmless otherwise).
	assert.Contains(t, inner, "export GPSSH_SSH_PORT="+clusterSSHPort)
	assert.Equal(t, "2022", clusterSSHPort, "the cluster rootless sshd port is 2022")

	// The SSH-port override must be established BEFORE gpexpand -i so the
	// segment-init phase uses it.
	assert.Less(t, strings.Index(inner, "Port "+clusterSSHPort),
		strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`),
		"the Port 2022 override must precede gpexpand -i")
}

// TestGpexpandScript_SegmentPortProbe asserts the inner flow TCP-probes port
// 2022 on the new segment host(s) before gpexpand -i, so an unreachable rootless
// sshd fails fast with a clear message instead of gpexpand's raw refusal.
func TestGpexpandScript_SegmentPortProbe(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// bash /dev/tcp probe against port 2022 (no nc dependency).
	assert.Contains(t, inner, "/dev/tcp/${h}/"+clusterSSHPort,
		"the inner script must TCP-probe port 2022 on each new segment host")
	assert.Contains(t, inner, "not reachable on "+clusterSSHPort,
		"an unreachable segment must fail fast with a clear message")
	// The probe iterates the segment hostnames extracted from the input file.
	assert.Contains(t, inner, `awk -F'|' 'NF>0 {print $1}' "${GPEXPAND_INPUT}"`)
	// The probe must run BEFORE gpexpand -i.
	assert.Less(t, strings.Index(inner, "/dev/tcp/${h}/"+clusterSSHPort),
		strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`),
		"the 2022 probe must precede gpexpand -i")
}

// TestGpexpandScript_ResumeCleanup asserts the idempotent resume/cleanup branch:
//   - `gpexpand -r` (rollback stale setup) guards the empty/SETUP path,
//   - the input file is regenerated (rm -f + restore) on that path,
//   - redistribution states (SETUP DONE / EXPANSION STARTED/STOPPED) skip -r and
//     resume with `gpexpand -a`,
//   - PHASE A (gpexpand -i) runs only when no expansion is set up (STATUS2 empty).
func TestGpexpandScript_ResumeCleanup(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// gpexpand -r rollback guard before -i.
	assert.Contains(t, inner, "gpexpand -r -a",
		"the resume guard must roll back a stale setup with gpexpand -r")
	assert.Less(t, strings.Index(inner, "gpexpand -r -a"),
		strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`),
		"gpexpand -r must precede gpexpand -i")

	// Status branch on the redistribution states (spaces stripped by tr).
	assert.Contains(t, inner, `[ "${STATUS}" = "SETUPDONE" ]`)
	assert.Contains(t, inner, `[ "${STATUS}" = "EXPANSIONSTARTED" ]`)
	assert.Contains(t, inner, `[ "${STATUS}" = "EXPANSIONSTOPPED" ]`)

	// Input file regeneration on the rollback path.
	assert.Contains(t, inner, `rm -f "${GPEXPAND_INPUT}"`,
		"the input file must be regenerated on the rollback path")
	assert.Contains(t, inner, `cp -f "${GPEXPAND_INPUT_ORIG}" "${GPEXPAND_INPUT}"`)

	// PHASE A gated on a re-read status (STATUS2 empty => not yet set up).
	assert.Contains(t, inner, `if [ -z "${STATUS2}" ]; then`,
		"gpexpand -i must run only when no expansion is set up (re-read status)")

	// On completion the input file(s) are removed (idempotent cleanup).
	assert.Contains(t, inner, `rm -f "${GPEXPAND_INPUT}" "${GPEXPAND_INPUT_ORIG}"`)
}

// TestGpexpandScript_FreshCleanPreState is the regression guard for the reused-
// PVC stale-state wedge: before a FRESH expansion (no in-progress gpexpand:
// empty/SETUP status) the inner script must establish a fully SELF-HEALING clean
// pre-state so a residual/stale catalog (from a prior aborted `gpexpand -i` OR a
// previous cluster incarnation on the reused coordinator PVC) can never wedge a
// new expansion — even when `gpexpand -r` itself cannot roll back. It asserts,
// IN ORDER and BEFORE `gpexpand -i`:
//   - `gpexpand -r` (supported rollback) with its failure TOLERATED (`|| true`),
//   - `DROP SCHEMA IF EXISTS gpexpand CASCADE` (leftover-schema removal),
//   - the fresh-path stale-segment cleanup DELETE bounded to content >=
//     ${EXPAND_BASE} (ANY status) — deleting all content>=base rows is safe here
//     because a genuine in-progress expansion routes to the resume branch, so any
//     content>=base row on the fresh path is stale by definition. It NEVER touches
//     existing healthy content < base rows.
func TestGpexpandScript_FreshCleanPreState(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// The new-segment content-id base bounds every cleanup DELETE and is decoded
	// from the 5th positional arg and validated as digits (fail-closed).
	assert.Contains(t, inner, `EXPAND_BASE=$(printf '%s' "$5" | base64 -d)`,
		"the content-id base (== oldCount) must be decoded from the 5th positional arg")
	assert.Contains(t, inner, "gpexpand: invalid expansion base content id",
		"the base must be validated as digits before use in SQL predicates")

	// (1) supported rollback with TOLERATED failure: when the stale rows -r
	// references point at segments that no longer exist, -r errors, and that must
	// NOT abort the fresh expansion (the manual cleanup below is the real remover).
	assert.Contains(t, inner, "gpexpand -r -a 2>/dev/null || true",
		"the fresh path must run gpexpand -r AND tolerate its failure (|| true)")
	// (2) DROP SCHEMA of any leftover gpexpand catalog schema -r missed.
	dropSchema := "DROP SCHEMA IF EXISTS gpexpand CASCADE;"
	assert.Contains(t, inner, dropSchema,
		"the fresh path must DROP SCHEMA IF EXISTS gpexpand CASCADE")
	// (3) fresh-path stale-segment cleanup DELETE, bounded to content >= base
	// (ANY status). Both the count probe and the DELETE carry the same bound.
	// The DELETE is NOT gated on -r success (it must run even when -r failed).
	freshPredicate := "content >= ${EXPAND_BASE};"
	staleDelete := "DELETE FROM gp_segment_configuration WHERE " + freshPredicate
	assert.Contains(t, inner, staleDelete,
		"the fresh-path DELETE must be bounded to content>=base (any status)")
	assert.Contains(t, inner,
		"SELECT count(*) FROM gp_segment_configuration WHERE "+freshPredicate,
		"the stale-row count probe must carry the same content>=base bound")

	// ORDERING: -r, then DROP SCHEMA, then the fresh DELETE, all BEFORE
	// gpexpand -i consumes the input file.
	idxRollback := strings.Index(inner, "gpexpand -r -a")
	idxDropSchema := strings.Index(inner, dropSchema)
	idxStaleDelete := strings.Index(inner, staleDelete)
	idxExpandI := strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`)
	assert.Less(t, idxRollback, idxDropSchema, "gpexpand -r must precede DROP SCHEMA")
	assert.Less(t, idxDropSchema, idxStaleDelete, "DROP SCHEMA must precede the fresh DELETE")
	assert.Less(t, idxStaleDelete, idxExpandI, "the cleanup must precede gpexpand -i")

	// The DELETE must NOT be gated on -r success: the fresh-path cleanup line
	// stands on its own (no `&&` chaining it to the rollback), so a failed
	// gpexpand -r cannot skip the manual catalog cleanup.
	assert.NotContains(t, inner, "gpexpand -r -a 2>/dev/null && "+staleDelete,
		"the manual DELETE must not be gated on gpexpand -r success")
}

// TestGpexpandScript_FreshPathDeletesAllContentGeBase proves the strengthened
// self-healing invariant: on the FRESH path the cleanup deletes ALL
// content>=base rows REGARDLESS of status (the reused-PVC fix — stale rows may be
// orphaned yet marked UP 'u' with a wrong dbid/hostname). The DELETE must NOT
// carry a `status <> 'u'` guard (that would leave an orphaned UP row wedging
// `gpexpand -i`). It also proves the invariant echo/comment is present and that
// gpexpand -r's failure is tolerated so a stale-referencing rollback cannot abort.
func TestGpexpandScript_FreshPathDeletesAllContentGeBase(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// The fresh-path DELETE removes ALL content>=base rows (any status).
	assert.Contains(t, inner,
		"DELETE FROM gp_segment_configuration WHERE content >= ${EXPAND_BASE};",
		"the fresh path must delete ALL content>=base rows regardless of status")
	// It must NOT re-add the status<>'u' guard that would spare an orphaned UP row.
	assert.NotContains(t, inner,
		"DELETE FROM gp_segment_configuration WHERE content >= ${EXPAND_BASE} AND status <> 'u';",
		"the fresh-path DELETE must not be status-guarded (orphaned UP rows must be removed)")

	// gpexpand -r failure is tolerated so a stale-referencing rollback cannot abort.
	assert.Contains(t, inner, "gpexpand -r -a 2>/dev/null || true",
		"gpexpand -r failure must be tolerated on the fresh path")

	// The fresh-vs-resume invariant is documented in-script (echo) for operators.
	assert.Contains(t, inner,
		"fresh path invariant: no in-progress expansion, so any content>=base row is stale",
		"the fresh-path invariant must be echoed for observability")

	// The DELETE runs even when gpexpand -r fails: it is on its own line, not
	// chained to the rollback with `&&`.
	idxRollback := strings.Index(inner, "gpexpand -r -a 2>/dev/null || true")
	idxDelete := strings.Index(inner,
		"DELETE FROM gp_segment_configuration WHERE content >= ${EXPAND_BASE};")
	require.Positive(t, idxRollback)
	require.Positive(t, idxDelete)
	assert.Less(t, idxRollback, idxDelete,
		"the tolerated rollback precedes the (ungated) manual DELETE")
}

// TestGpexpandScript_CleanupNeverTouchesHealthySegments proves the cleanup can
// NEVER delete a healthy EXISTING-segment row: EVERY DELETE against
// gp_segment_configuration is lower-bounded by content >= ${EXPAND_BASE} (the NEW
// content-id base). Existing healthy segments are provably content < base, so the
// fresh-path cleanup (which deletes ALL content>=base rows regardless of status —
// safe because a real in-progress expansion routes to the resume branch) can never
// touch them. There is NO unbounded/broad delete.
func TestGpexpandScript_CleanupNeverTouchesHealthySegments(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// Locate every DELETE against gp_segment_configuration and assert each one
	// carries the content>=base lower-bound (never an existing content<base row).
	const del = "DELETE FROM gp_segment_configuration"
	found := 0
	for idx := 0; ; {
		rel := strings.Index(inner[idx:], del)
		if rel < 0 {
			break
		}
		start := idx + rel
		end := strings.Index(inner[start:], "\n")
		require.GreaterOrEqual(t, end, 0, "DELETE statement must be on a single line")
		stmt := inner[start : start+end]
		found++
		assert.Contains(t, stmt, "content >= ${EXPAND_BASE}",
			"every gp_segment_configuration DELETE must be lower-bounded by the new-segment base")
		// The DELETE must NEVER carry a `content <` predicate (which could reach
		// an existing healthy segment).
		assert.NotContains(t, stmt, "content <",
			"no gp_segment_configuration DELETE may target content < base (existing segments)")
		idx = start + end
	}
	require.Equal(t, 1, found, "exactly one content>=base gp_segment_configuration DELETE is expected")

	// There must be NO DELETE that omits the content>=base guard (e.g. a bare
	// status='d' delete, or a whole-table wipe, that could hit an existing segment).
	assert.NotContains(t, inner, "DELETE FROM gp_segment_configuration WHERE status='d'")
	assert.NotContains(t, inner, "DELETE FROM gp_segment_configuration WHERE status<>'u'")
	assert.NotContains(t, inner, "DELETE FROM gp_segment_configuration WHERE content <")
	assert.NotContains(t, inner, "DELETE FROM gp_segment_configuration;")
}

// TestGpexpandScript_ResumePathDoesNotWipe proves idempotency/resume is
// preserved: when a REAL expansion is in progress (SETUP DONE / EXPANSION
// STARTED / EXPANSION STOPPED) the fresh-cleanup block (gpexpand -r + DROP
// SCHEMA + stale DELETE) is INSIDE the else-branch of the redistribution-state
// guard, so it never runs on the resume path. The resume branch only echoes and
// falls through to `gpexpand -a`.
func TestGpexpandScript_ResumePathDoesNotWipe(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// The redistribution-state guard opens BEFORE the cleanup, and the cleanup
	// (DROP SCHEMA + guarded DELETE) sits AFTER the `else` (fresh path) — never
	// on the in-progress branch.
	idxGuard := strings.Index(inner, `[ "${STATUS}" = "SETUPDONE" ]`)
	idxResumeEcho := strings.Index(inner, "expansion in progress; resuming with gpexpand -a")
	idxElse := strings.Index(inner, "\nelse\n")
	idxDropSchema := strings.Index(inner, "DROP SCHEMA IF EXISTS gpexpand CASCADE;")
	idxStaleDelete := strings.Index(inner, "DELETE FROM gp_segment_configuration")

	require.Positive(t, idxGuard)
	require.Positive(t, idxResumeEcho)
	require.Positive(t, idxElse)
	require.Positive(t, idxDropSchema)
	require.Positive(t, idxStaleDelete)

	// resume echo is on the in-progress (then) branch: after the guard, before else.
	assert.Less(t, idxGuard, idxResumeEcho, "resume echo must be inside the guard's then-branch")
	assert.Less(t, idxResumeEcho, idxElse, "resume echo must precede the else (fresh) branch")
	// the wipe (DROP SCHEMA + DELETE) is on the fresh (else) branch only.
	assert.Less(t, idxElse, idxDropSchema, "DROP SCHEMA must be inside the else (fresh) branch")
	assert.Less(t, idxElse, idxStaleDelete, "the guarded DELETE must be inside the else (fresh) branch")
}

// TestGpexpandScript_ExistingSegmentHealthReverify proves the post-cleanup
// RE-VERIFY: after the resume/cleanup branch, the script re-checks that every
// EXISTING (pre-scale) segment (content < ${EXPAND_BASE}) is up, and fails fast
// on a genuine cluster fault (an existing segment down that is NOT expansion
// residue). This runs BEFORE gpexpand -i. The old unconditional
// "refusing to expand with down/unknown segments" preflight (which fired on
// stale new-segment rows) is gone.
func TestGpexpandScript_ExistingSegmentHealthReverify(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// Existing-segment health check bounded to content < base.
	assert.Contains(t, inner,
		"SELECT count(*) FROM gp_segment_configuration WHERE content < ${EXPAND_BASE} AND status <> 'u'",
		"the health re-verify must check only EXISTING segments (content < base) are up")
	assert.Contains(t, inner,
		"refusing to expand: existing segment(s) down (real cluster fault)",
		"a genuinely-down existing segment must fail fast")
	assert.Contains(t, inner, "existing segments healthy; proceeding")

	// The re-verify must run AFTER the cleanup and BEFORE gpexpand -i.
	idxCleanup := strings.Index(inner, "DELETE FROM gp_segment_configuration")
	idxReverify := strings.Index(inner, "content < ${EXPAND_BASE} AND status <> 'u'")
	idxExpandI := strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`)
	assert.Less(t, idxCleanup, idxReverify, "the health re-verify must follow the stale-row cleanup")
	assert.Less(t, idxReverify, idxExpandI, "the health re-verify must precede gpexpand -i")

	// The old unconditional preflight is REMOVED (it wedged on stale down rows).
	assert.NotContains(t, inner, "refusing to expand with down/unknown segments",
		"the unconditional down-segment preflight must be replaced by the scoped checks")
}

// TestGpexpandScript_ExistingSegmentHealthRetryLoop proves the existing-segment
// health RE-VERIFY is now a BOUNDED RETRY/WAIT loop (not a single shot) that
// absorbs the transient scale-up connectivity blip: when the operator scales up
// the segment StatefulSets, the EXISTING segments (content < base) briefly flip
// to status<>'u' (headless-service DNS / controller churn) and recover within
// seconds. The check must:
//   - poll `content < base AND status <> 'u'` in a bounded loop until it returns 0,
//   - echo progress on each retry,
//   - succeed (proceed) as soon as it reaches 0 down,
//   - fail with the real-fault message + exit 1 ONLY on a PERSISTENT down state
//     (still down after the timeout),
//   - run BEFORE gpexpand -i, and KEEP the content<base guard (never weakened).
func TestGpexpandScript_ExistingSegmentHealthRetryLoop(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// The retry-loop construct: a bounded seq loop whose iteration count is the
	// total wait divided by the poll interval.
	attempts := gpexpandExistingSegWaitSeconds / gpexpandExistingSegPollSeconds
	require.Positive(t, attempts, "the wait loop must iterate at least once")
	assert.Contains(t, inner, fmt.Sprintf("for _ in $(seq 1 %d); do", attempts),
		"the existing-segment health check must be a bounded retry loop")
	assert.Contains(t, inner, fmt.Sprintf("sleep %d", gpexpandExistingSegPollSeconds),
		"the loop must poll every gpexpandExistingSegPollSeconds")

	// The polled query is the content<base health check (guard preserved), via
	// the coordinator psql exactly like the other catalog reads.
	assert.Contains(t, inner,
		"SELECT count(*) FROM gp_segment_configuration WHERE content < ${EXPAND_BASE} AND status <> 'u'",
		"the loop must poll the content<base existing-segment health count")

	// SUCCESS condition: reaching 0 down breaks the loop and proceeds.
	assert.Contains(t, inner, `if [ "${EXIST_DOWN:-1}" = "0" ]; then EXIST_HEALTHY=1; break; fi`,
		"the loop must succeed (break/proceed) as soon as there are 0 existing segments down")
	assert.Contains(t, inner, "existing segments healthy; proceeding")

	// PROGRESS echo on each retry.
	assert.Contains(t, inner, "waiting for existing segments to be up:",
		"each retry must echo progress")

	// TIMEOUT-FAILURE: a PERSISTENT down state (never reached healthy) fails with
	// the real-fault message and exits 1.
	assert.Contains(t, inner, `if [ "${EXIST_HEALTHY}" != "1" ]; then`,
		"the timeout failure must be gated on the loop never reaching healthy")
	assert.Contains(t, inner,
		"refusing to expand: existing segment(s) down (real cluster fault)",
		"a PERSISTENT existing-segment down (after the wait) must fail fast")

	// ORDERING: the retry loop runs AFTER the stale-row cleanup and BEFORE
	// gpexpand -i (still a pre-flight, just tolerant of the blip).
	idxCleanup := strings.Index(inner, "DELETE FROM gp_segment_configuration")
	idxLoop := strings.Index(inner, fmt.Sprintf("for _ in $(seq 1 %d); do", attempts))
	idxHealthQuery := strings.Index(inner, "content < ${EXPAND_BASE} AND status <> 'u'")
	idxExpandI := strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`)
	assert.Less(t, idxCleanup, idxLoop, "the retry loop must follow the stale-row cleanup")
	assert.Less(t, idxLoop, idxHealthQuery, "the health query must be inside the retry loop")
	assert.Less(t, idxHealthQuery, idxExpandI, "the health re-verify must precede gpexpand -i")

	// The guard is NOT weakened: it still fails on a genuine persistent fault and
	// still bounds the check to content < base (never touches content>=base).
	assert.NotContains(t, inner, "refusing to expand with down/unknown segments",
		"the old unconditional single-shot preflight must be gone")
}

// TestGpexpandScript_InitialSettleWait proves the inner flow performs a short
// initial settle sleep (letting the StatefulSet scale-up churn quiesce) BEFORE
// the first existing-segment health poll. The retry loop is the primary fix; this
// just trims the first blip.
func TestGpexpandScript_InitialSettleWait(t *testing.T) {
	inner := renderGpexpandInnerScript()

	require.Positive(t, gpexpandSettleSeconds)
	assert.Contains(t, inner, fmt.Sprintf("sleep %d", gpexpandSettleSeconds),
		"the inner flow must perform an initial settle sleep")
	assert.Contains(t, inner, "settling",
		"the settle wait must be echoed for observability")

	// The settle sleep must precede the existing-segment health retry loop.
	attempts := gpexpandExistingSegWaitSeconds / gpexpandExistingSegPollSeconds
	idxSettle := strings.Index(inner, fmt.Sprintf("sleep %d", gpexpandSettleSeconds))
	idxLoop := strings.Index(inner, fmt.Sprintf("for _ in $(seq 1 %d); do", attempts))
	assert.Less(t, idxSettle, idxLoop,
		"the initial settle sleep must precede the existing-segment health retry loop")
}

// TestGpexpandScript_PassesBaseContentId proves the outer wrapper passes the
// new-segment content-id base (== oldCount) as the 5th positional arg (base64)
// to the inner script, so the guarded cleanup is bounded correctly.
func TestGpexpandScript_PassesBaseContentId(t *testing.T) {
	cluster := newBackupCluster()
	// oldCount=2 -> the base content id that bounds the cleanup.
	script := renderGpexpandScript(cluster, 2, 4)

	assert.Contains(t, script, "BASE_B64=$(printf '%s' '2' | base64 | tr -d '\\n')",
		"the outer wrapper must base64 the oldCount base and pass it to the inner script")
	// The 5th positional arg ($4 in the remote bash's argv, $5 inside inner) is BASE_B64.
	assert.Contains(t, script, `"${BASE_B64}"`,
		"BASE_B64 must be passed as the trailing positional arg to the remote inner script")
	assert.Contains(t, script, `"$3" "$4"'`,
		"the remote bash must forward the 5th positional (base) into the inner script")
}

// TestGpexpandScript_PurgesStaleInputBeforeStaging is the regression guard for
// BOTH the iter22 wedge AND the iter23 exit-2 wedge:
//
//   - iter22: a PREVIOUS Job attempt can leave a stale input file (with the
//     unresolved __CBK_DBID__ placeholder) on the coordinator's /tmp (or a
//     persistent PVC), which gpexpand -r/-i then read and abort on
//     ("Invalid dbid on line 1"). So the wrapper must rm -f ALL stale gpexpand
//     input artifacts — the primary input path, its `.orig` copy, and the
//     `.dbid` substitution temp — BEFORE staging the fresh input file.
//   - iter23: the earlier purge used a SEPARATE `kubectl exec -- bash -c
//     'rm -f "$0" ...' "${GPEXPAND_INPUT}"`. Inside that remote shell `$0` is the
//     shell's own name, NOT the input path, and the extra positional argv
//     plumbing produced a shell usage error => exit code 2, killing the Job pod
//     right after the settle. The corrected purge is folded into the SAME single
//     coordinator staging exec and targets the LITERAL input path (never `$0`).
//
// This test asserts: (a) the purge targets the LITERAL input path (contains the
// input filename, NOT `$0`), (b) it is folded into the SAME exec that stages the
// base64 input and runs BEFORE that write, and (c) there is NO separate broken
// `$0`-based purge exec anywhere in the script.
func TestGpexpandScript_PurgesStaleInputBeforeStaging(t *testing.T) {
	cluster := newBackupCluster()
	script := renderGpexpandScript(cluster, 2, 3)

	// The inner staging command (before it is shell-quoted for the outer `bash
	// -c`) rm -f's the LITERAL primary path + `.orig` + `.dbid` (the
	// gpexpandInputFile constant), NEVER the fragile `$0`, then writes the fresh
	// base64 input to the SAME literal path — one folded command, one exec.
	lit := shellQuote(gpexpandInputFile) // '/tmp/cbk-gpexpand-input'
	innerStage := fmt.Sprintf(
		"rm -f %[1]s %[1]s.orig %[1]s.dbid 2>/dev/null || true; base64 -d > %[1]s", lit)
	// The whole inner command is shell-quoted into the outer `bash -c <arg>`, so
	// the rendered script contains its `'\''`-escaped form. Assert THAT is present
	// (proves the literal-path purge folded into the single staging exec).
	assert.Contains(t, script, shellQuote(innerStage),
		"the purge+stage must be a single literal-path inner command (no $0)")

	// (a) The purge targets the LITERAL input filename, never `$0`.
	assert.Contains(t, script, gpexpandInputFile,
		"the purge must reference the literal input filename")
	assert.Equal(t, "/tmp/cbk-gpexpand-input", gpexpandInputFile,
		"the gpexpand input file path is the known literal used by the purge")

	// (b) The purge (rm -f) must precede the staging write (base64 -d >) inside
	// the SAME single inner command, so the fresh write lands on a clean slate.
	idxPurge := strings.Index(innerStage, "rm -f")
	idxStage := strings.Index(innerStage, "base64 -d >")
	require.Positive(t, idxStage, "the input-staging write must be present")
	assert.Less(t, idxPurge, idxStage,
		"the stale-input purge must precede staging the fresh input file")

	// (c) The separate broken `$0`-based purge exec must be GONE, and no `$0`
	// staging write survives (both caused the iter23 exit-2 wedge).
	assert.NotContains(t, script, `rm -f "$0" "$0.orig" "$0.dbid"`,
		"the broken $0-based purge exec must be removed (it caused the exit-2 wedge)")
	assert.NotContains(t, script, `base64 -d > "$0"`,
		"the staging write must use the literal path, not $0")

	// Only ONE coordinator exec runs the base64 stage: assert exactly one
	// `base64 -d > ` occurs across the whole outer wrapper (single exec).
	assert.Equal(t, 1, strings.Count(script, "base64 -d > "),
		"there must be exactly one coordinator exec that stages the input (single exec)")
}

// TestGpexpandScript_ResolvesDbidBeforeRollback is the core regression guard for
// the iter22 stale-placeholder cascade: the __CBK_DBID__ placeholder must be
// resolved to REAL dbids in the ACTUAL GPEXPAND_INPUT file BEFORE any gpexpand
// invocation — crucially BEFORE `gpexpand -r` (which, in a prior version, read the
// still-placeholder'd file and aborted with "Invalid dbid on line 1"). It also
// asserts a fail-closed no-placeholder grep guard fires before gpexpand consumes
// the file, and that the substitution writes to the real GPEXPAND_INPUT path
// (atomic mv over it), not merely a temp/.orig that gets ignored.
func TestGpexpandScript_ResolvesDbidBeforeRollback(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// The substituted output is written to a `.dbid` temp then atomically mv'd
	// over the ACTUAL GPEXPAND_INPUT path gpexpand reads (never only a .orig/temp).
	assert.Contains(t, inner,
		`> "${GPEXPAND_INPUT}.dbid" && mv "${GPEXPAND_INPUT}.dbid" "${GPEXPAND_INPUT}"`,
		"the dbid substitution must write into the actual GPEXPAND_INPUT path")

	// The dbid substitution (MAXDBID compute) must run BEFORE gpexpand -r so the
	// rollback never reads an unresolved placeholder.
	idxDbid := strings.Index(inner, "MAXDBID=")
	idxRollback := strings.Index(inner, "gpexpand -r -a")
	idxExpandI := strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`)
	require.Positive(t, idxDbid, "the dbid substitution must be present")
	require.Positive(t, idxRollback, "the gpexpand -r rollback must be present")
	require.Positive(t, idxExpandI, "gpexpand -i must be present")
	assert.Less(t, idxDbid, idxRollback,
		"dbids must be resolved BEFORE gpexpand -r (so -r never reads a placeholder)")
	assert.Less(t, idxDbid, idxExpandI,
		"dbids must be resolved before gpexpand -i")

	// FAIL-CLOSED no-placeholder guard: assert a grep for the placeholder that
	// aborts, and that it appears BEFORE the rollback (i.e. before ANY gpexpand
	// consumes the file).
	guard := "if grep -q '__CBK_DBID__' \"${GPEXPAND_INPUT}\"; then"
	assert.Contains(t, inner, guard,
		"a fail-closed no-placeholder grep guard must be present")
	assert.Contains(t, inner, "placeholder survived",
		"the fail-closed guard must abort with a clear message")
	idxGuard := strings.Index(inner, guard)
	require.Positive(t, idxGuard)
	assert.Less(t, idxGuard, idxRollback,
		"the fail-closed no-placeholder guard must fire before gpexpand -r consumes the file")

	// The .orig snapshot must be taken AFTER the dbid resolution so it is
	// placeholder-free by construction (a fresh-path regenerate from .orig can
	// never reintroduce the placeholder).
	idxOrigCopy := strings.Index(inner, `cp -f "${GPEXPAND_INPUT}" "${GPEXPAND_INPUT_ORIG}"`)
	require.Positive(t, idxOrigCopy, "the .orig snapshot must be present")
	assert.Less(t, idxDbid, idxOrigCopy,
		"the .orig snapshot must be taken AFTER dbid resolution (so it is placeholder-free)")
}

// TestGpexpandScript_RetrySafeRegenerate proves the cleanup+regenerate+substitute
// +verify sequence is retry-safe across Job attempts (coordinator /tmp or PVC may
// persist): the fresh path regenerates GPEXPAND_INPUT from the placeholder-free
// `.orig`, and PHASE A re-runs the idempotent dbid substitution + fail-closed
// no-placeholder guard before gpexpand -i, so a stale/regenerated file can never
// reintroduce an unresolved placeholder into the file gpexpand consumes.
func TestGpexpandScript_RetrySafeRegenerate(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// Fresh-path regenerate restores GPEXPAND_INPUT from the placeholder-free
	// `.orig` snapshot.
	assert.Contains(t, inner, `cp -f "${GPEXPAND_INPUT_ORIG}" "${GPEXPAND_INPUT}"`,
		"the fresh path must regenerate the input from the placeholder-free .orig")

	// PHASE A re-substitutes dbids and re-asserts no placeholder before gpexpand -i.
	// There must be a MAXDBID substitution AND a fail-closed guard positioned just
	// before gpexpand -i (i.e. after the resume/cleanup regenerate).
	idxRegenerate := strings.Index(inner, `cp -f "${GPEXPAND_INPUT_ORIG}" "${GPEXPAND_INPUT}"`)
	idxExpandI := strings.Index(inner, `gpexpand -i "${GPEXPAND_INPUT}"`)
	require.Positive(t, idxRegenerate)
	require.Positive(t, idxExpandI)

	// A dbid substitution occurs AFTER the regenerate and BEFORE gpexpand -i
	// (the PHASE-A re-substitute-and-verify).
	guard := "if grep -q '__CBK_DBID__' \"${GPEXPAND_INPUT}\"; then"
	region := inner[idxRegenerate:idxExpandI]
	assert.Contains(t, region, "MAXDBID=",
		"PHASE A must re-run the idempotent dbid substitution after the regenerate")
	assert.Contains(t, region, guard,
		"PHASE A must re-assert the fail-closed no-placeholder guard before gpexpand -i")

	// The dbid substitution appears at least twice (early + PHASE A) so both the
	// -r-path and the -i-path files are guaranteed placeholder-free.
	assert.GreaterOrEqual(t, strings.Count(inner, "MAXDBID="), 2,
		"dbid substitution must run early (pre -r) AND in PHASE A (pre -i) for retry-safety")
	assert.GreaterOrEqual(t, strings.Count(inner, guard), 2,
		"the fail-closed no-placeholder guard must protect both the -r and -i consumers")
}

// TestGpexpandScript_IsValidBash runs `bash -n` over the FULL rendered gpexpand
// wrapper (and its quoted inner heredoc) so a quoting/syntax regression fails
// fast. It also parses the inner gpexpand script standalone.
func TestGpexpandScript_IsValidBash(t *testing.T) {
	shell, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available; skipping syntax check")
	}
	cluster := newBackupCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	for _, script := range []string{
		renderGpexpandScript(cluster, 2, 4),
		renderGpexpandInnerScript(),
	} {
		cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
		cmd.Stdin = strings.NewReader(script)
		out, runErr := cmd.CombinedOutput()
		require.NoError(t, runErr, "bash -n reported a syntax error: %s", string(out))
	}
}

// TestGpexpandScript_InitRetryLoop proves ISSUE B's fix: `gpexpand -i` (add+init)
// is wrapped in a BOUNDED RETRY loop that absorbs a transient segment-restart
// blip (rsync/ssh "Connection refused") during segment-init. The loop must:
//   - iterate up to gpexpandInitMaxAttempts times with a re-probe + re-clean each
//     retry (so gpexpand.status never wedges),
//   - RE-PROBE sshd:2022 on every new segment host before each attempt,
//   - RE-RUN the resume/cleanup (fresh clean pre-state + input regeneration) and
//     the existing-segment health re-verify each attempt,
//   - capture gpexpand -i's exit code (not abort under set -e), back off
//     gpexpandInitRetrySleepSeconds and retry on a non-zero exit, and
//   - fail terminally (exit 1) after the last attempt (unchanged behavior).
func TestGpexpandScript_InitRetryLoop(t *testing.T) {
	inner := renderGpexpandInnerScript()

	// Bounded retry loop over the add-segment phase.
	require.Positive(t, gpexpandInitMaxAttempts, "the init retry must have at least one attempt")
	assert.Contains(t, inner, fmt.Sprintf("for CBK_ATTEMPT in $(seq 1 %d); do", gpexpandInitMaxAttempts),
		"gpexpand -i must be wrapped in a bounded retry loop")

	// gpexpand -i's exit code is captured (retry-safe under set -e), not fatal.
	assert.Contains(t, inner, `gpexpand -i "${GPEXPAND_INPUT}" -a -v || CBK_I_RC=$?`,
		"the gpexpand -i exit code must be captured so a transient failure can be retried")
	assert.Contains(t, inner, `if [ "${CBK_I_RC}" != "0" ]; then`,
		"a non-zero gpexpand -i exit must trigger the retry path")

	// Backoff + continue on a non-last attempt.
	assert.Contains(t, inner, fmt.Sprintf("sleep %d; continue", gpexpandInitRetrySleepSeconds),
		"a transient gpexpand -i failure must back off and retry")
	assert.Contains(t, inner, fmt.Sprintf(`if [ "${CBK_ATTEMPT}" -lt %d ]`, gpexpandInitMaxAttempts),
		"retries must be bounded by gpexpandInitMaxAttempts")

	// Terminal failure after the last attempt (unchanged fail-closed behavior).
	assert.Contains(t, inner, fmt.Sprintf("gpexpand: -i failed after %d attempts", gpexpandInitMaxAttempts),
		"a persistent gpexpand -i failure must fail the Job after all attempts")

	// The re-probe (2022) and resume/cleanup + input-regeneration run INSIDE the
	// loop (before gpexpand -i) so each retry re-converges from a clean state.
	idxLoop := strings.Index(inner, fmt.Sprintf("for CBK_ATTEMPT in $(seq 1 %d); do", gpexpandInitMaxAttempts))
	idxProbeInLoop := strings.Index(inner[idxLoop:], "/dev/tcp/${h}/"+clusterSSHPort)
	idxRegenInLoop := strings.Index(inner[idxLoop:], `cp -f "${GPEXPAND_INPUT_ORIG}" "${GPEXPAND_INPUT}"`)
	idxExpandIInLoop := strings.Index(inner[idxLoop:], `gpexpand -i "${GPEXPAND_INPUT}"`)
	require.Positive(t, idxProbeInLoop, "the 2022 re-probe must be inside the retry loop")
	require.Positive(t, idxRegenInLoop, "the input regeneration (resume/cleanup) must be inside the retry loop")
	require.Positive(t, idxExpandIInLoop, "gpexpand -i must be inside the retry loop")
	assert.Less(t, idxProbeInLoop, idxExpandIInLoop, "the 2022 re-probe must precede gpexpand -i in each attempt")
	assert.Less(t, idxRegenInLoop, idxExpandIInLoop, "the resume/cleanup+regeneration must precede gpexpand -i in each attempt")

	// The existing-segment health re-verify (writeInnerResumeGuard tail) also
	// runs each attempt (inside the loop), keeping the guard intact per retry.
	idxHealthInLoop := strings.Index(inner[idxLoop:], "content < ${EXPAND_BASE} AND status <> 'u'")
	require.Positive(t, idxHealthInLoop, "the existing-segment health re-verify must run inside the retry loop")
	assert.Less(t, idxHealthInLoop, idxExpandIInLoop, "the health re-verify must precede gpexpand -i in each attempt")
}

// TestBuildGpexpandJob_ImplementsInterface confirms DefaultBuilder satisfies the
// ResourceBuilder interface with the new BuildGpexpandJob method.
func TestBuildGpexpandJob_ImplementsInterface(t *testing.T) {
	var _ ResourceBuilder = &DefaultBuilder{}
}

// TestSegmentStatefulSet_ExpansionBaseCountEnv proves the segment StatefulSet
// carries CLOUDBERRY_EXPANSION_BASE_COUNT (= oldCount) ONLY while a scale-out is
// in progress (util.AnnotationScaleState present with newCount > oldCount). This
// is the per-pod gpexpand-managed signal: the entrypoint marks any segment whose
// ordinal >= base as gpexpand-managed so it skips self-initdb and lets gpexpand
// initialize its datadir.
func TestSegmentStatefulSet_ExpansionBaseCountEnv(t *testing.T) {
	b := NewBuilder()

	// The env name the operator sets must stay in lockstep with the entrypoint
	// source (documentation anchor).
	assert.Equal(t, "CLOUDBERRY_EXPANSION_BASE_COUNT", envCloudberryExpansionBaseCount)

	t.Run("scale-out in progress -> base count set to oldCount", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Segments.Count = 3
		cluster.Annotations = map[string]string{
			util.AnnotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":3}`,
		}

		sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, err)
		require.NotNil(t, sts)

		env := envMap(sts.Spec.Template.Spec.Containers[0].Env)
		assert.Equal(t, "2", env[envCloudberryExpansionBaseCount],
			"the pre-scale (old) segment count must be exported so new pods skip initdb")
	})

	t.Run("no scale-out -> base count env absent", func(t *testing.T) {
		cluster := newTestCluster()

		sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, err)
		require.NotNil(t, sts)

		env := envMap(sts.Spec.Template.Spec.Containers[0].Env)
		_, present := env[envCloudberryExpansionBaseCount]
		assert.False(t, present,
			"normal bring-up (no scale-out) must NOT set the expansion base count")
	})

	t.Run("malformed / non-scale-out annotation -> base count env absent", func(t *testing.T) {
		for _, raw := range []string{
			"not-json",
			`{"phase":"x","oldCount":3,"newCount":3}`, // no growth
			`{"phase":"x","oldCount":4,"newCount":2}`, // scale-in
		} {
			cluster := newTestCluster()
			cluster.Annotations = map[string]string{util.AnnotationScaleState: raw}

			sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
			require.NoError(t, err)
			env := envMap(sts.Spec.Template.Spec.Containers[0].Env)
			_, present := env[envCloudberryExpansionBaseCount]
			assert.Falsef(t, present, "annotation %q must not set the base count", raw)
		}
	})
}

// TestScaleOutBaseCount_Cases unit-tests the scaleOutBaseCount helper directly:
// the (base, ok) contract across nil/empty/malformed/scale-in/scale-out inputs.
func TestScaleOutBaseCount_Cases(t *testing.T) {
	mk := func(anno string) *cbv1alpha1.CloudberryCluster {
		c := newTestCluster()
		if anno != "" {
			c.Annotations = map[string]string{util.AnnotationScaleState: anno}
		}
		return c
	}

	cases := []struct {
		name     string
		cluster  *cbv1alpha1.CloudberryCluster
		wantBase int32
		wantOK   bool
	}{
		{"nil cluster", nil, 0, false},
		{"no annotation", mk(""), 0, false},
		{"malformed json", mk("::"), 0, false},
		{"scale-out 2->3", mk(`{"oldCount":2,"newCount":3}`), 2, true},
		{"scale-out 0->1", mk(`{"oldCount":0,"newCount":1}`), 0, true},
		{"no growth 3->3", mk(`{"oldCount":3,"newCount":3}`), 0, false},
		{"scale-in 4->2", mk(`{"oldCount":4,"newCount":2}`), 0, false},
		{"negative old", mk(`{"oldCount":-1,"newCount":3}`), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, ok := scaleOutBaseCount(tc.cluster)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantBase, base)
		})
	}
}

// TestSegmentStatefulSet_ScaleOutMarksNewOrdinalsGpexpandManaged proves BUG-1's
// end-to-end intent at the StatefulSet level: once the scale-state annotation
// (oldCount=2, newCount=3) is set, the segment StatefulSet template carries
// CLOUDBERRY_EXPANSION_BASE_COUNT=2, which — per the entrypoint's
// is_gpexpand_managed_segment(ordinal >= base) contract — marks ONLY the new
// pod (ordinal 2) as gpexpand-managed while the existing pods (ordinals 0,1)
// remain normal. The exported ScaleOutBaseCount / EnvCloudberryExpansionBaseCount
// (used by the controller's fail-closed guard) are asserted alongside.
func TestSegmentStatefulSet_ScaleOutMarksNewOrdinalsGpexpandManaged(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Count = 3
	cluster.Annotations = map[string]string{
		util.AnnotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":3}`,
	}

	// Exported helpers the controller relies on for the BUG-1 guard.
	base, ok := ScaleOutBaseCount(cluster)
	require.True(t, ok, "a genuine scale-out must report a base count")
	require.Equal(t, int32(2), base)
	assert.Equal(t, "CLOUDBERRY_EXPANSION_BASE_COUNT", EnvCloudberryExpansionBaseCount)

	sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(t, err)
	require.NotNil(t, sts)

	env := envMap(sts.Spec.Template.Spec.Containers[0].Env)
	require.Equal(t, "2", env[EnvCloudberryExpansionBaseCount],
		"segment StatefulSet must carry the base count after the scale-state annotation is set")

	// Model the entrypoint's per-pod decision: ordinal >= base => gpexpand-managed.
	gpexpandManaged := func(ordinal int) bool { return ordinal >= int(base) }
	assert.False(t, gpexpandManaged(0), "existing segment 0 must NOT be gpexpand-managed")
	assert.False(t, gpexpandManaged(1), "existing segment 1 must NOT be gpexpand-managed")
	assert.True(t, gpexpandManaged(2), "the NEW segment 2 (ordinal >= base) MUST be gpexpand-managed")
}

// TestSegmentStatefulSet_ScaleOutStartupProbeTolerance proves BUG-2's PXF/DB
// pre-init tolerance at the container level: while a scale-out is in progress the
// SEGMENT DB container gains a StartupProbe (a generous budget that holds off the
// TCP LivenessProbe until postgres first listens) so a gpexpand-managed new
// segment with an EMPTY datadir is NOT SIGKILLed before gpexpand initializes it —
// keeping the pod Running so the operator's scale-out gate proceeds. Outside a
// scale-out (and for the coordinator) NO StartupProbe is added, so non-scale
// behavior is unchanged.
func TestSegmentStatefulSet_ScaleOutStartupProbeTolerance(t *testing.T) {
	b := NewBuilder()

	t.Run("scale-out -> segment DB container has a generous StartupProbe", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Segments.Count = 3
		cluster.Annotations = map[string]string{
			util.AnnotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":3}`,
		}

		sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, err)
		c := sts.Spec.Template.Spec.Containers[0]
		require.NotNil(t, c.StartupProbe,
			"a scale-out segment DB container must have a StartupProbe to survive pre-init")
		require.NotNil(t, c.StartupProbe.TCPSocket)
		// Budget must comfortably cover a gpexpand basebackup-based init.
		budget := c.StartupProbe.PeriodSeconds * c.StartupProbe.FailureThreshold
		assert.GreaterOrEqual(t, budget, int32(600),
			"StartupProbe budget must be generous enough for gpexpand datadir init")
		// The LivenessProbe (TCP) is retained; the StartupProbe just holds it off.
		require.NotNil(t, c.LivenessProbe)
	})

	t.Run("no scale-out -> segment DB container has NO StartupProbe", func(t *testing.T) {
		cluster := newTestCluster()
		sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, err)
		c := sts.Spec.Template.Spec.Containers[0]
		assert.Nil(t, c.StartupProbe,
			"normal (non-scale) segment must NOT gain a StartupProbe")
	})

	t.Run("scale-out -> coordinator DB container has NO StartupProbe", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Segments.Count = 3
		cluster.Annotations = map[string]string{
			util.AnnotationScaleState: `{"phase":"scaling-sts","oldCount":2,"newCount":3}`,
		}
		sts, err := b.BuildCoordinatorStatefulSet(cluster)
		require.NoError(t, err)
		c := sts.Spec.Template.Spec.Containers[0]
		assert.Nil(t, c.StartupProbe,
			"the coordinator is never gpexpand-managed; it must NOT gain a StartupProbe")
	})
}
