// Package entrypoint contains black-box shell tests for the Cloudberry DB
// container entrypoint (hack/docker-entrypoint-cloudberry.sh). They run under
// the default `go test ./...` so a regression in the two scale-out / SSH fixes
// (rootless sshd -f /dev/null, per-pod gpexpand-managed detection) fails CI.
package entrypoint

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// entrypointPath resolves hack/docker-entrypoint-cloudberry.sh relative to the
// repo root (two levels up from this test package).
func entrypointPath(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	p := filepath.Clean(filepath.Join(wd, "..", "..", "hack", "docker-entrypoint-cloudberry.sh"))
	if _, statErr := os.Stat(p); statErr != nil {
		t.Fatalf("entrypoint not found at %s: %v", p, statErr)
	}
	return p
}

// TestEntrypoint_WaitForGpexpandInit_ReturnsWhenInitialized proves the BUG-2
// pre-init idle path: wait_for_gpexpand_init keeps the container alive (rather
// than exec'ing postgres on an empty datadir -> CrashLoopBackOff) but returns
// immediately (0) once PG_VERSION exists, so once gpexpand has initialized the
// datadir the normal start path resumes. Here PG_VERSION is pre-created so the
// function must return without idling.
func TestEntrypoint_WaitForGpexpandInit_ReturnsWhenInitialized(t *testing.T) {
	shell := requireBash(t)
	src := readEntrypoint(t)

	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("14\n"), 0o600); err != nil {
		t.Fatalf("seed PG_VERSION: %v", err)
	}

	fn := extractShellFunc(t, src, "wait_for_gpexpand_init")
	// Stub the logging + start_sshd helpers the function references so it can run
	// standalone, then invoke it against the pre-initialized datadir.
	script := "log_info() { :; }\nlog_warn() { :; }\nlog_error() { :; }\nstart_sshd() { :; }\n" +
		"CLUSTER_SSH_PORT=2022\nCLOUDBERRY_CONTENT_ID=2\nSSHD_PID_FILE=\n" +
		fn + "\nif wait_for_gpexpand_init \"" + dataDir + "\"; then echo INIT_DONE; else echo INIT_FAIL; fi\n"

	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("wait_for_gpexpand_init probe failed: %v\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "INIT_DONE") {
		t.Errorf("wait_for_gpexpand_init must return success when PG_VERSION exists; got %q", string(out))
	}
}

// TestEntrypoint_MainSkipsPostgresOnEmptyGpexpandDatadir proves the primary/mirror
// case in main() does NOT exec postgres on an empty datadir for a gpexpand-managed
// segment: the source must guard the postgres start behind is_gpexpand_managed_segment
// + a PG_VERSION check that routes to wait_for_gpexpand_init.
func TestEntrypoint_MainSkipsPostgresOnEmptyGpexpandDatadir(t *testing.T) {
	src := readEntrypoint(t)
	for _, want := range []string{
		"wait_for_gpexpand_init",
		"is_gpexpand_managed_segment",
		`if [ ! -f "${SEGMENT_DATA_DIR}/PG_VERSION" ]; then`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("entrypoint must gate segment postgres start on gpexpand pre-init; missing %q", want)
		}
	}
}

// TestEntrypoint_WaitLoopDoesNotHoldSegmentPort proves REQUIRED BEHAVIOR #1: the
// pre-init idle loop must NOT bind/hold the segment postgres port (5432) or run
// any postgres process, so gpexpand's `pg_ctl start` can bind 5432. We assert the
// wait_for_gpexpand_init body only polls the filesystem (PG_VERSION) + sshd and
// never invokes postgres / pg_ctl / binds a port.
func TestEntrypoint_WaitLoopDoesNotHoldSegmentPort(t *testing.T) {
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "wait_for_gpexpand_init")
	code := stripShellComments(fn)

	for _, forbidden := range []string{
		"exec postgres",
		"postgres -D",
		"pg_ctl",
		"initdb",
	} {
		if strings.Contains(code, forbidden) {
			t.Errorf("wait_for_gpexpand_init must NOT run postgres/hold port 5432; found %q in code", forbidden)
		}
	}
	// The loop condition must be purely filesystem-based (PG_VERSION), not a port
	// probe that could bind/hold 5432.
	if !strings.Contains(fn, `[ ! -f "${data_dir}/PG_VERSION" ]`) {
		t.Errorf("wait_for_gpexpand_init must poll PG_VERSION on the filesystem, not the port")
	}
}

// TestEntrypoint_GpexpandHandoffPresent proves REQUIRED BEHAVIOR #2/#3: after
// gpexpand initializes the datadir AND starts postgres via pg_ctl, the entrypoint
// must NOT race gpexpand — it hands off by stopping gpexpand's pg_ctl-started
// instance, fixing ownership, then exec'ing postgres as the container main
// process. This is the fix for the observed "could not start server" collision.
func TestEntrypoint_GpexpandHandoffPresent(t *testing.T) {
	src := readEntrypoint(t)

	// The dedicated handoff function must exist.
	fn := extractShellFunc(t, src, "gpexpand_handoff_to_steady_state")

	for _, want := range []string{
		"pg_ctl",              // stops gpexpand's pg_ctl-started instance
		"stop",                // fast shutdown of that instance
		"chown",               // ownership normalization for gpadmin
		"postmaster.pid",      //
		"upsert_gp_contentid", // corrects gp_contentid inherited from the coordinator template
	} {
		if !strings.Contains(fn, want) {
			t.Errorf("gpexpand_handoff_to_steady_state must include %q for a clean, no-data-loss handoff", want)
		}
	}

	// main() must call the handoff and then exec postgres (start_postgres) for a
	// gpexpand-managed segment, and must NOT run the racing background
	// password-setup postgres start on that path.
	if !strings.Contains(src, "gpexpand_handoff_to_steady_state") {
		t.Errorf("main() must invoke gpexpand_handoff_to_steady_state for gpexpand-managed segments")
	}
}

// TestEntrypoint_GpexpandManagedSkipsBackgroundPasswordStart proves REQUIRED
// BEHAVIOR #2: the gpexpand-managed primary path must exit BEFORE the background
// `postgres -D ... &` password-setup start (which would race gpexpand's pg_ctl).
// We assert the managed branch reaches start_postgres and returns/exits before
// the shared password-setup block.
func TestEntrypoint_GpexpandManagedSkipsBackgroundPasswordStart(t *testing.T) {
	src := readEntrypoint(t)

	fn := extractShellFunc(t, src, "main")

	// Locate the gpexpand handoff call and the racing background start.
	handoffIdx := strings.Index(fn, "gpexpand_handoff_to_steady_state")
	if handoffIdx < 0 {
		t.Fatalf("main() must call gpexpand_handoff_to_steady_state")
	}
	// Within the primary branch, the managed path must terminate (exit 0) after
	// start_postgres so control never reaches the background password start.
	after := fn[handoffIdx:]
	startIdx := strings.Index(after, "start_postgres")
	exitIdx := strings.Index(after, "exit 0")
	if startIdx < 0 || exitIdx < 0 || exitIdx < startIdx {
		t.Errorf("gpexpand-managed primary path must exec postgres then exit BEFORE the background password-setup start (avoid racing gpexpand)")
	}
}

// TestEntrypoint_HandoffRunsAndReleasesPort exercises gpexpand_handoff_to_steady_state
// end-to-end with stubs: it must detect a stale postmaster.pid, invoke pg_ctl
// stop, and leave the datadir ready (no live lock) for the caller's exec postgres.
func TestEntrypoint_HandoffRunsAndReleasesPort(t *testing.T) {
	shell := requireBash(t)
	src := readEntrypoint(t)

	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("14\n"), 0o600); err != nil {
		t.Fatalf("seed PG_VERSION: %v", err)
	}
	// Seed a STALE postmaster.pid (a PID that is definitely not running) so the
	// handoff's stale-pid path removes it and releases the datadir.
	if err := os.WriteFile(filepath.Join(dataDir, "postmaster.pid"), []byte("2147480000\n"), 0o600); err != nil {
		t.Fatalf("seed postmaster.pid: %v", err)
	}
	// Seed internal.auto.conf with the WRONG (coordinator-template) content id
	// so we can assert the handoff corrects it to this pod's content id.
	if err := os.WriteFile(filepath.Join(dataDir, "internal.auto.conf"),
		[]byte("gp_contentid = -1\ngp_dbid = 5\n"), 0o600); err != nil {
		t.Fatalf("seed internal.auto.conf: %v", err)
	}

	handoffFn := extractShellFunc(t, src, "gpexpand_handoff_to_steady_state")
	upsertFn := extractShellFunc(t, src, "upsert_gp_contentid")
	// Stub logging + GPHOME/pg_ctl so the function runs standalone. pg_ctl stop is
	// a no-op stub (the pid is already dead), exercising the stale-pid cleanup.
	// upsert_gp_contentid is included so the handoff's content-id correction runs.
	script := "log_info() { :; }\nlog_warn() { :; }\nlog_error() { :; }\n" +
		"GPHOME=/tmp/nogphome\nCLOUDBERRY_CONTENT_ID=2\nCLOUDBERRY_SEGMENT_PORT=5432\n" +
		upsertFn + "\n" + handoffFn + "\n" +
		"gpexpand_handoff_to_steady_state \"" + dataDir + "\" && echo HANDOFF_DONE\n"

	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("handoff probe failed: %v\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "HANDOFF_DONE") {
		t.Errorf("gpexpand_handoff_to_steady_state must complete; got %q", string(out))
	}
	// The stale pidfile must be gone so the caller's exec postgres can bind 5432.
	if _, statErr := os.Stat(filepath.Join(dataDir, "postmaster.pid")); statErr == nil {
		t.Errorf("handoff must remove the stale postmaster.pid so the datadir/port are free")
	}
	// The handoff must have corrected gp_contentid from -1 to 2 (this pod's id),
	// replacing the value inherited from the coordinator template basebackup.
	conf, readErr := os.ReadFile(filepath.Join(dataDir, "internal.auto.conf"))
	if readErr != nil {
		t.Fatalf("read internal.auto.conf: %v", readErr)
	}
	confStr := string(conf)
	if !strings.Contains(confStr, "gp_contentid = 2") {
		t.Errorf("handoff must upsert `gp_contentid = 2` into internal.auto.conf; got %q", confStr)
	}
	if strings.Contains(confStr, "gp_contentid = -1") {
		t.Errorf("handoff must REPLACE the inherited `gp_contentid = -1`; still present in %q", confStr)
	}
}

// TestEntrypoint_UpsertGpContentID_ReplacesInherited proves the content-id
// mechanism directly: upsert_gp_contentid replaces any existing gp_contentid
// line (the coordinator template's -1) with `gp_contentid = <id>` — the EXACT
// line format init_segment writes for normal segments — and appends one when
// none is present. It must never emit a -C/-c flag.
func TestEntrypoint_UpsertGpContentID_ReplacesInherited(t *testing.T) {
	shell := requireBash(t)
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "upsert_gp_contentid")

	// Case 1: existing gp_contentid = -1 is REPLACED with 3.
	d1 := t.TempDir()
	if err := os.WriteFile(filepath.Join(d1, "internal.auto.conf"),
		[]byte("gp_contentid = -1\ngp_dbid = 6\n"), 0o600); err != nil {
		t.Fatalf("seed d1: %v", err)
	}
	// Case 2: no internal.auto.conf yet -> file created with gp_contentid appended.
	d2 := t.TempDir()

	script := "log_info() { :; }\nlog_warn() { :; }\n" + fn + "\n" +
		"upsert_gp_contentid \"" + d1 + "\" 3\n" +
		"upsert_gp_contentid \"" + d2 + "\" 4\n"
	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("upsert probe failed: %v\n%s", err, string(out))
	}

	c1, _ := os.ReadFile(filepath.Join(d1, "internal.auto.conf"))
	if !strings.Contains(string(c1), "gp_contentid = 3") || strings.Contains(string(c1), "gp_contentid = -1") {
		t.Errorf("upsert must replace inherited gp_contentid with `gp_contentid = 3`; got %q", string(c1))
	}
	// gp_dbid must be preserved (only gp_contentid is touched).
	if !strings.Contains(string(c1), "gp_dbid = 6") {
		t.Errorf("upsert must preserve other GUCs (gp_dbid); got %q", string(c1))
	}
	c2, _ := os.ReadFile(filepath.Join(d2, "internal.auto.conf"))
	if !strings.Contains(string(c2), "gp_contentid = 4") {
		t.Errorf("upsert must create internal.auto.conf with `gp_contentid = 4` when absent; got %q", string(c2))
	}
}

// TestEntrypoint_ExistingSegmentPathUnchanged proves REQUIRED BEHAVIOR: the
// normal (non-gpexpand) primary path is unchanged — it still runs the background
// password-setup start and start_postgres, and init_segment still self-initdb's
// for a non-managed segment.
func TestEntrypoint_ExistingSegmentPathUnchanged(t *testing.T) {
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "main")

	for _, want := range []string{
		`postgres -D "${SEGMENT_DATA_DIR}" &`, // background password-setup start
		`set_admin_password "${SEGMENT_DATA_DIR}"`,
		`start_postgres "${SEGMENT_DATA_DIR}"`,
	} {
		if !strings.Contains(fn, want) {
			t.Errorf("existing (non-gpexpand) segment path must be unchanged; missing %q", want)
		}
	}
	// init_segment must still self-initdb for non-managed segments.
	initSeg := extractShellFunc(t, src, "init_segment")
	if !strings.Contains(initSeg, "initdb") {
		t.Errorf("init_segment must still self-initdb for non-gpexpand segments")
	}
	if !strings.Contains(initSeg, "is_gpexpand_managed_segment") {
		t.Errorf("init_segment must still skip initdb for gpexpand-managed segments")
	}
}

// TestEntrypoint_SegmentStartDoesNotPassContentIDFlag proves the Cloudberry 2.1
// correction: the segment (primary|mirror) postgres start in start_postgres must
// NOT pass `-C <contentid>` (nor `-c <contentid>`). In standard PostgreSQL 14
// (which Cloudberry 2.1 is based on) `postgres -C <name>` means "print the value
// of config parameter <name> and exit", so `-C 0` FATALs with `unrecognized
// configuration parameter "0"`. The segment content id is instead carried by the
// GUC `gp_contentid` in the datadir's internal.auto.conf. The segment start must
// therefore mirror the normal execute-mode path: `postgres -D <dir> -c gp_role=execute`.
func TestEntrypoint_SegmentStartDoesNotPassContentIDFlag(t *testing.T) {
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "start_postgres")

	// The segment (execute-mode) start must NOT carry -C at all — content id
	// comes from internal.auto.conf (gp_contentid), not a command-line flag.
	if strings.Contains(fn, `-C "${CLOUDBERRY_CONTENT_ID}"`) || strings.Contains(fn, "-C ${CLOUDBERRY_CONTENT_ID}") {
		t.Errorf("segment start must NOT pass -C <contentid>: `postgres -C <name>` prints a config " +
			"parameter and exits in PostgreSQL 14 / Cloudberry 2.1 => FATAL. Content id is read from " +
			"internal.auto.conf (gp_contentid).")
	}
	// The segment execute-mode start must be exactly the normal path (no -C, no
	// content-id flag), identical in shape to a normal first-boot segment.
	if !strings.Contains(fn, `exec postgres -D "${data_dir}" -c gp_role=execute`) {
		t.Errorf("segment execute-mode start command must be `postgres -D <dir> -c gp_role=execute` (no -C flag)")
	}
	// STRICT: there must be NO content-id passed via lowercase `-c <contentid>`
	// either (that sets a bogus GUC and FATALs).
	for _, forbidden := range []string{
		`-c "${CLOUDBERRY_CONTENT_ID}"`,
		`-c ${CLOUDBERRY_CONTENT_ID}`,
		`-c "$CLOUDBERRY_CONTENT_ID"`,
		`-C "${CLOUDBERRY_CONTENT_ID}"`,
		`-C ${CLOUDBERRY_CONTENT_ID}`,
	} {
		if strings.Contains(fn, forbidden) {
			t.Errorf("segment start must NOT pass the content id as a postgres flag (found %q); "+
				"content id is set via gp_contentid in internal.auto.conf", forbidden)
		}
	}
	// The coordinator start must remain UNCHANGED (no -C; content is -1 via
	// internal.auto.conf and gp_role=dispatch).
	if !strings.Contains(fn, `exec postgres -D "${data_dir}" -c gp_role=dispatch`) {
		t.Errorf("coordinator start (dispatch mode) must be unchanged")
	}
}

// TestEntrypoint_NoContentIDPostgresFlagAnywhere is a whole-script guard: the
// segment content id must NEVER be passed to postgres/pg_ctl as EITHER `-C` or
// `-c` with the content id value. In Cloudberry 2.1 (PostgreSQL 14) `postgres -C
// <name>` prints a config parameter and exits (so `-C 0` FATALs), and lowercase
// `-c 0` sets a bogus GUC named "0" (also FATAL). The content id is set solely
// via the `gp_contentid` GUC in internal.auto.conf. Legitimate lowercase -c GUCs
// (`-c gp_role=...`) and psql -c are unaffected by these patterns.
func TestEntrypoint_NoContentIDPostgresFlagAnywhere(t *testing.T) {
	src := readEntrypoint(t)
	for _, forbidden := range []string{
		// lowercase -c <contentid> (bogus GUC)
		`-c "${CLOUDBERRY_CONTENT_ID}"`,
		`-c ${CLOUDBERRY_CONTENT_ID}`,
		`-c "$CLOUDBERRY_CONTENT_ID"`,
		`-c $CLOUDBERRY_CONTENT_ID`,
		`-c ${CBK_CONTENT_ID}`,
		`-c "${CBK_CONTENT_ID}"`,
		`-c ${content_id}`,
		`-c "${content_id}"`,
		// uppercase -C <contentid> (print-param-and-exit => FATAL)
		`-C "${CLOUDBERRY_CONTENT_ID}"`,
		`-C ${CLOUDBERRY_CONTENT_ID}`,
		`-C "$CLOUDBERRY_CONTENT_ID"`,
		`-C $CLOUDBERRY_CONTENT_ID`,
		`-C ${CBK_CONTENT_ID}`,
		`-C "${CBK_CONTENT_ID}"`,
		`-C ${content_id}`,
		`-C "${content_id}"`,
	} {
		if strings.Contains(src, forbidden) {
			t.Errorf("entrypoint must NOT pass the segment content id as a postgres/pg_ctl flag "+
				"(found %q); content id is set ONLY via gp_contentid in internal.auto.conf", forbidden)
		}
	}
}

// TestEntrypoint_DerivesContentIDFromOrdinal proves the per-pod content id is
// derived from the StatefulSet pod ordinal for primary/mirror segments (so the
// gp_contentid written into internal.auto.conf is correct per pod), and is fixed
// at -1 for the coordinator/standby.
func TestEntrypoint_DerivesContentIDFromOrdinal(t *testing.T) {
	shell := requireBash(t)
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "derive_ids_from_pod_name")

	cases := []struct {
		role, pod, wantContent string
	}{
		{"primary", "mycluster-segment-primary-0", "0"},
		{"primary", "mycluster-segment-primary-3", "3"},
		{"mirror", "mycluster-segment-mirror-2", "2"},
		{"coordinator", "mycluster-coordinator-0", "-1"},
		{"standby", "mycluster-coordinator-1", "-1"},
	}
	for _, tc := range cases {
		t.Run(tc.role+"/"+tc.pod, func(t *testing.T) {
			script := "CLOUDBERRY_ROLE=" + tc.role + "\nPOD_NAME=" + tc.pod + "\n" +
				"CLOUDBERRY_SEGMENT_COUNT=4\n" + fn +
				"\nread -r c d <<< \"$(derive_ids_from_pod_name)\"\necho \"CONTENT=${c}\"\n"
			cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("derive probe failed: %v\n%s", err, string(out))
			}
			if !strings.Contains(string(out), "CONTENT="+tc.wantContent+"\n") &&
				!strings.HasSuffix(strings.TrimSpace(string(out)), "CONTENT="+tc.wantContent) {
				t.Errorf("derive_ids_from_pod_name content = %q, want %s", string(out), tc.wantContent)
			}
		})
	}
}

// shimScriptPath resolves hack/gpexpand-pgctl-shim.sh (the BAKED shim) relative
// to the repo root (two levels up from this test package).
func shimScriptPath(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	p := filepath.Clean(filepath.Join(wd, "..", "..", "hack", "gpexpand-pgctl-shim.sh"))
	if _, statErr := os.Stat(p); statErr != nil {
		t.Fatalf("baked shim not found at %s: %v", p, statErr)
	}
	return p
}

// setupBakedShimEnv builds a fake ${GPHOME}/bin containing the REAL BAKED shim
// (hack/gpexpand-pgctl-shim.sh) installed AT pg_ctl, next to a "real" pg_ctl.real
// (an echo stub), plus a gpseg<N> datadir whose internal.auto.conf carries an
// INHERITED gp_contentid = -1 (as a coordinator-template basebackup would). It
// returns the gphome dir and the datadir. This mirrors exactly what the
// Dockerfile bakes at build time: pg_ctl = shim, pg_ctl.real = original binary.
func setupBakedShimEnv(t *testing.T, contentSuffix string) (gphome, dataDir string) {
	t.Helper()
	gphome = t.TempDir()
	bin := filepath.Join(gphome, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil { //nolint:gosec // test fixture
		t.Fatalf("mkdir gphome/bin: %v", err)
	}
	// The real pg_ctl (renamed to pg_ctl.real at build time): echoes its args so
	// we can assert no -C is injected and args pass through unchanged.
	if err := os.WriteFile(filepath.Join(bin, "pg_ctl.real"),
		[]byte("#!/usr/bin/env bash\necho \"REALARGS: $*\"\n"), 0o755); err != nil { //nolint:gosec // test fixture
		t.Fatalf("write fake pg_ctl.real: %v", err)
	}
	// Install the ACTUAL baked shim at pg_ctl (what the Dockerfile COPY+mv does).
	shimBytes, err := os.ReadFile(shimScriptPath(t))
	if err != nil {
		t.Fatalf("read baked shim: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bin, "pg_ctl"), shimBytes, 0o755); err != nil { //nolint:gosec // test fixture
		t.Fatalf("install baked shim: %v", err)
	}

	// The datadir is a gpseg<N> dir (the shim DERIVES the content id from N).
	pgdata := filepath.Join(t.TempDir(), "pgdata")
	dataDir = filepath.Join(pgdata, "gpseg"+contentSuffix)
	if err := os.MkdirAll(dataDir, 0o700); err != nil { //nolint:gosec // test fixture
		t.Fatalf("mkdir datadir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "internal.auto.conf"),
		[]byte("gp_contentid = -1\ngp_dbid = 5\n"), 0o600); err != nil {
		t.Fatalf("seed internal.auto.conf: %v", err)
	}
	return gphome, dataDir
}

// TestEntrypoint_BakedPgCtlShim_DerivesContentIDFromDatadir exercises the BAKED
// pg_ctl shim (hack/gpexpand-pgctl-shim.sh) end-to-end. The shim is baked AT
// ${GPHOME}/bin/pg_ctl at BUILD time (the real binary renamed to pg_ctl.real) so
// it wins regardless of PATH order — gpexpand's ssh sessions source
// cloudberry-env.sh which prepends ${GPHOME}/bin FIRST. For start/restart it
// must DERIVE the content id from the `-D <datadir>` gpseg<N> suffix (NO runtime
// env needed) and UPSERT `gp_contentid = <N>` into that datadir's
// internal.auto.conf (replacing the inherited coordinator-template -1), then
// pass ALL original args through to the real pg_ctl UNCHANGED — it must NEVER
// inject `-C` (nor `-c <contentid>`). Other subcommands (stop) pass through and
// do NOT touch the config.
func TestEntrypoint_BakedPgCtlShim_DerivesContentIDFromDatadir(t *testing.T) {
	shell := requireBash(t)
	// Datadir gpseg2 => the shim must derive content id 2 from the path.
	gphome, dataDir := setupBakedShimEnv(t, "2")
	pgctl := filepath.Join(gphome, "bin", "pg_ctl")

	// Invoke the shim: start (with -o) then stop.
	script := "echo START_O:; \"" + pgctl + "\" -D " + dataDir + " -o \" -p 5432 -c gp_role=utility -M \" start\n" +
		"echo STOP:; \"" + pgctl + "\" -D " + dataDir + " stop\n"

	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("baked shim probe failed: %v\n%s", err, string(out))
	}
	got := string(out)

	// The shim must NOT inject -C anywhere in the args it forwards to pg_ctl.
	for _, l := range strings.Split(got, "\n") {
		if !strings.HasPrefix(l, "REALARGS:") {
			continue
		}
		if strings.Contains(l, "-C ") || strings.Contains(l, "-C\t") {
			t.Errorf("shim must NOT inject -C into pg_ctl args (that FATALs on Cloudberry 2.1); got %q", l)
		}
		// The only legitimate lowercase -c is gpexpand's pre-existing gp_role GUC.
		if strings.Contains(l, "-c 2") {
			t.Errorf("shim must NOT inject the content id as a flag; got %q", l)
		}
	}

	// The datadir's internal.auto.conf must now carry gp_contentid = 2 (derived
	// from gpseg2, replacing the inherited -1) after the start.
	conf, readErr := os.ReadFile(filepath.Join(dataDir, "internal.auto.conf"))
	if readErr != nil {
		t.Fatalf("read internal.auto.conf: %v", readErr)
	}
	confStr := string(conf)
	if !strings.Contains(confStr, "gp_contentid = 2") {
		t.Errorf("shim must upsert `gp_contentid = 2` (derived from gpseg2) into internal.auto.conf; got %q", confStr)
	}
	if strings.Contains(confStr, "gp_contentid = -1") {
		t.Errorf("shim must REPLACE the inherited `gp_contentid = -1`; still present in %q", confStr)
	}
}

// TestEntrypoint_BakedPgCtlShim_CoordinatorIsNoOp proves the baked shim treats a
// coordinator datadir (gpseg-1) as content id -1 — the upsert of an already-
// correct value is a harmless idempotent no-op, and the real pg_ctl still runs.
func TestEntrypoint_BakedPgCtlShim_CoordinatorIsNoOp(t *testing.T) {
	shell := requireBash(t)
	// gpseg-1 => content id -1 (coordinator).
	gphome, dataDir := setupBakedShimEnv(t, "-1")
	pgctl := filepath.Join(gphome, "bin", "pg_ctl")

	cmd := exec.Command(shell, "-c", "\""+pgctl+"\" -D "+dataDir+" start") //nolint:gosec // fixed shell
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("baked shim coordinator probe failed: %v\n%s", err, string(out))
	}
	conf, _ := os.ReadFile(filepath.Join(dataDir, "internal.auto.conf"))
	if !strings.Contains(string(conf), "gp_contentid = -1") {
		t.Errorf("shim must keep `gp_contentid = -1` for the coordinator gpseg-1 datadir; got %q", string(conf))
	}
}

// TestBakedShim_DerivesFromGpsegAndUpsertsNoDashC is a SOURCE-level guard on the
// baked shim script: it must derive the content id from the gpseg<N> datadir
// suffix, upsert gp_contentid, and NEVER inject a -C flag.
func TestBakedShim_DerivesFromGpsegAndUpsertsNoDashC(t *testing.T) {
	shimBytes, err := os.ReadFile(shimScriptPath(t))
	if err != nil {
		t.Fatalf("read baked shim: %v", err)
	}
	shim := string(shimBytes)

	// The shim must carry the marker line (the Dockerfile assertion greps it).
	if !strings.Contains(shim, "cloudberry-k8s gpexpand pg_ctl shim") {
		t.Errorf("baked shim must carry the marker line")
	}
	// Content id DERIVED from the gpseg<N> datadir suffix (self-contained).
	if !strings.Contains(shim, "gpseg(-?[0-9]+)") {
		t.Errorf("baked shim must derive the content id from the gpseg<N> datadir suffix")
	}
	if !strings.Contains(shim, "cbk_derive_content_id_from_datadir") {
		t.Errorf("baked shim must have the datadir->content-id derivation helper")
	}
	// It must upsert gp_contentid (not a -C flag).
	if !strings.Contains(shim, "gp_contentid = ${content_id}") {
		t.Errorf("baked shim must upsert `gp_contentid = <id>` into internal.auto.conf")
	}
	// It must exec the real pg_ctl unchanged and NEVER inject -C.
	if !strings.Contains(shim, `exec "${CBK_REAL_PGCTL}" "$@"`) {
		t.Errorf("baked shim must exec the real pg_ctl with the original args unchanged")
	}
	if strings.Contains(shim, "-C ") || strings.Contains(shim, "-C\t") {
		t.Errorf("baked shim must NEVER inject a -C flag")
	}
}

// TestBakedShim_IsValidBash runs `bash -n` over the baked shim so a syntax
// regression fails fast.
func TestBakedShim_IsValidBash(t *testing.T) {
	shell := requireBash(t)
	cmd := exec.Command(shell, "-n", shimScriptPath(t)) //nolint:gosec // fixed args
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n reported a syntax error in the baked shim: %v\n%s", err, string(out))
	}
}

// TestDockerfile_BakesPgCtlShim asserts the cluster image Dockerfile bakes the
// shim at build time: it COPYs hack/gpexpand-pgctl-shim.sh, renames the real
// pg_ctl to pg_ctl.real, installs the shim at pg_ctl, and carries build
// assertions that pg_ctl.real exists and pg_ctl is the shim.
func TestDockerfile_BakesPgCtlShim(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	df := filepath.Clean(filepath.Join(wd, "..", "..", "Dockerfile.cloudberry-official"))
	b, err := os.ReadFile(df)
	if err != nil {
		t.Fatalf("read Dockerfile.cloudberry-official: %v", err)
	}
	src := string(b)

	for _, want := range []string{
		// COPYs the shim script into the image.
		"hack/gpexpand-pgctl-shim.sh",
		// Renames the real binary (idempotent guard) and installs the shim.
		`mv "${GPBIN}/pg_ctl" "${GPBIN}/pg_ctl.real"`,
		// Build assertion: the real binary is preserved.
		`test -x "${GPBIN}/pg_ctl.real"`,
		// Build assertion: pg_ctl is the shim (marker present).
		`grep -q 'cloudberry-k8s gpexpand pg_ctl shim' "${GPBIN}/pg_ctl"`,
		// Build assertion: no -C flag baked into the shim.
		`! grep -q '\-C ' "${GPBIN}/pg_ctl"`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("Dockerfile.cloudberry-official must bake the pg_ctl shim; missing %q", want)
		}
	}
}

// TestEntrypoint_InstallShimIsTolerantNoOp proves the entrypoint's
// install_gpexpand_pgctl_shim is now a TOLERANT no-op: the shim is baked into
// the image, so the function must NOT attempt the (SCC-forbidden) runtime rename
// of the root-owned ${GPHOME}/bin/pg_ctl, and must NOT fail. It verifies the
// baked shim and returns success.
func TestEntrypoint_InstallShimIsTolerantNoOp(t *testing.T) {
	shell := requireBash(t)
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "install_gpexpand_pgctl_shim")

	// The function must NOT perform a runtime rename/write of ${GPHOME}/bin/pg_ctl
	// (that failed under restricted-v2 with "cannot write ...").
	code := stripShellComments(fn)
	for _, forbidden := range []string{
		`mv "${pgctl}" "${real_pgctl}"`,
		`cat > "${pgctl}"`,
	} {
		if strings.Contains(code, forbidden) {
			t.Errorf("install_gpexpand_pgctl_shim must NOT install the shim at runtime (it is baked); found %q", forbidden)
		}
	}

	// Run it against a fake GPHOME whose pg_ctl already IS the baked shim: it
	// must succeed (return 0) and NOT fail on the read-only bindir.
	gphome, _ := setupBakedShimEnv(t, "2")
	script := "log_info() { :; }\nlog_warn() { :; }\n" +
		"GPEXPAND_REAL_PGCTL_SUFFIX=.real\n" +
		"GPEXPAND_SHIM_MARKER='# cloudberry-k8s gpexpand pg_ctl shim'\n" +
		"GPHOME=" + gphome + "\nCLOUDBERRY_CONTENT_ID=2\n" +
		fn + "\nif install_gpexpand_pgctl_shim; then echo SHIM_OK; else echo SHIM_FAIL; fi\n"
	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install_gpexpand_pgctl_shim no-op probe failed: %v\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "SHIM_OK") {
		t.Errorf("install_gpexpand_pgctl_shim must succeed (tolerant no-op) when the shim is baked; got %q", string(out))
	}
}

// TestEntrypoint_GpexpandManagedInstallsShim proves main() invokes the (now
// no-op) shim verification for gpexpand-managed primary and mirror segments
// BEFORE waiting for gpexpand to initialize the datadir. The shim itself is
// baked into the image; this call just verifies/logs it.
func TestEntrypoint_GpexpandManagedInstallsShim(t *testing.T) {
	src := readEntrypoint(t)
	// Strip comments so the ordering check targets actual CODE, not the
	// explanatory comments that legitimately mention wait_for_gpexpand_init.
	fn := stripShellComments(extractShellFunc(t, src, "main"))

	installIdx := strings.Index(fn, "install_gpexpand_pgctl_shim")
	if installIdx < 0 {
		t.Fatalf("main() must verify the pg_ctl shim for gpexpand-managed segments")
	}
	// The shim verify must occur before the wait_for_gpexpand_init call.
	waitIdx := strings.Index(fn, "wait_for_gpexpand_init")
	if waitIdx < 0 || installIdx > waitIdx {
		t.Errorf("install_gpexpand_pgctl_shim must run BEFORE wait_for_gpexpand_init in main()")
	}
}

// requireBash skips the test when bash is unavailable (keeps `go test ./...`
// green on minimal environments while still exercising the shell logic where
// bash exists — including CI and the dev machine).
func requireBash(t *testing.T) string {
	t.Helper()
	shell, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available; skipping entrypoint shell test")
	}
	return shell
}

// readEntrypoint returns the entrypoint script source.
func readEntrypoint(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(entrypointPath(t))
	if err != nil {
		t.Fatalf("read entrypoint: %v", err)
	}
	return string(b)
}

// TestEntrypoint_BashSyntax runs `bash -n` over the entrypoint so a syntax
// regression fails fast (mirrors the builder gpexpand `bash -n` harness).
func TestEntrypoint_BashSyntax(t *testing.T) {
	shell := requireBash(t)
	cmd := exec.Command(shell, "-n", entrypointPath(t)) //nolint:gosec // fixed args
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n reported a syntax error: %v\n%s", err, string(out))
	}
}

// TestEntrypoint_RootlessSSHD_UsesDevNullConfig asserts the source folds in the
// DevOps in-place fix: start_rootless_sshd must invoke sshd with `-f /dev/null`
// (so it does NOT read the root-owned, gpadmin-unreadable /etc/ssh/sshd_config
// under restricted-v2) while keeping the -o options (Port, UsePAM=no, HostKey).
func TestEntrypoint_RootlessSSHD_UsesDevNullConfig(t *testing.T) {
	src := readEntrypoint(t)

	if !strings.Contains(src, "/usr/sbin/sshd -f /dev/null") {
		t.Errorf("start_rootless_sshd must invoke sshd with `-f /dev/null` " +
			"(skip the root-owned /etc/ssh/sshd_config under restricted-v2)")
	}
	// The -o options must remain so the rootless daemon is fully configured.
	for _, opt := range []string{
		`-o "Port=${CLUSTER_SSH_PORT}"`,
		`-o "UsePAM=no"`,
		`-o "HostKey=${SSHD_HOSTKEY}"`,
		`-o "PasswordAuthentication=no"`,
	} {
		if !strings.Contains(src, opt) {
			t.Errorf("start_rootless_sshd must keep the sshd option %q", opt)
		}
	}
}

// runManagedProbe sources ONLY the is_gpexpand_managed_segment function out of
// the entrypoint and evaluates it with the given env, returning true when the
// function reports the pod is gpexpand-managed (exit 0). It extracts the
// function body by range so no side effects from the top-level entrypoint run.
func runManagedProbe(t *testing.T, shell, src string, env map[string]string) bool {
	t.Helper()

	fn := extractShellFunc(t, src, "is_gpexpand_managed_segment")
	// Drive the function; echo a stable token on success so we don't rely on the
	// process exit code alone (set -e is intentionally NOT used here).
	script := fn + "\nif is_gpexpand_managed_segment; then echo MANAGED; else echo NORMAL; fi\n"

	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("probe failed: %v\n%s", err, string(out))
	}
	return strings.Contains(string(out), "MANAGED")
}

// stripShellComments removes whole-line `#` comments so string-presence checks
// assert on actual shell CODE, not on explanatory comments (which legitimately
// mention pg_ctl/postgres). Inline trailing comments are rare in this script and
// intentionally left; the checks target command invocations at line start.
func stripShellComments(s string) string {
	var b strings.Builder
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		b.WriteString(l)
		b.WriteString("\n")
	}
	return b.String()
}

// extractShellFunc returns the source text of a top-level `name() { ... }`
// function from the script, matched by brace depth from its opening line.
func extractShellFunc(t *testing.T, src, name string) string {
	t.Helper()
	lines := strings.Split(src, "\n")
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), name+"() {") {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("function %s not found in entrypoint", name)
	}
	depth := 0
	for i := start; i < len(lines); i++ {
		depth += strings.Count(lines[i], "{") - strings.Count(lines[i], "}")
		if depth == 0 {
			return strings.Join(lines[start:i+1], "\n")
		}
	}
	t.Fatalf("unterminated function %s", name)
	return ""
}

// TestEntrypoint_IsGpexpandManagedSegment covers the PER-POD gpexpand-managed
// detection the operator relies on (the StatefulSet env is shared across all
// replicas, so each pod decides from its own ordinal vs the operator-provided
// base count):
//   - explicit CLOUDBERRY_GPEXPAND_MANAGED=true always wins (local/manual runs);
//   - with CLOUDBERRY_EXPANSION_BASE_COUNT set, a pod whose content id (ordinal)
//     >= base is a NEW scale-out segment (managed => skip initdb); ordinal < base
//     is an existing segment (NOT managed => normal initdb);
//   - absent/blank base count => normal initdb (unchanged first-boot behavior).
func TestEntrypoint_IsGpexpandManagedSegment(t *testing.T) {
	shell := requireBash(t)
	src := readEntrypoint(t)

	cases := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{
			name: "explicit override true",
			env:  map[string]string{"CLOUDBERRY_GPEXPAND_MANAGED": "true"},
			want: true,
		},
		{
			name: "new segment: ordinal >= base",
			env: map[string]string{
				"CLOUDBERRY_EXPANSION_BASE_COUNT": "2",
				"CLOUDBERRY_CONTENT_ID":           "2",
			},
			want: true,
		},
		{
			name: "new segment: ordinal well above base",
			env: map[string]string{
				"CLOUDBERRY_EXPANSION_BASE_COUNT": "2",
				"CLOUDBERRY_CONTENT_ID":           "5",
			},
			want: true,
		},
		{
			name: "existing segment: ordinal < base",
			env: map[string]string{
				"CLOUDBERRY_EXPANSION_BASE_COUNT": "2",
				"CLOUDBERRY_CONTENT_ID":           "1",
			},
			want: false,
		},
		{
			name: "no base count -> normal initdb",
			env:  map[string]string{"CLOUDBERRY_CONTENT_ID": "3"},
			want: false,
		},
		{
			name: "coordinator content -1 -> not managed",
			env: map[string]string{
				"CLOUDBERRY_EXPANSION_BASE_COUNT": "2",
				"CLOUDBERRY_CONTENT_ID":           "-1",
			},
			want: false,
		},
		{
			name: "non-numeric base ignored",
			env: map[string]string{
				"CLOUDBERRY_EXPANSION_BASE_COUNT": "abc",
				"CLOUDBERRY_CONTENT_ID":           "3",
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the two env vars unless the case sets them, so os.Environ()
			// leakage can't taint the result.
			env := map[string]string{
				"CLOUDBERRY_GPEXPAND_MANAGED":     "",
				"CLOUDBERRY_EXPANSION_BASE_COUNT": "",
				"CLOUDBERRY_CONTENT_ID":           "",
			}
			for k, v := range tc.env {
				env[k] = v
			}
			got := runManagedProbe(t, shell, src, env)
			if got != tc.want {
				t.Errorf("is_gpexpand_managed_segment = %v, want %v (env=%v)",
					got, tc.want, tc.env)
			}
		})
	}
}
