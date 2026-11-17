// Package entrypoint contains black-box shell tests for the Cloudberry DB
// container entrypoint (hack/docker-entrypoint-cloudberry.sh). This file
// covers the DEF-2 fix (TASK-U027): the standby coordinator must register
// ITSELF in the coordinator's gp_segment_configuration once its WAL receiver
// is streaming — register_segments() only ever inserts the coordinator,
// primaries and mirrors, so a streaming-healthy standby stayed invisible to
// FTS/gpstate/gpactivatestandby until it was registered by hand (U015).
package entrypoint

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runRegisterStandby extracts register_standby from the entrypoint and runs it
// with stubbed log helpers, psql and sleep, under set -euo pipefail (the
// entrypoint's own strictness). The psql stub records every invocation to
// psqlLog and answers the three queries register_standby makes from env:
// WAL_STATUS (pg_stat_wal_receiver.status), STANDBY_COUNT (existing standby
// rows). It returns the function's exit status (via the REGISTER_OK token)
// and the psql call log.
func runRegisterStandby(t *testing.T, env map[string]string) (bool, string) {
	t.Helper()
	shell := requireBash(t)
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "register_standby")

	psqlLog := filepath.ToSlash(filepath.Join(t.TempDir(), "psql.log"))

	var prefix strings.Builder
	prefix.WriteString("set -euo pipefail\n")
	prefix.WriteString("log_info() { :; }\nlog_warn() { :; }\nlog_error() { :; }\n")
	prefix.WriteString("sleep() { :; }\n")
	// psql stub: log the full invocation, then answer by query content.
	prefix.WriteString("psql() {\n")
	prefix.WriteString("  printf 'CALL:%s\\n' \"$*\" >> \"" + psqlLog + "\"\n")
	prefix.WriteString("  case \"$*\" in\n")
	prefix.WriteString("    *pg_stat_wal_receiver*) echo \"${WAL_STATUS-streaming}\" ;;\n")
	prefix.WriteString("    *\"SELECT count(*) FROM gp_segment_configuration\"*) echo \"${STANDBY_COUNT-0}\" ;;\n")
	prefix.WriteString("    *\"SELECT 1\"*) echo \"1\" ;;\n")
	prefix.WriteString("  esac\n")
	prefix.WriteString("}\n")
	// Production-shaped defaults; overridden per-case via env.
	for k, v := range map[string]string{
		"CLOUDBERRY_SEGMENT_PORT":              "5432",
		"CLOUDBERRY_COORDINATOR_HOST":          "test-cluster-coord-hl",
		"COORDINATOR_DATA_DIR":                 "/data/pgdata/gpseg-1",
		"CLOUDBERRY_DB_ID":                     "2",
		"CLOUDBERRY_STANDBY_REGISTER_RETRIES":  "1",
		"CLOUDBERRY_STANDBY_REGISTER_INTERVAL": "1",
		"HOSTNAME":                             "test-cluster-coordinator-standby-0",
		"WAL_STATUS":                           "streaming",
		"STANDBY_COUNT":                        "0",
	} {
		prefix.WriteString(k + "=\"" + v + "\"\n")
	}
	for k, v := range env {
		prefix.WriteString(k + "=\"" + v + "\"\n")
	}

	script := prefix.String() + fn +
		"\nif register_standby; then echo REGISTER_OK; else echo REGISTER_FAIL; fi\n"
	cmd := exec.Command(shell, "-c", script) //nolint:gosec // fixed shell, generated body
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("register_standby probe failed: %v\n%s", err, string(out))
	}

	logBytes, readErr := os.ReadFile(psqlLog)
	if readErr != nil {
		t.Fatalf("read psql log: %v", readErr)
	}
	return strings.Contains(string(out), "REGISTER_OK"), string(logBytes)
}

// TestEntrypoint_RegisterStandby_InsertsRowWhenStreaming proves the DEF-2 fix:
// once the standby's WAL receiver reports streaming, register_standby inserts
// the standby row (dbid=2, content=-1, role/preferred_role m/m, s/u) into the
// COORDINATOR's gp_segment_configuration over TCP (-h), with
// allow_system_table_mods — the exact row U015 had to insert by hand.
func TestEntrypoint_RegisterStandby_InsertsRowWhenStreaming(t *testing.T) {
	ok, log := runRegisterStandby(t, nil)
	if !ok {
		t.Fatalf("register_standby must succeed when the WAL receiver streams; log:\n%s", log)
	}

	// The INSERT went to the coordinator over TCP, gated on the local
	// streaming check first.
	if !strings.Contains(log, "SELECT status FROM pg_stat_wal_receiver") {
		t.Errorf("registration must be gated on pg_stat_wal_receiver; log:\n%s", log)
	}
	if !strings.Contains(log, "-h test-cluster-coord-hl") {
		t.Errorf("catalog INSERT must target the coordinator over TCP (-h); log:\n%s", log)
	}
	for _, want := range []string{
		"SET allow_system_table_mods = true",
		"INSERT INTO gp_segment_configuration",
		"(2, -1, 'm', 'm', 's', 'u', 5432,",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("standby INSERT must carry %q; log:\n%s", want, log)
		}
	}
	// The catalog hostname is the standby pod FQDN under the derived standby
	// headless service (<cluster>-coord-hl -> <cluster>-standby-hl), matching
	// util.StandbyServiceName and the row live-verified in U015.
	host := "test-cluster-coordinator-standby-0.test-cluster-standby-hl"
	if !strings.Contains(log, host) {
		t.Errorf("standby INSERT must register hostname %q; log:\n%s", host, log)
	}
}

// TestEntrypoint_RegisterStandby_SkipsWhenAlreadyRegistered proves
// idempotence: an existing standby row (previous boot or manual registration)
// suppresses a second INSERT.
func TestEntrypoint_RegisterStandby_SkipsWhenAlreadyRegistered(t *testing.T) {
	ok, log := runRegisterStandby(t, map[string]string{"STANDBY_COUNT": "1"})
	if !ok {
		t.Fatalf("register_standby must succeed (skip) when already registered; log:\n%s", log)
	}
	if strings.Contains(log, "INSERT INTO gp_segment_configuration") {
		t.Errorf("register_standby must NOT insert when a standby row exists; log:\n%s", log)
	}
}

// TestEntrypoint_RegisterStandby_GatedOnStreaming proves the registration only
// happens when sync is healthy: a WAL receiver that never reaches streaming
// (empty status) makes register_standby give up WITHOUT issuing ANY
// coordinator query — no catalog row is invented for a non-streaming standby.
func TestEntrypoint_RegisterStandby_GatedOnStreaming(t *testing.T) {
	ok, log := runRegisterStandby(t, map[string]string{
		"WAL_STATUS": "", // never streaming
	})
	if ok {
		t.Errorf("register_standby must fail (bounded give-up) when never streaming; log:\n%s", log)
	}
	if strings.Contains(log, "-h test-cluster-coord-hl") {
		t.Errorf("a non-streaming standby must not touch the coordinator catalog; log:\n%s", log)
	}
	if strings.Contains(log, "INSERT INTO gp_segment_configuration") {
		t.Errorf("no INSERT may be emitted before streaming; log:\n%s", log)
	}
}

// TestEntrypoint_RegisterStandby_ServiceOverride proves the derived standby
// headless service can be overridden via CLOUDBERRY_STANDBY_SERVICE (mirroring
// the CLOUDBERRY_SEGMENT_SERVICE override register_segments honors).
func TestEntrypoint_RegisterStandby_ServiceOverride(t *testing.T) {
	_, log := runRegisterStandby(t, map[string]string{
		"CLOUDBERRY_STANDBY_SERVICE": "custom-standby-svc",
	})
	host := "test-cluster-coordinator-standby-0.custom-standby-svc"
	if !strings.Contains(log, host) {
		t.Errorf("standby INSERT must honor CLOUDBERRY_STANDBY_SERVICE (%q); log:\n%s", host, log)
	}
}

// TestEntrypoint_MainStandbyBranchRegistersStandby proves main() actually
// spawns the registration on the standby path, BEFORE start_postgres execs
// postgres (after the exec nothing else in the entrypoint can run).
func TestEntrypoint_MainStandbyBranchRegistersStandby(t *testing.T) {
	src := readEntrypoint(t)
	fn := stripShellComments(extractShellFunc(t, src, "main"))

	start := strings.Index(fn, "standby)")
	end := strings.Index(fn, "primary)")
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("main() must have a standby branch before the primary branch")
	}
	branch := fn[start:end]

	regIdx := strings.Index(branch, "register_standby &")
	startIdx := strings.Index(branch, `start_postgres "${COORDINATOR_DATA_DIR}"`)
	if regIdx < 0 {
		t.Fatalf("main() standby branch must spawn `register_standby &` (DEF-2)")
	}
	if startIdx < 0 || startIdx < regIdx {
		t.Errorf("register_standby must be spawned BEFORE start_postgres execs postgres " +
			"(nothing runs after the exec)")
	}
}

// TestEntrypoint_RegisterSegmentsStillOmitsStandby documents the (unchanged)
// coordinator-side behavior: register_segments itself still inserts only
// coordinator/primaries/mirrors — standby registration is the standby's own
// job now. Guards against someone re-pointing the fix at the wrong site.
func TestEntrypoint_RegisterSegmentsStillOmitsStandby(t *testing.T) {
	src := readEntrypoint(t)
	fn := extractShellFunc(t, src, "register_segments")
	if strings.Contains(stripShellComments(fn), "register_standby") {
		t.Errorf("register_segments must not spawn the standby task itself; " +
			"registration is gated on the standby's own streaming state")
	}
}
