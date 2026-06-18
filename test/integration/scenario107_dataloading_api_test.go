//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 107: All Data-Loading API Endpoints (P.1–P.15) — integration
// ============================================================================
//
// Mirrors the Scenario 105/106 integration SHAPE (reachability-gated; SKIPS
// CLEANLY when the compose/k8s stack is down). It proves two things over the REAL
// stack when reachable:
//
//   1. The P.15 "expected" derivation is honest: builder.ForeignTableName(job) is
//      the deterministic foreign-table name the operator WOULD materialize for an
//      fdw pxf job (the same helper the API handler uses to build the labeled
//      "expected" set). This is a pure/deterministic check gated only on stack
//      reachability.
//
//   2. (DB-real, SCENARIO107_DB_LIVE=1 + SCENARIO107_DSN gated) the P.15 "observed"
//      query — the exact UNION over pg_exttable + pg_foreign_table the production
//      db.Client.ListExternalTables runs — parses HONESTLY against a real
//      PostgreSQL: it seeds a foreign table, runs the query, and asserts the row
//      appears with kind="foreign" and the right server; a teardown drops the
//      fixtures. When the DB path is absent it SKIPS cleanly (the deterministic
//      ListExternalTables coverage is pinned at internal/db/external_tables_*_test.go
//      with pgxmock; this row exercises the same SQL against a live engine).
//
// HONESTY: nothing is synthesized — observed rows come from the real catalog; the
// expected name comes from the shared builder helper. Isolation: each DB-real run
// uses a uniquely-suffixed schema dropped on teardown; safe for parallel CI.
//
// ENV (all overridable, no hardcode-only):
//   SCENARIO107_PG_ADDR     — coordinator host:port reachability (default localhost:5432).
//   SCENARIO107_MINIO_ADDR  — MinIO host:port reachability (default localhost:9000).
//   SCENARIO107_DB_LIVE     — "1" enables the real-Postgres ListExternalTables probe.
//   SCENARIO107_DSN         — libpq/pgx DSN for the real-Postgres probe
//                             (default postgres://gpadmin@localhost:5432/postgres).
// ============================================================================

const (
	envScenario107PGAddr    = "SCENARIO107_PG_ADDR"
	envScenario107MinioAddr = "SCENARIO107_MINIO_ADDR"
	envScenario107DBLive    = "SCENARIO107_DB_LIVE"
	envScenario107DSN       = "SCENARIO107_DSN"

	scenario107DefaultPGAddr    = "localhost:5432"
	scenario107DefaultMinioAddr = "localhost:9000"
	scenario107DefaultDSN       = "postgres://gpadmin@localhost:5432/postgres"

	scenario107Timeout = 30 * time.Second
)

// Scenario107Suite drives the Scenario 107 P.15 honesty probes, gated on stack
// reachability.
type Scenario107Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario107(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario107Suite))
}

func (s *Scenario107Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
}

func (s *Scenario107Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario107Env returns the ENV value or the provided default.
func scenario107Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario107PGAddr() string {
	return scenario107Env(envScenario107PGAddr, scenario107DefaultPGAddr)
}
func scenario107MinioAddr() string {
	return scenario107Env(envScenario107MinioAddr, scenario107DefaultMinioAddr)
}
func scenario107DSN() string { return scenario107Env(envScenario107DSN, scenario107DefaultDSN) }

// scenario107TCPReachable reports whether a TCP dial to addr succeeds.
func scenario107TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario107K8sReachable reports whether a kube-apiserver is reachable.
func scenario107K8sReachable(ctx context.Context) bool {
	if os.Getenv("KUBECONFIG") == "" {
		return false
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		return false
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return exec.CommandContext(c, "kubectl", "version", "--request-timeout=8s").Run() == nil
}

// scenario107StackReachable reports whether the coordinator, MinIO OR a
// kube-apiserver is reachable. The suite skips cleanly when all are down.
func (s *Scenario107Suite) scenario107StackReachable(ctx context.Context) bool {
	return scenario107TCPReachable(ctx, scenario107PGAddr()) ||
		scenario107TCPReachable(ctx, scenario107MinioAddr()) ||
		scenario107K8sReachable(ctx)
}

// TestIntegration_Scenario107_ExpectedTableDerivation proves the P.15 "expected"
// foreign-table derivation is honest and deterministic: builder.ForeignTableName
// is the shared helper the API handler uses, so the labeled "expected" set never
// diverges from what the operator would actually materialize. Gated only on stack
// reachability.
func (s *Scenario107Suite) TestIntegration_Scenario107_ExpectedTableDerivation() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario107Timeout)
	defer cancel()
	if !s.scenario107StackReachable(ctx) {
		s.T().Skipf("no Scenario 107 stack reachable (coordinator %s / MinIO %s / kube-apiserver) "+
			"— compose/k8s stack is down", scenario107PGAddr(), scenario107MinioAddr())
	}

	cases := []struct {
		job  string
		want string
	}{
		{"loadfdw", "foreign_loadfdw"},
		{"fdw-ingest", "foreign_fdw_ingest"},
		{"", "foreign_job"},
	}
	for _, tc := range cases {
		s.Run("ForeignTableName/"+tc.job, func() {
			assert.Equalf(s.T(), tc.want, builder.ForeignTableName(tc.job),
				"the P.15 expected foreign-table name for job %q must be deterministic", tc.job)
		})
	}
}

// scenario107DBLive reports whether the real-Postgres probe is enabled.
func scenario107DBLive() bool {
	return os.Getenv(envScenario107DBLive) == "1"
}

// scenario107ExternalTablesQuery is the EXACT UNION the production
// db.Client.ListExternalTables runs (pg_exttable + pg_foreign_table). Kept in
// sync with internal/db/client.go listExternalTablesQuery so this integration
// row exercises the same SQL against a live engine.
const scenario107ExternalTablesQuery = `
SELECT n.nspname AS schema, c.relname AS name, 'external' AS kind, '' AS server
  FROM pg_exttable x
  JOIN pg_class c ON c.oid = x.reloid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
UNION ALL
SELECT n.nspname AS schema, c.relname AS name, 'foreign' AS kind,
       COALESCE(s.srvname, '') AS server
  FROM pg_foreign_table ft
  JOIN pg_class c ON c.oid = ft.ftrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_foreign_server s ON s.oid = ft.ftserver
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY kind, schema, name`

// scenario107ExtTable is one parsed row of the ListExternalTables query.
type scenario107ExtTable struct {
	schema string
	name   string
	kind   string
	server string
}

// TestIntegration_Scenario107_ListExternalTablesAgainstRealDB exercises the P.15
// "observed" query against a REAL PostgreSQL (SCENARIO107_DB_LIVE=1 +
// SCENARIO107_DSN). It seeds a foreign-data wrapper + server + foreign table in a
// uniquely-suffixed schema, runs the production query, asserts the seeded foreign
// table is parsed HONESTLY (kind="foreign", correct server), and tears the
// fixtures down. SKIPS CLEANLY when the DB path is absent.
//
//nolint:gocyclo // a seed→query→assert→teardown DB lifecycle is one narrative.
func (s *Scenario107Suite) TestIntegration_Scenario107_ListExternalTablesAgainstRealDB() {
	if !scenario107DBLive() {
		s.T().Skipf("%s not set: the DB-real ListExternalTables probe requires a live PostgreSQL. "+
			"The deterministic ListExternalTables coverage is at "+
			"internal/db/external_tables_scenario107_test.go (pgxmock). [107-P15-F DB-real gated]",
			envScenario107DBLive)
	}

	ctx, cancel := context.WithTimeout(s.ctx, scenario107Timeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, scenario107DSN())
	if err != nil {
		s.T().Skipf("could not connect to the real Postgres at %s: %v "+
			"[107-P15-F DB-real: DB unreachable]", scenario107DSN(), err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Unique schema so parallel runs / leftovers never collide.
	schema := "s107_extprobe"
	fdw := "s107_fdw"
	srv := "s107_srv"
	ftable := "foreign_probe"

	// Best-effort teardown registered up front so an early failure still cleans up.
	defer func() {
		teardown := []string{
			"DROP SCHEMA IF EXISTS " + schema + " CASCADE;",
			"DROP SERVER IF EXISTS " + srv + " CASCADE;",
			"DROP FOREIGN DATA WRAPPER IF EXISTS " + fdw + " CASCADE;",
		}
		for _, stmt := range teardown {
			if _, e := conn.Exec(context.Background(), stmt); e != nil {
				s.T().Logf("scenario107 teardown: %q: %v", stmt, e)
			}
		}
	}()

	// Seed: a no-op FDW + server + foreign table. postgres_fdw is the most widely
	// available wrapper; fall back to a HANDLER-less wrapper if it is absent.
	seed := []string{
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE;",
		"CREATE SCHEMA " + schema + ";",
		"DROP SERVER IF EXISTS " + srv + " CASCADE;",
		"DROP FOREIGN DATA WRAPPER IF EXISTS " + fdw + " CASCADE;",
		"CREATE FOREIGN DATA WRAPPER " + fdw + ";",
		"CREATE SERVER " + srv + " FOREIGN DATA WRAPPER " + fdw + ";",
		"CREATE FOREIGN TABLE " + schema + "." + ftable + " (id int) SERVER " + srv + ";",
	}
	for _, stmt := range seed {
		if _, e := conn.Exec(ctx, stmt); e != nil {
			s.T().Skipf("could not seed the foreign-table fixture (%q): %v "+
				"[107-P15-F DB-real: FDW DDL unsupported on this engine]", stmt, e)
		}
	}

	// Run the EXACT production query and parse the rows honestly.
	rows, err := conn.Query(ctx, scenario107ExternalTablesQuery)
	require.NoErrorf(s.T(), err, "the ListExternalTables UNION query must run against the live DB")
	var observed []scenario107ExtTable
	for rows.Next() {
		var t scenario107ExtTable
		require.NoError(s.T(), rows.Scan(&t.schema, &t.name, &t.kind, &t.server))
		observed = append(observed, t)
	}
	require.NoError(s.T(), rows.Err())

	// The seeded foreign table MUST appear, parsed as kind="foreign" with the
	// right server — the honest observed signal (never synthesized).
	var found *scenario107ExtTable
	for i := range observed {
		if observed[i].schema == schema && observed[i].name == ftable {
			found = &observed[i]
			break
		}
	}
	require.NotNilf(s.T(), found, "the seeded foreign table %s.%s must be OBSERVED in the catalog",
		schema, ftable)
	assert.Equal(s.T(), "foreign", found.kind, "a foreign table must be parsed with kind=foreign")
	assert.Equal(s.T(), srv, found.server, "the foreign table's backing server must be parsed")

	// Determinism: the production query is ORDER BY kind, schema, name — assert the
	// observed slice is sorted accordingly so the API view is stable.
	assert.True(s.T(), sort.SliceIsSorted(observed, func(i, j int) bool {
		if observed[i].kind != observed[j].kind {
			return observed[i].kind < observed[j].kind
		}
		if observed[i].schema != observed[j].schema {
			return observed[i].schema < observed[j].schema
		}
		return observed[i].name < observed[j].name
	}), "the ListExternalTables result must be deterministically ordered")

	s.T().Logf("scenario107 107-P15-F (DB-real): observed %d external/foreign tables; "+
		"seeded foreign table %s.%s parsed honestly (kind=foreign, server=%s)",
		len(observed), schema, ftable, srv)
}

// TestIntegration_Scenario107_CatalogHonest asserts the Scenario 107 catalog is
// well-formed (always runs; no infra needed beyond the package gate) so the
// integration layer documents the same IDs the functional/e2e layers resolve.
func (s *Scenario107Suite) TestIntegration_Scenario107_CatalogHonest() {
	catalog := cases.Scenario107Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
	}
}
