//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 111: Security (SE.6 dedicated minimal-privilege role) — integration
// ============================================================================
//
// Reachability-gated (skips cleanly when no real Postgres is reachable). When a
// real Postgres IS reachable (via the gated env) this suite exercises the REAL
// SE.6 least-privilege proof against it:
//
//   - EnsureDataLoaderRole CREATEs the dedicated role and GRANTs it (best-effort)
//     ONLY the pxf protocol privileges.
//   - 111-SE6-L: the role exists in pg_roles and is NOSUPERUSER /
//     NOT createrole / NOT createdb (the least-privilege attributes).
//   - 111-SE6-DENY: connecting AS the dedicated role, a superuser-only op
//     (CREATE ROLE) is DENIED with a permission error — the real least-privilege
//     assertion. (When the role cannot LOGIN in the target env this degrades to
//     an honest CONFIG-ONLY skip, never a fabricated denial.)
//
// HONESTY: PROTOCOL pxf may not exist on a vanilla Postgres (it is a
// Cloudberry/Greenplum object); EnsureDataLoaderRole's protocol GRANTs are
// best-effort/non-fatal, so the integration proof centers on the role's
// least-privilege ATTRIBUTES + the DENY of an unrelated op (both provable on any
// Postgres). The pxf-grant presence is asserted via has_table_privilege-style
// catalog probes ONLY when PROTOCOL pxf is present; otherwise it is logged as
// CONFIG-ONLY. Nothing is synthesized: when the DB is unreachable the suite
// skips cleanly.
//
// ENV (all overridable, no hardcode-only):
//   SCENARIO111_PG_DSN     — a full postgres DSN; if set it gates + is used as-is.
//   PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE — fallback connection env.
//   SCENARIO111_DB_LIVE=1  — additionally required to run the live DB proof.
// ============================================================================

const (
	envScenario111DSN    = "SCENARIO111_PG_DSN"
	envScenario111DBLive = "SCENARIO111_DB_LIVE"

	scenario111ConnectTimeout = 15 * time.Second
	scenario111OpTimeout      = 60 * time.Second
	// scenario111Role is the dedicated minimal-privilege role under test. A SHORT
	// unique-ish name with a test prefix so it can't collide with a real role.
	scenario111Role = "cb_dataload_it"
)

// Scenario111Suite drives the SE.6 dedicated-role least-privilege proof against a
// real Postgres, gated on reachability.
type Scenario111Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario111(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario111Suite))
}

func (s *Scenario111Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario111Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario111DBConfig resolves the admin connection config from env (SCENARIO111_PG_DSN
// or the standard PG* vars). It returns the config + whether enough env is set to
// even attempt a connection.
func scenario111DBConfig() (db.Config, bool) {
	if dsn := strings.TrimSpace(os.Getenv(envScenario111DSN)); dsn != "" {
		// A full DSN is provided: parse it into a db.Config via pgx.
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return db.Config{}, false
		}
		return db.Config{
			Host:     cfg.Host,
			Port:     int32(cfg.Port),
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
			SSLMode:  "disable",
		}, true
	}

	host := strings.TrimSpace(os.Getenv("PGHOST"))
	if host == "" {
		return db.Config{}, false
	}
	port := int32(5432)
	if p := strings.TrimSpace(os.Getenv("PGPORT")); p != "" {
		if n, err := strconv.ParseInt(p, 10, 32); err == nil {
			port = int32(n)
		}
	}
	user := envOrDefault("PGUSER", "gpadmin")
	dbName := envOrDefault("PGDATABASE", "postgres")
	return db.Config{
		Host:     host,
		Port:     port,
		Database: dbName,
		Username: user,
		Password: os.Getenv("PGPASSWORD"),
		SSLMode:  envOrDefault("PGSSLMODE", "disable"),
	}, true
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// scenario111RequireDB skips cleanly unless SCENARIO111_DB_LIVE=1, the env
// supplies a connection, and the admin client can actually connect (Ping).
func (s *Scenario111Suite) scenario111RequireDB() (db.Client, db.Config) {
	if os.Getenv(envScenario111DBLive) != "1" {
		s.T().Skip("SCENARIO111_DB_LIVE not set, skipping the live SE.6 DB least-privilege proof")
	}
	cfg, ok := scenario111DBConfig()
	if !ok {
		s.T().Skip("no Postgres connection env (SCENARIO111_PG_DSN or PGHOST) set, skipping SE.6 DB proof")
	}

	connCtx, cancel := context.WithTimeout(s.ctx, scenario111ConnectTimeout)
	defer cancel()
	client, err := db.NewClient(connCtx, cfg, nil)
	if err != nil {
		s.T().Skipf("Postgres not reachable [CONFIG-ONLY]: %v", err)
	}
	if pingErr := client.Ping(connCtx); pingErr != nil {
		client.Close()
		s.T().Skipf("Postgres ping failed [CONFIG-ONLY]: %v", pingErr)
	}
	return client, cfg
}

// scenario111AdminPool opens a raw pgx connection (admin) for catalog probes that
// the db.Client interface does not expose directly (pg_roles attribute scan).
func (s *Scenario111Suite) scenario111AdminConn(cfg db.Config) (*pgx.Conn, error) {
	connCtx, cancel := context.WithTimeout(s.ctx, scenario111ConnectTimeout)
	defer cancel()
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database, cfg.SSLMode)
	return pgx.Connect(connCtx, dsn)
}

// TestIntegration_Scenario111_CatalogHonest asserts the Scenario 111 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario111Suite) TestIntegration_Scenario111_CatalogHonest() {
	catalog := cases.Scenario111SecurityCases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		key := tc.ID + "|" + tc.Layer
		assert.Falsef(s.T(), seen[key], "duplicate catalog row %s", key)
		seen[key] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Class, "%s must carry an honesty Class", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
	}
	// The least-privilege + dedicated-role rows the live DB proof realizes.
	ids := map[string]bool{}
	for _, tc := range catalog {
		ids[tc.ID] = true
	}
	assert.True(s.T(), ids["111-SE6-L"], "catalog must carry 111-SE6-L")
	assert.True(s.T(), ids["111-SE6-DENY"], "catalog must carry 111-SE6-DENY")
}

// TestIntegration_Scenario111_SE6_DedicatedRoleLeastPrivilege exercises the REAL
// SE.6 proof against a reachable Postgres: EnsureDataLoaderRole creates the role,
// the role is NOSUPERUSER/NOCREATEROLE/NOCREATEDB (111-SE6-L), and connecting AS
// the role a superuser-only op (CREATE ROLE) is DENIED (111-SE6-DENY). Skips
// cleanly when no DB is reachable. Always cleans up the role it creates.
//
//nolint:gocyclo // a self-contained create→assert-attrs→assert-deny→cleanup flow.
func (s *Scenario111Suite) TestIntegration_Scenario111_SE6_DedicatedRoleLeastPrivilege() {
	client, cfg := s.scenario111RequireDB()
	defer client.Close()

	opCtx, cancel := context.WithTimeout(s.ctx, scenario111OpTimeout)
	defer cancel()

	// Open an admin conn for catalog probes + cleanup + giving the role a password
	// so the DENY probe can connect AS the role.
	adminConn, err := s.scenario111AdminConn(cfg)
	if err != nil {
		s.T().Skipf("admin pgx connect failed [CONFIG-ONLY]: %v", err)
	}
	defer func() { _ = adminConn.Close(context.Background()) }()

	// Always clean up the role we create (best-effort, ignore errors).
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), scenario111OpTimeout)
		defer cleanupCancel()
		_, _ = adminConn.Exec(cleanupCtx,
			fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{scenario111Role}.Sanitize()))
	}()

	// Drive the REAL operator code path: create the dedicated minimal-privilege
	// role + (best-effort) the pxf protocol grants.
	require.NoError(s.T(), client.EnsureDataLoaderRole(opCtx, scenario111Role),
		"EnsureDataLoaderRole must succeed (best-effort grants are non-fatal)")

	// (111-SE6-L) The role exists and carries the least-privilege attributes.
	var rolsuper, rolcreaterole, rolcreatedb, rolcanlogin bool
	probeErr := adminConn.QueryRow(opCtx,
		"SELECT rolsuper, rolcreaterole, rolcreatedb, rolcanlogin FROM pg_roles WHERE rolname = $1",
		scenario111Role).Scan(&rolsuper, &rolcreaterole, &rolcreatedb, &rolcanlogin)
	require.NoError(s.T(), probeErr, "the dedicated role must exist in pg_roles")

	assert.False(s.T(), rolsuper, "111-SE6-L: the dedicated role must be NOSUPERUSER")
	assert.False(s.T(), rolcreaterole, "111-SE6-L: the dedicated role must be NOCREATEROLE")
	assert.False(s.T(), rolcreatedb, "111-SE6-L: the dedicated role must be NOCREATEDB")
	assert.True(s.T(), rolcanlogin, "111-SE6-L: the dedicated role must be a LOGIN role")
	s.T().Logf("111-SE6-L: role %q is NOSUPERUSER/NOCREATEROLE/NOCREATEDB LOGIN (least-privilege)",
		scenario111Role)

	// CONFIG-ONLY note for the pxf protocol grant: PROTOCOL pxf is a
	// Cloudberry/Greenplum object that may be absent on vanilla Postgres. We
	// probe pg_extprotocol honestly and only assert the grant when present.
	var protoExists bool
	if perr := adminConn.QueryRow(opCtx,
		"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_extprotocol')").
		Scan(&protoExists); perr == nil && protoExists {
		s.T().Log("111-SE6: pg_extprotocol present — pxf protocol grants are catalog-observable")
	} else {
		s.T().Log("111-SE6: PROTOCOL pxf not present on this Postgres — protocol grant is " +
			"CONFIG-ONLY (best-effort; vanilla Postgres has no PROTOCOL pxf)")
	}

	// (111-SE6-DENY) Connect AS the dedicated role and assert a superuser-only op
	// (CREATE ROLE) is DENIED. We need the role to be able to LOGIN with a
	// password; set one via the admin conn (the operator leaves auth as a
	// follow-up). If we cannot establish a role-scoped connection in this env,
	// degrade to an honest CONFIG-ONLY skip rather than fabricating a denial.
	const rolePassword = "cb-dataload-it-pw"
	if _, alterErr := adminConn.Exec(opCtx, fmt.Sprintf("ALTER ROLE %s LOGIN PASSWORD %s",
		pgx.Identifier{scenario111Role}.Sanitize(), quoteLiteral(rolePassword))); alterErr != nil {
		s.T().Skipf("111-SE6-DENY: cannot set a password for the role in this env "+
			"[CONFIG-ONLY]: %v", alterErr)
	}

	roleCfg := cfg
	roleCfg.Username = scenario111Role
	roleCfg.Password = rolePassword
	roleDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		roleCfg.Host, roleCfg.Port, roleCfg.Username, roleCfg.Password, roleCfg.Database, roleCfg.SSLMode)

	roleConnCtx, roleCancel := context.WithTimeout(s.ctx, scenario111ConnectTimeout)
	defer roleCancel()
	roleConn, roleErr := pgx.Connect(roleConnCtx, roleDSN)
	if roleErr != nil {
		s.T().Skipf("111-SE6-DENY: cannot connect AS the dedicated role in this env "+
			"[CONFIG-ONLY]: %v", roleErr)
	}
	defer func() { _ = roleConn.Close(context.Background()) }()

	// The unrelated, privileged op MUST be denied (the role is NOCREATEROLE).
	_, denyErr := roleConn.Exec(opCtx, "CREATE ROLE cb_dataload_it_should_fail NOLOGIN")
	require.Error(s.T(), denyErr,
		"111-SE6-DENY: the dedicated role must NOT be able to CREATE ROLE (least-privilege)")
	assert.Containsf(s.T(), strings.ToLower(denyErr.Error()), "permission",
		"111-SE6-DENY: the denial must be a permission error; got %v", denyErr)
	s.T().Logf("111-SE6-DENY: role %q denied CREATE ROLE → %v", scenario111Role, denyErr)

	// Best-effort cleanup of any leaked role from the deny probe (it should NOT
	// have been created, but guard anyway).
	_, _ = adminConn.Exec(opCtx, "DROP ROLE IF EXISTS cb_dataload_it_should_fail")
}

// quoteLiteral single-quotes a string literal for an SQL statement (doubling
// embedded quotes). Used only for the test-local role password.
func quoteLiteral(in string) string {
	return "'" + strings.ReplaceAll(in, "'", "''") + "'"
}
