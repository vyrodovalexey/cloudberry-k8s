//go:build integration

package integration

import (
	"context"
	"net"
	"os"
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
// Scenario 108: All CLI Commands (L.1–L.16) — integration
// ============================================================================
//
// Mirrors the Scenario 107 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the stack is down). The only Scenario 108 vertical with new operator code
// is L.15 test-read, backed by db.Client.ReadPXFSourceSample. This suite proves
// that backing read's HONESTY + cleanup contract against a REAL PostgreSQL/
// Cloudberry engine when one is reachable:
//
//   1. (always, no infra) the L.15 backing constants/contract are documented and
//      the catalog is well-formed.
//
//   2. (DB-real, SCENARIO108_DB_LIVE=1 + SCENARIO108_DSN gated) ReadPXFSourceSample
//      is run against the live engine over a tiny seeded source, and — REGARDLESS
//      of whether the read succeeds (a non-PXF engine errors at CREATE EXTERNAL
//      TABLE, an honest ABSENT) — the suite asserts the TRANSIENT preview table
//      (prefix cb_pxf_sample_) is NEVER left behind in the catalog (the deferred
//      DROP cleanup). When the DB path is absent it SKIPS cleanly; the
//      deterministic unit coverage is pinned at
//      internal/db/readpxfsource_scenario108_test.go (in-process PG mock).
//
// HONESTY: nothing is synthesized — the read either returns the REAL rows or a
// wrapped error mapped to available:false; the transient scaffolding is always
// dropped. Isolation: catalog probes are read-only; the transient table is
// uniquely named per read.
//
// ENV (all overridable, no hardcode-only):
//   SCENARIO108_PG_ADDR  — coordinator host:port reachability (default localhost:5432).
//   SCENARIO108_DB_LIVE  — "1" enables the real-Postgres ReadPXFSourceSample probe.
//   SCENARIO108_DSN      — libpq/pgx DSN for the real-Postgres probe
//                          (default postgres://gpadmin@localhost:5432/postgres).
// ============================================================================

const (
	envScenario108PGAddr = "SCENARIO108_PG_ADDR"
	envScenario108DBLive = "SCENARIO108_DB_LIVE"
	envScenario108DSN    = "SCENARIO108_DSN"

	scenario108DefaultPGAddr = "localhost:5432"
	scenario108DefaultDSN    = "postgres://gpadmin@localhost:5432/postgres"

	scenario108Timeout = 30 * time.Second

	// scenario108SampleTablePrefix mirrors internal/db pxfSampleTablePrefix so the
	// cleanup probe can detect any leftover transient preview table.
	scenario108SampleTablePrefix = "cb_pxf_sample_"
)

// Scenario108Suite drives the Scenario 108 L.15 backing-read honesty probe, gated
// on stack reachability.
type Scenario108Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario108(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario108Suite))
}

func (s *Scenario108Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
}

func (s *Scenario108Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario108Env returns the ENV value or the provided default.
func scenario108Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario108PGAddr() string {
	return scenario108Env(envScenario108PGAddr, scenario108DefaultPGAddr)
}
func scenario108DSN() string  { return scenario108Env(envScenario108DSN, scenario108DefaultDSN) }
func scenario108DBLive() bool { return os.Getenv(envScenario108DBLive) == "1" }

// scenario108TCPReachable reports whether a TCP dial to addr succeeds.
func scenario108TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario108CountSampleTables counts any leftover transient PXF preview tables
// (relname LIKE cb_pxf_sample_%) in the catalog — the cleanup proof.
func scenario108CountSampleTables(ctx context.Context, conn *pgx.Conn) (int, error) {
	var n int
	err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_class WHERE relname LIKE $1",
		scenario108SampleTablePrefix+"%").Scan(&n)
	return n, err
}

// TestIntegration_Scenario108_ReadPXFSourceSampleCleanup exercises the L.15
// backing read against a REAL engine (SCENARIO108_DB_LIVE=1 + SCENARIO108_DSN)
// and asserts the HONESTY + cleanup contract: whatever the read returns (real
// rows on a PXF-capable engine, or a wrapped error = honest ABSENT otherwise),
// NO transient preview table (cb_pxf_sample_*) is left behind. SKIPS CLEANLY when
// the DB path is absent.
//
//nolint:gocyclo // a connect→baseline→read→cleanup-assert flow is one narrative.
func (s *Scenario108Suite) TestIntegration_Scenario108_ReadPXFSourceSampleCleanup() {
	if !scenario108DBLive() {
		s.T().Skipf("%s not set: the DB-real ReadPXFSourceSample probe requires a live engine. "+
			"The deterministic cleanup coverage is at "+
			"internal/db/readpxfsource_scenario108_test.go (in-process PG mock). [108-L15-B DB-real gated]",
			envScenario108DBLive)
	}

	ctx, cancel := context.WithTimeout(s.ctx, scenario108Timeout)
	defer cancel()

	if !scenario108TCPReachable(ctx, scenario108PGAddr()) {
		s.T().Skipf("coordinator %s not reachable [108-L15-B: DB unreachable]", scenario108PGAddr())
	}

	// A raw connection for baseline + cleanup catalog probes.
	conn, err := pgx.Connect(ctx, scenario108DSN())
	if err != nil {
		s.T().Skipf("could not connect to the real engine at %s: %v [108-L15-B: DB unreachable]",
			scenario108DSN(), err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	// Baseline: there must be no stray preview tables before we start (and if there
	// are, record the baseline so we assert we did not ADD any).
	baseline, err := scenario108CountSampleTables(ctx, conn)
	require.NoErrorf(s.T(), err, "must be able to probe the catalog for %s tables",
		scenario108SampleTablePrefix)

	// Build a real db.Client from the DSN and run the transient sample read.
	cfg, parseErr := scenario108ConfigFromDSN(scenario108DSN())
	require.NoErrorf(s.T(), parseErr, "must parse the DSN into a db.Config")
	client, clientErr := db.NewClient(ctx, cfg, nil)
	if clientErr != nil {
		s.T().Skipf("db.NewClient failed against %s: %v [108-L15-B: DB unreachable]",
			scenario108DSN(), clientErr)
	}
	defer client.Close()

	// Attempt the preview read. On a PXF-capable engine this returns rows; on a
	// plain PostgreSQL it errors at CREATE EXTERNAL TABLE (an honest ABSENT). BOTH
	// outcomes are acceptable — the invariant under test is the cleanup.
	sample, readErr := client.ReadPXFSourceSample(ctx, "s108srv", "s3:text", "data/probe.csv", 5)
	if readErr != nil {
		s.T().Logf("scenario108 108-L15-B: ReadPXFSourceSample returned an honest error "+
			"(ABSENT; expected on a non-PXF engine): %v", readErr)
	} else {
		require.NotNil(s.T(), sample, "a non-error read must carry a (possibly empty) sample")
		assert.LessOrEqual(s.T(), len(sample.Rows), 5, "the read must bound the rows to ≤ limit")
		s.T().Logf("scenario108 108-L15-B: ReadPXFSourceSample read %d real rows (PXF-capable engine)",
			len(sample.Rows))
	}

	// CLEANUP PROOF: the transient preview table is ALWAYS dropped, so the catalog
	// count must not have grown above the baseline. Eventually-poll briefly since
	// the deferred DROP uses a fresh short-lived context.
	require.Eventuallyf(s.T(), func() bool {
		n, e := scenario108CountSampleTables(context.Background(), conn)
		return e == nil && n <= baseline
	}, 15*time.Second, time.Second,
		"the transient %s preview table must ALWAYS be dropped (no leftover above baseline %d)",
		scenario108SampleTablePrefix, baseline)

	s.T().Logf("scenario108 108-L15-B (DB-real): preview read honest; "+
		"no transient %s table leaked (baseline=%d)", scenario108SampleTablePrefix, baseline)
}

// scenario108ConfigFromDSN parses a libpq/pgx DSN into a db.Config (host/port/
// database/user/password/sslmode) for db.NewClient.
func scenario108ConfigFromDSN(dsn string) (db.Config, error) {
	pgxCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return db.Config{}, err
	}
	sslMode := "disable"
	if pgxCfg.TLSConfig != nil {
		sslMode = "require"
	}
	return db.Config{
		Host:     pgxCfg.Host,
		Port:     int32(pgxCfg.Port),
		Database: pgxCfg.Database,
		Username: pgxCfg.User,
		Password: pgxCfg.Password,
		SSLMode:  sslMode,
		MaxConns: 2,
	}, nil
}

// TestIntegration_Scenario108_CatalogHonest asserts the Scenario 108 catalog is
// well-formed (always runs; no infra needed beyond the package gate) so the
// integration layer documents the same IDs the functional/e2e layers resolve.
func (s *Scenario108Suite) TestIntegration_Scenario108_CatalogHonest() {
	catalog := cases.Scenario108Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
	}
}
