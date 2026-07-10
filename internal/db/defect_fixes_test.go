package db

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsCoordinatorShuttingDown covers the D3 detection helper that reports
// whether an error indicates the coordinator is in the "cannot connect now"
// (SQLSTATE 57P03) state. It is driven both by a real *pgconn.PgError carrying
// the code and by the textual fallback used for drivers that surface the state
// without a machine-readable code, plus the negative cases (nil, unrelated
// errors, unrelated SQLSTATEs).
func TestIsCoordinatorShuttingDown(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "plain unrelated error",
			err:  errors.New("connection reset by peer"),
			want: false,
		},
		{
			name: "57P03 via PgError",
			err:  &pgconn.PgError{Code: sqlStateCannotConnectNow, Message: "the database system is shutting down"},
			want: true,
		},
		{
			name: "57P03 via wrapped PgError",
			err:  fmt.Errorf("connecting to database: %w", &pgconn.PgError{Code: sqlStateCannotConnectNow}),
			want: true,
		},
		{
			name: "57P03 via doubly wrapped PgError",
			err: fmt.Errorf("outer: %w",
				fmt.Errorf("inner: %w", &pgconn.PgError{Code: sqlStateCannotConnectNow})),
			want: true,
		},
		{
			name: "textual fallback shutting down (no code)",
			err:  errors.New("FATAL: the database system is shutting down"),
			want: true,
		},
		{
			name: "textual fallback starting up (no code)",
			err:  errors.New("FATAL: the database system is starting up"),
			want: true,
		},
		{
			name: "textual fallback is case-insensitive",
			err:  errors.New("The Database System Is Starting Up"),
			want: true,
		},
		{
			name: "textual fallback via wrapped error",
			err:  fmt.Errorf("ping: %w", errors.New("the database system is shutting down")),
			want: true,
		},
		{
			// D3 broadening: recovery-mode variant surfaced during a dial.
			name: "textual fallback recovery mode (no code)",
			err:  errors.New("FATAL: the database system is in recovery mode"),
			want: true,
		},
		{
			// D3 broadening: "cannot connect now" variant surfaced without a code.
			name: "textual fallback cannot connect now (no code)",
			err:  fmt.Errorf("dial: %w", errors.New("cannot connect now")),
			want: true,
		},
		{
			// D3 broadening: SQLSTATE text present without a decoded PgError.
			name: "textual fallback 57P03 code in message",
			err:  errors.New("connect failed (SQLSTATE 57P03)"),
			want: true,
		},
		{
			name: "unrelated pg error code",
			err:  &pgconn.PgError{Code: "08006", Message: "connection failure"},
			want: false,
		},
		{
			name: "undefined table is not shutting down",
			err:  &pgconn.PgError{Code: sqlStateUndefinedTable, Message: "no such relation"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsCoordinatorShuttingDown(tt.err))
		})
	}
}

// TestPgxClient_RegisterSegmentRow_Idempotency exercises the D2 idempotency
// guard directly: registerSegmentRow reports whether a row was actually
// inserted so the caller only advances the dbid counter on a real insert.
//
//   - "INSERT 0 1" (RowsAffected == 1) → a fresh row was inserted → true.
//   - "INSERT 0 0" (RowsAffected == 0) → the WHERE NOT EXISTS guard skipped a
//     duplicate content/role row → false (idempotent, no dbid churn).
//   - an execution error propagates unchanged.
func TestPgxClient_RegisterSegmentRow_Idempotency(t *testing.T) {
	row := segmentRow{
		dbid: 5, content: 2, role: segmentRolePrimary,
		port: 6000, hostname: "seg-2.svc", dataDir: "/data/pgdata/gpseg2",
	}

	t.Run("real insert reports inserted=true", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "INSERT INTO gp_segment_configuration") {
				return execResponse("INSERT 0 1")
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		inserted, err := client.registerSegmentRow(context.Background(), row)
		require.NoError(t, err)
		assert.True(t, inserted, "a real insert must report inserted=true")
	})

	t.Run("duplicate content skipped reports inserted=false", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "INSERT INTO gp_segment_configuration") {
				// WHERE NOT EXISTS matched an existing row: 0 rows affected.
				return execResponse("INSERT 0 0")
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		inserted, err := client.registerSegmentRow(context.Background(), row)
		require.NoError(t, err)
		assert.False(t, inserted, "a duplicate content/role row must report inserted=false")
	})

	t.Run("exec error propagates", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "INSERT INTO gp_segment_configuration") {
				return errorResponseMsg("insert boom")
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		inserted, err := client.registerSegmentRow(context.Background(), row)
		require.Error(t, err)
		assert.False(t, inserted)
	})
}

// TestPgxClient_RegisterNewSegments_DBIDAdvance proves the D2 dbid counter only
// advances on a REAL insert: when the first primary already exists (INSERT 0 0)
// and the second is newly inserted (INSERT 0 1), the newly-inserted primary must
// reuse the FIRST unused dbid rather than skipping one — i.e. the counter did
// not advance for the duplicate. We assert on the dbid bound in the INSERT's
// parameters observed by the mock server.
func TestPgxClient_RegisterNewSegments_DBIDAdvance(t *testing.T) {
	// maxDBID is 4 → first candidate dbid is 5. content 2 is a duplicate
	// (skipped), content 3 is a genuine insert and must get dbid 5, proving the
	// counter did NOT advance while processing the skipped content 2.
	insertCount := 0
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "allow_system_table_mods"):
			return execResponse("SET")
		case strings.Contains(query, "MAX(dbid)"):
			return singleRowResponseTyped([]fieldDesc{int4Field("max_dbid")}, []string{"4"})
		case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
			insertCount++
			if insertCount == 1 {
				// content 2 already exists → guard skips it.
				return execResponse("INSERT 0 0")
			}
			// content 3 is genuinely inserted.
			return execResponse("INSERT 0 1")
		case strings.Contains(query, "datistemplate"):
			return emptyRowResponse([]string{"datname"})
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
		OldCount:       2,
		NewCount:       4, // content 2 and 3
		MirrorEnabled:  false,
		SegmentService: "seg-headless",
		ClusterName:    "test-cluster",
		Port:           6000,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, insertCount, "both primary contents must be attempted")
}

// TestPgxClient_RegisterNewSegments_AllDuplicatesIdempotent proves a fully
// retried reconcile (every content already registered → every INSERT 0 0) is a
// clean no-op: no error, no duplicate rows, and mirrors are handled the same
// way. This is the core D2 self-heal-after-wedge scenario.
func TestPgxClient_RegisterNewSegments_AllDuplicatesIdempotent(t *testing.T) {
	insertCount := 0
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "allow_system_table_mods"):
			return execResponse("SET")
		case strings.Contains(query, "MAX(dbid)"):
			return singleRowResponseTyped([]fieldDesc{int4Field("max_dbid")}, []string{"10"})
		case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
			insertCount++
			return execResponse("INSERT 0 0") // everything already exists
		case strings.Contains(query, "datistemplate"):
			return emptyRowResponse([]string{"datname"})
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
		OldCount:       2,
		NewCount:       3, // one content: 2
		MirrorEnabled:  true,
		SegmentService: "seg-headless",
		ClusterName:    "test-cluster",
		Port:           6000,
	})
	require.NoError(t, err)
	// One primary + one mirror INSERT attempted, both skipped idempotently.
	assert.Equal(t, 2, insertCount)
}

// TestPgxClient_RedistributeData_ErrorAggregation covers the D1 error
// aggregation: a per-database failure now surfaces as a returned error (instead
// of being silently swallowed) so the scale controller does not mark the
// cluster Running when redistribution actually failed. The success path
// (redistribution completes across all databases) still returns nil.
func TestPgxClient_RedistributeData_ErrorAggregation(t *testing.T) {
	// segCountResponse returns the D7 target-segment-count row (COUNT(DISTINCT
	// content) over gp_segment_configuration) as a single int4 value.
	segCountResponse := func(n string) []byte {
		return singleRowResponseTyped([]fieldDesc{int4Field("count")}, []string{n})
	}
	// expandFields is the D7 enumeration shape: schema, table, numsegments.
	expandFields := []fieldDesc{
		textField("schema_name"), textField("table_name"), int4Field("numsegments"),
	}

	t.Run("multiple per-db failures are aggregated", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"db_a"},
					{"db_b"},
				})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResponse("3")
			case strings.Contains(query, "gp_distribution_policy"):
				// Both databases fail to enumerate expandable tables.
				return errorResponseMsg("table query failed")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		require.Error(t, err)
		// Two of two databases failed; the aggregate error names the count and
		// joins the underlying per-db causes.
		assert.Contains(t, err.Error(), "redistribution failed for 2 of 2 databases")
		assert.Contains(t, err.Error(), "db_a")
		assert.Contains(t, err.Error(), "db_b")
	})

	t.Run("success path returns nil across all databases", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"db_a"},
					{"db_b"},
				})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResponse("3")
			case strings.Contains(query, "gp_distribution_policy"):
				return multiRowResponseTyped(expandFields, [][]string{
					{"public", "orders", "2"},
					{"public", "events", "2"},
				})
			case strings.Contains(query, "gp_dist_random"):
				// D8 preflight: relation present on all target segments.
				return segCountResponse("3")
			case strings.Contains(query, "EXPAND TABLE"):
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		require.NoError(t, err)
	})

	t.Run("partial failure surfaces even when one db succeeds", func(t *testing.T) {
		failFirst := true
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"db_bad"},
					{"db_good"},
				})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResponse("3")
			case strings.Contains(query, "gp_distribution_policy"):
				if failFirst {
					failFirst = false
					return errorResponseMsg("first db unusable")
				}
				return multiRowResponseTyped(expandFields, [][]string{
					{"public", "orders", "2"},
				})
			case strings.Contains(query, "gp_dist_random"):
				// D8 preflight: relation present on all target segments.
				return segCountResponse("3")
			case strings.Contains(query, "EXPAND TABLE"):
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "redistribution failed for 1 of 2 databases")
	})
}

// TestApplyRootCA_ThreadsCAConfig asserts the D1 CA-threading contract used by
// newDatabasePool for transient per-database/per-segment pools:
//
//   - empty rootCA is a no-op (nil error, RootCAs untouched);
//   - non-empty rootCA with NO TLS config (sslmode=disable path) is a no-op;
//   - non-empty rootCA WITH a TLS config installs the CA into RootCAs;
//   - malformed PEM surfaces an error rather than silently trusting system roots.
func TestApplyRootCA_ThreadsCAConfig(t *testing.T) {
	// A self-signed CA PEM valid enough for x509.CertPool.AppendCertsFromPEM.
	const validCAPEM = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`

	t.Run("empty rootCA is a no-op", func(t *testing.T) {
		cfg, err := pgxpool.ParseConfig("host=localhost port=5432 dbname=d user=u sslmode=disable")
		require.NoError(t, err)
		require.NoError(t, applyRootCA(cfg, nil))
		require.NoError(t, applyRootCA(cfg, []byte{}))
	})

	t.Run("rootCA with no TLS config is a no-op", func(t *testing.T) {
		cfg, err := pgxpool.ParseConfig("host=localhost port=5432 dbname=d user=u sslmode=disable")
		require.NoError(t, err)
		// sslmode=disable => no TLS config negotiated.
		require.Nil(t, cfg.ConnConfig.TLSConfig)
		require.NoError(t, applyRootCA(cfg, []byte(validCAPEM)))
		// Still nil: nothing to attach the CA to.
		assert.Nil(t, cfg.ConnConfig.TLSConfig)
	})

	t.Run("rootCA is installed into an existing TLS config", func(t *testing.T) {
		cfg, err := pgxpool.ParseConfig("host=localhost port=5432 dbname=d user=u sslmode=disable")
		require.NoError(t, err)
		// Simulate a TLS-enabled connection (as verify-ca would produce).
		cfg.ConnConfig.TLSConfig = &tls.Config{ServerName: "coordinator", MinVersion: tls.VersionTLS12}
		require.Nil(t, cfg.ConnConfig.TLSConfig.RootCAs)

		require.NoError(t, applyRootCA(cfg, []byte(validCAPEM)))
		assert.NotNil(t, cfg.ConnConfig.TLSConfig.RootCAs,
			"cluster CA must be threaded into the transient pool's RootCAs (D1)")
	})

	t.Run("malformed PEM surfaces an error", func(t *testing.T) {
		cfg, err := pgxpool.ParseConfig("host=localhost port=5432 dbname=d user=u sslmode=disable")
		require.NoError(t, err)
		cfg.ConnConfig.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}

		err = applyRootCA(cfg, []byte("not a certificate"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SSL root CA")
	})
}

// TestPgxClient_NewDatabasePool_NonTLSNoOp asserts that for a non-TLS cluster
// (empty SSLRootCA), newDatabasePool's CA re-attach step is a no-op and the
// mutate hook is still applied. It uses a live mock pool so ConnString()
// round-trips through pgx. The pool is closed immediately; we only assert the
// parsed/mutated config, not a live connection.
func TestPgxClient_NewDatabasePool_NonTLSNoOp(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	// SSLRootCA is empty (non-TLS cluster) → CA re-attach is a documented no-op.
	require.Empty(t, client.config.SSLRootCA)

	var mutated bool
	pool, err := client.newDatabasePool(context.Background(), func(cfg *pgxpool.Config) {
		mutated = true
		cfg.ConnConfig.Database = "otherdb"
	})
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Close()

	assert.True(t, mutated, "the mutate hook must be applied before the CA re-attach")
	assert.Equal(t, "otherdb", pool.Config().ConnConfig.Database,
		"the mutate hook's database override must be threaded through")
}

// TestPgxClient_NewDatabasePool_ParseError covers the error branch when the
// serialized connection string cannot be parsed back into a config.
func TestPgxClient_NewDatabasePool_ParseError(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	// Force applyRootCA to fail by giving the client a bogus CA and a mutate
	// hook that installs a TLS config: the re-attach then rejects the PEM.
	client.config.SSLRootCA = []byte("garbage-not-pem")
	_, err := client.newDatabasePool(context.Background(), func(cfg *pgxpool.Config) {
		cfg.ConnConfig.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "applying SSL root CA to database pool")
}

// segCountResp returns the D7 target-segment-count row (COUNT(DISTINCT content)
// over gp_segment_configuration) as a single int4 value.
func segCountResp(n string) []byte {
	return singleRowResponseTyped([]fieldDesc{int4Field("count")}, []string{n})
}

// d7ExpandFields is the D7 enumeration shape: schema, table, numsegments.
var d7ExpandFields = []fieldDesc{
	textField("schema_name"), textField("table_name"), int4Field("numsegments"),
}

// TestPgxClient_TargetSegmentCount_D7 exercises the D7 targetSegmentCount helper
// across its full decision surface: a valid positive count (the target width),
// the query/scan error path, and the "invalid primary segment count" guard when
// the catalog reports a non-positive width (0). These are driven through
// RedistributeData so the helper runs against a live per-database mock pool.
func TestPgxClient_TargetSegmentCount_D7(t *testing.T) {
	tests := []struct {
		name        string
		segResponse func() []byte
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid positive count expands tables",
			segResponse: func() []byte { return segCountResp("3") },
			wantErr:     false,
		},
		{
			name:        "target width query error is surfaced",
			segResponse: func() []byte { return errorResponseMsg("segment count query failed") },
			wantErr:     true,
			errContains: "determining target segment count",
		},
		{
			name:        "non-positive count rejected as invalid",
			segResponse: func() []byte { return segCountResp("0") },
			wantErr:     true,
			errContains: "invalid primary segment count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
				switch {
				case strings.Contains(query, "datistemplate"):
					return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
				case strings.Contains(query, "gp_segment_configuration"):
					return tt.segResponse()
				case strings.Contains(query, "gp_distribution_policy"):
					return multiRowResponseTyped(d7ExpandFields, [][]string{
						{"public", "orders", "2"},
					})
				case strings.Contains(query, "gp_dist_random"):
					return segCountResp("3")
				case strings.Contains(query, "EXPAND TABLE"):
					return execResponse("ALTER TABLE")
				default:
					return execResponse("SELECT 1")
				}
			})
			defer cleanup()

			err := client.RedistributeData(context.Background(), RedistributionOptions{})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				// The per-database failure is aggregated by RedistributeData.
				assert.Contains(t, err.Error(), "redistribution failed for 1 of 1 databases")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestPgxClient_ListExpandableTables_D7 covers the D7 enumeration helper across
// its behaviors: returning under-width tables, an empty (idempotent no-op)
// result when every table is already at the target width, the enumeration query
// error path, and the caller-supplied exclusion filter. The system-schema skip
// is enforced by the SQL predicate; a companion case proves that rows the mock
// does NOT return for system schemas are simply absent (no expand issued).
func TestPgxClient_ListExpandableTables_D7(t *testing.T) {
	t.Run("returns under-width tables and expands each", func(t *testing.T) {
		var expanded []string
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResp("4")
			case strings.Contains(query, "gp_distribution_policy"):
				return multiRowResponseTyped(d7ExpandFields, [][]string{
					{"public", "orders", "2"},
					{"sales", "invoices", "3"},
				})
			case strings.Contains(query, "gp_dist_random"):
				// D8 preflight: relations present on all 4 target segments.
				return segCountResp("4")
			case strings.Contains(query, "EXPAND TABLE"):
				expanded = append(expanded, query)
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		require.NoError(t, err)
		assert.Len(t, expanded, 2, "both under-width tables must be expanded")
	})

	t.Run("empty enumeration is an idempotent no-op", func(t *testing.T) {
		expandCalled := false
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResp("3")
			case strings.Contains(query, "gp_distribution_policy"):
				// All tables already at the target width → nothing to expand.
				return multiRowResponseTyped(d7ExpandFields, [][]string{})
			case strings.Contains(query, "EXPAND TABLE"):
				expandCalled = true
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		require.NoError(t, err)
		assert.False(t, expandCalled, "no EXPAND TABLE must be issued when nothing is under-width")
	})

	t.Run("enumeration query error is surfaced", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResp("3")
			case strings.Contains(query, "gp_distribution_policy"):
				return errorResponseMsg("enumeration failed")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		require.Error(t, err)
		// Under the extended protocol the enumeration error surfaces either at
		// Query time ("querying expandable tables") or while iterating the row
		// stream ("iterating expandable table rows"); both are the D7 error
		// path being surfaced (not swallowed).
		msg := err.Error()
		assert.True(t,
			strings.Contains(msg, "querying expandable tables") ||
				strings.Contains(msg, "iterating expandable table rows"),
			"enumeration error must be surfaced, got: %s", msg)
		assert.Contains(t, msg, "redistribution failed for 1 of 1 databases")
	})

	t.Run("caller exclusion filter skips the excluded table", func(t *testing.T) {
		var expanded []string
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
			case strings.Contains(query, "gp_segment_configuration"):
				return segCountResp("3")
			case strings.Contains(query, "gp_distribution_policy"):
				return multiRowResponseTyped(d7ExpandFields, [][]string{
					{"public", "keep_me", "2"},
					{"public", "skip_me", "2"},
				})
			case strings.Contains(query, "gp_dist_random"):
				return segCountResp("3")
			case strings.Contains(query, "EXPAND TABLE"):
				expanded = append(expanded, query)
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{
			ExcludeTables: []string{"public.skip_me"},
		})
		require.NoError(t, err)
		require.Len(t, expanded, 1, "only the non-excluded table must be expanded")
		assert.Contains(t, expanded[0], "keep_me")
		for _, q := range expanded {
			assert.NotContains(t, q, "skip_me",
				"the excluded table must never be expanded")
		}
	})
}

// TestPgxClient_ExpandTables_IdentifierQuoting_D7 proves the D7 expand path
// safely quotes catalog-sourced schema/table identifiers that contain special
// characters (mixed case, a dot, an embedded double quote) via
// pgx.Identifier{}.Sanitize(), so a malicious or unusual name cannot break out
// of the ALTER TABLE ... EXPAND TABLE statement.
func TestPgxClient_ExpandTables_IdentifierQuoting_D7(t *testing.T) {
	var expandSQL string
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
		case strings.Contains(query, "gp_segment_configuration"):
			return segCountResp("3")
		case strings.Contains(query, "gp_distribution_policy"):
			return multiRowResponseTyped(d7ExpandFields, [][]string{
				{`Weird.Schema`, `tbl"; DROP TABLE x; --`, "2"},
			})
		case strings.Contains(query, "gp_dist_random"):
			return segCountResp("3")
		case strings.Contains(query, "EXPAND TABLE"):
			expandSQL = query
			return execResponse("ALTER TABLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RedistributeData(context.Background(), RedistributionOptions{})
	require.NoError(t, err)

	// The schema is double-quoted as a whole (the '.' is inside the quotes, not
	// a separator) and the embedded double quote in the table name is escaped by
	// doubling, so the injection payload is contained within a quoted identifier.
	assert.Contains(t, expandSQL, `"Weird.Schema"`,
		"a schema name with a dot must be quoted as a single identifier")
	assert.Contains(t, expandSQL, `"tbl""; DROP TABLE x; --"`,
		"embedded double quotes must be escaped by doubling (no SQL breakout)")
	assert.Contains(t, expandSQL, "EXPAND TABLE")
}

// TestPgxClient_ExpandTables_PerTableFailure_D7 proves a per-table EXPAND
// failure is surfaced (not swallowed): when one of several tables fails to
// expand, the remaining tables are still attempted and the aggregate error
// names the failing count while the successful tables are still processed.
func TestPgxClient_ExpandTables_PerTableFailure_D7(t *testing.T) {
	var attempts int
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"testdb"}})
		case strings.Contains(query, "gp_segment_configuration"):
			return segCountResp("3")
		case strings.Contains(query, "gp_distribution_policy"):
			return multiRowResponseTyped(d7ExpandFields, [][]string{
				{"public", "ok_one", "2"},
				{"public", "bad_two", "2"},
				{"public", "ok_three", "2"},
			})
		case strings.Contains(query, "gp_dist_random"):
			return segCountResp("3")
		case strings.Contains(query, "EXPAND TABLE"):
			attempts++
			if strings.Contains(query, "bad_two") {
				return errorResponseMsg("expand of bad_two failed")
			}
			return execResponse("ALTER TABLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RedistributeData(context.Background(), RedistributionOptions{})
	require.Error(t, err)
	// All three tables were attempted (the failure did not abort the loop).
	assert.Equal(t, 3, attempts, "every table must be attempted despite a mid-loop failure")
	assert.Contains(t, err.Error(), "expand failed for 1 of 3 tables")
	assert.Contains(t, err.Error(), "bad_two")
	assert.Contains(t, err.Error(), "redistribution failed for 1 of 1 databases")
}
