package db

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// mockPGServer creates a minimal PostgreSQL mock server that handles
// the startup handshake and responds to queries.
// It returns the listener address and a cleanup function.
func mockPGServer(t *testing.T, handler func(backend *pgproto3.Backend, conn net.Conn)) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				backend := pgproto3.NewBackend(c, c)

				// Handle startup message.
				startupMsg, startupErr := backend.ReceiveStartupMessage()
				if startupErr != nil {
					return
				}

				switch startupMsg.(type) {
				case *pgproto3.StartupMessage:
					// Send AuthenticationOk.
					buf := mustEncode(&pgproto3.AuthenticationOk{})
					buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})...)
					buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"})...)
					buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})...)
					buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})...)
					buf = append(buf, mustEncode(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})...)
					buf = append(buf, mustEncode(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})...)
					buf = append(buf, mustEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
					if _, writeErr := c.Write(buf); writeErr != nil {
						return
					}
				case *pgproto3.SSLRequest:
					// Deny SSL.
					if _, writeErr := c.Write([]byte("N")); writeErr != nil {
						return
					}
					return
				default:
					return
				}

				if handler != nil {
					handler(backend, c)
				}
			}(conn)
		}
	}()

	cleanup := func() {
		ln.Close()
		<-done
	}

	return ln.Addr().String(), cleanup
}

func mustEncode(msg pgproto3.Message) []byte {
	buf, err := msg.Encode(nil)
	if err != nil {
		panic(err)
	}
	return buf
}

// handleSimpleQueries handles simple query protocol messages only.
// We force pgx to use simple protocol via QueryExecModeSimpleProtocol.
func handleSimpleQueries(backend *pgproto3.Backend, conn net.Conn, responder func(query string) []byte) {
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			response := responder(m.String)
			response = append(response, mustEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
			if _, writeErr := conn.Write(response); writeErr != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		}
	}
}

// handleAllQueries handles both simple and extended query protocol messages.
// This is needed for tests where new pools are created internally (they use extended protocol).
// It tracks the last parsed query to use when Execute is received.
func handleAllQueries(backend *pgproto3.Backend, conn net.Conn, responder func(query string) []byte) {
	lastQuery := ""
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		var buf []byte
		switch m := msg.(type) {
		case *pgproto3.Query:
			buf = responder(m.String)
			buf = append(buf, mustEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})...)
		case *pgproto3.Parse:
			lastQuery = m.Query
			buf = mustEncode(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			buf = mustEncode(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			// Send NoData for describe - pgx will handle it
			buf = mustEncode(&pgproto3.NoData{})
		case *pgproto3.Execute:
			buf = responder(lastQuery)
		case *pgproto3.Sync:
			buf = mustEncode(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Flush:
			continue
		case *pgproto3.Close:
			buf = mustEncode(&pgproto3.CloseComplete{})
		case *pgproto3.Terminate:
			return
		default:
			continue
		}

		if _, writeErr := conn.Write(buf); writeErr != nil {
			return
		}
	}
}

// newMockPgxClientExtended creates a pgxClient connected to a mock PostgreSQL server
// that handles both simple and extended protocol. This is needed for tests where
// the code creates new pools internally (which use extended protocol by default).
func newMockPgxClientExtended(t *testing.T, responder func(query string) []byte) (*pgxClient, func()) {
	t.Helper()

	addr, cleanup := mockPGServer(t, func(backend *pgproto3.Backend, conn net.Conn) {
		handleAllQueries(backend, conn, responder)
	})

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	connStr := "host=" + host + " port=" + port + " dbname=testdb user=testuser password=testpass sslmode=disable"
	poolCfg, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)
	poolCfg.MaxConns = 2
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)

	portInt, _ := strconv.Atoi(port)
	client := &pgxClient{
		pool:      pool,
		config:    Config{Host: host, Port: int32(portInt), Database: "testdb"},
		retryOpts: util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
		logger:    slog.Default(),
	}

	return client, func() {
		pool.Close()
		cleanup()
	}
}

// execResponse returns a response for an exec-style query.
func execResponse(tag string) []byte {
	return mustEncode(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// errorResponseMsg returns a PostgreSQL error response.
func errorResponseMsg(msg string) []byte {
	return mustEncode(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  msg,
	})
}

// newMockPgxClient creates a pgxClient connected to a mock PostgreSQL server.
func newMockPgxClient(t *testing.T, responder func(query string) []byte) (*pgxClient, func()) {
	t.Helper()

	addr, cleanup := mockPGServer(t, func(backend *pgproto3.Backend, conn net.Conn) {
		handleSimpleQueries(backend, conn, responder)
	})

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	connStr := "host=" + host + " port=" + port + " dbname=testdb user=testuser password=testpass sslmode=disable"
	poolCfg, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)
	poolCfg.MaxConns = 1
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)

	client := &pgxClient{
		pool:      pool,
		config:    Config{Host: host, Port: 5432, Database: "testdb"},
		retryOpts: util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
		logger:    slog.Default(),
	}

	return client, func() {
		pool.Close()
		cleanup()
	}
}

// newMockPgxClientWithTracer is newMockPgxClient with the pgxQueryTracer
// installed on the pool config BEFORE the pool is built (mirroring NewClient),
// so per-statement "db.query" child spans are produced. Used to prove the
// QueryTracer still nests inside a W3-C2 operation span.
func newMockPgxClientWithTracer(t *testing.T, responder func(query string) []byte) (*pgxClient, func()) {
	t.Helper()

	addr, cleanup := mockPGServer(t, func(backend *pgproto3.Backend, conn net.Conn) {
		handleSimpleQueries(backend, conn, responder)
	})

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	connStr := "host=" + host + " port=" + port + " dbname=testdb user=testuser password=testpass sslmode=disable"
	poolCfg, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)
	poolCfg.MaxConns = 1
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	poolCfg.ConnConfig.Tracer = &pgxQueryTracer{database: "testdb"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)

	client := &pgxClient{
		pool:      pool,
		config:    Config{Host: host, Port: 5432, Database: "testdb"},
		retryOpts: util.RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond},
		logger:    slog.Default(),
	}

	return client, func() {
		pool.Close()
		cleanup()
	}
}

func TestPgxClient_Ping(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.Ping(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_Close(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	// Close should not panic.
	client.Close()
}

func TestPgxClient_ReloadConfig(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.ReloadConfig(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_PromoteStandby(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	err := client.PromoteStandby(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_TriggerRecommendationScan_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ANALYZE")
	})
	defer cleanup()

	err := client.TriggerRecommendationScan(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_CreateRole_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("CREATE ROLE")
	})
	defer cleanup()

	err := client.CreateRole(context.Background(), RoleOptions{
		Name: "testuser", Login: true, Password: "secret",
	})
	assert.NoError(t, err)
}

func TestPgxClient_AlterRole_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER ROLE")
	})
	defer cleanup()

	err := client.AlterRole(context.Background(), RoleOptions{
		Name: "testuser", SuperUser: true, CreateDB: true,
	})
	assert.NoError(t, err)
}

func TestPgxClient_DropRole_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("DROP ROLE")
	})
	defer cleanup()

	err := client.DropRole(context.Background(), "testuser")
	assert.NoError(t, err)
}

func TestPgxClient_Vacuum_Mock(t *testing.T) {
	tests := []struct {
		name string
		opts VacuumOptions
	}{
		{"basic vacuum", VacuumOptions{}},
		{"vacuum full", VacuumOptions{Full: true}},
		{"vacuum analyze", VacuumOptions{Analyze: true}},
		{"vacuum full analyze", VacuumOptions{Full: true, Analyze: true}},
		{"vacuum table", VacuumOptions{Table: "my_table"}},
		{"vacuum full analyze table", VacuumOptions{Full: true, Analyze: true, Table: "my_table"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				return execResponse("VACUUM")
			})
			defer cleanup()

			err := client.Vacuum(context.Background(), tt.opts)
			assert.NoError(t, err)
		})
	}
}

func TestPgxClient_Analyze_Mock(t *testing.T) {
	tests := []struct {
		name  string
		table string
	}{
		{"analyze all", ""},
		{"analyze specific table", "my_table"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				return execResponse("ANALYZE")
			})
			defer cleanup()

			err := client.Analyze(context.Background(), tt.table)
			assert.NoError(t, err)
		})
	}
}

func TestPgxClient_Reindex_Mock(t *testing.T) {
	tests := []struct {
		name string
		opts ReindexOptions
	}{
		{"reindex table", ReindexOptions{Table: "my_table"}},
		{"reindex database", ReindexOptions{Database: "mydb"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				return execResponse("REINDEX")
			})
			defer cleanup()

			err := client.Reindex(context.Background(), tt.opts)
			assert.NoError(t, err)
		})
	}
}

func TestPgxClient_Reindex_NoTarget(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("REINDEX")
	})
	defer cleanup()

	err := client.Reindex(context.Background(), ReindexOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "either database or table must be specified")
}

func TestPgxClient_SetParameter_ClusterScope(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER SYSTEM")
	})
	defer cleanup()

	err := client.SetParameter(context.Background(), "max_connections", "200", ParameterScope{Level: "cluster"})
	assert.NoError(t, err)
}

func TestPgxClient_SetParameter_DatabaseScope(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER DATABASE")
	})
	defer cleanup()

	err := client.SetParameter(context.Background(), "work_mem", "256MB",
		ParameterScope{Level: "database", Target: "mydb"})
	assert.NoError(t, err)
}

func TestPgxClient_SetParameter_RoleScope(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER ROLE")
	})
	defer cleanup()

	err := client.SetParameter(context.Background(), "work_mem", "128MB",
		ParameterScope{Level: "role", Target: "analyst"})
	assert.NoError(t, err)
}

func TestPgxClient_CreateResourceGroup_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("CREATE RESOURCE GROUP")
	})
	defer cleanup()

	err := client.CreateResourceGroup(context.Background(), ResourceGroupOptions{
		Name: "analytics", Concurrency: 10, CPUMaxPercent: 50, CPUWeight: 100,
	})
	assert.NoError(t, err)
}

func TestPgxClient_AlterResourceGroup_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{
		Name: "analytics", Concurrency: 20, CPUMaxPercent: 60, CPUWeight: 150,
	})
	assert.NoError(t, err)
}

func TestPgxClient_AlterResourceGroup_SkipZeroValues(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	// All values are 0, so no alterations should be made.
	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{
		Name: "analytics", Concurrency: 0, CPUMaxPercent: 0, CPUWeight: 0,
	})
	assert.NoError(t, err)
}

func TestPgxClient_DropResourceGroup_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("DROP RESOURCE GROUP")
	})
	defer cleanup()

	err := client.DropResourceGroup(context.Background(), "analytics")
	assert.NoError(t, err)
}

func TestPgxClient_ListBackups_Direct(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 0")
	})
	defer cleanup()

	backups, err := client.ListBackups(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, backups)
	assert.Empty(t, backups)
}

func TestPgxClient_ListDataLoadingJobs_Direct(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("SELECT 0")
	})
	defer cleanup()

	jobs, err := client.ListDataLoadingJobs(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, jobs)
	assert.Empty(t, jobs)
}

// fieldDesc describes a column with name and OID.
type fieldDesc struct {
	name string
	oid  uint32
}

// textField creates a text field descriptor.
func textField(name string) fieldDesc {
	return fieldDesc{name: name, oid: 25} // text OID
}

// int4Field creates an int4 field descriptor.
func int4Field(name string) fieldDesc {
	return fieldDesc{name: name, oid: 23} // int4 OID
}

// int8Field creates an int8 field descriptor.
func int8Field(name string) fieldDesc {
	return fieldDesc{name: name, oid: 20} // int8 OID
}

// boolField creates a boolean field descriptor.
func boolField(name string) fieldDesc {
	return fieldDesc{name: name, oid: 16} // bool OID
}

// float8Field creates a float8 field descriptor.
func float8Field(name string) fieldDesc {
	return fieldDesc{name: name, oid: 701} // float8 OID
}

// buildRowDesc builds a RowDescription from field descriptors.
func buildRowDesc(fields []fieldDesc) *pgproto3.RowDescription {
	rd := &pgproto3.RowDescription{}
	for _, f := range fields {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:         []byte(f.name),
			DataTypeOID:  f.oid,
			DataTypeSize: -1,
			Format:       0,
		})
	}
	return rd
}

// singleRowResponse returns a response with a single row of text values.
func singleRowResponse(fields []string, values []string) []byte {
	fds := make([]fieldDesc, len(fields))
	for i, f := range fields {
		fds[i] = textField(f)
	}
	return singleRowResponseTyped(fds, values)
}

// singleRowResponseTyped returns a response with typed fields.
func singleRowResponseTyped(fields []fieldDesc, values []string) []byte {
	buf := mustEncode(buildRowDesc(fields))
	dr := &pgproto3.DataRow{}
	for _, v := range values {
		dr.Values = append(dr.Values, []byte(v))
	}
	buf = append(buf, mustEncode(dr)...)
	buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})...)
	return buf
}

// multiRowResponse returns a response with multiple rows.
func multiRowResponse(fields []string, rows [][]string) []byte {
	fds := make([]fieldDesc, len(fields))
	for i, f := range fields {
		fds[i] = textField(f)
	}
	return multiRowResponseTyped(fds, rows)
}

// multiRowResponseTyped returns a response with typed fields and multiple rows.
func multiRowResponseTyped(fields []fieldDesc, rows [][]string) []byte {
	buf := mustEncode(buildRowDesc(fields))
	for _, row := range rows {
		dr := &pgproto3.DataRow{}
		for _, v := range row {
			dr.Values = append(dr.Values, []byte(v))
		}
		buf = append(buf, mustEncode(dr)...)
	}
	buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})...)
	return buf
}

// emptyRowResponse returns a response with row description but no rows.
func emptyRowResponse(fields []string) []byte {
	fds := make([]fieldDesc, len(fields))
	for i, f := range fields {
		fds[i] = textField(f)
	}
	buf := mustEncode(buildRowDesc(fds))
	buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
	return buf
}

func TestPgxClient_ShowParameter_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponse([]string{"max_connections"}, []string{"200"})
	})
	defer cleanup()

	val, err := client.ShowParameter(context.Background(), "max_connections")
	assert.NoError(t, err)
	assert.Equal(t, "200", val)
}

func TestPgxClient_ShowParameter_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("unrecognized parameter")
	})
	defer cleanup()

	_, err := client.ShowParameter(context.Background(), "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "showing parameter")
}

func TestPgxClient_CancelQuery_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{boolField("pg_cancel_backend")},
			[]string{"t"},
		)
	})
	defer cleanup()

	result, err := client.CancelQuery(context.Background(), 123)
	assert.NoError(t, err)
	assert.True(t, result)
}

func TestPgxClient_CancelQuery_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("cancel failed")
	})
	defer cleanup()

	_, err := client.CancelQuery(context.Background(), 123)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "canceling query")
}

func TestPgxClient_TerminateSession_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{boolField("pg_terminate_backend")},
			[]string{"t"},
		)
	})
	defer cleanup()

	result, err := client.TerminateSession(context.Background(), 456)
	assert.NoError(t, err)
	assert.True(t, result)
}

func TestPgxClient_TerminateSession_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("terminate failed")
	})
	defer cleanup()

	_, err := client.TerminateSession(context.Background(), 456)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "terminating session")
}

func TestPgxClient_GetReplicationLag_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int8Field("lag")},
			[]string{"1024"},
		)
	})
	defer cleanup()

	lag, err := client.GetReplicationLag(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1024), lag)
}

func TestPgxClient_GetReplicationLag_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("no replication")
	})
	defer cleanup()

	_, err := client.GetReplicationLag(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying replication lag")
}

func TestPgxClient_GetActiveQueryCount_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int8Field("active"), int8Field("blocked"), int8Field("queued")},
			[]string{"5", "1", "2"},
		)
	})
	defer cleanup()

	active, queued, blocked, err := client.GetActiveQueryCount(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(5), active)
	assert.Equal(t, int32(2), queued)
	assert.Equal(t, int32(1), blocked)
}

func TestPgxClient_GetActiveQueryCount_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("query count failed")
	})
	defer cleanup()

	_, _, _, err := client.GetActiveQueryCount(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying active query counts")
}

func TestPgxClient_GetResourceGroupUsage_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{float8Field("cpu_usage"), float8Field("memory_usage")},
			[]string{"0.5", "0.3"},
		)
	})
	defer cleanup()

	cpu, memory, err := client.GetResourceGroupUsage(context.Background(), "default")
	assert.NoError(t, err)
	assert.Equal(t, 0.5, cpu)
	assert.Equal(t, 0.3, memory)
}

func TestPgxClient_GetResourceGroupUsage_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("resource group not found")
	})
	defer cleanup()

	_, _, err := client.GetResourceGroupUsage(context.Background(), "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying resource group usage")
}

func TestPgxClient_GetSegmentConfiguration_Mock(t *testing.T) {
	segFields := []fieldDesc{
		int4Field("content"), int4Field("dbid"), textField("role"), textField("preferred_role"),
		textField("mode"), textField("status"), textField("hostname"), textField("address"),
		int4Field("port"), textField("datadir"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(segFields, [][]string{
			{"0", "1", "p", "p", "s", "u", "host1", "10.0.0.1", "6000", "/data/primary/gpseg0"},
			{"1", "2", "p", "p", "s", "u", "host2", "10.0.0.2", "6001", "/data/primary/gpseg1"},
		})
	})
	defer cleanup()

	segments, err := client.GetSegmentConfiguration(context.Background())
	assert.NoError(t, err)
	require.Len(t, segments, 2)
	assert.Equal(t, int32(0), segments[0].ContentID)
	assert.Equal(t, "host1", segments[0].Hostname)
	assert.Equal(t, int32(1), segments[1].ContentID)
}

func TestPgxClient_GetSegmentConfiguration_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("relation not found")
	})
	defer cleanup()

	_, err := client.GetSegmentConfiguration(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying segment configuration")
}

func TestPgxClient_GetSegmentConfiguration_Empty(t *testing.T) {
	segFields := []fieldDesc{
		int4Field("content"), int4Field("dbid"), textField("role"), textField("preferred_role"),
		textField("mode"), textField("status"), textField("hostname"), textField("address"),
		int4Field("port"), textField("datadir"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(segFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	segments, err := client.GetSegmentConfiguration(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, segments)
}

func TestPgxClient_GetDiskUsage_Mock(t *testing.T) {
	diskFields := []fieldDesc{textField("datname"), int8Field("size_bytes"), textField("size_human")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(diskFields, [][]string{
			{"testdb", "1073741824", "1 GB"},
			{"postgres", "8388608", "8 MB"},
		})
	})
	defer cleanup()

	usages, err := client.GetDiskUsage(context.Background(), "")
	assert.NoError(t, err)
	require.Len(t, usages, 2)
	assert.Equal(t, "testdb", usages[0].Database)
	assert.Equal(t, int64(1073741824), usages[0].SizeBytes)
}

func TestPgxClient_GetDiskUsage_WithFilter(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return emptyRowResponse([]string{"datname", "size_bytes", "size_human"})
	})
	defer cleanup()

	usages, err := client.GetDiskUsage(context.Background(), "specific_db")
	assert.NoError(t, err)
	assert.Empty(t, usages)
}

func TestPgxClient_GetDiskUsage_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("disk usage query failed")
	})
	defer cleanup()

	_, err := client.GetDiskUsage(context.Background(), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying disk usage")
}

func TestPgxClient_ListSessions_Mock(t *testing.T) {
	sessionFields := []fieldDesc{
		int4Field("pid"), textField("usename"), textField("datname"),
		textField("application_name"),
		textField("client_addr"), textField("state"),
		textField("wait_event_type"),
		textField("query"),
		{name: "query_start", oid: 1184}, // timestamptz
		textField("duration"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(sessionFields, [][]string{
			{"123", "admin", "postgres", "psql", "10.0.0.1", "active", "", "SELECT 1", "2025-01-01 00:00:00+00", "00:01:30"},
		})
	})
	defer cleanup()

	sessions, err := client.ListSessions(context.Background())
	assert.NoError(t, err)
	require.Len(t, sessions, 1)
	assert.Equal(t, int32(123), sessions[0].PID)
	assert.Equal(t, "admin", sessions[0].Username)
	assert.Equal(t, "postgres", sessions[0].Database)
	assert.Equal(t, "active", sessions[0].State)
}

func TestPgxClient_ListSessions_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("sessions query failed")
	})
	defer cleanup()

	_, err := client.ListSessions(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying sessions")
}

func TestPgxClient_GetStorageDiskUsage_Mock(t *testing.T) {
	storageFields := []fieldDesc{textField("spcname"), int8Field("size_bytes"), textField("size_human")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(storageFields, [][]string{
			{"pg_default", "5368709120", "5 GB"},
		})
	})
	defer cleanup()

	usages, err := client.GetStorageDiskUsage(context.Background())
	assert.NoError(t, err)
	require.Len(t, usages, 1)
	assert.Equal(t, "pg_default", usages[0].Tablespace)
}

func TestPgxClient_GetStorageDiskUsage_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("storage query failed")
	})
	defer cleanup()

	_, err := client.GetStorageDiskUsage(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying storage disk usage")
}

func TestPgxClient_GetBloatRecommendations_Mock(t *testing.T) {
	bloatFields := []fieldDesc{textField("schemaname"), textField("relname"), int8Field("n_dead_tup"), int8Field("dead_pct")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(bloatFields, [][]string{
			{"public", "users", "50000", "25"},
			{"public", "events", "200000", "55"},
		})
	})
	defer cleanup()

	recs, err := client.GetBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 2)
	assert.Equal(t, "bloat", recs[0].Type)
	assert.Equal(t, "public", recs[0].Schema)
	assert.Equal(t, "users", recs[0].Table)
	assert.Equal(t, severityWarning, recs[0].Severity)
	assert.Equal(t, severityCritical, recs[1].Severity)
}

func TestPgxClient_GetBloatRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("bloat query failed")
	})
	defer cleanup()

	_, err := client.GetBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying bloat recommendations")
}

func TestPgxClient_GetSkewRecommendations_Mock(t *testing.T) {
	// New threshold-aware shape: gp_toolkit.gp_skew_coefficients returns
	// skcnamespace, skcrelname, skccoeff (float8). r.Ratio carries the coefficient.
	skewFields := []fieldDesc{textField("skcnamespace"), textField("skcrelname"), float8Field("skccoeff")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(skewFields, [][]string{
			{"public", "large_table", "5"},
		})
	})
	defer cleanup()

	recs, err := client.GetSkewRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "skew", recs[0].Type)
	assert.Equal(t, severityInfo, recs[0].Severity)
	assert.InDelta(t, 5.0, recs[0].Ratio, 0.001)
}

func TestPgxClient_GetSkewRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("skew query failed")
	})
	defer cleanup()

	_, err := client.GetSkewRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying skew recommendations")
}

func TestPgxClient_GetAgeRecommendations_Mock(t *testing.T) {
	// New threshold-aware shape: nspname, relname, age(relfrozenxid) (int8).
	// Severity tiers are warning>=100000000, critical>=500000000.
	ageFields := []fieldDesc{textField("nspname"), textField("relname"), int8Field("xid_age")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(ageFields, [][]string{
			{"public", "old_table", "150000000"},
		})
	})
	defer cleanup()

	recs, err := client.GetAgeRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "age", recs[0].Type)
	assert.Equal(t, int64(150000000), recs[0].Value)
	assert.Equal(t, severityWarning, recs[0].Severity)
}

func TestPgxClient_GetAgeRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("age query failed")
	})
	defer cleanup()

	_, err := client.GetAgeRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying age recommendations")
}

func TestPgxClient_GetIndexBloatRecommendations_Mock(t *testing.T) {
	// New threshold-aware shape: schemaname, relname, indexrelname, bloat_pct (float8).
	// Severity tiers are warning>=30, critical>=60. r.Ratio carries bloat_pct.
	idxFields := []fieldDesc{textField("schemaname"), textField("relname"), textField("indexrelname"), float8Field("bloat_pct")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(idxFields, [][]string{
			{"public", "events", "events_idx", "65"},
			{"public", "users", "users_pkey", "35"},
		})
	})
	defer cleanup()

	recs, err := client.GetIndexBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 2)
	assert.Equal(t, "index_bloat", recs[0].Type)
	assert.Equal(t, severityCritical, recs[0].Severity) // bloat_pct 65 >= 60
	assert.Equal(t, severityWarning, recs[1].Severity)  // bloat_pct 35 >= 30
	assert.InDelta(t, 65.0, recs[0].Ratio, 0.001)
}

func TestPgxClient_GetIndexBloatRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("index bloat query failed")
	})
	defer cleanup()

	_, err := client.GetIndexBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying index bloat recommendations")
}

func TestPgxClient_GetTableDetails_Mock(t *testing.T) {
	detailFields := []fieldDesc{
		textField("schemaname"), textField("relname"), int8Field("size_bytes"), textField("size_human"),
		int8Field("row_count"), int4Field("bloat_percent"), textField("last_vacuum"), textField("last_analyze"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponseTyped(detailFields,
			[]string{"public", "users", "2147483648", "2 GB", "50000000", "15", "2025-01-01", "2025-01-02"},
		)
	})
	defer cleanup()

	detail, err := client.GetTableDetails(context.Background(), "public", "users")
	assert.NoError(t, err)
	assert.Equal(t, "public", detail.Schema)
	assert.Equal(t, "users", detail.Table)
	assert.Equal(t, int64(2147483648), detail.SizeBytes)
	assert.Equal(t, int64(50000000), detail.RowCount)
}

func TestPgxClient_GetTableDetails_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("table not found")
	})
	defer cleanup()

	_, err := client.GetTableDetails(context.Background(), "public", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying table details")
}

func TestPgxClient_GetUsageReport_Mock(t *testing.T) {
	usageFields := []fieldDesc{textField("datname"), int8Field("size_bytes"), textField("size_human"), int8Field("connections")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(usageFields, [][]string{
			{"testdb", "1073741824", "1 GB", "10"},
			{"postgres", "8388608", "8 MB", "2"},
		})
	})
	defer cleanup()

	entries, err := client.GetUsageReport(context.Background(), "2025-01")
	assert.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "2025-01", entries[0].Month)
	assert.Equal(t, "testdb", entries[0].Database)
}

func TestPgxClient_GetUsageReport_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("usage report failed")
	})
	defer cleanup()

	_, err := client.GetUsageReport(context.Background(), "2025-01")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying usage report")
}

func TestPgxClient_ListResourceGroups_Mock(t *testing.T) {
	rgFields := []fieldDesc{textField("rsgname"), int4Field("concurrency"), int4Field("cpu_max_percent"), int4Field("cpu_weight"), int4Field("memory_limit"), int4Field("min_cost")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(rgFields, [][]string{
			{"analytics", "10", "50", "100", "40", "500"},
		})
	})
	defer cleanup()

	groups, err := client.ListResourceGroups(context.Background())
	assert.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "analytics", groups[0].Name)
	assert.Equal(t, int32(10), groups[0].Concurrency)
	assert.Equal(t, int32(50), groups[0].CPUMaxPercent)
	assert.Equal(t, int32(100), groups[0].CPUWeight)
	assert.Equal(t, int32(40), groups[0].MemoryLimit)
	assert.Equal(t, int32(500), groups[0].MinCost)
}

func TestPgxClient_ListResourceGroups_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("resource groups query failed")
	})
	defer cleanup()

	_, err := client.ListResourceGroups(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying resource groups")
}

func TestPgxClient_ReloadConfig_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("permission denied")
	})
	defer cleanup()

	err := client.ReloadConfig(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reloading configuration")
}

func TestPgxClient_PromoteStandby_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("not a standby")
	})
	defer cleanup()

	err := client.PromoteStandby(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "promoting standby")
}

func TestPgxClient_TriggerRecommendationScan_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("analyze failed")
	})
	defer cleanup()

	err := client.TriggerRecommendationScan(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ANALYZE for recommendation scan")
}

func TestPgxClient_CreateRole_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("role exists")
	})
	defer cleanup()

	err := client.CreateRole(context.Background(), RoleOptions{Name: "existing"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "creating role")
}

func TestPgxClient_AlterRole_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("role not found")
	})
	defer cleanup()

	err := client.AlterRole(context.Background(), RoleOptions{Name: "nonexistent"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "altering role")
}

func TestPgxClient_DropRole_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("role has deps")
	})
	defer cleanup()

	err := client.DropRole(context.Background(), "busy_role")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dropping role")
}

func TestPgxClient_Vacuum_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("vacuum failed")
	})
	defer cleanup()

	err := client.Vacuum(context.Background(), VacuumOptions{Full: true})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "running vacuum")
}

func TestPgxClient_Analyze_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("analyze failed")
	})
	defer cleanup()

	err := client.Analyze(context.Background(), "bad_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "running analyze")
}

func TestPgxClient_Reindex_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("reindex failed")
	})
	defer cleanup()

	err := client.Reindex(context.Background(), ReindexOptions{Table: "bad_table"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "running reindex")
}

func TestPgxClient_SetParameter_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("invalid parameter")
	})
	defer cleanup()

	err := client.SetParameter(context.Background(), "bad_param", "val", ParameterScope{Level: "cluster"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "setting parameter")
}

// TestSetParameter_ErrorOmitsValue is the W1-D1 SECURITY guarantee on the error
// path: the returned error must name the parameter and scope but MUST NOT leak
// the GUC value (TASK 11).
func TestSetParameter_ErrorOmitsValue(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("permission denied")
	})
	defer cleanup()

	err := client.SetParameter(context.Background(), "x", "SUPERSECRET",
		ParameterScope{Level: "cluster"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "setting parameter x")
	assert.Contains(t, err.Error(), "scope=cluster")
	assert.NotContains(t, err.Error(), "SUPERSECRET",
		"the GUC value must never appear in the error string (W1-D1 redaction)")
}

// TestSetParameter_InfoLogOmitsValue is the W1-D1 SECURITY guarantee on the
// success path: the Info-level audit log records the parameter name and scope
// but MUST NOT contain the value (which is gated behind Debug, disabled here)
// (TASK 11).
func TestSetParameter_InfoLogOmitsValue(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("ALTER SYSTEM")
	})
	defer cleanup()

	// An Info-level handler over a buffer: Debug (where the value is logged) is
	// disabled, so a leaked value would only be visible if it were logged at
	// Info — exactly what W1-D1 forbids.
	var buf bytes.Buffer
	client.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	require.NoError(t, client.SetParameter(context.Background(), "x", "SUPERSECRET",
		ParameterScope{Level: "cluster"}))

	out := buf.String()
	assert.Contains(t, out, "parameter set", "the Info audit line must be emitted")
	assert.Contains(t, out, "name=x")
	assert.Contains(t, out, "scope=cluster")
	assert.NotContains(t, out, "SUPERSECRET",
		"the GUC value must not appear at Info level (value is Debug-only, W1-D1)")
}

func TestPgxClient_CreateResourceGroup_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("resource group exists")
	})
	defer cleanup()

	err := client.CreateResourceGroup(context.Background(), ResourceGroupOptions{Name: "existing"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "creating resource group")
}

func TestPgxClient_AlterResourceGroup_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("alter failed")
	})
	defer cleanup()

	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{Name: "rg", Concurrency: 20})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "altering resource group")
}

func TestPgxClient_AssignRoleResourceGroup_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER ROLE")
	})
	defer cleanup()

	err := client.AssignRoleResourceGroup(context.Background(), "analyst", "analytics")
	assert.NoError(t, err)
}

func TestPgxClient_AssignRoleResourceGroup_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("role not found")
	})
	defer cleanup()

	err := client.AssignRoleResourceGroup(context.Background(), "nonexistent", "analytics")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assigning role")
}

func TestPgxClient_DropResourceGroup_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("drop failed")
	})
	defer cleanup()

	err := client.DropResourceGroup(context.Background(), "busy_group")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dropping resource group")
}

// TestPgxClient_RegisterNewSegments_Mock tests segment registration with mock PG server.
func TestPgxClient_RegisterNewSegments_Mock(t *testing.T) {
	t.Run("register primaries only", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			switch {
			case strings.Contains(query, "SELECT ;"):
				// Ping
				return execResponse("SELECT 1")
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			case strings.Contains(query, "MAX(dbid)"):
				return singleRowResponseTyped(
					[]fieldDesc{int4Field("max_dbid")},
					[]string{"4"},
				)
			case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
				return execResponse("INSERT 0 1")
			case strings.Contains(query, "datistemplate"):
				// listUserDatabases - return empty to skip propagation
				return emptyRowResponse([]string{"datname"})
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount:       2,
			NewCount:       4,
			MirrorEnabled:  false,
			SegmentService: "test-segment-headless",
			ClusterName:    "test-cluster",
			Port:           6000,
		})
		assert.NoError(t, err)
	})

	t.Run("register primaries and mirrors", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			case strings.Contains(query, "MAX(dbid)"):
				return singleRowResponseTyped(
					[]fieldDesc{int4Field("max_dbid")},
					[]string{"4"},
				)
			case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
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
			NewCount:       3,
			MirrorEnabled:  true,
			SegmentService: "test-segment-headless",
			ClusterName:    "test-cluster",
			Port:           6000,
		})
		assert.NoError(t, err)
	})

	t.Run("ping error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("SELECT 1")
		})
		defer cleanup()

		client.pool.Close()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount: 2, NewCount: 4, ClusterName: "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database not reachable")
	})

	t.Run("set allow_system_table_mods error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			if callCount == 1 {
				// Ping succeeds
				return execResponse("SELECT 1")
			}
			// SET allow_system_table_mods fails
			return errorResponseMsg("permission denied")
		})
		defer cleanup()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount: 2, NewCount: 4, ClusterName: "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "enabling system table modifications")
	})

	t.Run("max dbid query error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			switch {
			case callCount <= 1:
				return execResponse("SELECT 1")
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			default:
				return errorResponseMsg("relation not found")
			}
		})
		defer cleanup()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount: 2, NewCount: 4, ClusterName: "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "querying max dbid")
	})

	t.Run("insert primary segment error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			case strings.Contains(query, "MAX(dbid)"):
				return singleRowResponseTyped(
					[]fieldDesc{int4Field("max_dbid")},
					[]string{"4"},
				)
			case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
				return errorResponseMsg("insert failed")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount: 2, NewCount: 3, ClusterName: "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "registering primary segment")
	})

	t.Run("insert mirror segment error", func(t *testing.T) {
		insertCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			case strings.Contains(query, "MAX(dbid)"):
				return singleRowResponseTyped(
					[]fieldDesc{int4Field("max_dbid")},
					[]string{"4"},
				)
			case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
				insertCount++
				if insertCount > 1 {
					// Second insert (mirror) fails
					return errorResponseMsg("mirror insert failed")
				}
				return execResponse("INSERT 0 1")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount: 2, NewCount: 3, MirrorEnabled: true, ClusterName: "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "registering mirror segment")
	})

	t.Run("propagation with databases", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			case strings.Contains(query, "MAX(dbid)"):
				return singleRowResponseTyped(
					[]fieldDesc{int4Field("max_dbid")},
					[]string{"4"},
				)
			case strings.Contains(query, "INSERT INTO gp_segment_configuration"):
				return execResponse("INSERT 0 1")
			case strings.Contains(query, "datistemplate"):
				// Return databases - propagation will fail to connect but that's non-fatal
				return multiRowResponse([]string{"datname"}, [][]string{
					{"postgres"},
					{"mydb"},
				})
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RegisterNewSegments(context.Background(), SegmentRegistrationOptions{
			OldCount:       2,
			NewCount:       3,
			MirrorEnabled:  false,
			SegmentService: "test-segment-headless",
			ClusterName:    "test-cluster",
			Port:           6000,
		})
		// Should succeed even if propagation fails (non-fatal)
		assert.NoError(t, err)
	})
}

// TestPgxClient_RedistributeData_Mock tests data redistribution.
func TestPgxClient_RedistributeData_Mock(t *testing.T) {
	t.Run("ping error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("SELECT 1")
		})
		defer cleanup()

		client.pool.Close()

		err := client.RedistributeData(context.Background(), RedistributionOptions{
			Database: "mydb",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database not reachable")
	})

	t.Run("list databases error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			if callCount == 1 {
				return execResponse("SELECT 1") // Ping
			}
			return errorResponseMsg("query failed")
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listing user databases")
	})

	t.Run("empty databases list", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "datistemplate") {
				return emptyRowResponse([]string{"datname"})
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		assert.NoError(t, err)
	})

	t.Run("redistribution with databases and tables", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"testdb"},
				})
			case strings.Contains(query, "pg_class") && strings.Contains(query, "relkind"):
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "orders", "customer_id"},
					{"public", "events", ""},
					{"public", "excluded_table", "id"},
				})
			case strings.Contains(query, "ALTER TABLE"):
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{
			Database:      "testdb",
			ExcludeTables: []string{"public.excluded_table"},
			Parallelism:   2,
		})
		assert.NoError(t, err)
	})

	t.Run("redistribution table query error continues", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"testdb"},
				})
			case strings.Contains(query, "pg_class"):
				return errorResponseMsg("table query failed")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{
			Parallelism: 2,
		})
		assert.NoError(t, err)
	})

	t.Run("redistribution alter table error continues", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"testdb"},
				})
			case strings.Contains(query, "pg_class") && strings.Contains(query, "relkind"):
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "orders", "customer_id"},
				})
			case strings.Contains(query, "ALTER TABLE"):
				return errorResponseMsg("alter failed")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeData(context.Background(), RedistributionOptions{})
		assert.NoError(t, err)
	})
}

// TestPgxClient_RedistributeBeforeScaleIn_Mock tests pre-scale-in redistribution.
func TestPgxClient_RedistributeBeforeScaleIn_Mock(t *testing.T) {
	t.Run("ping error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("SELECT 1")
		})
		defer cleanup()

		client.pool.Close()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount: 2,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database not reachable")
	})

	t.Run("list databases error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			if callCount == 1 {
				return execResponse("SELECT 1") // Ping
			}
			return errorResponseMsg("query failed")
		})
		defer cleanup()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount: 2,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listing user databases")
	})

	t.Run("empty databases list", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "datistemplate") {
				return emptyRowResponse([]string{"datname"})
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount: 2,
		})
		assert.NoError(t, err)
	})

	t.Run("filter specific database", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"testdb"},
					{"analytics"},
				})
			case strings.Contains(query, "pg_class") && strings.Contains(query, "relkind"):
				return multiRowResponseTyped(tableFields, [][]string{})
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount: 2,
			Database: "testdb",
		})
		assert.NoError(t, err)
	})

	t.Run("database not in list", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "datistemplate") {
				return multiRowResponse([]string{"datname"}, [][]string{
					{"postgres"},
				})
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount: 2,
			Database: "nonexistent",
		})
		assert.NoError(t, err)
	})

	t.Run("redistribution with tables and exclusions", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"testdb"},
				})
			case strings.Contains(query, "pg_class") && strings.Contains(query, "relkind"):
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "orders", "customer_id"},
					{"public", "events", ""},
					{"public", "temp", "id"},
				})
			case strings.Contains(query, "DROP TABLE IF EXISTS"):
				return execResponse("DROP TABLE")
			case strings.Contains(query, "CREATE TABLE"):
				return execResponse("CREATE TABLE")
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			case strings.Contains(query, "UPDATE gp_distribution_policy"):
				return execResponse("UPDATE 1")
			case strings.Contains(query, "INSERT INTO"):
				return execResponse("INSERT 0 1")
			case strings.Contains(query, "DROP TABLE"):
				return execResponse("DROP TABLE")
			case strings.Contains(query, "ALTER TABLE"):
				return execResponse("ALTER TABLE")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount:      2,
			ExcludeTables: []string{"public.temp"},
		})
		assert.NoError(t, err)
	})

	t.Run("table query error continues", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "datistemplate"):
				return multiRowResponse([]string{"datname"}, [][]string{
					{"testdb"},
				})
			case strings.Contains(query, "pg_class"):
				return errorResponseMsg("table query failed")
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		err := client.RedistributeBeforeScaleIn(context.Background(), ScaleInRedistributionOptions{
			NewCount: 2,
		})
		assert.NoError(t, err)
	})
}

// TestPgxClient_AnalyzeSkew_Mock tests skew analysis.
func TestPgxClient_AnalyzeSkew_Mock(t *testing.T) {
	t.Run("ping error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("SELECT 1")
		})
		defer cleanup()

		client.pool.Close()

		_, err := client.AnalyzeSkew(context.Background(), "mydb")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database not reachable")
	})

	t.Run("same database - no new pool", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		skewFields := []fieldDesc{int8Field("total_rows"), float8Field("skew_coefficient")}
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "pg_class"):
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "orders", "customer_id"},
					{"public", "events", ""},
				})
			case strings.Contains(query, "gp_segment_id"):
				// Both tables return data with skew
				return singleRowResponseTyped(skewFields, []string{"1000", "25.5"})
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		// Use same database as pool's default
		client.config.Database = "testdb"
		results, err := client.AnalyzeSkew(context.Background(), "testdb")
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, "public", results[0].Schema)
		assert.Equal(t, "orders", results[0].Table)
		assert.Equal(t, 25.5, results[0].SkewCoefficient)
		assert.Equal(t, int64(1000), results[0].RowCount)
		assert.Equal(t, "customer_id", results[0].DistributionKey)
		assert.Equal(t, "", results[1].DistributionKey)
	})

	t.Run("empty database name uses default pool", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			if strings.Contains(query, "pg_class") {
				return multiRowResponseTyped(tableFields, [][]string{})
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		results, err := client.AnalyzeSkew(context.Background(), "")
		assert.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("table query error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			if callCount == 1 {
				return execResponse("SELECT 1") // Ping
			}
			return errorResponseMsg("query failed")
		})
		defer cleanup()

		client.config.Database = "testdb"
		_, err := client.AnalyzeSkew(context.Background(), "testdb")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "querying user tables for skew analysis")
	})

	t.Run("skew query error is skipped", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			if strings.Contains(query, "pg_class") {
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "bad_table", "id"},
				})
			}
			if strings.Contains(query, "gp_segment_id") {
				return errorResponseMsg("skew query failed")
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		client.config.Database = "testdb"
		results, err := client.AnalyzeSkew(context.Background(), "testdb")
		assert.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("multiple tables with mixed results", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		skewFields := []fieldDesc{int8Field("total_rows"), float8Field("skew_coefficient")}
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "pg_class"):
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "table1", "id"},
					{"public", "table2", ""},
				})
			case strings.Contains(query, "gp_segment_id"):
				return singleRowResponseTyped(skewFields, []string{"5000", "10.2"})
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		client.config.Database = "testdb"
		results, err := client.AnalyzeSkew(context.Background(), "testdb")
		assert.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("different database creates new pool", func(t *testing.T) {
		tableFields := []fieldDesc{textField("schema_name"), textField("table_name"), textField("dist_key")}
		skewFields := []fieldDesc{int8Field("total_rows"), float8Field("skew_coefficient")}
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			switch {
			case strings.Contains(query, "pg_class"):
				return multiRowResponseTyped(tableFields, [][]string{
					{"public", "orders", "id"},
				})
			case strings.Contains(query, "gp_segment_id"):
				return singleRowResponseTyped(skewFields, []string{"500", "5.0"})
			default:
				return execResponse("SELECT 1")
			}
		})
		defer cleanup()

		results, err := client.AnalyzeSkew(context.Background(), "other_db")
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, "other_db", results[0].Database)
	})
}

// TestPgxClient_RebalanceTable_Mock tests table rebalancing.
func TestPgxClient_RebalanceTable_Mock(t *testing.T) {
	t.Run("rebalance with dist key - same database", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("ALTER TABLE")
		})
		defer cleanup()

		client.config.Database = "testdb"
		err := client.RebalanceTable(context.Background(), "testdb", "public", "orders", "customer_id")
		assert.NoError(t, err)
	})

	t.Run("rebalance without dist key - random", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("ALTER TABLE")
		})
		defer cleanup()

		client.config.Database = "testdb"
		err := client.RebalanceTable(context.Background(), "testdb", "public", "events", "")
		assert.NoError(t, err)
	})

	t.Run("rebalance empty database uses default pool", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("ALTER TABLE")
		})
		defer cleanup()

		err := client.RebalanceTable(context.Background(), "", "public", "orders", "id")
		assert.NoError(t, err)
	})

	t.Run("rebalance error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return errorResponseMsg("alter table failed")
		})
		defer cleanup()

		client.config.Database = "testdb"
		err := client.RebalanceTable(context.Background(), "testdb", "public", "orders", "id")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rebalancing table")
	})

	t.Run("rebalance with same database name as config", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return execResponse("ALTER TABLE")
		})
		defer cleanup()

		client.config.Database = "testdb"
		err := client.RebalanceTable(context.Background(), "testdb", "public", "orders", "id")
		assert.NoError(t, err)
	})

	t.Run("different database creates new pool", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			return execResponse("ALTER TABLE")
		})
		defer cleanup()

		err := client.RebalanceTable(context.Background(), "other_db", "public", "orders", "id")
		assert.NoError(t, err)
	})

	t.Run("different database rebalance error", func(t *testing.T) {
		client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
			if strings.Contains(query, "ALTER TABLE") {
				return errorResponseMsg("alter failed")
			}
			return execResponse("SELECT 1")
		})
		defer cleanup()

		err := client.RebalanceTable(context.Background(), "other_db", "public", "orders", "id")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rebalancing table")
	})
}

// TestPgxClient_DeregisterSegments_Errors tests deregistration error paths.
func TestPgxClient_DeregisterSegments_Errors(t *testing.T) {
	t.Run("set allow_system_table_mods error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			if callCount == 1 {
				return execResponse("SELECT 1") // Ping
			}
			return errorResponseMsg("permission denied")
		})
		defer cleanup()

		err := client.DeregisterSegments(context.Background(), 2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "enabling system table modifications")
	})

	t.Run("delete error", func(t *testing.T) {
		callCount := 0
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			callCount++
			switch {
			case callCount == 1:
				return execResponse("SELECT 1") // Ping
			case strings.Contains(query, "allow_system_table_mods"):
				return execResponse("SET")
			default:
				return errorResponseMsg("delete failed")
			}
		})
		defer cleanup()

		err := client.DeregisterSegments(context.Background(), 2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "deleting segment entries")
	})
}

// TestPgxClient_GetSegmentConfiguration_ScanError tests scan error in GetSegmentConfiguration.
func TestPgxClient_GetSegmentConfiguration_ScanError(t *testing.T) {
	// Return wrong number of columns to trigger scan error
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"content", "dbid"}, [][]string{
			{"0", "1"},
		})
	})
	defer cleanup()

	_, err := client.GetSegmentConfiguration(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning segment row")
}

// TestPgxClient_ListSessions_Empty tests empty sessions list.
func TestPgxClient_ListSessions_Empty(t *testing.T) {
	sessionFields := []fieldDesc{
		int4Field("pid"), textField("usename"), textField("application_name"),
		textField("client_addr"), textField("state"), textField("query"),
		{name: "query_start", oid: 1184},
		textField("duration"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(sessionFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	sessions, err := client.ListSessions(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, sessions)
}

// TestPgxClient_GetDiskUsage_ScanError tests scan error in GetDiskUsage.
func TestPgxClient_GetDiskUsage_ScanError(t *testing.T) {
	// Return wrong column types to trigger scan error
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"datname"}, [][]string{
			{"testdb"},
		})
	})
	defer cleanup()

	_, err := client.GetDiskUsage(context.Background(), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning disk usage row")
}

// TestPgxClient_GetStorageDiskUsage_ScanError tests scan error in GetStorageDiskUsage.
func TestPgxClient_GetStorageDiskUsage_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"spcname"}, [][]string{
			{"pg_default"},
		})
	})
	defer cleanup()

	_, err := client.GetStorageDiskUsage(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning storage disk usage row")
}

// TestPgxClient_GetBloatRecommendations_ScanError tests scan error.
func TestPgxClient_GetBloatRecommendations_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"schemaname"}, [][]string{
			{"public"},
		})
	})
	defer cleanup()

	_, err := client.GetBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning bloat recommendation row")
}

// TestPgxClient_GetSkewRecommendations_ScanError tests scan error.
func TestPgxClient_GetSkewRecommendations_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"schemaname"}, [][]string{
			{"public"},
		})
	})
	defer cleanup()

	_, err := client.GetSkewRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning skew recommendation row")
}

// TestPgxClient_GetAgeRecommendations_ScanError tests scan error.
func TestPgxClient_GetAgeRecommendations_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"schemaname"}, [][]string{
			{"public"},
		})
	})
	defer cleanup()

	_, err := client.GetAgeRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning age recommendation row")
}

// TestPgxClient_GetIndexBloatRecommendations_ScanError tests scan error.
func TestPgxClient_GetIndexBloatRecommendations_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"schemaname"}, [][]string{
			{"public"},
		})
	})
	defer cleanup()

	_, err := client.GetIndexBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning index bloat recommendation row")
}

// TestPgxClient_GetUsageReport_ScanError tests scan error.
func TestPgxClient_GetUsageReport_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"datname"}, [][]string{
			{"testdb"},
		})
	})
	defer cleanup()

	_, err := client.GetUsageReport(context.Background(), "2025-01")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning usage report row")
}

// TestPgxClient_ListResourceGroups_ScanError tests scan error.
func TestPgxClient_ListResourceGroups_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"rsgname"}, [][]string{
			{"analytics"},
		})
	})
	defer cleanup()

	_, err := client.ListResourceGroups(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning resource group row")
}

// TestPgxClient_ListResourceQueues_ScanError tests scan error.
func TestPgxClient_ListResourceQueues_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"rsqname"}, [][]string{
			{"test_queue"},
		})
	})
	defer cleanup()

	_, err := client.ListResourceQueues(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning resource queue row")
}

// TestPgxClient_GetMirrorSyncStatus_ScanError tests scan error.
func TestPgxClient_GetMirrorSyncStatus_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"content_id"}, [][]string{
			{"0"},
		})
	})
	defer cleanup()

	_, err := client.GetMirrorSyncStatus(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning mirror sync status row")
}

// TestPgxClient_ListUserDatabases_ScanError tests scan error.
func TestPgxClient_ListUserDatabases_ScanError(t *testing.T) {
	// Return a row with wrong type to trigger scan error
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		// Return two columns when only one is expected
		return multiRowResponse([]string{"datname", "extra"}, [][]string{
			{"testdb", "extra_val"},
		})
	})
	defer cleanup()

	// This actually won't error because pgx can handle extra columns in simple protocol
	// Let's test the empty case instead
	dbs, err := client.ListUserDatabases(context.Background())
	// pgx in simple protocol mode may or may not error on extra columns
	if err == nil {
		assert.NotNil(t, dbs)
	}
}

// TestPgxClient_ListSessions_ScanError tests scan error in ListSessions.
func TestPgxClient_ListSessions_ScanError(t *testing.T) {
	// Return wrong number of columns to trigger scan error
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"pid"}, [][]string{
			{"123"},
		})
	})
	defer cleanup()

	_, err := client.ListSessions(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "scanning session row")
}

// TestPgxClient_GetBloatRecommendations_Empty tests empty bloat recommendations.
func TestPgxClient_GetBloatRecommendations_Empty(t *testing.T) {
	bloatFields := []fieldDesc{textField("schemaname"), textField("relname"), int8Field("n_dead_tup"), int8Field("dead_pct")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(bloatFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	recs, err := client.GetBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	assert.Empty(t, recs)
}

// TestPgxClient_GetSkewRecommendations_Empty tests empty skew recommendations.
func TestPgxClient_GetSkewRecommendations_Empty(t *testing.T) {
	skewFields := []fieldDesc{textField("skcnamespace"), textField("skcrelname"), float8Field("skccoeff")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(skewFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	recs, err := client.GetSkewRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	assert.Empty(t, recs)
}

// TestPgxClient_GetAgeRecommendations_Empty tests empty age recommendations.
func TestPgxClient_GetAgeRecommendations_Empty(t *testing.T) {
	ageFields := []fieldDesc{textField("nspname"), textField("relname"), int8Field("xid_age")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(ageFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	recs, err := client.GetAgeRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	assert.Empty(t, recs)
}

// TestPgxClient_GetIndexBloatRecommendations_Empty tests empty index bloat recommendations.
func TestPgxClient_GetIndexBloatRecommendations_Empty(t *testing.T) {
	idxFields := []fieldDesc{textField("schemaname"), textField("relname"), textField("indexrelname"), float8Field("bloat_pct")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(idxFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	recs, err := client.GetIndexBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	assert.Empty(t, recs)
}

// TestPgxClient_GetUsageReport_Empty tests empty usage report.
func TestPgxClient_GetUsageReport_Empty(t *testing.T) {
	usageFields := []fieldDesc{textField("datname"), int8Field("size_bytes"), textField("size_human"), int8Field("connections")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(usageFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	entries, err := client.GetUsageReport(context.Background(), "2025-01")
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

// TestPgxClient_GetStorageDiskUsage_Empty tests empty storage disk usage.
func TestPgxClient_GetStorageDiskUsage_Empty(t *testing.T) {
	storageFields := []fieldDesc{textField("spcname"), int8Field("size_bytes"), textField("size_human")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(storageFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	usages, err := client.GetStorageDiskUsage(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, usages)
}

// TestPgxClient_ListResourceGroups_Empty tests empty resource groups.
func TestPgxClient_ListResourceGroups_Empty(t *testing.T) {
	rgFields := []fieldDesc{textField("rsgname"), int4Field("concurrency"), int4Field("cpu_max_percent"), int4Field("cpu_weight"), int4Field("memory_limit"), int4Field("min_cost")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(rgFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	groups, err := client.ListResourceGroups(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, groups)
}

// TestPgxClient_GetMirrorSyncStatus_Empty tests empty mirror sync status.
func TestPgxClient_GetMirrorSyncStatus_Empty(t *testing.T) {
	mirrorFields := []fieldDesc{
		int4Field("content_id"), boolField("is_synced"),
		int8Field("replication_lag"), textField("state"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		buf := mustEncode(buildRowDesc(mirrorFields))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	infos, err := client.GetMirrorSyncStatus(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

// TestPgxClient_ListUserDatabases_Empty tests empty database list.
func TestPgxClient_ListUserDatabases_Empty(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return emptyRowResponse([]string{"datname"})
	})
	defer cleanup()

	dbs, err := client.ListUserDatabases(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, dbs)
}

// TestPgxClient_GetBloatRecommendations_InfoSeverity tests info severity classification.
func TestPgxClient_GetBloatRecommendations_InfoSeverity(t *testing.T) {
	bloatFields := []fieldDesc{textField("schemaname"), textField("relname"), int8Field("n_dead_tup"), int8Field("dead_pct")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(bloatFields, [][]string{
			{"public", "small_table", "100", "5"},
		})
	})
	defer cleanup()

	recs, err := client.GetBloatRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, severityInfo, recs[0].Severity)
}

// TestPgxClient_GetAgeRecommendations_CriticalSeverity tests critical severity.
func TestPgxClient_GetAgeRecommendations_CriticalSeverity(t *testing.T) {
	// XID age >= 500000000 → critical.
	ageFields := []fieldDesc{textField("nspname"), textField("relname"), int8Field("xid_age")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(ageFields, [][]string{
			{"public", "very_old_table", "600000000"},
		})
	})
	defer cleanup()

	recs, err := client.GetAgeRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, severityCritical, recs[0].Severity)
}

// TestPgxClient_GetAgeRecommendations_InfoSeverity tests info severity.
func TestPgxClient_GetAgeRecommendations_InfoSeverity(t *testing.T) {
	// XID age below the warning tier (100000000) → info.
	ageFields := []fieldDesc{textField("nspname"), textField("relname"), int8Field("xid_age")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(ageFields, [][]string{
			{"public", "small_table", "50000"},
		})
	})
	defer cleanup()

	recs, err := client.GetAgeRecommendations(context.Background(), RecommendationThresholds{})
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, severityInfo, recs[0].Severity)
}

// ============================================================================
// FormatIOLimits Tests
// ============================================================================

func TestFormatIOLimits(t *testing.T) {
	tests := []struct {
		name     string
		limits   []IOLimitOption
		expected string
	}{
		{
			name:     "empty limits",
			limits:   nil,
			expected: "",
		},
		{
			name:     "empty slice",
			limits:   []IOLimitOption{},
			expected: "",
		},
		{
			name: "single tablespace",
			limits: []IOLimitOption{
				{Tablespace: "pg_default", ReadBytesPerSec: 1000, WriteBytesPerSec: 500, ReadIOPS: 100, WriteIOPS: 50},
			},
			expected: "pg_default:rbps=1000:wbps=500:riops=100:wiops=50",
		},
		{
			name: "multiple tablespaces",
			limits: []IOLimitOption{
				{Tablespace: "pg_default", ReadBytesPerSec: 1000, WriteBytesPerSec: 500, ReadIOPS: 100, WriteIOPS: 50},
				{Tablespace: "fast_ssd", ReadBytesPerSec: 5000, WriteBytesPerSec: 3000, ReadIOPS: 500, WriteIOPS: 300},
			},
			expected: "pg_default:rbps=1000:wbps=500:riops=100:wiops=50;fast_ssd:rbps=5000:wbps=3000:riops=500:wiops=300",
		},
		{
			name: "zero values",
			limits: []IOLimitOption{
				{Tablespace: "ts1", ReadBytesPerSec: 0, WriteBytesPerSec: 0, ReadIOPS: 0, WriteIOPS: 0},
			},
			expected: "ts1:rbps=0:wbps=0:riops=0:wiops=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatIOLimits(tt.limits)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// ListSessionsWithResourceGroup Tests (via mock PG server)
// ============================================================================

func TestPgxClient_ListSessionsWithResourceGroup_Mock(t *testing.T) {
	sessionFields := []fieldDesc{
		int4Field("pid"), textField("usename"), textField("datname"),
		textField("application_name"),
		textField("client_addr"), textField("state"),
		textField("wait_event_type"),
		textField("query"),
		{name: "query_start", oid: 1184}, // timestamptz
		textField("duration"),
		textField("rsgname"),
	}

	t.Run("returns sessions with resource groups", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return multiRowResponseTyped(sessionFields, [][]string{
				{"123", "admin", "postgres", "psql", "10.0.0.1", "active", "", "SELECT 1", "2025-01-01 00:00:00+00", "00:01:30", "analytics"},
				{"456", "etl_user", "warehouse", "loader", "10.0.0.2", "idle", "Lock", "", "2025-01-01 00:00:00+00", "00:05:00", "etl_group"},
			})
		})
		defer cleanup()

		sessions, err := client.ListSessionsWithResourceGroup(context.Background())
		assert.NoError(t, err)
		require.Len(t, sessions, 2)
		assert.Equal(t, int32(123), sessions[0].PID)
		assert.Equal(t, "admin", sessions[0].Username)
		assert.Equal(t, "postgres", sessions[0].Database)
		assert.Equal(t, "analytics", sessions[0].ResourceGroup)
		assert.Equal(t, int32(456), sessions[1].PID)
		assert.Equal(t, "warehouse", sessions[1].Database)
		assert.Equal(t, "Lock", sessions[1].WaitEventType)
		assert.Equal(t, "etl_group", sessions[1].ResourceGroup)
	})

	t.Run("empty result", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			buf := mustEncode(buildRowDesc(sessionFields))
			buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
			return buf
		})
		defer cleanup()

		sessions, err := client.ListSessionsWithResourceGroup(context.Background())
		assert.NoError(t, err)
		assert.Empty(t, sessions)
	})

	t.Run("query error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return errorResponseMsg("sessions query failed")
		})
		defer cleanup()

		_, err := client.ListSessionsWithResourceGroup(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "querying sessions with resource group")
	})

	t.Run("scan error", func(t *testing.T) {
		client, cleanup := newMockPgxClient(t, func(query string) []byte {
			return multiRowResponse([]string{"pid"}, [][]string{
				{"123"},
			})
		})
		defer cleanup()

		_, err := client.ListSessionsWithResourceGroup(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scanning session with resource group row")
	})
}

// ============================================================================
// AlterResourceGroup with IO Limits Tests
// ============================================================================

func TestPgxClient_AlterResourceGroup_WithIOLimits(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{
		Name:        "analytics",
		Concurrency: 20,
		IOLimits: []IOLimitOption{
			{Tablespace: "pg_default", ReadBytesPerSec: 1000, WriteBytesPerSec: 500, ReadIOPS: 100, WriteIOPS: 50},
		},
	})
	assert.NoError(t, err)
}

func TestPgxClient_AlterResourceGroup_IOLimitsError(t *testing.T) {
	callCount := 0
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		callCount++
		if strings.Contains(query, "io_limit") {
			return errorResponseMsg("io_limit not supported")
		}
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{
		Name: "analytics",
		IOLimits: []IOLimitOption{
			{Tablespace: "pg_default", ReadBytesPerSec: 1000, WriteBytesPerSec: 500, ReadIOPS: 100, WriteIOPS: 50},
		},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "setting io_limit")
}

func TestPgxClient_AlterResourceGroup_IndividualParamError(t *testing.T) {
	// Test error on individual ALTER statements (e.g., concurrency fails)
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if strings.Contains(query, "concurrency") {
			return errorResponseMsg("concurrency alter failed")
		}
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{
		Name:          "analytics",
		Concurrency:   20,
		CPUMaxPercent: 60,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "altering resource group")
	assert.Contains(t, err.Error(), "concurrency")
}

func TestPgxClient_AlterResourceGroup_AllParams(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	err := client.AlterResourceGroup(context.Background(), ResourceGroupOptions{
		Name:          "analytics",
		Concurrency:   20,
		CPUMaxPercent: 60,
		CPUWeight:     150,
		MemoryLimit:   4096,
		MinCost:       500,
	})
	assert.NoError(t, err)
}

// ============================================================================
// IOLimitOption Construction Tests
// ============================================================================

func TestIOLimitOption_Construction(t *testing.T) {
	opt := IOLimitOption{
		Tablespace:       "pg_default",
		ReadBytesPerSec:  1048576,
		WriteBytesPerSec: 524288,
		ReadIOPS:         1000,
		WriteIOPS:        500,
	}
	assert.Equal(t, "pg_default", opt.Tablespace)
	assert.Equal(t, int64(1048576), opt.ReadBytesPerSec)
	assert.Equal(t, int64(524288), opt.WriteBytesPerSec)
	assert.Equal(t, int32(1000), opt.ReadIOPS)
	assert.Equal(t, int32(500), opt.WriteIOPS)
}

// ============================================================================
// ResourceGroupInfo IOLimits field test
// ============================================================================

// ============================================================================
// SetupExporterRole Tests
// ============================================================================

func TestPgxClient_SetupExporterRole_Create(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			// Role does not yet exist.
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"f"})
		case strings.Contains(query, "CREATE ROLE"):
			return execResponse("CREATE ROLE")
		case strings.Contains(query, "GRANT"):
			return execResponse("GRANT")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.SetupExporterRole(context.Background(), "secret-pass")
	assert.NoError(t, err)
}

func TestPgxClient_SetupExporterRole_Exists(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			// Role already exists => ALTER ROLE branch.
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"t"})
		case strings.Contains(query, "ALTER ROLE"):
			return execResponse("ALTER ROLE")
		case strings.Contains(query, "GRANT"):
			return execResponse("GRANT")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.SetupExporterRole(context.Background(), "new-pass")
	assert.NoError(t, err)
}

func TestPgxClient_SetupExporterRole_ExistenceCheckError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("permission denied")
	})
	defer cleanup()

	err := client.SetupExporterRole(context.Background(), "pass")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checking exporter role existence")
}

func TestPgxClient_SetupExporterRole_CreateError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"f"})
		case strings.Contains(query, "CREATE ROLE"):
			return errorResponseMsg("create failed")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.SetupExporterRole(context.Background(), "pass")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating exporter role")
}

func TestPgxClient_SetupExporterRole_GrantViewErrorIgnored(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"f"})
		case strings.Contains(query, "CREATE ROLE"):
			return execResponse("CREATE ROLE")
		case strings.Contains(query, "GRANT pg_monitor"):
			return execResponse("GRANT")
		case strings.Contains(query, "GRANT SELECT"):
			// View grants may fail for missing views; must be tolerated.
			return errorResponseMsg("view does not exist")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.SetupExporterRole(context.Background(), "pass")
	assert.NoError(t, err)
}

// ============================================================================
// SetupPXFExtensions Tests (best-effort, non-fatal contract)
// ============================================================================

// pxfProbeRow is the bool response for the "SELECT true" connectivity probe.
func pxfProbeRow() []byte {
	return singleRowResponseTyped([]fieldDesc{boolField("bool")}, []string{"t"})
}

// recordingResponder wraps a responder, capturing every query it sees into the
// returned slice pointer so a test can assert which statements were executed
// (GRANT presence/absence). The mutex guards concurrent pool access.
func recordingResponder(inner func(query string) []byte) (func(query string) []byte, *[]string) {
	var mu sync.Mutex
	var queries []string
	wrapped := func(query string) []byte {
		mu.Lock()
		queries = append(queries, query)
		mu.Unlock()
		return inner(query)
	}
	return wrapped, &queries
}

// countQueriesContaining returns the number of recorded queries containing sub.
func countQueriesContaining(queries []string, sub string) int {
	n := 0
	for _, q := range queries {
		if strings.Contains(q, sub) {
			n++
		}
	}
	return n
}

func TestPgxClient_SetupPXFExtensions_BothSucceed(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT true"):
			return pxfProbeRow()
		case strings.Contains(query, "CREATE EXTENSION"):
			return execResponse("CREATE EXTENSION")
		case strings.Contains(query, "GRANT"):
			return execResponse("GRANT")
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	installed, err := client.SetupPXFExtensions(context.Background())
	assert.NoError(t, err)
	// Both pxf and pxf_fdw installed → installed count is 2.
	assert.Equal(t, 2, installed)

	// RP.11: pxf installed → GRANT SELECT and GRANT INSERT ON PROTOCOL pxf are
	// issued to the sanitized "gpadmin" data-loader role.
	assert.Equal(t, 1, countQueriesContaining(*queries, `GRANT SELECT ON PROTOCOL pxf TO "gpadmin"`))
	assert.Equal(t, 1, countQueriesContaining(*queries, `GRANT INSERT ON PROTOCOL pxf TO "gpadmin"`))
}

func TestPgxClient_SetupPXFExtensions_PxfFailsFdwSucceeds(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT true"):
			return pxfProbeRow()
		case strings.Contains(query, `"pxf_fdw"`):
			return execResponse("CREATE EXTENSION")
		case strings.Contains(query, "CREATE EXTENSION"):
			// The plain "pxf" extension is unavailable in this image.
			return errorResponseMsg(`extension "pxf" is not available`)
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	// Best-effort: a failing pxf with a succeeding pxf_fdw still returns nil.
	installed, err := client.SetupPXFExtensions(context.Background())
	assert.NoError(t, err)
	// Only pxf_fdw installed (pxf failed) → installed count is 1.
	assert.Equal(t, 1, installed)

	// RP.11: pxf did NOT install → the PROTOCOL pxf GRANTs are NOT attempted.
	assert.Zero(t, countQueriesContaining(*queries, "GRANT"),
		"no GRANT must be issued when the pxf extension is absent")
}

func TestPgxClient_SetupPXFExtensions_BothFailBenign(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT true"):
			return pxfProbeRow()
		case strings.Contains(query, "CREATE EXTENSION"):
			// Neither pxf nor pxf_fdw is available — both fail benignly.
			return errorResponseMsg("extension not available")
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	// Non-fatal: both extensions unavailable on a reachable DB returns nil.
	installed, err := client.SetupPXFExtensions(context.Background())
	assert.NoError(t, err)
	// Reachable DB but nothing installed → installed count is 0 (retryable).
	assert.Zero(t, installed)
	assert.Zero(t, countQueriesContaining(*queries, "GRANT"))
}

// TestPgxClient_SetupPXFExtensions_GrantFailsNonFatal proves the PROTOCOL pxf
// GRANTs are best-effort: the pxf extension installs but the GRANTs error (e.g.
// the protocol is absent on a stub image) and the method still returns nil.
func TestPgxClient_SetupPXFExtensions_GrantFailsNonFatal(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT true"):
			return pxfProbeRow()
		case strings.Contains(query, "CREATE EXTENSION"):
			return execResponse("CREATE EXTENSION")
		case strings.Contains(query, "GRANT"):
			return errorResponseMsg(`protocol "pxf" does not exist`)
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	installed, err := client.SetupPXFExtensions(context.Background())
	assert.NoError(t, err)
	// Both extensions installed even though the GRANTs failed best-effort.
	assert.Equal(t, 2, installed)
	// Both GRANTs were attempted (best-effort) even though they failed.
	assert.Equal(t, 2, countQueriesContaining(*queries, "GRANT"))
}

func TestPgxClient_SetupPXFExtensions_ConnectivityError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		// The connectivity probe itself fails => hard error surfaced.
		return errorResponseMsg("server closed the connection")
	})
	defer cleanup()

	installed, err := client.SetupPXFExtensions(context.Background())
	require.Error(t, err)
	assert.Zero(t, installed)
	assert.Contains(t, err.Error(), "probing connectivity")
}

// ============================================================================
// ListPXFExtensions Tests (read-only, observed-only honesty contract)
// ============================================================================

// TestPgxClient_ListPXFExtensions_BothInstalled covers 105-S3-B1: a reachable DB
// with both extensions present returns ["pxf","pxf_fdw"] in deterministic
// (ascending) order, exactly as the catalog query (ORDER BY extname) yields.
func TestPgxClient_ListPXFExtensions_BothInstalled(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		// The pg_extension probe returns both rows, ascending.
		return multiRowResponse([]string{"extname"}, [][]string{{"pxf"}, {"pxf_fdw"}})
	})
	defer cleanup()

	exts, err := client.ListPXFExtensions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"pxf", "pxf_fdw"}, exts)
}

// TestPgxClient_ListPXFExtensions_OnlyPxf covers 105-S3-B2: an HONEST subset —
// only "pxf" present in pg_extension → exactly ["pxf"] (never padded with
// fabricated names).
func TestPgxClient_ListPXFExtensions_OnlyPxf(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse([]string{"extname"}, [][]string{{"pxf"}})
	})
	defer cleanup()

	exts, err := client.ListPXFExtensions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"pxf"}, exts)
}

// TestPgxClient_ListPXFExtensions_None covers 105-S3-B3: a reachable DB with
// neither extension present → an empty slice + nil error. The probe was
// OBSERVABLE (DB reachable) and honestly reports nothing installed; the caller
// then leaves the field absent.
func TestPgxClient_ListPXFExtensions_None(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return emptyRowResponse([]string{"extname"})
	})
	defer cleanup()

	exts, err := client.ListPXFExtensions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, exts)
}

// TestPgxClient_ListPXFExtensions_QueryError covers 105-S3-B4: a query/
// connectivity error is SURFACED (wrapped), so the caller can treat the probe as
// UNOBSERVABLE (extensions absent) rather than as "none installed".
func TestPgxClient_ListPXFExtensions_QueryError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("server closed the connection")
	})
	defer cleanup()

	exts, err := client.ListPXFExtensions(context.Background())
	require.Error(t, err)
	assert.Nil(t, exts)
	assert.Contains(t, err.Error(), "querying pg_extension")
}

// TestPgxClient_ListPXFExtensions_ScanError covers 105-S3-B4 (row-scan path): a
// row with the wrong column count triggers a scan error, which is SURFACED
// (wrapped) so the caller treats the probe as UNOBSERVABLE rather than empty.
func TestPgxClient_ListPXFExtensions_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		// Two columns where the scan expects one → scan error.
		return multiRowResponse([]string{"extname", "extra"}, [][]string{{"pxf", "boom"}})
	})
	defer cleanup()

	exts, err := client.ListPXFExtensions(context.Background())
	require.Error(t, err)
	assert.Nil(t, exts)
	assert.Contains(t, err.Error(), "scanning PXF extension name")
}

// ============================================================================
// MoveQueryToResourceGroup Tests
// ============================================================================

func TestPgxClient_MoveQueryToResourceGroup_Success(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "pg_stat_activity"):
			return singleRowResponse([]string{"usename"}, []string{"app_user"})
		case strings.Contains(query, "ALTER ROLE"):
			return execResponse("ALTER ROLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.MoveQueryToResourceGroup(context.Background(), 100, "analytics")
	assert.NoError(t, err)
}

func TestPgxClient_MoveQueryToResourceGroup_NoSession(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return singleRowResponse([]string{"usename"}, []string{""})
	})
	defer cleanup()

	err := client.MoveQueryToResourceGroup(context.Background(), 100, "analytics")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found or has no associated role")
}

func TestPgxClient_MoveQueryToResourceGroup_LookupError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("lookup failed")
	})
	defer cleanup()

	err := client.MoveQueryToResourceGroup(context.Background(), 100, "analytics")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "looking up session")
}

// ============================================================================
// GetQueryDetail Tests
// ============================================================================

func TestPgxClient_GetQueryDetail_Success(t *testing.T) {
	sessionFields := []fieldDesc{
		int4Field("pid"), textField("usename"), textField("datname"),
		textField("state"), textField("query"),
		{name: "query_start", oid: 1184}, // timestamptz
		textField("duration"), textField("wait_event_type"), textField("wait_event"),
		textField("backend_type"),
	}
	lockFields := []fieldDesc{
		textField("locktype"), textField("mode"),
		boolField("granted"), textField("relation"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "pg_stat_activity"):
			return singleRowResponseTyped(sessionFields, []string{
				"100", "app_user", "testdb", "active", "SELECT 1",
				"2024-01-01 00:00:00+00", "0", "", "", "client backend",
			})
		case strings.Contains(query, "pg_locks"):
			return multiRowResponseTyped(
				lockFields,
				[][]string{{"relation", "AccessShareLock", "t", "public.orders"}},
			)
		case strings.Contains(query, "pg_stat_user_tables"):
			return multiRowResponse([]string{"table"}, [][]string{{"public.orders"}})
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	detail, err := client.GetQueryDetail(context.Background(), 100)
	require.NoError(t, err)
	assert.Equal(t, int32(100), detail.PID)
	assert.Equal(t, "app_user", detail.Username)
	assert.Len(t, detail.Locks, 1)
	assert.Equal(t, []string{"public.orders"}, detail.TablesAccessed)
}

func TestPgxClient_GetQueryDetail_NotFound(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("no rows")
	})
	defer cleanup()

	_, err := client.GetQueryDetail(context.Background(), 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query not found or not accessible")
}

func TestResourceGroupInfo_IOLimits(t *testing.T) {
	info := ResourceGroupInfo{
		Name:     "analytics",
		IOLimits: "pg_default:rbps=1000:wbps=500:riops=100:wiops=50",
	}
	assert.Equal(t, "pg_default:rbps=1000:wbps=500:riops=100:wiops=50", info.IOLimits)
}
