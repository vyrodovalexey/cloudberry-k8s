package db

import (
	"context"
	"log/slog"
	"net"
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
		int4Field("pid"), textField("usename"), textField("application_name"),
		textField("client_addr"), textField("state"), textField("query"),
		{name: "query_start", oid: 1184}, // timestamptz
		textField("duration"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(sessionFields, [][]string{
			{"123", "admin", "psql", "10.0.0.1", "active", "SELECT 1", "2025-01-01 00:00:00+00", "00:01:30"},
		})
	})
	defer cleanup()

	sessions, err := client.ListSessions(context.Background())
	assert.NoError(t, err)
	require.Len(t, sessions, 1)
	assert.Equal(t, int32(123), sessions[0].PID)
	assert.Equal(t, "admin", sessions[0].Username)
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

	recs, err := client.GetBloatRecommendations(context.Background())
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

	_, err := client.GetBloatRecommendations(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying bloat recommendations")
}

func TestPgxClient_GetSkewRecommendations_Mock(t *testing.T) {
	skewFields := []fieldDesc{textField("schemaname"), textField("relname"), int8Field("n_live_tup")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(skewFields, [][]string{
			{"public", "large_table", "5000000"},
		})
	})
	defer cleanup()

	recs, err := client.GetSkewRecommendations(context.Background())
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "skew", recs[0].Type)
	assert.Equal(t, severityInfo, recs[0].Severity)
}

func TestPgxClient_GetSkewRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("skew query failed")
	})
	defer cleanup()

	_, err := client.GetSkewRecommendations(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying skew recommendations")
}

func TestPgxClient_GetAgeRecommendations_Mock(t *testing.T) {
	ageFields := []fieldDesc{textField("schemaname"), textField("relname"), int8Field("n_dead_tup"), int8Field("secs_since_vacuum")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(ageFields, [][]string{
			{"public", "old_table", "150000", "86400"},
		})
	})
	defer cleanup()

	recs, err := client.GetAgeRecommendations(context.Background())
	assert.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "age", recs[0].Type)
	assert.Equal(t, severityWarning, recs[0].Severity)
}

func TestPgxClient_GetAgeRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("age query failed")
	})
	defer cleanup()

	_, err := client.GetAgeRecommendations(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "querying age recommendations")
}

func TestPgxClient_GetIndexBloatRecommendations_Mock(t *testing.T) {
	idxFields := []fieldDesc{textField("schemaname"), textField("relname"), textField("indexrelname"), int8Field("index_size"), int8Field("idx_scan")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(idxFields, [][]string{
			{"public", "users", "users_pkey", "1048576", "0"},
			{"public", "events", "events_idx", "2097152", "100"},
		})
	})
	defer cleanup()

	recs, err := client.GetIndexBloatRecommendations(context.Background())
	assert.NoError(t, err)
	require.Len(t, recs, 2)
	assert.Equal(t, "index_bloat", recs[0].Type)
	assert.Equal(t, severityWarning, recs[0].Severity) // idx_scan == 0
	assert.Equal(t, severityInfo, recs[1].Severity)    // idx_scan > 0
}

func TestPgxClient_GetIndexBloatRecommendations_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("index bloat query failed")
	})
	defer cleanup()

	_, err := client.GetIndexBloatRecommendations(context.Background())
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
	rgFields := []fieldDesc{textField("rsgname"), int4Field("num_running"), int4Field("num_queueing"), float8Field("cpu_usage"), float8Field("memory_usage")}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponseTyped(rgFields, [][]string{
			{"default_group", "5", "2", "0.45", "0.30"},
		})
	})
	defer cleanup()

	groups, err := client.ListResourceGroups(context.Background())
	assert.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "default_group", groups[0].Name)
	assert.Equal(t, int32(7), groups[0].Concurrency) // numRunning + numQueueing
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

func TestPgxClient_DropResourceGroup_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("drop failed")
	})
	defer cleanup()

	err := client.DropResourceGroup(context.Background(), "busy_group")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dropping resource group")
}

func TestPgxClient_GetClusterState_Mock(t *testing.T) {
	callCount := 0
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		callCount++
		switch {
		case callCount == 1:
			// Ping
			return execResponse("SELECT 1")
		case callCount == 2:
			// SHOW server_version
			return singleRowResponse([]string{"server_version"}, []string{"14.0"})
		case callCount == 3:
			// Segment counts
			return singleRowResponseTyped(
				[]fieldDesc{int4Field("up"), int4Field("down"), int4Field("total")},
				[]string{"4", "0", "4"},
			)
		default:
			// Connection counts
			return singleRowResponseTyped(
				[]fieldDesc{int4Field("active"), int4Field("max_conn")},
				[]string{"10", "100"},
			)
		}
	})
	defer cleanup()

	state, err := client.GetClusterState(context.Background())
	assert.NoError(t, err)
	assert.True(t, state.IsUp)
	assert.Equal(t, "14.0", state.Version)
}
