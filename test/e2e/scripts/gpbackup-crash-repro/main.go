// gpbackup-crash-repro — TASK-U018 revalidation harness for the five gpbackup
// WORKAROUND crash patches carried in Dockerfile.cloudberry-backup (and the
// mirrored stage of Dockerfile.cloudberry-official).
//
// Purpose: dispatch each patch's crash-trigger SQL through the pgx v5 driver
// using QueryExecModeExec — the same extended-protocol exec mode the
// cloudberry-go-libs dbconn layer configures (default_query_exec_mode=exec)
// that the patched toolchain replaces with lib/pq — and record whether the
// Cloudberry segments crash (backend exit / "server closed the connection")
// or the statement executes cleanly.
//
// The five repros (source sites verified against
// apache-cloudberry-backup-2.2.0-incubating-src):
//
//	Patch 1  backup/queries_globals.go GetDBSize      — SELECT pg_size_pretty(pg_database_size(...))
//	Patch 2a backup/queries_relations.go LockTables    — LOCK TABLE ... IN ACCESS SHARE MODE (in the backup's REPEATABLE READ tx)
//	Patch 3a backup/queries_relations.go GetAllViews   — SAVEPOINT gpbackup_get_views
//	Patch 4  backup/data.go LockTableNoWait            — LOCK TABLE ... IN ACCESS SHARE MODE NOWAIT
//	Patch 5  backup/wrappers.go+snapshot.go            — pg_export_snapshot() + SET TRANSACTION SNAPSHOT, then a distributed query
//
// Each repro is followed by a canary distributed query (count over line_items,
// which fans out to both segments) so a delayed breakage — the Patch 5 symptom
// "crash on subsequent queries" — is also detected.
//
// Usage (run inside the coordinator pod; build for the pod's arch):
//
//	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o gpbackup-crash-repro .
//	kubectl cp gpbackup-crash-repro cloudberry-test/acceptance-test-coordinator-0:/tmp/
//	kubectl exec -n cloudberry-test acceptance-test-coordinator-0 -c cloudberry -- \
//	  /tmp/gpbackup-crash-repro -db mydb
//
// Output: one line per repro, "CLEAN" or "ERROR: <driver error>"; exit code 0
// when all repros are clean, 1 when any errored.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func connect(ctx context.Context, db string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=/tmp user=gpadmin database=%s", db))
	if err != nil {
		return nil, err
	}
	// cloudberry-go-libs dbconn.go sets default_query_exec_mode=exec on its
	// pgx DSNs; match it so the wire behavior under test is the same.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.ConnectTimeout = 30 * time.Second
	return pgx.ConnectConfig(ctx, cfg)
}

// canary runs a distributed query that fans out to all segments; returns the
// row count or the error that would surface a dead segment backend.
func canary(ctx context.Context, conn *pgx.Conn) error {
	var n int64
	return conn.QueryRow(ctx, "SELECT count(*) FROM line_items").Scan(&n)
}

func report(name string, err error) bool {
	if err != nil {
		fmt.Printf("%s ERROR: %v\n", name, err)
		return false
	}
	fmt.Printf("%s CLEAN\n", name)
	return true
}

func main() {
	db := flag.String("db", "mydb", "database to run the repros against")
	flag.Parse()
	ctx, contextCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer contextCancel()

	ok := true

	// R1 — Patch 1 trigger: GetDBSize's exact query.
	func() {
		conn, err := connect(ctx, *db)
		if err != nil {
			fmt.Printf("R1 CONNECT-ERROR: %v\n", err)
			ok = false
			return
		}
		defer conn.Close(ctx)
		var size string
		err = conn.QueryRow(ctx,
			"SELECT pg_size_pretty(pg_database_size('mydb')) as dbsize").Scan(&size)
		fmt.Printf("R1 (pg_database_size) result=%q\n", size)
		ok = report("R1", err) && ok
		ok = report("R1-canary", canary(ctx, conn)) && ok
	}()

	// R2 — Patch 2a trigger: LOCK TABLE in the backup's REPEATABLE READ tx.
	func() {
		conn, err := connect(ctx, *db)
		if err != nil {
			fmt.Printf("R2 CONNECT-ERROR: %v\n", err)
			ok = false
			return
		}
		defer conn.Close(ctx)
		tx, err := conn.Begin(ctx) // default isolation (read committed); see note below
		if err == nil {
			_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		}
		if err == nil {
			// Upstream LockTables batches table FQNs comma-separated:
			// LOCK TABLE <t1>, <t2>, ... IN ACCESS SHARE MODE
			_, err = tx.Exec(ctx,
				"LOCK TABLE public.customers, public.orders, public.line_items IN ACCESS SHARE MODE")
		}
		if rb := tx.Rollback(ctx); rb != nil && err == nil {
			err = rb
		}
		ok = report("R2 (LOCK TABLE ACCESS SHARE in REPEATABLE READ tx)", err) && ok
		ok = report("R2-canary", canary(ctx, conn)) && ok
	}()

	// R3 — Patch 3a trigger: the GetAllViews savepoint pair.
	func() {
		conn, err := connect(ctx, *db)
		if err != nil {
			fmt.Printf("R3 CONNECT-ERROR: %v\n", err)
			ok = false
			return
		}
		defer conn.Close(ctx)
		tx, err := conn.Begin(ctx)
		if err == nil {
			_, err = tx.Exec(ctx, "SAVEPOINT gpbackup_get_views")
		}
		if err == nil {
			_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT gpbackup_get_views")
		}
		if rb := tx.Rollback(ctx); rb != nil && err == nil {
			err = rb
		}
		ok = report("R3 (SAVEPOINT gpbackup_get_views)", err) && ok
		ok = report("R3-canary", canary(ctx, conn)) && ok
	}()

	// R4 — Patch 4 trigger: the per-table NOWAIT lock (data phase).
	func() {
		conn, err := connect(ctx, *db)
		if err != nil {
			fmt.Printf("R4 CONNECT-ERROR: %v\n", err)
			ok = false
			return
		}
		defer conn.Close(ctx)
		// Upstream LockTableNoWait runs inside the data-phase backup
		// transaction; wrap it so the statement is valid (25P01 otherwise).
		tx, terr := conn.Begin(ctx)
		if terr != nil {
			fmt.Printf("R4 BEGIN-ERROR: %v\n", terr)
			ok = false
			return
		}
		_, err = tx.Exec(ctx, "LOCK TABLE public.line_items IN ACCESS SHARE MODE NOWAIT")
		if rb := tx.Rollback(ctx); rb != nil && err == nil {
			err = rb
		}
		ok = report("R4 (LOCK TABLE ... NOWAIT in tx)", err) && ok
		ok = report("R4-canary", canary(ctx, conn)) && ok
	}()

	// R5 — Patch 5 trigger: synchronized snapshot export + import on a second
	// connection, then a distributed query on the importing connection.
	func() {
		connA, err := connect(ctx, *db)
		if err != nil {
			fmt.Printf("R5 CONNECT-ERROR: %v\n", err)
			ok = false
			return
		}
		defer connA.Close(ctx)
		// gpbackup's flow: the exporting connection is inside an open
		// REPEATABLE READ transaction when pg_export_snapshot() runs, and
		// stays open until every other connection has imported it.
		txA, terr := connA.Begin(ctx)
		if terr != nil {
			fmt.Printf("R5 BEGIN-ERROR: %v\n", terr)
			ok = false
			return
		}
		defer txA.Rollback(ctx)
		_, err = txA.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		var snap string
		if err == nil {
			err = txA.QueryRow(ctx, "SELECT pg_catalog.pg_export_snapshot()").Scan(&snap)
		}
		fmt.Printf("R5 exported snapshot=%q\n", snap)
		if err == nil {
			connB, cerr := connect(ctx, *db)
			if cerr != nil {
				fmt.Printf("R5B CONNECT-ERROR: %v\n", cerr)
				ok = false
				return
			}
			defer connB.Close(ctx)
			_, err = connB.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ")
			if err == nil {
				_, err = connB.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snap))
			}
			if err == nil {
				var n int64
				err = connB.QueryRow(ctx, "SELECT count(*) FROM line_items").Scan(&n)
			}
			connB.Exec(ctx, "ROLLBACK")
		}
		ok = report("R5 (pg_export_snapshot + SET TRANSACTION SNAPSHOT + distributed query)", err) && ok
		ok = report("R5-canary", func() error {
			conn, cerr := connect(ctx, *db)
			if cerr != nil {
				return cerr
			}
			defer conn.Close(ctx)
			return canary(ctx, conn)
		}()) && ok
	}()

	if !ok {
		os.Exit(1)
	}
}
