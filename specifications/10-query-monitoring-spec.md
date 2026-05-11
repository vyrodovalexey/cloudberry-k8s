# Cloudberry Operator - Query Monitoring & Analysis Specification

**Version**: 1.1.0
**API Group**: avsoft.io

---

## 1. Overview

Query monitoring provides live query observation, plan inspection, history search, and on-demand plan analysis. The operator exposes query monitoring capabilities through the CRD configuration, REST API, and cloudberry-ctl CLI.

## 2. CRD Schema

### 2.1 QueryMonitoringSpec

```yaml
spec:
  queryMonitoring:
    enabled: true
    historyRetention: "30d"
    samplingInterval: 5
    guestAccess: false
    planCollection: true
    slowQueryThreshold: "1000ms"
```

### 2.2 Configuration Fields

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| enabled | bool | Enable query monitoring | false |
| historyRetention | string | History retention period | "30d" |
| samplingInterval | int32 | Sampling interval in seconds | 5 |
| guestAccess | bool | Allow unauthenticated read-only access | false |
| planCollection | bool | Collect visual query plans | true |
| slowQueryThreshold | string | Threshold for slow query logging | "1000ms" |

### 2.3 Status Fields

Added to CloudberryClusterStatus:
- `activeQueries` (int32) - Currently running queries
- `queuedQueries` (int32) - Queries waiting in queue
- `blockedQueries` (int32) - Queries blocked by locks

## 3. Capabilities

### 3.1 Live Query Monitor
- List running, queued, and blocked queries
- Show query ID, status, user, database, workload, times, spill files, CPU
- Cancel running queries with reason message
- Reassign queries to different resource groups
- Filter by status (running/queued/blocked)
- Advanced search by time, database, resource group, user, tags

### 3.2 Query Details
- Execution metrics (CPU, memory, spill, disk I/O, locks)
- Real-time plan with progress animation
- Top slice metrics (CPU, memory, disk I/O)
- Inner queries of function calls
- Textual EXPLAIN plan
- Accessed tables list

### 3.3 Query History
- Browse completed queries with charts
- Advanced search with regex/wildcard
- Export to CSV
- Historical query details with plan

### 3.4 Plan Analysis
- Static plan checker for EXPLAIN ANALYZE output
- Performance issue identification

## 4. API Endpoints

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/queries | Operator Basic | List active queries |
| GET | /clusters/{name}/queries/active | Basic | Get active query count |
| GET | /clusters/{name}/queries/{qid} | Operator Basic | Get query details |
| POST | /clusters/{name}/queries/{qid}/cancel | Operator | Cancel query |
| POST | /clusters/{name}/queries/{qid}/move | Operator | Move query to resource group |
| GET | /clusters/{name}/queries/history | Operator Basic | Search query history |
| GET | /clusters/{name}/queries/history/{qid} | Operator Basic | Get historical query details |
| POST | /clusters/{name}/queries/history/export | Operator Basic | Export history to CSV |
| POST | /clusters/{name}/queries/plan-check | Basic | Run static plan checker |

## 5. CLI Commands

```bash
cloudberry-ctl queries list --cluster my-cluster
cloudberry-ctl queries list --cluster my-cluster --status running
cloudberry-ctl queries detail --cluster my-cluster --query-id 12345
cloudberry-ctl queries cancel --cluster my-cluster --query-id 12345 --reason "Too long"
cloudberry-ctl queries move --cluster my-cluster --query-id 12345 --target-group etl
cloudberry-ctl queries history --cluster my-cluster --last 24h
cloudberry-ctl queries history --cluster my-cluster --user analyst --database mydb
cloudberry-ctl queries plan-check --cluster my-cluster -f explain.txt
```

## 6. Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| cloudberry_queries_active | Gauge | Currently active queries |
| cloudberry_queries_queued | Gauge | Currently queued queries |
| cloudberry_queries_blocked | Gauge | Currently blocked queries |
| cloudberry_queries_slow_total | Counter | Slow queries exceeding threshold |
