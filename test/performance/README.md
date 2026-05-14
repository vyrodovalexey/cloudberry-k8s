# Cloudberry Operator - Performance Tests

Performance and load testing suite for the Cloudberry K8s Operator REST API using [Yandex Tank](https://yandextank.readthedocs.io/).

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Test Scenarios](#test-scenarios)
- [Configuration](#configuration)
- [Ammo Files](#ammo-files)
- [Running Tests](#running-tests)
- [Interpreting Results](#interpreting-results)
- [SLO Targets](#slo-targets)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required

- **Docker** (recommended) or native Yandex Tank installation
- **Running Cloudberry Operator** with REST API accessible on port `:8090` (default)
- **bash** 4.0+ (for the runner script)
- **Go** 1.26.3+ (for building the operator from source)

### Docker (Recommended)

```bash
# Pull the Yandex Tank image
docker pull direvius/yandex-tank
```

### Native Installation (Alternative)

```bash
pip install yandextank
```

### Test Data (Scenario 7)

Performance tests that exercise data-dependent endpoints (skew analysis, rebalance, query monitoring under load) require the Scenario 7 test dataset. Load it before running these tests:

```bash
# From the project root
bash test/scenarios/scenario7_load_data.sh
```

This populates the `mydb` database with ~1,450,000 rows (~218 MB) across 5 tables:

| Table | Rows | Distribution |
|-------|------|-------------|
| `orders` | 1,000,000 | hash (`customer_id`), Pareto-skewed |
| `logs` | 200,000 | random |
| `customers` | 100,000 | hash (`id`) |
| `audit_log` | 100,000 | hash (`id`), excluded from rebalance |
| `temp_staging` | 50,000 | hash (`id`), `temp_*` exclusion pattern |

The Pareto skew (80% of orders to 20% of customers) provides a realistic workload for testing rebalance detection and query performance under uneven data distribution. See [docs/user-guide.md](../docs/user-guide.md#test-data-setup) for full details.

> **Note**: Scenario 7 depends on Scenarios 1–6 having been run first (it expects the `mydb` database, `customers` and `orders` tables, and the `analyst` role to exist).

### Operator Setup

The operator must be running and accessible. The REST API server listens on `:8090` by default (configurable via `CLOUDBERRY_API_ADDRESS` or `--api-address` flag).

For local testing:

```bash
# Build and run the operator (from project root)
make build-operator
./bin/cloudberry-operator

# Or via port-forward if running in Kubernetes
# Note: The operator Service only exposes metrics (8080) and health (8081) ports.
# The REST API (port 8090) must be port-forwarded directly from the pod:
kubectl port-forward -n cloudberry-system \
  $(kubectl get pod -n cloudberry-system -l app.kubernetes.io/name=cloudberry-operator -o jsonpath='{.items[0].metadata.name}') \
  8090:8090
```

Default authentication: `admin:admin` (Basic Auth, bcrypt-hashed). The API server uses bcrypt for password verification.

> **Important**: When running in Kubernetes, the `CLOUDBERRY_API_ADMIN_PASSWORD` environment variable must be set on the operator pod for the `admin:admin` credentials to work. If not set, the operator generates a random password logged at startup.

### Test Infrastructure Status

- All unit, functional, integration, and e2e tests pass
- Docker images build successfully (`make docker-build`)
- Helm chart deploys to local Kubernetes clusters
- Monitoring stack (vmagent, otel-collector) can be deployed alongside the operator
- Performance tests target the REST API server on port `:8090`
- Yandex Tank Docker images available: `direvius/yandex-tank`, `yandex/yandex-tank`
- Test dataset (~87MB) pre-generated in `data/` directory (customers + orders CSVs)
- 4 test scenarios configured: smoke, baseline, stress, endurance
- 3 ammo files ready: health, api-read, api-mixed

### Controller Test Coverage (Scenarios 1–4)

The operator's controller tests include four comprehensive scenarios that validate core functionality. These are separate from the performance tests but provide the functional foundation:

| Scenario | Name | Coverage |
|----------|------|----------|
| 1 | Full Cluster Bootstrap | Coordinator + standby + segments + mirrors, OIDC, Vault, webhook validation, ConfigMaps, Secrets, Services, StatefulSets, status fields, Prometheus metrics, structured logging |
| 2 | Configuration Hot-Reload and Rolling Restart | Reload-safe vs restart-required parameter classification, ConfigMap updates, rolling restart state machine (mirrors → primaries → standby → coordinator), status conditions, events, metrics |
| 3 | Stop / Start Modes | Smart/fast/immediate stop, normal/restricted/maintenance start, restart, scale-down/up ordering, phase transitions (Running ↔ Stopping ↔ Stopped ↔ Restricted/Maintenance), events |
| 4 | Maintenance Operations | `BuildMaintenanceJob` builder method, Job creation for vacuum/analyze/reindex, Job properties (BackoffLimit, TTL, RestartPolicy), PGPASSWORD from Secret, unknown operation handling, events |

Run the controller tests:

```bash
go test ./internal/controller/... -v
```

> **Note**: The API enforces per-IP rate limiting (10 requests/minute by default). For performance testing, you may need to increase the rate limit or disable it by configuring a higher limit.

## Quick Start

```bash
cd test/performance

# 1. Run a smoke test (quick validation)
./run-perftest.sh --scenario smoke

# 2. Run baseline performance test
./run-perftest.sh --scenario baseline

# 3. Dry run (validate config without executing)
./run-perftest.sh --scenario baseline --dry-run

# 4. Analyze previous results
./run-perftest.sh --analyze-only
```

## Test Scenarios

### Smoke Test (`smoke`)

Quick validation that the API responds correctly under minimal load.

| Parameter | Value |
|-----------|-------|
| Load | 10 RPS constant |
| Duration | ~1.5 minutes |
| Ammo | `ammo/health.txt` (health endpoints) |
| Expected | All responses < 1s, 0% errors |

```bash
./run-perftest.sh --scenario smoke
```

### Baseline Test (`baseline`)

Establishes reference performance metrics. Run this before and after changes.

| Parameter | Value |
|-----------|-------|
| Load | Ramp to 100 RPS, hold 5 minutes |
| Duration | ~7 minutes |
| Ammo | `ammo/api-read.txt` (read-only API) |
| Expected | p95 < 200ms, p99 < 500ms, errors < 0.1% |

```bash
./run-perftest.sh --scenario baseline
```

### Stress Test (`stress`)

Finds the breaking point by ramping load from 10 to 1000 RPS in steps.

| Parameter | Value |
|-----------|-------|
| Load | Step: 10 → 50 → 100 → 200 → 500 → 1000 RPS |
| Duration | ~15 minutes |
| Ammo | `ammo/api-read.txt` (read-only API) |
| Expected | Identify max sustainable throughput |

```bash
./run-perftest.sh --scenario stress
```

### Endurance Test (`endurance`)

Detects memory leaks, connection exhaustion, and time-dependent degradation.

| Parameter | Value |
|-----------|-------|
| Load | 50 RPS constant |
| Duration | ~32 minutes |
| Ammo | `ammo/api-read.txt` (read-only API) |
| Expected | Stable latency, no memory growth |

```bash
./run-perftest.sh --scenario endurance
```

## Configuration

### Runner Options

```
./run-perftest.sh [OPTIONS]

Options:
  --scenario <name>    Test scenario: smoke, baseline, stress, endurance
  --target <host:port> Target address (default: localhost:8090)
  --ssl                Enable SSL/TLS
  --rps <number>       Override max RPS
  --duration <seconds> Override test duration
  --ammo <file>        Override ammo file path
  --docker             Run via Docker (default)
  --native             Run via native yandex-tank
  --analyze-only       Only analyze existing results
  --dry-run            Validate config without running
  --help               Show help
```

### Custom Target

```bash
# Test against a remote operator
./run-perftest.sh --scenario baseline --target operator.example.com:8090 --ssl

# Test with custom RPS
./run-perftest.sh --scenario baseline --rps 200 --duration 600
```

### Direct Yandex Tank Usage

```bash
# Using Docker directly
docker run --rm \
  -v $(pwd):/var/loadtest \
  -v $(pwd)/.yandextank:/tmp/artifacts \
  --net host \
  direvius/yandex-tank -c scenarios/baseline.yaml

# Using native installation
yandex-tank -c scenarios/baseline.yaml
```

### Configuration Files

| File | Description |
|------|-------------|
| `load.yaml` | Default/custom configuration |
| `scenarios/smoke.yaml` | Smoke test (10 RPS, 1 min) |
| `scenarios/baseline.yaml` | Baseline test (100 RPS, 5 min) |
| `scenarios/stress.yaml` | Stress test (ramp to 1000 RPS) |
| `scenarios/endurance.yaml` | Endurance test (50 RPS, 30 min) |

### Key Configuration Parameters

```yaml
phantom:
  address: localhost:8090    # Target host:port
  ssl: false                 # Enable HTTPS
  ammo_type: uri             # uri (simple) or request (full HTTP)
  ammofile: ammo/api-read.txt
  load_profile:
    load_type: rps
    schedule: line(1, 100, 60s) const(100, 300s)
  timeout: 3000              # Request timeout (ms)
  instances: 100             # Max concurrent connections
```

### Autostop Conditions

Safety conditions that automatically stop the test:

```yaml
autostop:
  autostop:
    - time(2000, 30s)        # Avg response > 2s for 30s
    - http(5xx, 10%, 15s)    # 5xx rate > 10% for 15s
    - http(4xx, 50%, 15s)    # 4xx rate > 50% for 15s
    - net(xx, 5%, 10s)       # Network errors > 5% for 10s
    - quantile(99, 5000, 30s) # p99 > 5s for 30s
```

## Ammo Files

### URI-Style (`ammo/health.txt`, `ammo/api-read.txt`)

Simple format for GET requests. Headers are defined once at the top:

```
[Host: localhost:8090]
[Authorization: Basic YWRtaW46YWRtaW4=]
/healthz
/readyz
/api/v1alpha1/clusters
```

### Request-Style (`ammo/api-mixed.txt`)

Full HTTP request format for mixed methods (GET, POST, PUT, DELETE).
Each request is prefixed with its byte count:

```
160 /api/v1alpha1/clusters
GET /api/v1alpha1/clusters HTTP/1.1
Host: localhost:8090
Authorization: Basic YWRtaW46YWRtaW4=
...
```

### Available Ammo Files

| File | Description | Endpoints |
|------|-------------|-----------|
| `ammo/health.txt` | Health check endpoints | `/healthz`, `/readyz` |
| `ammo/api-read.txt` | Read-only API endpoints | clusters, status, segments, sessions, config |
| `ammo/api-mixed.txt` | Mixed read/write operations | GET + POST (start, stop) |

### Custom Ammo

To create custom ammo for specific endpoints:

```bash
# URI-style (GET only)
cat > ammo/custom.txt << 'EOF'
[Host: localhost:8090]
[Authorization: Basic YWRtaW46YWRtaW4=]
[Accept: application/json]
/api/v1alpha1/clusters/my-cluster/status
/api/v1alpha1/clusters/my-cluster/segments
EOF

# Use with runner
./run-perftest.sh --scenario baseline --ammo ammo/custom.txt
```

## Interpreting Results

### Output Location

Results are stored in `.yandextank/<timestamp>/`:

```
.yandextank/
└── 20260511_143022/
    ├── phout.txt              # Raw request/response data
    ├── results_summary.json   # Parsed summary (JSON)
    ├── runtime_config.yaml    # Config used for this run
    ├── test_start             # Test start timestamp
    ├── test_end               # Test end timestamp
    └── scenario               # Scenario name
```

### Results Summary

The runner automatically generates:

1. **Request Statistics** - Total requests, status code distribution, error rate
2. **Latency Statistics** - Min, avg, max, p50, p95, p99 in milliseconds
3. **SLO Evaluation** - Pass/fail against scenario-specific thresholds
4. **Latency Distribution Chart** - ASCII histogram of response times
5. **RPS Over Time Chart** - ASCII chart of actual throughput
6. **HTTP Status Distribution** - Color-coded status codes over time

### phout.txt Format

Tab-separated values per request:

```
timestamp  tag  response_time  connect_time  send_time  latency  receive_time  interval_event  size_out  size_in  http_code  net_code
```

Times are in microseconds. Use for custom analysis:

```bash
# Average response time
awk -F'\t' '{sum+=$3; n++} END {print sum/n/1000 " ms"}' .yandextank/*/phout.txt

# Count by HTTP status
awk -F'\t' '{codes[$11]++} END {for(c in codes) print c, codes[c]}' .yandextank/*/phout.txt

# Requests per second
awk -F'\t' '{sec=int($1); rps[sec]++} END {for(s in rps) print s, rps[s]}' .yandextank/*/phout.txt | sort -n
```

### JSON Summary

```json
{
  "scenario": "baseline",
  "timestamp": "20260511_143022",
  "target": "localhost:8090",
  "total_requests": 35000,
  "status_codes": {
    "2xx": 34990,
    "4xx": 5,
    "5xx": 3,
    "other": 2
  },
  "error_rate_percent": 0.01,
  "latency_ms": {
    "min": 0.45,
    "avg": 12.30,
    "max": 450.20,
    "p50": 8.50,
    "p95": 35.00,
    "p99": 120.00
  }
}
```

## SLO Targets

### Smoke Test

| Metric | Target |
|--------|--------|
| p95 Latency | < 1000ms |
| Error Rate | 0% |

### Baseline Test

| Metric | Target |
|--------|--------|
| p50 Latency | < 50ms |
| p95 Latency | < 200ms |
| p99 Latency | < 500ms |
| Error Rate | < 0.1% |
| Availability | >= 99.9% |

### Endurance Test

| Metric | Target |
|--------|--------|
| p95 Latency | < 200ms |
| p99 Latency | < 500ms |
| Error Rate | < 0.5% |
| Memory Growth | None (stable) |

### Stress Test

No fixed SLOs. The goal is to identify:
- Maximum sustainable RPS
- RPS threshold where latency spikes
- RPS threshold where errors begin
- Resource bottlenecks (CPU, memory, connections)

## Best Practices

1. **Start with smoke**: Always run smoke first to validate setup
2. **Establish baseline**: Run baseline 3+ times for consistent reference
3. **Warmup phase**: All scenarios include warmup ramps
4. **Isolate variables**: Change one thing at a time between test runs
5. **Monitor target**: Watch operator CPU, memory, goroutines during tests
6. **Check autostop**: If a test stops early, check autostop conditions
7. **Multiple runs**: Run each scenario at least 3 times for reliability
8. **Clean state**: Restart the operator between stress/endurance tests

## Troubleshooting

### Test fails to connect

```bash
# Verify the operator is running
curl -u admin:admin http://localhost:8090/healthz

# Check if port is open
nc -z localhost 8090
```

### Docker networking issues

```bash
# Use --net host for Docker (Linux only)
docker run --rm --net host -v $(pwd):/var/loadtest direvius/yandex-tank -c scenarios/smoke.yaml

# On macOS, --net host does NOT work. Instead:
# 1. Use host.docker.internal instead of localhost in configs and ammo files
# 2. Or run yandex-tank natively (pip install yandextank) instead of Docker
# 3. Or use the --native flag: ./run-perftest.sh --scenario smoke --native
```

### High error rate

- Check operator logs for errors
- Verify authentication credentials
- Ensure the cluster resource exists (for cluster-specific endpoints)
- Check if autostop triggered (look for autostop messages in output)

### Permission denied on run-perftest.sh

```bash
chmod +x run-perftest.sh
```

### No phout.txt in results

- Check Docker volume mounts
- Ensure `.yandextank/` directory exists and is writable
- Check Yandex Tank output for configuration errors

## API Endpoints Under Test

| Endpoint | Method | Auth Level | Ammo File |
|----------|--------|------------|-----------|
| `/healthz` | GET | None | health.txt |
| `/readyz` | GET | None | health.txt |
| `/api/v1alpha1/clusters` | GET | Basic | api-read.txt |
| `/api/v1alpha1/clusters/{name}` | GET | Basic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/status` | GET | Basic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/segments` | GET | Basic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/sessions` | GET | OperatorBasic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/config` | GET | OperatorBasic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/mirroring` | GET | Basic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/standby` | GET | Basic | api-read.txt |
| `/api/v1alpha1/clusters/{name}/start` | POST | Operator | api-mixed.txt |
| `/api/v1alpha1/clusters/{name}/stop` | POST | Operator | api-mixed.txt |
