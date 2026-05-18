# Cloudberry Operator - Workload Management Specification

**Version**: 1.2.0
**API Group**: avsoft.io

---

## 1. Overview

Workload management provides resource governance for the Cloudberry cluster through resource groups, workload rules, query tags, and idle-session control. All capabilities are managed declaratively via the CloudberryCluster CRD and imperatively via the cloudberry-ctl CLI and operator REST API.

## 2. CRD Schema

### 2.1 WorkloadSpec

```yaml
spec:
  workload:
    enabled: true
    resourceGroups:
      - name: analytics
        concurrency: 10
        cpuMaxPercent: 50
        cpuWeight: 100
        memoryLimit: 30
        minCost: 500
      - name: etl
        concurrency: 5
        cpuMaxPercent: 30
        cpuWeight: 50
        memoryLimit: 20
        minCost: 0
    rules:
      - name: cancel-long-queries
        enabled: true
        resourceGroup: analytics
        action: cancel
        thresholdType: running_time
        threshold: "3600"
        priority: 1
      - name: move-heavy-queries
        enabled: true
        queryTag: heavy
        action: move
        moveTarget: etl
        thresholdType: spill_size
        threshold: "1073741824"
        priority: 2
    idleRules:
      - name: terminate-idle-analytics
        enabled: true
        resourceGroup: analytics
        idleTimeout: "30m"
        excludeInTransaction: true
        terminateMessage: "Session terminated due to inactivity"
```

### 2.2 Resource Group Attributes

| Attribute | Type | Description | Default |
|-----------|------|-------------|---------|
| name | string | Resource group name (required) | - |
| concurrency | int32 | Max concurrent transactions | 20 |
| cpuMaxPercent | int32 | Maximum CPU percentage (1-100) | 100 |
| cpuWeight | int32 | Relative CPU weight | 100 |
| memoryLimit | int32 | Memory limit as percentage of total | 0 (unlimited) |
| minCost | int32 | Minimum query cost to be managed | 0 |

### 2.3 Workload Rule Actions

| Action | Description |
|--------|-------------|
| cancel | Cancel the matching query |
| move | Move the query to another resource group |
| log | Log the event without taking action |

### 2.4 Threshold Types

| Type | Description |
|------|-------------|
| cpu_skew | CPU skew ratio |
| cpu_time | Total CPU time in seconds |
| running_time | Query running time in seconds |
| spill_size | Spill file size in bytes |
| planner_cost | Estimated planner cost |
| disk_io | Total disk I/O in bytes |
| slice_count | Number of slices |

## 3. Operator Reconciliation

When `spec.workload.enabled` is true, the Admin Controller's `reconcileWorkload()` method performs the following steps:

### 3.1 Resource Group Diff Algorithm

The operator diffs desired (CRD spec) vs actual (database) resource groups:

1. **List actual groups**: Calls `dbClient.ListResourceGroups()` to retrieve all resource groups currently in the database
2. **Build lookup maps**: Creates maps of desired groups (from `spec.workload.resourceGroups`) and actual groups (from the database) keyed by group name
3. **Create missing groups**: For each desired group not present in the database, executes `CREATE RESOURCE GROUP` via `dbClient.CreateResourceGroup()` with the full set of attributes (concurrency, cpuMaxPercent, cpuWeight, memoryLimit, minCost)
4. **Alter changed groups**: For each desired group that exists in the database but has different parameters, executes `ALTER RESOURCE GROUP` via `dbClient.AlterResourceGroup()` with the updated attributes
5. **Drop orphaned groups**: For each group in the database that is not in the desired spec, executes `DROP RESOURCE GROUP` via `dbClient.DropResourceGroup()`

The reconciliation is idempotent: running it multiple times with the same spec produces no additional changes. Resource groups that already match the desired state are left untouched.

### 3.2 ConfigMap Storage for Rules

Workload rules and idle session rules are serialized to JSON and stored in a ConfigMap named `{cluster}-workload-rules` in the cluster namespace:

- **`rules.json`**: Contains the serialized `[]WorkloadRule` array from `spec.workload.rules`. Each rule includes name, enabled, resourceGroup, action, thresholdType, threshold, priority, queryTag, and moveTarget fields
- **`idle-rules.json`**: Contains the serialized `[]IdleSessionRule` array from `spec.workload.idleRules`. Each rule includes name, enabled, resourceGroup, idleTimeout, excludeInTransaction, and terminateMessage fields

The ConfigMap has:
- Owner references to the `CloudberryCluster` resource for garbage collection
- Labels: `app.kubernetes.io/managed-by=cloudberry-operator`, `app.kubernetes.io/component=workload-rules`, `app.kubernetes.io/instance={cluster}`

### 3.3 Graceful Fallback

The operator handles DB unavailability gracefully:

- **`dbFactory` is nil** (unit tests): Falls back to condition-only mode — sets the `WorkloadConfigured` condition and emits events, but does not perform any DB operations
- **DB client creation fails** (coordinator is down): Sets `WorkloadConfigured=False` with reason `DBUnavailable` and includes the error details in the condition message. The overall reconciliation does not fail — it continues with other reconciliation steps

### 3.4 Metrics Update

After successful resource group reconciliation, the operator queries actual CPU and memory usage for each resource group via `dbClient.GetResourceGroupUsage()` and updates Prometheus metrics:

- `cloudberry_resource_group_cpu_usage` — CPU usage per resource group
- `cloudberry_resource_group_memory_usage` — Memory usage per resource group

### 3.5 Condition Updates

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `WorkloadConfigured` | `True` | `WorkloadReconciled` | All resource groups, workload rules, and idle rules reconciled successfully |
| `WorkloadConfigured` | `False` | `DBUnavailable` | Database connection unavailable — resource groups not reconciled |

## 4. API Endpoints

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/workload | Operator Basic | Get workload config |
| GET | /clusters/{name}/workload/resource-groups | Basic | List resource groups |
| POST | /clusters/{name}/workload/resource-groups | Operator | Create resource group |
| PUT | /clusters/{name}/workload/resource-groups/{rg} | Operator | Update resource group |
| DELETE | /clusters/{name}/workload/resource-groups/{rg} | Admin | Delete resource group |
| GET | /clusters/{name}/workload/rules | Operator Basic | List workload rules |
| POST | /clusters/{name}/workload/rules | Operator | Create rule |
| PUT | /clusters/{name}/workload/rules/{rule} | Operator | Update rule |
| DELETE | /clusters/{name}/workload/rules/{rule} | Operator | Delete rule |

## 5. CLI Commands

```bash
cloudberry-ctl workload status --cluster my-cluster
cloudberry-ctl workload resource-groups list --cluster my-cluster
cloudberry-ctl workload resource-groups create --cluster my-cluster --name analytics --concurrency 10
cloudberry-ctl workload rules list --cluster my-cluster
cloudberry-ctl workload rules create --cluster my-cluster --name cancel-long -f rule.yaml
cloudberry-ctl workload rules import --cluster my-cluster -f rules.yaml
cloudberry-ctl workload rules export --cluster my-cluster -o rules.yaml
```

## 6. Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| cloudberry_workload_rule_actions_total | Counter | Workload rule action count by action type |
| cloudberry_resource_group_cpu_usage | Gauge | CPU usage per resource group |
| cloudberry_resource_group_memory_usage | Gauge | Memory usage per resource group |
| cloudberry_idle_session_terminations_total | Counter | Idle session terminations |

## 7. Per-Tablespace I/O Limits

Resource groups support per-tablespace disk I/O limits:

```yaml
resourceGroups:
  - name: analytics
    ioLimits:
      - tablespace: "*"
        readBytesPerSec: 104857600
        writeBytesPerSec: 52428800
        readIOPS: 1000
        writeIOPS: 500
```

Applied via `ALTER RESOURCE GROUP ... SET io_limit ...`.

## 8. Query Tags

Query tags enable workload routing based on user-defined session tags:

```yaml
rules:
  - name: route-by-tag
    queryTag: "etl-batch"
    action: move
    moveTarget: etl
```

Tags are set per-session via `SET gp_query_tag = 'etl-batch'`.

## 9. Rule Import/Export

Workload rules can be imported and exported as YAML:

```bash
cloudberry-ctl workload rules export --cluster my-cluster -o rules.yaml
cloudberry-ctl workload rules import --cluster my-cluster -f rules.yaml
```

The operator reconciles imported rules the same way as CRD-defined rules.

## 10. Rule Ordering

Rules are evaluated in priority order (lowest number first). The `priority` field
on each rule controls evaluation order. Rules with the same priority are evaluated
in the order they appear in the CRD spec.

## 11. Scenario 25 — Bootstrap Workload Management via CRD

Scenario 25 validates the full workload bootstrap flow using a mock DB client, verifying resource group CRUD operations, ConfigMap creation for workload/idle rules, metrics updates, and fallback behavior when the database is unavailable.

- **Resource group creation**: When the CRD spec contains resource groups that do not exist in the database, the operator creates them via `CREATE RESOURCE GROUP` with the correct attributes (concurrency, cpuMaxPercent, cpuWeight, memoryLimit, minCost)
- **Resource group update**: When a resource group exists in the database but has different parameters than the CRD spec, the operator alters it via `ALTER RESOURCE GROUP`
- **Resource group removal**: When a resource group exists in the database but is not in the CRD spec, the operator drops it via `DROP RESOURCE GROUP`
- **Workload rules ConfigMap**: Workload rules from `spec.workload.rules` are serialized to JSON and stored in `{cluster}-workload-rules` ConfigMap under the `rules.json` key
- **Idle session rules ConfigMap**: Idle session rules from `spec.workload.idleRules` are serialized to JSON and stored in the same ConfigMap under the `idle-rules.json` key
- **Full bootstrap**: All components (resource groups, workload rules, idle rules, metrics) are reconciled in a single pass with `WorkloadConfigured=True/WorkloadReconciled` condition
- **DB unavailable fallback**: When the DB client factory returns an error, the operator sets `WorkloadConfigured=False/DBUnavailable` without failing the overall reconciliation

### Test Cases

| Test | What It Verifies |
|------|-----------------|
| `TestScenario25a_BootstrapResourceGroups_CreatesInDB` | Two resource groups (analytics, etl) created with correct parameters; `WorkloadConfigured=True/WorkloadReconciled` condition set |
| `TestScenario25b_BootstrapWorkloadRules_CreatesConfigMap` | ConfigMap created with `rules.json` containing 2 workload rules (cancel, move) with correct action, thresholdType, threshold, priority |
| `TestScenario25c_BootstrapIdleRules_CreatesConfigMap` | ConfigMap created with `idle-rules.json` containing 1 idle session rule with correct idleTimeout, excludeInTransaction, terminateMessage |
| `TestScenario25d_FullBootstrap_AllComponents` | Full bootstrap: 2 resource groups created, ConfigMap with both `rules.json` and `idle-rules.json`, `WorkloadConfigured=True` condition |
| `TestScenario25e_ResourceGroupUpdate_AltersInDB` | Existing resource group with different parameters triggers `ALTER RESOURCE GROUP` (not CREATE); parameters updated correctly |
| `TestScenario25f_ResourceGroupRemoval_DropsFromDB` | Orphaned resource group (in DB but not in spec) triggers `DROP RESOURCE GROUP`; matching groups are not altered or created |
| `TestScenario25g_DBUnavailable_FallsBackToConditionOnly` | DB factory error → `WorkloadConfigured=False/DBUnavailable` with error message; reconciliation succeeds without error |

### E2E Tests (Real Cloudberry Cluster)

In addition to the mock-based functional tests, Scenario 25 includes E2E tests that run against a real Apache Cloudberry 2.1.0 cluster deployed in Kubernetes. These tests verify that the operator's SQL DDL operations actually work against the Cloudberry database engine.

**Prerequisites:**
- Apache Cloudberry 2.1.0 cluster deployed via `test/examples/scenario1-cluster.yaml`
- Operator deployed with webhooks and vault-pki via Helm chart
- `pg_resgroup` and `pg_resgroupcapability` catalogs available

**Cloudberry 2.1.0 Compatibility Notes:**
- `gp_toolkit.gp_resgroup_config` view does not exist in this version; verification uses `pg_resgroupcapability` directly
- `memory_limit` and `min_cost` parameters are not settable via `CREATE RESOURCE GROUP` in Cloudberry 2.1.0; the database returns `-1` (default) for these fields
- Resource group operations work even when `gp_resource_manager` is not set to `group` (catalog operations succeed, enforcement is disabled)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario25a_ResourceGroups_CreatedInRealDB` | Creates analytics and etl resource groups in real Cloudberry DB via `db.Client.CreateResourceGroup()`; verifies parameters via `ListResourceGroups()` |
| `TestScenario25b_FullBootstrap_ViaReconcilerWithRealDB` | Full reconciler bootstrap: AdminReconciler creates resource groups in real DB, ConfigMap with rules.json and idle-rules.json, WorkloadConfigured=True condition |
| `TestScenario25c_Idempotency_WithRealDB` | Pre-creates resource group, runs reconciler with matching spec, verifies no ALTER calls (true idempotency against real DB) |

**Running E2E tests:**
```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario25 -race -count=1 -v -timeout=5m
```

### Example CR

See `test/examples/scenario25-bootstrap-workload.yaml` for a complete example CR with resource groups, workload rules, and idle session rules.
