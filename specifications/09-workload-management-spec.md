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

## 12. Scenario 26 — Resource Group Default Values with Real Cloudberry Cluster

Scenario 26 validates that when a resource group is created with only the name specified, the mutating webhook and Cloudberry database apply correct default values.

### Default Values Flow

1. **User specifies** only `name: defaults-test` in the CRD
2. **Mutating webhook** sets: `concurrency=20`, `cpuMaxPercent=100`, `cpuWeight=100`
3. **memoryLimit** and **minCost** remain `0` (Go zero values — webhook does not touch them)
4. **Operator's CreateResourceGroup** omits parameters with value `0` from the SQL
5. **Cloudberry DB** applies its own defaults: `memory_limit=-1` (unlimited), `min_cost=-1`

### Expected Values After Creation

| Attribute | Webhook Default | DB Value | Notes |
|-----------|----------------|----------|-------|
| concurrency | 20 | 20 | Set by webhook, passed to CREATE SQL |
| cpuMaxPercent | 100 | 100 | Set by webhook, passed to CREATE SQL |
| cpuWeight | 100 | 100 | Set by webhook, passed to CREATE SQL |
| memoryLimit | 0 (unchanged) | -1 | Omitted from SQL; Cloudberry default = -1 (unlimited) |
| minCost | 0 (unchanged) | -1 | Omitted from SQL; Cloudberry default = -1 |

### Functional Test Cases (Mock DB)

| Test | What It Verifies |
|------|-----------------|
| `TestScenario26a_DefaultsApplied_NameOnly` | Webhook defaults applied, resource group created in mock DB with correct parameters, WorkloadConfigured=True |
| `TestScenario26b_MutatingWebhook_SetsDefaults` | Webhook sets concurrency=20, cpuMaxPercent=100, cpuWeight=100 on zero-value fields |
| `TestScenario26c_DefaultsVsExplicit_NoAlterNeeded` | No ALTER when DB values match webhook defaults |
| `TestScenario26d_DefaultsCreatedInDB_VerifySQL` | Verifies exact ResourceGroupOptions passed to CreateResourceGroup |
| `TestScenario26e_DefaultsListedFromDB_MatchesSpec` | DB returns matching values → no create/alter/drop |

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario26a_DefaultsApplied_RealCluster` | Creates resource group with webhook defaults in real Cloudberry DB; verifies concurrency=20, cpuMaxPercent=100, cpuWeight=100, memoryLimit=-1, minCost=-1 |
| `TestScenario26b_DefaultsViaReconciler_RealCluster` | Full flow: CRD with name-only → webhook defaults → reconciler creates in real DB → verify DB values + WorkloadConfigured condition |
| `TestScenario26c_DefaultsIdempotent_NoAlterNeeded` | Pre-creates group with defaults, runs reconciler with matching spec, verifies zero ALTER calls |

### Running Tests

```bash
# Functional tests (mock DB):
go test ./test/functional/... -tags=functional -run TestScenario26 -race -count=1 -v

# E2E tests (real Cloudberry cluster):
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario26 -race -count=1 -v -timeout=5m
```

### Example CR

See `test/examples/scenario26-resource-group-defaults.yaml` for a minimal CR with a name-only resource group.

## 13. Scenario 27 — All Three Workload Rule Actions + Query Tags

Scenario 27 validates that the operator correctly configures all three workload rule actions (cancel, move, log), query tags, minCost filtering, and role-to-resource-group assignment against a real Cloudberry cluster.

### Cloudberry 2.1.0 Runtime Limitations

In Cloudberry 2.1.0, `gp_resource_manager` defaults to `queue` (not `group`). This means:
- Resource group **catalog operations** (CREATE/ALTER/DROP/LIST) work correctly
- Role assignment to resource groups (`ALTER ROLE ... RESOURCE GROUP ...`) works
- `pg_resgroup_move_query(pid, group_name)` function exists but requires `gp_resource_manager=group`
- `gp_query_tag` GUC parameter does **not exist** in Cloudberry 2.1.0
- Runtime enforcement of cancel/move/log actions requires a workload management daemon (not yet implemented in the operator)

The operator stores workload rules in a ConfigMap (`{cluster}-workload-rules`) for future enforcement. The E2E tests verify the operator's **configuration and reconciliation** rather than runtime behavior.

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario27a_CancelRuleConfiguration_WithRealDB` | Creates resource group + role in real DB, assigns role to group, reconciler stores cancel rule (action=cancel, thresholdType=running_time, threshold=3600) in ConfigMap, metrics API accepts action="cancel" |
| `TestScenario27b_MinCostFiltering_Configuration` | Creates resource group with minCost=500, reconciler stores rule referencing the group, verifies minCost is stored in DB catalog (enforcement requires gp_resource_manager=group) |
| `TestScenario27c_MoveRuleWithQueryTag_Configuration` | Creates analytics + etl groups, reconciler stores move rule with queryTag=heavy and moveTarget=etl in ConfigMap, verifies pg_resgroup_move_query function exists, metrics API accepts action="move" |
| `TestScenario27d_LogRuleConfiguration` | Creates resource group, reconciler stores cancel (priority=1) + log (priority=3) rules in ConfigMap with correct priority ordering, metrics API accepts all three actions (cancel/move/log) |

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario27 -race -count=1 -v -timeout=5m
```

## 14. Scenario 28 — All Remaining Threshold Types

Scenario 28 validates that the operator correctly stores workload rules for every threshold type defined in the specification (cpu_skew, cpu_time, planner_cost, disk_io, slice_count) in the ConfigMap, each with `action: log`. A final test (28f) verifies all 7 threshold types coexist in a single reconciliation.

### Threshold Types Tested

| Test | Threshold Type | Threshold Value | Priority | Description |
|------|---------------|-----------------|----------|-------------|
| 28a | cpu_skew | 0.5 | 10 | CPU skew ratio exceeding 0.5 |
| 28b | cpu_time | 60 | 11 | Cumulative CPU time exceeding 60 seconds |
| 28c | planner_cost | 100000 | 12 | Estimated planner cost ≥ 100,000 |
| 28d | disk_io | 536870912 | 13 | Total disk I/O exceeding 512 MB |
| 28e | slice_count | 100 | 14 | Query slice count exceeding 100 |
| 28f | all 7 types | various | 1–14 | All threshold types in a single reconciliation |

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

Each test (28a–28e) follows the same pattern:
1. Creates a resource group in the real Cloudberry DB
2. Builds a CloudberryCluster with a log rule for the specific threshold type
3. Runs the AdminReconciler with a real DB factory
4. Verifies the ConfigMap contains the rule with correct thresholdType, threshold, action, and priority
5. Verifies the resource group exists in the real DB
6. Verifies the WorkloadConfigured condition is True
7. Cleans up the resource group

Test 28f creates all 7 threshold types (including running_time and spill_size from earlier scenarios) in a single workload spec and verifies all are stored correctly in one ConfigMap.

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario28 -race -count=1 -v -timeout=5m
```

## 15. Scenario 29 — Resource Group Update via Reconciliation

Scenario 29 validates that when a resource group's parameters are changed in the CRD spec, the operator detects the diff and issues `ALTER RESOURCE GROUP` statements to update the real Cloudberry database.

### Bug Fix: ALTER RESOURCE GROUP SQL Syntax

During Scenario 29 implementation, a bug was discovered in `internal/db/client.go` `AlterResourceGroup()`: the parameter names (concurrency, cpu_max_percent, etc.) were being wrapped in double quotes by `pgx.Identifier{}.Sanitize()`, producing invalid SQL like `ALTER RESOURCE GROUP "grp" SET "concurrency" 20`. Cloudberry's ALTER RESOURCE GROUP syntax requires **unquoted** parameter names: `ALTER RESOURCE GROUP "grp" SET concurrency 20`. The fix removes the `pgx.Identifier` wrapper for parameter names since they are hardcoded constants, not user input.

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario29a_FullUpdateCycle_AlterInRealDB` | Creates group with initial values (concurrency=10, cpuMaxPercent=50), reconciles with matching spec (no ALTER), then reconciles with changed values (concurrency=20, cpuMaxPercent=70) — verifies ALTER is triggered and real DB reflects new values |
| `TestScenario29b_PartialUpdate_OnlyConcurrencyChanged` | Changes only concurrency (5→15), verifies ALTER is issued and unchanged parameters (cpuMaxPercent, cpuWeight) remain intact |
| `TestScenario29c_MultipleSequentialUpdates` | Applies 3 sequential updates, verifies each one is correctly applied to the real DB |
| `TestScenario29d_UpdateDoesNotAffectOtherGroups` | Updates analytics group while etl group is unchanged — verifies only analytics is altered, etl remains intact |

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario29 -race -count=1 -v -timeout=5m
```

## 16. Scenario 30 — Resource Group Utilization Monitoring and Metrics

Scenario 30 validates the operator's metrics pipeline for resource group CPU and memory utilization monitoring.

### Metrics Exposed

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_resource_group_cpu_usage` | Gauge | cluster, namespace, group | CPU usage percentage per resource group |
| `cloudberry_resource_group_memory_usage` | Gauge | cluster, namespace, group | Memory usage percentage per resource group |

### Metrics Pipeline

1. During reconciliation, the operator calls `dbClient.GetResourceGroupUsage(group)` for each resource group
2. The DB query reads from `gp_toolkit.gp_resgroup_status` (requires `gp_resource_manager=group`)
3. CPU and memory values are passed to `metrics.SetResourceGroupUsage(cluster, namespace, group, cpu, memory)`
4. The `PrometheusRecorder` sets the corresponding Prometheus gauges
5. If the DB query fails (e.g., `gp_resource_manager=queue`), the metrics update is silently skipped — the reconciliation does NOT fail

### Cloudberry 2.1.0 Note

In Cloudberry 2.1.0 with `gp_resource_manager=queue` (default), `gp_toolkit.gp_resgroup_status` does not exist. The `GetResourceGroupUsage` query fails, and the reconciler gracefully skips the metrics update. The `WorkloadConfigured` condition remains `True`.

### E2E Test Cases

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario30a_MetricsPipeline_RecordsUsageForBothGroups` | Creates analytics + etl groups in real DB, reconciler records CPU/memory usage for both groups via metrics recorder (using controlled usage values) |
| `TestScenario30b_MetricsChangeInResponseToLoad` | Runs reconciler twice with different usage values (low load → high load), verifies metrics values change between reconciliations (not static) |
| `TestScenario30c_PrometheusRecorder_GaugesSetCorrectly` | Creates a real PrometheusRecorder with a dedicated registry, sets usage values, verifies no panics or errors |
| `TestScenario30d_RealDBUsageQuery_GracefulDegradation` | Queries GetResourceGroupUsage against real Cloudberry DB (fails because gp_resgroup_status doesn't exist), verifies reconciler handles failure gracefully — WorkloadConfigured remains True |

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario30 -race -count=1 -v -timeout=5m
```

## 17. Scenario 31 — All REST API Endpoints + Permission Model

Scenario 31 validates all workload management REST API endpoints and the permission model.

### New API Endpoints Implemented

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| PUT | /clusters/{name}/workload/resource-groups/{rg} | Operator | Update resource group parameters via `AlterResourceGroup` |
| POST | /clusters/{name}/workload/rules | Operator | Create a new workload rule (patches CRD) |
| PUT | /clusters/{name}/workload/rules/{rule} | Operator | Update an existing workload rule (patches CRD) |
| DELETE | /clusters/{name}/workload/rules/{rule} | Operator | Delete a workload rule (patches CRD) |

### Permission Change

`DELETE /clusters/{name}/workload/resource-groups/{rg}` changed from `Operator` to `Admin` per specification.

### E2E Test Cases

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario31a_UpdateResourceGroup_ViaAPI` | PUT updates resource group in real DB, verifies new values |
| `TestScenario31b_CreateWorkloadRule_ViaAPI` | POST creates rule, visible in subsequent GET |
| `TestScenario31c_UpdateWorkloadRule_ViaAPI` | PUT updates rule threshold, verifies change |
| `TestScenario31d_DeleteWorkloadRule_ViaAPI` | DELETE removes rule, verifies removal |
| `TestScenario31e_PermissionModel_DeleteRequiresAdmin` | Operator-level denied for DELETE resource-groups, Admin allowed |
| `TestScenario31f_FullCRUDLifecycle_WorkloadRules` | Complete Create→Read→Update→Delete lifecycle |
| `TestScenario31g_GetEndpoints_ReturnCorrectData` | All GET endpoints return correct workload data |
| `TestScenario31h_ErrorCases_404ForMissingResources` | 404 for nonexistent clusters, rules |
| `TestScenario31i_Validation_InvalidIdentifiersRejected` | Invalid SQL identifiers rejected with 400 |

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario31 -race -count=1 -v -timeout=5m
```

## 18. Scenario 32 — All CLI Commands with Real Cloudberry Cluster

Scenario 32 validates all `cloudberry-ctl workload` CLI commands against a real Cloudberry cluster, including workload status, resource group CRUD, rule CRUD, bulk import/export, and round-trip verification.

### CLI Commands Tested

| Command | API Endpoint | Description |
|---------|-------------|-------------|
| `cloudberry-ctl workload status` | GET /workload | Show workload management status |
| `cloudberry-ctl workload resource-groups list` | GET /workload/resource-groups | List resource groups from DB |
| `cloudberry-ctl workload resource-groups create` | POST /workload/resource-groups | Create resource group in DB |
| `cloudberry-ctl workload rules list` | GET /workload/rules | List workload rules from CRD |
| `cloudberry-ctl workload rules create -f rule.yaml` | POST /workload/rules | Create rule from YAML file |
| `cloudberry-ctl workload rules import -f rules.yaml` | POST+PUT /workload/rules | Bulk import with upsert |
| `cloudberry-ctl workload rules export -O rules.yaml` | GET /workload/rules | Export rules to YAML file |

### Import/Export YAML Format

Single rule (`rule.yaml`):
```yaml
name: cli_test_rule
enabled: true
resourceGroup: analytics
action: cancel
thresholdType: running_time
threshold: "3600"
priority: 5
```

Multiple rules (`rules.yaml`):
```yaml
- name: import_rule_one
  enabled: true
  action: log
  thresholdType: cpu_time
  threshold: "120"
  priority: 10
- name: import_rule_two
  enabled: true
  action: cancel
  thresholdType: running_time
  threshold: "7200"
  priority: 1
```

### Import Upsert Semantics

The `rules import` command uses upsert semantics:
1. For each rule in the YAML file, attempt POST (create)
2. If the API returns `DUPLICATE_RULE` (HTTP 400), fall back to PUT (update)
3. Report summary: created count, updated count, failed count

### Round-Trip Verification

The `rules export` command writes all current rules to a YAML file. The round-trip test verifies:
1. Export rules to file
2. Clear all rules from the cluster
3. Import the exported file
4. Verify the rule set is identical to the original

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario32a_WorkloadStatus_CLI` | GET /workload returns enabled=true, correct resource group count, correct rule count after reconciliation |
| `TestScenario32b_ResourceGroupsList_CLI` | GET /workload/resource-groups returns groups from real DB with all attributes (concurrency, cpuMaxPercent, cpuWeight, memoryLimit, minCost) |
| `TestScenario32c_ResourceGroupsCreate_CLI` | POST creates group in real DB, verifies parameters, tests duplicate rejection |
| `TestScenario32d_RulesList_CLI` | GET /workload/rules returns all rules with names, actions, thresholds, priorities |
| `TestScenario32e_RulesCreate_FromFile` | Reads rule from YAML file, POSTs to API, verifies in list, tests --name override, tests DUPLICATE_RULE error |
| `TestScenario32f_RulesImport_Upsert` | Full upsert: POST creates new, PUT updates existing, verifies summary counts, tests idempotent re-import |
| `TestScenario32g_RulesExport_RoundTrip` | Export → verify file → round-trip (delete all → re-import → verify identical), tests empty export |

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario32 -race -count=1 -v -timeout=10m
```

## 19. Scenario 33 — Idle Session Rules with Real Cloudberry Cluster

### Overview

Scenario 33 validates the idle session enforcement daemon that periodically scans database sessions and terminates those exceeding configured idle timeouts per resource group. Unlike the mock-based unit tests in `internal/idle/daemon_test.go`, these E2E tests run against a real Apache Cloudberry 2.1.0 cluster and verify end-to-end behavior including actual session creation, idle detection, termination via `pg_terminate_backend()`, and metrics recording.

### Daemon Architecture

The idle session enforcement daemon lives in `internal/idle/` and provides the following capabilities:

- **Configurable scan interval**: The daemon scans sessions at a configurable interval (default 30s via `DefaultScanInterval`, reduced to 2s for E2E tests)
- **Thread-safe rule updates**: Rules are stored in a `[]IdleRule` slice protected by `sync.RWMutex`, allowing the operator to update rules at any time via `UpdateRules()` without stopping the daemon
- **Graceful shutdown**: The daemon's `Start(ctx)` method launches a background goroutine controlled by a derived context. `Stop()` cancels the context and waits on a `done` channel for the scan loop to exit cleanly
- **Session discovery**: Each scan cycle calls `ListSessionsWithResourceGroup()` on the DB client to retrieve all active sessions with their resource group assignment
- **Transaction awareness**: The daemon respects the `excludeInTransaction` flag by inspecting `pg_stat_activity.state` to determine whether a session is in a transaction

### Session State Detection

The daemon evaluates each session's `pg_stat_activity.state` to determine termination eligibility:

| Session State | Behavior |
|---------------|----------|
| `idle` | Eligible for termination if idle duration exceeds the rule's timeout |
| `idle in transaction` | Excluded when `excludeInTransaction=true`; otherwise eligible |
| `idle in transaction (aborted)` | Excluded when `excludeInTransaction=true`; otherwise eligible |
| `active` | Never terminated by idle rules |
| Empty or unrecognized | Not eligible for termination |

When a session matches a rule's resource group and its idle duration (calculated as `now - query_start`) exceeds the configured `IdleTimeout`, the daemon calls `TerminateSession(ctx, pid)` which executes `pg_terminate_backend(pid)`.

### New DB Client Method

`ListSessionsWithResourceGroup()` returns sessions enriched with resource group information. The query joins three system catalogs:

```sql
SELECT s.pid, s.usename, s.application_name, s.client_addr,
       s.state, s.query, s.query_start,
       (now() - s.query_start)::text,
       COALESCE(rg.rsgname, '')
FROM pg_stat_activity s
LEFT JOIN pg_roles r ON s.usename = r.rolname
LEFT JOIN pg_resgroup rg ON r.rolresgroup = rg.oid
WHERE s.pid != pg_backend_pid()
  AND s.usename IS NOT NULL
ORDER BY s.query_start DESC NULLS LAST
```

The `LEFT JOIN` ensures sessions without a resource group assignment are still returned (with an empty `ResourceGroup` string). The result is mapped to `db.SessionWithGroup`, which embeds `db.Session` and adds a `ResourceGroup` field.

### Metrics

The daemon increments the `cloudberry_idle_session_terminations_total` counter (defined in section 6) each time it successfully terminates an idle session. The counter is labeled with `cluster`, `namespace`, and `rule` to enable per-rule monitoring. The metric is recorded via `metrics.Recorder.RecordIdleSessionTermination(cluster, namespace, rule)`.

### E2E Test Cases

All E2E tests use **10s idle timeouts** (instead of the production default of 30m) and a **2s scan interval** for practical test execution. Each test creates an isolated resource group and test role with a unique suffix to avoid conflicts between parallel test runs.

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario33a_IdleSessionTerminated` | Session idle beyond 10s timeout is terminated by daemon. Custom termination message logged. `cloudberry_idle_session_terminations_total` incremented. |
| `TestScenario33b_InTransactionExcluded_ThenTerminated` | Session in transaction (`BEGIN`) survives past idle timeout (`excludeInTransaction=true`). After `COMMIT`, session transitions to `idle` and is terminated. Exactly one metric event recorded. |
| `TestScenario33c_DisabledRuleNoTermination` | Disabled rule (`enabled=false`) does not terminate idle sessions. No metrics recorded. |

#### Test 33a — Idle Session Terminated After Timeout

1. Creates a resource group and a test role assigned to that group in the real Cloudberry DB
2. Starts the idle daemon with `ScanInterval=2s` and an idle rule with `IdleTimeout=10s`, `excludeInTransaction=true`
3. Opens a new database connection as the test role and executes `SELECT 1` to establish the session
4. Leaves the session idle and monitors `pg_stat_activity` from the admin connection (without pinging the test connection, which would reset `query_start`)
5. Verifies the session disappears from `pg_stat_activity` within 30s (10s timeout + 2s scan + buffer)
6. Verifies the test connection is no longer valid (ping fails)
7. Verifies `RecordIdleSessionTermination` was called with the correct rule name, cluster, and namespace

#### Test 33b — In-Transaction Excluded, Then Terminated After COMMIT

1. Creates a resource group and test role, starts the daemon with the same configuration as 33a
2. Opens a test connection and executes `BEGIN` to start a transaction (session state becomes `idle in transaction`)
3. Leaves idle for 15s — verifies the session survives (not terminated due to `excludeInTransaction=true`)
4. Verifies zero termination metric events during the in-transaction phase
5. Executes `COMMIT` — session state transitions to `idle`
6. Waits for the daemon to terminate the now-idle session (within 30s)
7. Verifies exactly one termination metric event was recorded (for the post-COMMIT termination only)

#### Test 33c — Disabled Rule Does Not Terminate

1. Creates a resource group and test role, starts the daemon with a rule that has `enabled=false`
2. Opens a test connection, executes `SELECT 1`, then leaves idle for 18s (well beyond the 10s timeout)
3. Verifies the session survives the entire wait period
4. Verifies the session is still functional by executing another `SELECT 1`
5. Verifies zero termination metric events were recorded

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario33 -race -count=1 -v -timeout=10m
```

## 20. Scenario 34 — Workload Management Disabled with Real Cloudberry Cluster

### Overview

When `spec.workload.enabled` is set to `false`, the operator performs a full cleanup of all workload management resources. The `cleanupWorkload()` method in the Admin Controller drops resource groups from the database, deletes the workload-rules ConfigMap, stops the idle session daemon, zeros out resource group metrics, and updates the `WorkloadConfigured` condition to `False`. This ensures a clean transition to the disabled state with no orphaned resources.

When `enabled` is set back to `true`, the existing `reconcileWorkload()` path recreates everything — resource groups are re-created in the database, the ConfigMap is rebuilt with workload and idle rules, the idle daemon is restarted, and the `WorkloadConfigured` condition returns to `True`.

### Cleanup Actions

When `spec.workload.enabled` transitions to `false`, the operator executes the following cleanup steps in order:

1. **Drop resource groups from DB**: Calls `ListResourceGroups()` to enumerate all user-created resource groups, then calls `DropResourceGroup()` for each one. System groups (`default_group`, `admin_group`, `system_group`) are excluded by `ListResourceGroups()` and are never dropped. Errors on individual drops are logged but do not fail the cleanup.

2. **Delete workload-rules ConfigMap**: Deletes the `{cluster}-workload-rules` ConfigMap that stores serialized workload rules (`rules.json`) and idle session rules (`idle-rules.json`). If the ConfigMap does not exist, this is a no-op.

3. **Stop idle session daemon**: Calls `Stop()` on the idle session daemon if it is running. The daemon's context is cancelled and the scan loop exits cleanly. The daemon reference is set to `nil`.

4. **Zero resource group metrics**: Calls `SetResourceGroupUsage(cluster, namespace, group, 0, 0)` for each dropped resource group, zeroing out the `cloudberry_resource_group_cpu_usage` and `cloudberry_resource_group_memory_usage` Prometheus gauges.

5. **Update condition**: Sets `WorkloadConfigured` condition to `False` with reason `WorkloadDisabled` and message `"Workload management is disabled"`.

6. **Emit event**: Emits a `WorkloadDisabled` event with message `"Workload management disabled: resource groups dropped, rules cleared"`.

### Re-enable Behavior

When `spec.workload.enabled` is set back to `true`, the standard `reconcileWorkload()` path handles recreation:

- Resource groups listed in `spec.workload.resourceGroups` are created in the database via `CreateResourceGroup()`
- Workload rules are serialized to JSON and stored in a new `{cluster}-workload-rules` ConfigMap under `rules.json`
- Idle session rules are stored in the same ConfigMap under `idle-rules.json`
- The idle session daemon is started with the configured rules
- Resource group usage metrics are queried and updated
- `WorkloadConfigured` condition is set to `True` with reason `WorkloadReconciled`

No special re-enable logic is needed — the existing reconciliation path treats the absence of resource groups and ConfigMap as a fresh bootstrap.

### Idempotency

The cleanup is safe to call multiple times:

- **Missing ConfigMap**: `deleteWorkloadRulesConfigMap()` checks for `IsNotFound` and returns without error
- **Empty resource group list**: If `ListResourceGroups()` returns an empty list (no user-created groups), the drop loop simply does not execute
- **Stopped daemon**: `stopIdleDaemon()` checks if the daemon reference is `nil` before calling `Stop()`
- **Condition updates**: `SetCondition()` overwrites the existing condition value, producing the same result regardless of how many times it is called

### Condition Updates

| Condition | Status | Reason | Description |
|-----------|--------|--------|-------------|
| `WorkloadConfigured` | `False` | `WorkloadDisabled` | Workload management is disabled — all resources cleaned up |
| `WorkloadConfigured` | `True` | `WorkloadReconciled` | Workload management re-enabled and fully reconciled |

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario34a_DisableDropsResourceGroups_ReenableRecreates` | Full 3-phase lifecycle: Enable (groups created, ConfigMap created, condition=True) → Disable (groups dropped, ConfigMap deleted, condition=False/WorkloadDisabled) → Re-enable (groups recreated, ConfigMap recreated, condition=True/WorkloadReconciled) |
| `TestScenario34b_IdleDaemonStopsOnDisable` | Daemon terminates idle sessions when active, stops on disable (sessions survive), restarts on re-enable (sessions terminated again) |
| `TestScenario34c_MetricsZeroedOnDisable` | `SetResourceGroupUsage` called with `(0, 0)` for each dropped group during disable |
| `TestScenario34d_IdempotentDisable` | Multiple disable reconciliations produce no errors, condition remains False/WorkloadDisabled |

#### Test 34a — Full Disable/Re-enable Lifecycle

1. **Enable phase**: Creates a cluster with `workload.enabled=true`, 2 resource groups (analytics, etl), 2 workload rules (cancel, move), and 1 idle rule. Runs reconciliation. Verifies resource groups exist in the real DB, ConfigMap contains `rules.json` and `idle-rules.json`, and `WorkloadConfigured=True/WorkloadReconciled`.

2. **Disable phase**: Sets `workload.enabled=false` and bumps generation. Runs reconciliation. Verifies resource groups are dropped from the real DB, ConfigMap is deleted, and `WorkloadConfigured=False/WorkloadDisabled`.

3. **Re-enable phase**: Sets `workload.enabled=true` with the same groups and rules, bumps generation. Runs reconciliation. Verifies resource groups are recreated in the real DB with correct parameters, ConfigMap is recreated with both `rules.json` (2 rules) and `idle-rules.json` (1 rule), and `WorkloadConfigured=True/WorkloadReconciled`.

#### Test 34b — Idle Daemon Stops on Disable, Restarts on Re-enable

1. Creates a resource group and test role in the real DB, assigns the role to the group
2. Starts the idle daemon with `ScanInterval=2s` and `IdleTimeout=10s`
3. Opens a test connection as the test role, executes `SELECT 1`, and leaves idle
4. Verifies the daemon terminates the idle session (sanity check)
5. Stops the daemon (simulating workload disable)
6. Opens a new test connection, leaves idle for 15s — verifies the session survives (daemon stopped)
7. Restarts the daemon (simulating workload re-enable)
8. Verifies the session is terminated (it was already idle >15s when the daemon restarted)

#### Test 34c — Metrics Zeroed on Disable

1. Creates a cluster with 2 resource groups and a tracking metrics recorder
2. Runs reconciliation with `enabled=true` (metrics may or may not be set depending on `gp_resource_manager` mode)
3. Resets the metrics recorder, then disables workload and runs reconciliation
4. Verifies `SetResourceGroupUsage` was called with `(0, 0)` for both the analytics and etl groups during the disable cleanup

#### Test 34d — Idempotent Disable

1. Creates a cluster with `workload.enabled=false` from the start (no prior enable)
2. Runs reconciliation — verifies no errors, `WorkloadConfigured=False/WorkloadDisabled`, ConfigMap does not exist
3. Bumps generation and runs reconciliation again — verifies the same result with no errors
4. Confirms cleanup is safe when there is nothing to clean up (no resource groups, no ConfigMap, no daemon)

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario34 -race -count=1 -v -timeout=10m
```

## 21. Scenario 35 — API Permission Negative Tests with Real Cloudberry Cluster

### Overview

Scenario 35 validates the API permission model with negative test cases, ensuring that the authentication and authorization layers correctly reject unauthorized requests. Specifically:

- Basic-level users cannot perform Operator-level operations (403 Forbidden)
- Operator-level users cannot perform Admin-level operations (403 Forbidden)
- Unauthenticated requests are rejected (401 Unauthorized)
- Health endpoints (`/healthz`, `/readyz`) remain accessible without authentication

The tests use `InMemoryCredentialStore` with real bcrypt-based BasicAuth, an `httptest` API server with real `AuthMiddleware`, and a real Cloudberry DB connection for verifying that denied operations have no side effects (e.g., resource groups are not deleted after a rejected DELETE).

### Permission Model Reference

| Level | Value | Can Access |
|-------|-------|------------|
| Basic | 1 | GET endpoints (view cluster state) |
| OperatorBasic | 2 | Basic + view all sessions |
| Operator | 3 | POST, PUT, most DELETE operations |
| Admin | 4 | Full access including DELETE resource-groups, cluster create/delete |

### Endpoints Tested

| Endpoint | Required Permission | Test |
|----------|-------------------|------|
| POST /workload/resource-groups | Operator | 35a: Basic → 403 |
| DELETE /workload/resource-groups/{rg} | Admin | 35b: Operator → 403 |
| GET /workload | Basic | 35c: No auth → 401 |
| /healthz, /readyz | None | 35c: No auth → 200 |

### Error Response Format

All error responses follow a consistent JSON structure with security headers (`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`).

**401 Unauthorized** — missing or invalid credentials:
```json
{"error":{"code":"UNAUTHORIZED","message":"missing Authorization header"}}
```
```json
{"error":{"code":"UNAUTHORIZED","message":"authentication failed"}}
```

**403 Forbidden** — authenticated but insufficient permission level:
```json
{"error":{"code":"FORBIDDEN","message":"insufficient permissions: requires Operator"}}
```
```json
{"error":{"code":"FORBIDDEN","message":"insufficient permissions: requires Admin"}}
```

### Test Users

The test suite creates three users via `InMemoryCredentialStore` with bcrypt-hashed passwords:

| User | Password | Permission Level | Purpose |
|------|----------|-----------------|---------|
| viewer | viewerpass | Basic (1) | Represents read-only users |
| operator | operatorpass | Operator (3) | Represents operational users |
| admin | adminpass | Admin (4) | Represents full-access administrators |

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | Sub-tests | What It Verifies |
|----------|-----------|-----------------|
| `TestScenario35a_BasicRole_POST_ResourceGroups_Forbidden` | viewer_denied_403, operator_allowed_201, admin_allowed | Basic user gets 403 on POST resource-groups; Operator and Admin succeed |
| `TestScenario35b_OperatorRole_DELETE_ResourceGroups_Forbidden` | operator_denied_403, resource_group_still_exists, admin_allowed_200, resource_group_deleted | Operator gets 403 on DELETE resource-groups; resource group survives; Admin succeeds and group is deleted from real DB |
| `TestScenario35c_Unauthenticated_Request_Unauthorized` | no_auth_GET/POST/DELETE_401, healthz/readyz_200, wrong_password_401, unknown_user_401 | No auth → 401 on all protected endpoints; health endpoints work without auth; wrong credentials → 401 |

#### Test 35a — Basic Role POST Resource-Groups Forbidden

1. Creates a cluster with `workload.enabled=true` and sets up the authenticated API server with three users (viewer, operator, admin)
2. Sends POST `/workload/resource-groups` as `viewer` (Basic) — verifies 403 with `FORBIDDEN` code and message `"insufficient permissions: requires Operator"`
3. Verifies security headers (`X-Content-Type-Options`, `X-Frame-Options`, `Cache-Control`) are present on the 403 response
4. Sends the same POST as `operator` — verifies 201 Created (resource group created in real DB)
5. Sends POST with a different group name as `admin` — verifies 201 Created
6. Cleans up all created resource groups

#### Test 35b — Operator Role DELETE Resource-Groups Forbidden

1. Creates a resource group in the real Cloudberry DB via `dbClient.CreateResourceGroup()`
2. Sends DELETE `/workload/resource-groups/{rg}` as `operator` — verifies 403 with `FORBIDDEN` code and message `"insufficient permissions: requires Admin"`
3. Queries the real DB to confirm the resource group still exists (denied DELETE had no side effect)
4. Sends the same DELETE as `admin` — verifies 200 OK with response `{"status":"deleted","group":"<name>"}`
5. Queries the real DB to confirm the resource group is now deleted

#### Test 35c — Unauthenticated Request Unauthorized

1. Sends requests without any `Authorization` header to three protected endpoints (GET /workload, POST /resource-groups, DELETE /resource-groups/{rg}) — verifies each returns 401 with `UNAUTHORIZED` code and message `"missing Authorization header"`
2. Verifies security headers are present on all 401 responses
3. Sends GET `/healthz` and GET `/readyz` without auth — verifies 200 OK (health endpoints are exempt from authentication)
4. Sends GET /workload with wrong password (`viewer:wrongpassword`) — verifies 401 with message `"authentication failed"`
5. Sends GET /workload with unknown user (`nonexistent:somepassword`) — verifies 401 with message `"authentication failed"`

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario35 -race -count=1 -v -timeout=5m
```

## 22. Scenario 36 — Per-Tablespace I/O Limits with Real Cloudberry Cluster

### Overview

Scenario 36 validates the operator's per-tablespace I/O limit reconciliation logic against a real Apache Cloudberry 2.1.0 cluster. Resource groups support disk I/O throttling via `ALTER RESOURCE GROUP ... SET io_limit`, allowing operators to cap read/write throughput and IOPS on a per-tablespace basis. The CRD exposes this through the `ioLimits` field on each `ResourceGroupSpec` entry.

### CRD Schema

The `TablespaceIOLimitSpec` struct defines per-tablespace I/O constraints:

```go
type TablespaceIOLimitSpec struct {
    Tablespace       string `json:"tablespace"`
    ReadBytesPerSec  int64  `json:"readBytesPerSec,omitempty"`
    WriteBytesPerSec int64  `json:"writeBytesPerSec,omitempty"`
    ReadIOPS         int32  `json:"readIOPS,omitempty"`
    WriteIOPS        int32  `json:"writeIOPS,omitempty"`
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tablespace` | string | Target tablespace name. Use `"*"` for all tablespaces (wildcard). |
| `readBytesPerSec` | int64 | Maximum read throughput in bytes per second |
| `writeBytesPerSec` | int64 | Maximum write throughput in bytes per second |
| `readIOPS` | int32 | Maximum read I/O operations per second |
| `writeIOPS` | int32 | Maximum write I/O operations per second |

Example CRD snippet:

```yaml
spec:
  workload:
    enabled: true
    resourceGroups:
      - name: analytics
        concurrency: 10
        cpuMaxPercent: 50
        cpuWeight: 100
        ioLimits:
          - tablespace: "fast_storage"
            readBytesPerSec: 209715200   # 200 MB/s
            writeBytesPerSec: 104857600  # 100 MB/s
            readIOPS: 5000
            writeIOPS: 2500
          - tablespace: "*"
            readBytesPerSec: 52428800    # 50 MB/s
            writeBytesPerSec: 26214400   # 25 MB/s
            readIOPS: 500
            writeIOPS: 250
```

### SQL Format

The `FormatIOLimits` function in `internal/db/client.go` converts `[]IOLimitOption` into the Cloudberry `io_limit` string format:

```
tablespace:rbps=X:wbps=X:riops=X:wiops=X
```

Multiple tablespace entries are joined by semicolons (`;`). Examples:

| Input | Formatted Output |
|-------|-----------------|
| Single wildcard | `*:rbps=104857600:wbps=52428800:riops=1000:wiops=500` |
| Named + wildcard | `fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500;*:rbps=52428800:wbps=26214400:riops=500:wiops=250` |

The formatted string is passed to the database via:

```sql
ALTER RESOURCE GROUP "analytics" SET io_limit 'fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500;*:rbps=52428800:wbps=26214400:riops=500:wiops=250'
```

### Cloudberry 2.1.0 Limitation

The `io_limit` feature requires `gp_resource_manager=group`. The test cluster runs in queue mode (`gp_resource_manager=queue`), which causes Cloudberry to reject the ALTER with:

```
ERROR: resource group must be enabled to use io limit feature
```

The operator handles this error gracefully — the reconciliation does not crash, and the resource group is not dropped. This limitation is documented and expected for Cloudberry 2.1.0 deployments that have not switched to resource group mode.

### Reconciliation Behavior

The reconciler maps `ioLimits` from the CRD spec to `db.ResourceGroupOptions.IOLimits` during the resource group diff loop. Key behaviors:

1. **IOLimits mapping**: Each `TablespaceIOLimitSpec` in the CRD is converted to a `db.IOLimitOption` with matching fields (`Tablespace`, `ReadBytesPerSec`, `WriteBytesPerSec`, `ReadIOPS`, `WriteIOPS`).

2. **`needsAlter` always returns `true` when IOLimits are present**: The operator cannot easily read back `io_limit` from the database in a structured way, so any desired spec with `ioLimits` triggers an `ALTER RESOURCE GROUP`. This is safe because `ALTER RESOURCE GROUP ... SET io_limit` is idempotent — applying the same limits repeatedly produces no side effects.

3. **ALTER execution**: `AlterResourceGroup` first applies standard parameters (concurrency, cpu_max_percent, etc.) via individual ALTER statements, then applies I/O limits as a single `ALTER RESOURCE GROUP <name> SET io_limit '<formatted_string>'` statement.

4. **Error propagation**: If the `io_limit` ALTER fails (e.g., in queue mode), the error propagates up through the reconciler. The resource group itself remains intact in the database.

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario36a_WildcardTablespaceIOLimits` | `FormatIOLimits` produces correct wildcard format (`*:rbps=...:wbps=...:riops=...:wiops=...`); `AlterResourceGroup` is called with IOLimits containing the wildcard entry; reconciliation handles `io_limit` error gracefully (no crash); resource group survives in DB after reconciliation |
| `TestScenario36b_NamedAndWildcardTablespaceIOLimits` | `FormatIOLimits` produces correct multi-tablespace format with semicolon separator; ALTER call contains both `fast_storage` and `*` entries; formatted string contains both complete entries; exactly 1 semicolon separator for 2 entries; resource group survives in DB |

#### Test 36a — Wildcard Tablespace I/O Limits

1. Verifies `FormatIOLimits` output for a single wildcard tablespace: `*:rbps=104857600:wbps=52428800:riops=1000:wiops=500`
2. Creates a resource group (`analytics`) in the real Cloudberry DB with concurrency=10, cpuMaxPercent=50, cpuWeight=100
3. Builds a `CloudberryCluster` with one IOLimits entry: wildcard (`*`) at 100 MB/s read, 50 MB/s write, 1000/500 IOPS
4. Uses a tracking wrapper (`scenario36AlterTracker`) to intercept and record `AlterResourceGroup` calls with full `ResourceGroupOptions` including IOLimits
5. Runs reconciliation via `AdminReconciler`
6. Verifies the captured ALTER call contains 1 IOLimit entry with tablespace=`*`, readBytesPerSec=104857600, writeBytesPerSec=52428800, readIOPS=1000, writeIOPS=500
7. Verifies `FormatIOLimits` output from the captured ALTER call matches the expected format
8. Verifies graceful error handling — in queue mode, the ALTER fails with an `io_limit` or `resource group must be enabled` error, but the reconciliation does not panic
9. Verifies the resource group still exists in the real DB after reconciliation (the `io_limit` error does not cause the group to be dropped)

#### Test 36b — Named + Wildcard Tablespace I/O Limits

1. Verifies `FormatIOLimits` output for two tablespaces: `fast_storage:rbps=209715200:wbps=104857600:riops=5000:wiops=2500;*:rbps=52428800:wbps=26214400:riops=500:wiops=250`
2. Confirms the formatted string contains the `fast_storage:` entry, the `*:rbps=` wildcard entry, and a semicolon separator
3. Creates a resource group in the real Cloudberry DB
4. Builds a `CloudberryCluster` with two IOLimits entries: `fast_storage` (200 MB/s read, 100 MB/s write, 5000/2500 IOPS) and `*` (50 MB/s read, 25 MB/s write, 500/250 IOPS)
5. Runs reconciliation with a tracking wrapper
6. Verifies the captured ALTER call contains 2 IOLimit entries with correct tablespace names and all throughput/IOPS values
7. Verifies `FormatIOLimits` output from the captured ALTER call matches the expected multi-tablespace format
8. Verifies the formatted string contains both complete entries and exactly 1 semicolon separator
9. Verifies graceful error handling and resource group survival in the real DB

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario36 -race -count=1 -v -timeout=5m
```

## 23. Scenario 37 — Rule Priority Ordering with Real Cloudberry Cluster

### Overview

Scenario 37 validates that the operator sorts workload rules by priority (lowest number first) before storing them in the ConfigMap. Rules with the same priority preserve their CRD spec order (stable sort). This ensures deterministic evaluation order regardless of how rules are ordered in the CRD spec.

Specification reference: Section 10 "Rule Ordering":
> "Rules are evaluated in priority order (lowest number first). The `priority` field on each rule controls evaluation order. Rules with the same priority are evaluated in the order they appear in the CRD spec."

### Implementation

The `applyWorkloadRules()` method in `internal/controller/admin_controller.go` sorts rules before JSON serialization:

```go
// Sort rules by priority (lowest number first), preserving CRD spec order
// for rules with the same priority (stable sort).
sortedRules := make([]cbv1alpha1.WorkloadRule, len(cluster.Spec.Workload.Rules))
copy(sortedRules, cluster.Spec.Workload.Rules)
sort.SliceStable(sortedRules, func(i, j int) bool {
    return sortedRules[i].Priority < sortedRules[j].Priority
})
rulesJSON, err := json.Marshal(sortedRules)
```

Key implementation details:

1. **Copy before sort**: The method copies the rules slice before sorting to avoid mutating the original CRD spec in memory.
2. **`sort.SliceStable`**: Uses Go's stable sort algorithm, which guarantees that elements with equal `Priority` values retain their original relative order (i.e., CRD spec order).
3. **Sort key**: The `Priority` field (`int32`) is compared with `<` — lowest number sorts first.
4. **JSON marshaling**: The sorted slice is marshaled to JSON and stored in the `rules.json` key of the `{cluster}-workload-rules` ConfigMap.

### Bug Fix

Prior to Scenario 37, the `applyWorkloadRules()` method serialized rules directly from `cluster.Spec.Workload.Rules` without sorting. Rules were stored in the ConfigMap in CRD spec order, which meant the evaluation order depended on how the user happened to list rules in the YAML — not on the `priority` field. This violated the contract described in Section 10.

The fix added `sort.SliceStable` in `applyWorkloadRules()` to sort rules by the `Priority` field before JSON marshaling. The stable sort variant was chosen specifically to preserve CRD spec order as the tiebreaker for rules with identical priorities.

### E2E Test Cases (Real Cloudberry 2.1.0 Cluster)

| E2E Test | What It Verifies |
|----------|-----------------|
| `TestScenario37a_DifferentPriorities_LowestFirst` | 3 rules in non-priority order (p3, p1, p2) → ConfigMap stores them sorted as p1, p2, p3. Evaluation order: log(1) → move(2) → cancel(3). |
| `TestScenario37b_SamePriority_CRDSpecOrder` | 2 rules with identical priority=1 → ConfigMap preserves CRD spec order (`first_in_spec` before `second_in_spec`). Stable sort tiebreaker verified. |

#### Test 37a — Different Priorities, Lowest Number First

1. Creates two resource groups (`analytics`, `etl`) in the real Cloudberry DB
2. Builds a `CloudberryCluster` with 3 rules intentionally in **non-priority order**:
   - `rule_p3` (priority=3, action=cancel, threshold=30)
   - `rule_p1` (priority=1, action=log, threshold=10)
   - `rule_p2` (priority=2, action=move, moveTarget=etl, threshold=20)
3. Runs reconciliation via `AdminReconciler` with a real DB factory
4. Reads the `{cluster}-workload-rules` ConfigMap and parses `rules.json`
5. Verifies rules are sorted by priority (lowest first):
   - Index 0: `rule_p1` (priority=1, action=log)
   - Index 1: `rule_p2` (priority=2, action=move)
   - Index 2: `rule_p3` (priority=3, action=cancel)
6. Verifies each rule retains correct fields (enabled, resourceGroup, thresholdType, threshold, moveTarget)
7. Verifies the evaluation order is `log(1) → move(2) → cancel(3)`

#### Test 37b — Same Priority, CRD Spec Order Preserved

1. Creates one resource group (`analytics`) in the real Cloudberry DB
2. Builds a `CloudberryCluster` with 2 rules that have **identical priority=1**:
   - `first_in_spec` (priority=1, action=log, threshold=5)
   - `second_in_spec` (priority=1, action=log, threshold=5)
3. Runs reconciliation via `AdminReconciler` with a real DB factory
4. Reads the ConfigMap and parses `rules.json`
5. Verifies both rules have priority=1
6. Verifies CRD spec order is preserved: `first_in_spec` at index 0, `second_in_spec` at index 1
7. Verifies the stable sort tiebreaker — both rules have identical action, thresholdType, threshold, and resourceGroup; only the name differs, confirming that original CRD spec order (not alphabetical or any other order) is the tiebreaker

### Running Tests

```bash
CLOUDBERRY_TEST_USER=gpadmin CLOUDBERRY_TEST_DB=postgres \
  go test ./test/e2e/... -tags=e2e -run TestE2E_Scenario37 -race -count=1 -v -timeout=5m
```
