# Cloudberry Operator - Workload Management Specification

**Version**: 1.1.0
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

When `spec.workload.enabled` is true:
1. Create/update resource groups via SQL
2. Apply workload rules to the workload configuration table
3. Apply idle session rules
4. Monitor resource group utilization
5. Report metrics

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
