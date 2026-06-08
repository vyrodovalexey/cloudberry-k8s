# Specification 13: Storage Management & Recommendations

## Overview

This specification defines the storage management, recommendation scanning, and usage reporting
capabilities for the Cloudberry Kubernetes Operator. These features enable operators to monitor
disk usage, receive actionable recommendations for table maintenance, and generate monthly
resource usage reports.

## CRD Changes

### StorageManagementSpec (spec.storage)

Added to `CloudberryClusterSpec`:

```yaml
storage:
  diskMonitoring: true
  recommendationScan:
    enabled: true
    schedule: "0 3 * * 0"        # Weekly Sunday 3 AM
    bloatThreshold: 20            # Dead tuple % threshold
    skewThreshold: 50             # Skew coefficient %
    ageThreshold: 500000000       # XID age threshold
    indexBloatThreshold: 30       # Index bloat %
    scanDuration: "2h"            # Max scan duration
  usageReport:
    enabled: true
    monthly: true
```

### RecommendationScanSpec

| Field               | Type   | Default     | Description                          |
|---------------------|--------|-------------|--------------------------------------|
| enabled             | bool   | false       | Enable recommendation scanning       |
| schedule            | string | 0 3 * * 0   | Cron schedule for scans              |
| bloatThreshold      | int32  | 20          | Dead tuple percentage threshold      |
| skewThreshold       | int32  | 50          | Data skew coefficient threshold      |
| ageThreshold        | int64  | 500000000   | XID age threshold                    |
| indexBloatThreshold | int32  | 30          | Index bloat percentage threshold     |
| scanDuration        | string | 2h          | Maximum scan duration                |

### UsageReportSpec

| Field   | Type | Default | Description                    |
|---------|------|---------|--------------------------------|
| enabled | bool | false   | Enable usage reporting         |
| monthly | bool | false   | Generate monthly reports       |

### Status Fields

Added to `CloudberryClusterStatus`:

| Field               | Type  | Description                          |
|---------------------|-------|--------------------------------------|
| diskUsagePercent    | int32 | Current disk usage percentage        |
| recommendationCount | int32 | Number of active recommendations     |

## API Endpoints

| Method | Path                                                  | Description                    |
|--------|-------------------------------------------------------|--------------------------------|
| GET    | /clusters/{name}/storage/disk-usage                   | Get disk usage info            |
| GET    | /clusters/{name}/storage/tables                       | List tables with storage info  |
| GET    | /clusters/{name}/storage/tables/{schema}/{table}      | Get table detail               |
| GET    | /clusters/{name}/storage/recommendations              | List recommendations           |
| POST   | /clusters/{name}/storage/recommendations/scan         | Trigger recommendation scan    |
| GET    | /clusters/{name}/storage/usage-report                 | Get usage report               |

## CLI Commands

```
cloudberry-ctl storage disk-usage --cluster my-cluster
cloudberry-ctl storage tables list --cluster my-cluster
cloudberry-ctl storage tables detail --cluster my-cluster --schema public --table orders
cloudberry-ctl storage recommendations list --cluster my-cluster
cloudberry-ctl storage recommendations scan --cluster my-cluster
cloudberry-ctl storage usage-report --cluster my-cluster --month 2026-05
```

## Prometheus Metrics

| Metric                                          | Type      | Labels                    | Description                        |
|-------------------------------------------------|-----------|---------------------------|------------------------------------|
| cloudberry_disk_usage_percent                   | Gauge     | cluster, namespace        | Disk usage percentage per cluster  |
| cloudberry_recommendations_total                | Gauge     | cluster, namespace, type  | Recommendations by type            |
| cloudberry_recommendation_scan_duration_seconds | Histogram | cluster, namespace        | Scan duration in seconds           |
| cloudberry_table_bloat_ratio                    | Gauge     | cluster, namespace, table | Bloat ratio for top tables         |

## Validation Rules

- `bloatThreshold`: must be between 0 and 100
- `skewThreshold`: must be between 0 and 100
- `indexBloatThreshold`: must be between 0 and 100
- `ageThreshold`: must be non-negative

## Default Values (applied by mutating webhook)

When `recommendationScan.enabled` is true:
- `schedule`: "0 3 * * 0" (weekly Sunday 3 AM)
- `bloatThreshold`: 20
- `skewThreshold`: 50
- `ageThreshold`: 500000000
- `indexBloatThreshold`: 30
- `scanDuration`: "2h"

## Reconciliation

The `AdminReconciler.reconcileStorage()` method:
1. Checks if storage management is enabled (diskMonitoring = true)
2. Updates disk usage metrics
3. Processes recommendation scan configuration if enabled
4. Updates recommendation count in status
5. Sets `StorageConfigured` condition
