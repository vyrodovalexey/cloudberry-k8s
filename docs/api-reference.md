# API Reference

The Cloudberry Operator exposes a REST API for programmatic access to cluster management operations. This document covers all endpoints, authentication, request/response schemas, and error codes.

## Table of Contents

- [Base URL](#base-url)
- [Authentication](#authentication)
- [Endpoints](#endpoints)
  - [Health](#health)
  - [Cluster Management](#cluster-management)
  - [Cluster Operations](#cluster-operations)
  - [Configuration](#configuration)
  - [Storage](#storage)
  - [Scale Status](#scale-status)
  - [High Availability](#high-availability)
  - [Sessions](#sessions)
  - [Query History](#query-history)
  - [Plan Analysis](#plan-analysis)
  - [Query Operations](#query-operations)
  - [Exporter Health](#exporter-health)
  - [Resource Groups](#resource-groups)
  - [Maintenance](#maintenance)
  - [Authentication Management](#authentication-management)
- [Request/Response Schemas](#requestresponse-schemas)
- [Error Handling](#error-handling)
- [Pagination](#pagination)
- [Rate Limiting](#rate-limiting)
- [HTTP Server Timeouts](#http-server-timeouts)
- [Request Body Limits](#request-body-limits)
- [Response Body Limits (CLI)](#response-body-limits-cli)
- [API Observability](#api-observability)
- [Data Loading Endpoints](#data-loading-endpoints)
- [Input Validation](#input-validation)
- [Annotations Reference](#annotations-reference)
  - [Action Annotations](#action-annotations-avsoftioaction)
  - [Maintenance Annotations](#maintenance-annotations-avsofiomaintenance)
  - [Rolling Restart Annotation](#rolling-restart-annotation-avsoftiorolling-restart)
  - [Status Phases](#status-phases)
- [Webhook Endpoints](#webhook-endpoints)

## Base URL

```
http://{operator-service}:{port}/api/v1alpha1
```

Default port: `8090` (configurable via `APIAddress` / `CLOUDBERRY_API_ADDRESS`)

## Authentication

All API endpoints (except health checks) require authentication. The API supports two authentication methods simultaneously.

> **Guest Access**: When `guestAccess: true` is set in the cluster's `queryMonitoring` spec, certain read-only GET endpoints allow unauthenticated access with a guest identity (`Basic` permission). See [Guest Access](#guest-access) for details.

### Basic Authentication

```
Authorization: Basic base64(username:password)
```

Example:

```bash
curl -u admin:password http://operator:8090/api/v1alpha1/clusters
```

### Bearer Token (JWT)

```
Authorization: Bearer <JWT token>
```

### OIDC Redirect Protection

The OIDC provider's HTTP client enforces a maximum of 5 redirects during OIDC discovery and token validation. This prevents infinite redirect loops when the identity provider misconfigures its endpoints. If the redirect limit is exceeded, the authentication attempt fails with a `401 UNAUTHORIZED` response.

### OIDC Lazy Discovery

If OIDC discovery fails at operator startup (e.g. the identity provider is briefly unavailable), Bearer authentication is not permanently disabled: the first Bearer request after the IdP recovers re-runs discovery, subject to a 30-second cooldown. Concurrent Bearer requests share a **single** in-flight discovery (singleflight), and each attempt is bounded by a **10-second timeout**, so a burst of requests against an unavailable IdP fails fast together instead of piling up. Successful per-request identity details (username/email/roles) are logged at **Debug** level only.

### Obtaining a JWT Token

**Via Keycloak (password grant — for testing only):**

```bash
TOKEN=$(curl -s -X POST \
  http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=cloudberry-ctl" \
  -d "username=admin" \
  -d "password=adminpass" | jq -r '.access_token')

curl -H "Authorization: Bearer $TOKEN" \
  http://operator:8090/api/v1alpha1/clusters
```

**Via Keycloak (client credentials — for service accounts):**

```bash
TOKEN=$(curl -s -X POST \
  http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=cloudberry-operator" \
  -d "client_secret=<secret>" | jq -r '.access_token')
```

## Endpoints

### Health

These endpoints do not require authentication.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Operator health check |
| `GET` | `/readyz` | Operator readiness check |
| `GET` | `/metrics` | Prometheus metrics (text format) |

**Health Check Response:**

```json
{
  "status": "ok"
}
```

**Readiness Check Response:**

```json
{
  "status": "ready"
}
```

### Cluster Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters` | Basic | List all clusters |
| `GET` | `/clusters/{name}` | Basic | Get cluster details |
| `GET` | `/clusters/{name}/status` | Basic | Get cluster status |
| `POST` | `/clusters` | Admin | Create a cluster |
| `PUT` | `/clusters/{name}` | Operator | Update a cluster |
| `DELETE` | `/clusters/{name}` | Admin | Delete a cluster |

#### List Clusters

```bash
curl -u admin:password http://operator:8090/api/v1alpha1/clusters
```

**Response (200 OK):**

```json
{
  "items": [
    {
      "name": "my-cluster",
      "namespace": "cloudberry-test",
      "phase": "Running",
      "version": "2.1.0"
    }
  ],
  "total": 1
}
```

#### Get Cluster Status

```bash
curl -u admin:password \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/status
```

**Response (200 OK):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "status": {
    "phase": "Running",
    "coordinatorReady": true,
    "standbyReady": true,
    "segmentsReady": 4,
    "segmentsTotal": 4,
    "mirroringStatus": "InSync",
    "clusterVersion": "2.1.0",
    "lastReconcileTime": "2026-05-11T18:00:00Z",
    "conditions": [
      {
        "type": "ClusterReady",
        "status": "True",
        "lastTransitionTime": "2026-05-11T17:55:00Z",
        "reason": "AllComponentsReady",
        "message": "All cluster components are running and healthy"
      }
    ]
  }
}
```

#### Create Cluster

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "avsoft.io/v1alpha1",
    "kind": "CloudberryCluster",
    "metadata": {
      "name": "new-cluster",
      "namespace": "cloudberry-test"
    },
    "spec": {
      "coordinator": {
        "storage": {"size": "10Gi"}
      },
      "segments": {
        "count": 4,
        "storage": {"size": "20Gi"}
      }
    }
  }'
```

**Response (201 Created):** Returns the created cluster resource.

#### Delete Cluster

```bash
curl -u admin:password -X DELETE \
  http://operator:8090/api/v1alpha1/clusters/my-cluster
```

**Response (200 OK):**

```json
{
  "status": "deleting"
}
```

### Cluster Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `POST` | `/clusters/{name}/start` | Operator | Start cluster |
| `POST` | `/clusters/{name}/stop` | Operator | Stop cluster |
| `POST` | `/clusters/{name}/restart` | Operator | Restart cluster |
| `POST` | `/clusters/{name}/reload` | Operator | Reload configuration |

#### Start Cluster

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/start \
  -H "Content-Type: application/json" \
  -d '{"mode": "normal"}'
```

**Request Body:**

```json
{
  "mode": "normal"
}
```

| Mode | Description |
|------|-------------|
| `normal` | Start all coordinator and segment processes |
| `restricted` | Start with superuser connections only |
| `maintenance` | Start coordinator only in utility mode |

**Response (202 Accepted):**

```json
{
  "status": "start initiated"
}
```

#### Stop Cluster

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/stop \
  -H "Content-Type: application/json" \
  -d '{"mode": "fast"}'
```

**Request Body:**

```json
{
  "mode": "fast"
}
```

| Mode | Description |
|------|-------------|
| `smart` | Wait for clients to disconnect (default) |
| `fast` | Rollback active transactions, disconnect clients |
| `immediate` | Abort all connections immediately |

### Configuration

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/config` | Operator Basic | Get configuration |
| `PUT` | `/clusters/{name}/config` | Operator | Update configuration |
| `GET` | `/clusters/{name}/config/parameters` | Operator Basic | List parameters |
| `PUT` | `/clusters/{name}/config/parameters` | Operator | Set parameters |
| `GET` | `/clusters/{name}/config/hba` | Operator Basic | Get HBA rules |
| `PUT` | `/clusters/{name}/config/hba` | Admin | Update HBA rules |

#### Update Configuration

```bash
curl -u admin:password -X PUT \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/config \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "max_connections": "200",
      "work_mem": "128MB"
    },
    "coordinatorParameters": {
      "optimizer": "on"
    }
  }'
```

**Response (200 OK):**

```json
{
  "status": "updated"
}
```

#### Update HBA Rules

```bash
curl -u admin:password -X PUT \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/config/hba \
  -H "Content-Type: application/json" \
  -d '{
    "rules": [
      {
        "type": "host",
        "database": "all",
        "user": "all",
        "address": "10.0.0.0/8",
        "method": "scram-sha-256"
      }
    ]
  }'
```

### Storage

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/storage/pvcs` | Basic | List all PVCs for a cluster with sizes |

#### List Cluster PVCs

Returns all PersistentVolumeClaims associated with a cluster, including their current sizes, component labels, and binding status.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/storage/pvcs?namespace=cloudberry-test"
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | No | Kubernetes namespace (defaults to operator's configured namespace) |

**Response (200 OK):**

```json
{
  "pvcs": [
    {
      "name": "data-my-cluster-coordinator-0",
      "component": "coordinator",
      "size": "10Gi",
      "phase": "Bound"
    },
    {
      "name": "data-my-cluster-standby-0",
      "component": "standby",
      "size": "5Gi",
      "phase": "Bound"
    },
    {
      "name": "data-my-cluster-segment-primary-0",
      "component": "segment-primary",
      "size": "20Gi",
      "phase": "Bound"
    },
    {
      "name": "data-my-cluster-segment-mirror-0",
      "component": "segment-mirror",
      "size": "20Gi",
      "phase": "Bound"
    }
  ],
  "total": 4
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `pvcs` | array | List of PVC objects |
| `pvcs[].name` | string | PVC name (e.g., `data-my-cluster-coordinator-0`) |
| `pvcs[].component` | string | Component label (`coordinator`, `standby`, `segment-primary`, `segment-mirror`) |
| `pvcs[].size` | string | Requested storage size (e.g., `10Gi`) |
| `pvcs[].phase` | string | PVC binding phase (`Bound`, `Pending`, `Lost`) |
| `total` | int | Total number of PVCs |

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (500 Internal Server Error — list failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to list PVCs"
  }
}
```

### Scale Status

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/scale/status` | Basic | Get scale operation status |

#### Get Scale Status

Returns the current scaling state of a cluster, including whether a scale-out or scale-in is in progress, segment readiness, and data redistribution status.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/scale/status?namespace=cloudberry-test"
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | No | Kubernetes namespace (defaults to operator's configured namespace) |

**Response (200 OK — scaling in progress):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": true,
  "phase": "Scaling",
  "segmentsReady": 4,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "InProgress",
    "message": "Data redistribution in progress"
  }
}
```

**Response (200 OK — scaling completed):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": false,
  "phase": "Running",
  "segmentsReady": 6,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "Completed",
    "message": "Data redistribution completed"
  }
}
```

**Response (200 OK — scale-in in progress):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": true,
  "phase": "Scaling",
  "segmentsReady": 6,
  "segmentsTotal": 4,
  "redistribution": {
    "status": "True",
    "reason": "InProgress",
    "message": "Data redistribution in progress for scale-in"
  }
}
```

**Response (200 OK — scale-out failed):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": true,
  "phase": "Scaling",
  "segmentsReady": 4,
  "segmentsTotal": 6,
  "redistribution": {
    "status": "True",
    "reason": "SegmentsNotReady",
    "message": "Scale-out failed: 2 segments not ready after 10m0s"
  },
  "failedSegments": [
    {
      "contentID": 4,
      "hostname": "my-cluster-segment-primary-4",
      "role": "primary",
      "status": "NotReady"
    },
    {
      "contentID": 5,
      "hostname": "my-cluster-segment-primary-5",
      "role": "primary",
      "status": "NotReady"
    }
  ],
  "conditions": [
    {
      "type": "ScaleOutFailed",
      "status": "True",
      "reason": "SegmentsNotReady",
      "message": "Scale-out failed: 2 segments not ready after 10m0s"
    }
  ]
}
```

**Response (200 OK — no scaling activity):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "scaling": false,
  "phase": "Running",
  "segmentsReady": 4,
  "segmentsTotal": 4
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Cluster name |
| `namespace` | string | Cluster namespace |
| `scaling` | bool | `true` if the cluster is currently in the `Scaling` phase |
| `phase` | string | Current cluster phase (`Running`, `Scaling`, etc.) |
| `segmentsReady` | int | Number of segment pods that are ready |
| `segmentsTotal` | int | Total desired segment count |
| `redistribution` | object | Present only when a `DataRedistribution` condition exists |
| `redistribution.status` | string | Condition status (`True` or `False`) |
| `redistribution.reason` | string | Condition reason (`ScaleOutStarted`, `ScaleInStarted`, `InProgress`, `Completed`, `SegmentsNotReady`) |
| `redistribution.message` | string | Human-readable description |

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

### High Availability

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/segments` | Basic | List segments |
| `GET` | `/clusters/{name}/segments/{id}` | Basic | Get segment details |
| `GET` | `/clusters/{name}/mirroring` | Basic | Get mirroring status |
| `POST` | `/clusters/{name}/recovery` | Operator | Start recovery |
| `POST` | `/clusters/{name}/rebalance` | Operator | Rebalance segments |
| `GET` | `/clusters/{name}/rebalance/status` | Basic | Get rebalance status |
| `GET` | `/clusters/{name}/standby` | Basic | Get standby status |
| `POST` | `/clusters/{name}/standby/activate` | Admin | Activate standby |
| `POST` | `/clusters/{name}/standby/reinitialize` | Operator | Reinitialize standby |

#### Start Recovery

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/recovery \
  -H "Content-Type: application/json" \
  -d '{
    "type": "incremental",
    "targetSegments": [0, 1],
    "parallelStreams": 4
  }'
```

**Request Body:**

```json
{
  "type": "incremental",
  "targetSegments": [0, 1],
  "targetNode": "node-3",
  "parallelStreams": 4
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | `incremental`, `full`, or `differential`. Other values are rejected with `400 INVALID_REQUEST` |
| `targetSegments` | int[] | No | Specific segments to recover (all failed if omitted) |
| `targetNode` | string | No | Target node for recovery (in-place if omitted) |
| `parallelStreams` | int | No | Parallel copy streams for differential recovery |

**Response (202 Accepted):**

```json
{
  "status": "recovery started",
  "type": "incremental"
}
```

> **Implementation status**: segment recovery is **not implemented yet**. The endpoint validates and accepts the request (the recovery annotation is set on the cluster), but the operator currently only acknowledges it: the annotation is removed, a `RecoveryNotImplemented` Warning event is emitted, and `cloudberry_recovery_operations_total` records `result="noop"`. No segment recovery work is performed. By contrast, `POST /clusters/{name}/standby/activate` **does** perform a real standby promotion (`PromoteStandby` / `pg_promote()`) with at-most-once semantics.

**Error (400 Bad Request — invalid recovery type):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "invalid recovery type \"partial\": must be one of incremental, full, differential"
  }
}
```

#### Rebalance Status

Returns the current rebalance configuration and status for a cluster, including the `DataRedistribution` condition.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/rebalance/status?namespace=cloudberry-test"
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | No | Kubernetes namespace (defaults to operator's configured namespace) |

**Response (200 OK — rebalance configured, completed):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "config": {
    "skewThreshold": 10,
    "parallelism": 2,
    "excludeTables": ["audit_log", "temp_*"]
  },
  "redistribution": {
    "status": "True",
    "reason": "RebalanceCompleted",
    "message": "Rebalance completed successfully",
    "lastTransition": "2026-05-14T10:05:00Z"
  }
}
```

**Response (200 OK — rebalance in progress):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test",
  "config": {
    "skewThreshold": 10,
    "parallelism": 2,
    "excludeTables": ["audit_log", "temp_*"]
  },
  "redistribution": {
    "status": "True",
    "reason": "RebalanceStarted",
    "message": "Rebalance started: threshold=10%, parallelism=2, excluded=[audit_log temp_*]",
    "lastTransition": "2026-05-14T10:00:00Z"
  }
}
```

**Response (200 OK — no rebalance configuration):**

```json
{
  "name": "my-cluster",
  "namespace": "cloudberry-test"
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Cluster name |
| `namespace` | string | Cluster namespace |
| `config` | object | Present only when `spec.segments.rebalance` is configured |
| `config.skewThreshold` | int | Percentage skew threshold |
| `config.parallelism` | int | Number of concurrent table redistributions |
| `config.excludeTables` | string[] | Tables excluded from rebalance (supports glob patterns) |
| `redistribution` | object | Present only when a `DataRedistribution` condition exists |
| `redistribution.status` | string | Condition status (`True` or `False`) |
| `redistribution.reason` | string | Condition reason (`RebalanceStarted`, `RebalanceCompleted`) |
| `redistribution.message` | string | Human-readable description |
| `redistribution.lastTransition` | string | ISO 8601 timestamp of the last condition transition |

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

#### Get Mirroring Status

Returns the current mirroring configuration and synchronization status for a cluster.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/mirroring"
```

**Response (200 OK — mirroring enabled and in sync):**

```json
{
  "enabled": true,
  "layout": "spread",
  "status": "InSync",
  "segments": 4
}
```

**Response (200 OK — mirroring initializing after enable):**

```json
{
  "enabled": true,
  "layout": "group",
  "status": "Initializing",
  "segments": 4
}
```

**Response (200 OK — mirroring syncing):**

```json
{
  "enabled": true,
  "layout": "group",
  "status": "Syncing",
  "segments": 4
}
```

**Response (200 OK — mirroring not configured):**

```json
{
  "enabled": false,
  "status": "NotConfigured"
}
```

**Response (200 OK — mirroring degraded after timeout):**

```json
{
  "enabled": true,
  "layout": "group",
  "status": "Degraded",
  "segments": 4
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `enabled` | bool | Whether mirroring is enabled in the cluster spec |
| `layout` | string | Mirroring layout (`group` or `spread`). Present only when mirroring is enabled |
| `status` | string | Current mirroring status: `NotConfigured`, `Initializing`, `Syncing`, `InSync`, `Degraded`, or `Down` |
| `segments` | int | Number of mirror segments. Present only when mirroring is enabled |

**Mirroring status values:**

| Status | Description |
|--------|-------------|
| `NotConfigured` | Mirroring is not set up |
| `Initializing` | Mirror StatefulSet created, mirrors are being initialized from primaries via WAL replication |
| `Syncing` | Mirrors are actively synchronizing data from primaries. Replication lag is decreasing |
| `InSync` | All mirrors are fully synchronized with their primaries |
| `Degraded` | One or more mirrors are out of sync, or mirroring enable timed out after 30 minutes |
| `Down` | Mirroring is completely down |

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

#### Get Standby Status

```bash
curl -u admin:password \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/standby
```

**Response (200 OK):**

```json
{
  "enabled": true,
  "ready": true
}
```

### Sessions

Session endpoints query `pg_stat_activity` on the cluster's coordinator database via the `DBClientFactory`. Each request creates a short-lived database connection, executes the operation, and closes the connection.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/sessions` | Operator Basic | List active sessions from `pg_stat_activity` |
| `POST` | `/clusters/{name}/sessions/{pid}/cancel` | Operator | Cancel a running query via `pg_cancel_backend()` |
| `DELETE` | `/clusters/{name}/sessions/{pid}` | Operator | Terminate a session via `pg_terminate_backend()` |

#### List Sessions

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/sessions"
```

**Response (200 OK):**

```json
{
  "sessions": [
    {
      "pid": 1234,
      "username": "gpadmin",
      "application": "psql",
      "clientAddress": "10.0.0.1",
      "state": "active",
      "query": "SELECT * FROM orders",
      "queryStart": "2026-05-14T10:00:00Z",
      "duration": "00:05:30"
    },
    {
      "pid": 5678,
      "username": "appuser",
      "application": "pgbench",
      "clientAddress": "10.0.0.2",
      "state": "idle",
      "query": "INSERT INTO logs VALUES ($1)",
      "queryStart": "2026-05-14T09:50:00Z",
      "duration": "00:15:30"
    }
  ],
  "total": 2
}
```

**Session fields:**

| Field | Type | Description |
|-------|------|-------------|
| `pid` | int | PostgreSQL backend process ID |
| `username` | string | Database user running the session |
| `application` | string | Application name (e.g., `psql`, `pgbench`) |
| `clientAddress` | string | Client IP address |
| `state` | string | Session state (`active`, `idle`, `idle in transaction`, etc.) |
| `query` | string | Current or last executed query |
| `queryStart` | string | ISO 8601 timestamp when the current query started |
| `duration` | string | Elapsed time of the current query (e.g., `00:05:30`) |

**Graceful degradation (200 OK — no DB factory):**

When the `DBClientFactory` is not configured, the endpoint returns an empty result with an informational message instead of an error:

```json
{
  "sessions": [],
  "total": 0,
  "message": "database connection not available"
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

**Error (500 Internal Server Error — query failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to list sessions"
  }
}
```

#### Cancel Query

Cancels a running query by calling `pg_cancel_backend()` on the coordinator. The session remains connected.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/sessions/1234/cancel
```

**Response (200 OK):**

```json
{
  "pid": 1234,
  "canceled": true
}
```

The `canceled` field reflects the return value of `pg_cancel_backend()`. A value of `false` means the PID was not found or the query had already completed.

**Graceful degradation (200 OK — no DB factory):**

```json
{
  "pid": 1234,
  "canceled": false,
  "message": "database connection not available"
}
```

**Error (400 Bad Request — invalid PID):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "PID must be a positive integer"
  }
}
```

**Error (400 Bad Request — non-numeric PID):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "invalid PID"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

**Error (500 Internal Server Error — cancel failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to cancel query"
  }
}
```

> **Note**: The PID must be a positive integer. Zero, negative, and non-numeric values are rejected with a `400 Bad Request` response.

#### Terminate Session

Terminates a session by calling `pg_terminate_backend()` on the coordinator. The client is disconnected.

```bash
curl -u admin:password -X DELETE \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/sessions/5678
```

**Response (200 OK):**

```json
{
  "pid": 5678,
  "terminated": true
}
```

The `terminated` field reflects the return value of `pg_terminate_backend()`. A value of `false` means the PID was not found or the session had already ended.

**Graceful degradation (200 OK — no DB factory):**

```json
{
  "pid": 5678,
  "terminated": false,
  "message": "database connection not available"
}
```

**Error (400 Bad Request — invalid PID):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "PID must be a positive integer"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

**Error (500 Internal Server Error — terminate failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to terminate session"
  }
}
```

> **Note**: The PID must be a positive integer. Zero, negative, and non-numeric values are rejected with a `400 Bad Request` response.

### Query History

Query history endpoints provide access to completed queries stored in the `cloudberry_query_history` table. The `cloudberry-query-exporter` sidecar automatically collects completed queries and inserts them into this table. These endpoints query the coordinator database via the `DBClientFactory`.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/queries/history` | Operator Basic | Search query history with filters and pagination |
| `GET` | `/clusters/{name}/queries/history/{qid}` | Operator Basic | Get historical query details (including EXPLAIN plan) |
| `POST` | `/clusters/{name}/queries/history/export` | Operator Basic | Export query history to CSV |

#### List Query History

Returns paginated query history with optional filters. All filters are AND-combined.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/history?namespace=default&limit=20&user=analyst"
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | No | Cluster namespace |
| `pattern` | string | No | Search pattern for query text |
| `patternType` | string | No | `regex` (default) or `wildcard` |
| `user` | string | No | Filter by username |
| `database` | string | No | Filter by database name |
| `resourceGroup` | string | No | Filter by resource group |
| `state` | string | No | Filter by state (`completed`, `cancelled`, `error`) |
| `minDuration` | float | No | Minimum duration in milliseconds |
| `since` | string | No | Start time — RFC 3339 timestamp or Go duration (e.g., `24h`, `30m`) |
| `until` | string | No | End time — RFC 3339 timestamp |
| `limit` | int | No | Page size (default: 50, max: 100) |
| `offset` | int | No | Pagination offset (default: 0) |

**Response (200 OK):**

```json
{
  "items": [
    {
      "id": 42,
      "queryId": "q-1234-1716984000000000000",
      "pid": 1234,
      "username": "analyst",
      "databaseName": "warehouse",
      "queryText": "SELECT * FROM orders WHERE created_at > '2026-01-01'",
      "queryStart": "2026-05-29T10:00:00Z",
      "queryEnd": "2026-05-29T10:00:02.5Z",
      "durationMs": 2500.00,
      "state": "completed",
      "rowsAffected": 15000,
      "cpuTimeMs": 1800.50,
      "memoryBytes": 67108864,
      "spillBytes": 0,
      "diskReadBytes": 134217728,
      "diskWriteBytes": 0,
      "waitEvents": "",
      "resourceGroup": "default_group",
      "createdAt": "2026-05-29T10:00:02.5Z"
    }
  ],
  "total": 156,
  "limit": 50,
  "offset": 0
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `items` | array | List of query history entries |
| `items[].id` | int | Auto-incremented row ID |
| `items[].queryId` | string | Unique query identifier (format: `q-{pid}-{query_start_unix_nano}`) |
| `items[].pid` | int | PostgreSQL backend process ID |
| `items[].username` | string | Database user who ran the query |
| `items[].databaseName` | string | Database the query was executed against |
| `items[].queryText` | string | Full SQL text |
| `items[].queryStart` | string | ISO 8601 timestamp when the query started |
| `items[].queryEnd` | string | ISO 8601 timestamp when the query completed |
| `items[].durationMs` | float | Execution duration in milliseconds |
| `items[].state` | string | Final state (`completed`, `cancelled`, `error`) |
| `items[].rowsAffected` | int | Rows affected or returned |
| `items[].cpuTimeMs` | float | CPU time in milliseconds |
| `items[].memoryBytes` | int | Peak memory usage in bytes |
| `items[].spillBytes` | int | Data spilled to disk in bytes |
| `items[].diskReadBytes` | int | Bytes read from disk |
| `items[].diskWriteBytes` | int | Bytes written to disk |
| `items[].waitEvents` | string | Wait events encountered |
| `items[].resourceGroup` | string | Resource group the query ran in |
| `items[].createdAt` | string | ISO 8601 timestamp when the entry was recorded |
| `total` | int | Total number of matching entries |
| `limit` | int | Page size used |
| `offset` | int | Pagination offset used |

**Graceful degradation (200 OK — no DB factory):**

```json
{
  "items": [],
  "total": 0,
  "limit": 50,
  "offset": 0,
  "message": "database connection not available"
}
```

**Error (400 Bad Request — invalid regex pattern):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "invalid regex pattern \"[invalid\": error parsing regexp: missing closing ]: `[invalid`"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

**Error (500 Internal Server Error — query failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to get query history"
  }
}
```

#### Get Query History Detail

Returns detailed information for a specific historical query, including the EXPLAIN execution plan if collected.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/history/q-1234-1716984000000000000?namespace=default"
```

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Cluster name |
| `qid` | string | Query ID (e.g., `q-1234-1716984000000000000`) |

**Response (200 OK):**

```json
{
  "id": 42,
  "queryId": "q-1234-1716984000000000000",
  "pid": 1234,
  "username": "analyst",
  "databaseName": "warehouse",
  "queryText": "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > 1000",
  "queryStart": "2026-05-29T10:00:00Z",
  "queryEnd": "2026-05-29T10:00:05.2Z",
  "durationMs": 5200.00,
  "state": "completed",
  "rowsAffected": 3200,
  "cpuTimeMs": 4100.25,
  "memoryBytes": 134217728,
  "spillBytes": 268435456,
  "diskReadBytes": 536870912,
  "diskWriteBytes": 134217728,
  "waitEvents": "IO",
  "resourceGroup": "analytics_group",
  "explainPlan": "Gather Motion 4:1  (slice1; segments: 4)\n  ->  Hash Join\n        Hash Cond: (o.customer_id = c.id)\n        ->  Seq Scan on orders o\n              Filter: (total > 1000)\n        ->  Hash\n              ->  Broadcast Motion 4:4  (slice2; segments: 4)\n                    ->  Seq Scan on customers c",
  "errorMessage": "",
  "createdAt": "2026-05-29T10:00:05.2Z"
}
```

The response includes all fields from the list endpoint, plus:

| Field | Type | Description |
|-------|------|-------------|
| `explainPlan` | string | EXPLAIN execution plan (present when plan collection is enabled) |
| `errorMessage` | string | Error message (present when state is `error`) |

**Error (400 Bad Request — missing query ID):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "query ID is required"
  }
}
```

**Error (404 Not Found — query not found):**

```json
{
  "error": {
    "code": "QUERY_NOT_FOUND",
    "message": "historical query \"q-nonexistent\" not found"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (503 Service Unavailable — database not available):**

```json
{
  "error": {
    "code": "DB_NOT_AVAILABLE",
    "message": "database connection not configured"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_CONNECTION_FAILED",
    "message": "failed to connect to database"
  }
}
```

#### Export Query History to CSV

Exports query history as a CSV file. Accepts an optional JSON body to filter the exported data. All matching rows are exported (no pagination limits). The response streams rows directly from the database to avoid buffering the entire result set in memory.

```bash
# Export all history
curl -u admin:password -X POST \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/history/export?namespace=default" \
  -o query-history.csv

# Export with filters
curl -u admin:password -X POST \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/history/export?namespace=default" \
  -H "Content-Type: application/json" \
  -d '{
    "pattern": "SELECT.*FROM orders",
    "patternType": "regex",
    "user": "analyst",
    "database": "warehouse",
    "since": "24h"
  }' \
  -o filtered-history.csv
```

**Request Body** (optional, `Content-Type: application/json`):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pattern` | string | No | Search pattern for query text |
| `patternType` | string | No | `regex` (default) or `wildcard` |
| `user` | string | No | Filter by username |
| `database` | string | No | Filter by database name |
| `resourceGroup` | string | No | Filter by resource group |
| `state` | string | No | Filter by state |
| `since` | string | No | Start time — RFC 3339 or Go duration |
| `until` | string | No | End time — RFC 3339 |

**Response (200 OK):**

```
Content-Type: text/csv
Content-Disposition: attachment; filename="query-history.csv"

query_id,username,database,query_text,start_time,end_time,duration_ms,rows_affected,cpu_time_ms,memory_bytes,spill_bytes,state
q-1234-1716984000000000000,analyst,warehouse,"SELECT * FROM orders WHERE created_at > '2026-01-01'",2026-05-29T10:00:00Z,2026-05-29T10:00:02.5Z,2500.00,15000,1800.50,67108864,0,completed
```

**CSV columns:**

| Column | Description |
|--------|-------------|
| `query_id` | Unique query identifier |
| `username` | Database user |
| `database` | Database name |
| `query_text` | Full SQL text |
| `start_time` | Query start time (RFC 3339) |
| `end_time` | Query end time (RFC 3339) |
| `duration_ms` | Duration in milliseconds |
| `rows_affected` | Rows affected or returned |
| `cpu_time_ms` | CPU time in milliseconds |
| `memory_bytes` | Peak memory usage |
| `spill_bytes` | Data spilled to disk |
| `state` | Final query state |

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (503 Service Unavailable — database not available):**

```json
{
  "error": {
    "code": "DB_NOT_AVAILABLE",
    "message": "database connection not configured"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_CONNECTION_FAILED",
    "message": "failed to connect to database"
  }
}
```

> **Note**: Once CSV streaming begins (HTTP 200 headers sent), errors during row iteration cannot be communicated via HTTP status codes. Partial CSV output may result if the database connection drops mid-export.

### Plan Analysis

The plan analysis endpoint provides static analysis of PostgreSQL/Cloudberry `EXPLAIN ANALYZE` output. No database connection is required — the analysis is purely text-based.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `POST` | `/clusters/{name}/queries/plan-check` | Basic | Run static plan checker on EXPLAIN ANALYZE output |

#### Run Plan Check

Analyzes an EXPLAIN ANALYZE plan text and returns identified performance issues with actionable recommendations.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/plan-check \
  -H "Content-Type: application/json" \
  -d '{
    "planText": "Seq Scan on orders  (cost=0.00..5000.00 rows=100 width=36) (actual time=0.010..10.000 rows=50000 loops=1)\n  Filter: (status = '\''pending'\'')\n  Rows Removed by Filter: 950000\nExecution Time: 10.500 ms"
  }'
```

**Request Body** (`Content-Type: application/json`):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `planText` | string | Yes | Raw EXPLAIN ANALYZE text output |

**Response (200 OK):**

```json
{
  "issues": [
    {
      "severity": "warning",
      "category": "sequential_scan",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Sequential scan on orders returned 50000 rows",
      "recommendation": "Consider creating an index on orders for filter condition (status = 'pending')",
      "details": {
        "actualRows": 50000,
        "filter": "(status = 'pending')",
        "totalCost": 5000.00
      }
    },
    {
      "severity": "warning",
      "category": "row_estimate_mismatch",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Row estimate mismatch on orders: estimated 100 rows, actual 50000 rows (499x off)",
      "recommendation": "Run ANALYZE on the tables involved to update statistics",
      "details": {
        "planRows": 100,
        "actualRows": 50000,
        "ratio": 499.0
      }
    },
    {
      "severity": "warning",
      "category": "excessive_filter_rows",
      "nodeType": "Seq Scan",
      "relation": "orders",
      "description": "Filter removed 19x more rows than returned (950000 removed vs 50000 returned)",
      "recommendation": "Filter removed 19x more rows than returned; consider adding index on filter column",
      "details": {
        "rowsRemoved": 950000,
        "actualRows": 50000,
        "ratio": 19,
        "filter": "(status = 'pending')"
      }
    }
  ],
  "summary": "Found 3 performance issues: 3 warning(s)",
  "totalNodes": 1,
  "executionTime": 10.5
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `issues` | array | List of identified performance issues |
| `issues[].severity` | string | Issue severity: `"warning"` (actionable) or `"info"` (optimization opportunity) |
| `issues[].category` | string | Issue category: `sequential_scan`, `row_estimate_mismatch`, `sort_spill`, `nested_loop_high_rows`, `excessive_filter_rows`, `high_cost_node` |
| `issues[].nodeType` | string | Plan node type where the issue was found |
| `issues[].relation` | string | Table name (if applicable) |
| `issues[].description` | string | Human-readable description |
| `issues[].recommendation` | string | Actionable recommendation |
| `issues[].details` | object | Additional details (varies by category) |
| `summary` | string | Human-readable summary of all issues found |
| `totalNodes` | int | Total number of plan nodes parsed |
| `executionTime` | float | Total execution time from the plan footer (ms), 0 if not present |

**Error (400 Bad Request — empty plan text):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "planText is required"
  }
}
```

**Error (400 Bad Request — parse failure):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "failed to parse plan: empty plan text"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

### Monitor Pause/Resume

Monitor pause/resume endpoints allow operators to freeze query monitoring data at a point in time. While paused, query monitoring endpoints return cached snapshot data with a stale indicator.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `POST` | `/clusters/{name}/queries/monitor/pause` | Operator | Pause the query monitor |
| `POST` | `/clusters/{name}/queries/monitor/resume` | Operator | Resume the query monitor |
| `GET` | `/clusters/{name}/queries/monitor/state` | Basic | Get current monitor state |

#### Pause Monitor

Pauses the query monitor for a cluster. Takes a snapshot of the current query data and stores it in memory. Subsequent requests to query monitoring endpoints return the cached snapshot with a `stale` flag.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/monitor/pause?namespace=default
```

**Response (200 OK):**

```json
{
  "status": "paused",
  "pausedAt": "2026-05-30T10:00:00Z",
  "message": "Query monitor paused"
}
```

**Response (200 OK — already paused):**

```json
{
  "status": "paused",
  "pausedAt": "2026-05-30T10:00:00Z",
  "message": "Query monitor is already paused"
}
```

**Prometheus metric**: `cloudberry_monitor_pause_total` counter is incremented on each pause request.

#### Resume Monitor

Resumes the query monitor for a cluster. Removes the cached snapshot so subsequent requests return fresh data.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/monitor/resume?namespace=default
```

**Response (200 OK):**

```json
{
  "status": "resumed",
  "message": "Query monitor resumed"
}
```

Resuming when not paused succeeds idempotently.

**Prometheus metric**: `cloudberry_monitor_resume_total` counter is incremented on each resume request.

#### Get Monitor State

Returns the current pause/resume state of the query monitor.

```bash
curl -u admin:password \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/monitor/state?namespace=default
```

**Response (200 OK — not paused):**

```json
{
  "paused": false,
  "stale": false
}
```

**Response (200 OK — paused):**

```json
{
  "paused": true,
  "stale": true,
  "pausedAt": "2026-05-30T10:00:00Z"
}
```

### Query Operations

Query operation endpoints provide active query management capabilities including cancellation, resource group reassignment, and CSV export. Operations execute on the coordinator database via the `DBClientFactory`.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `POST` | `/clusters/{name}/queries/{pid}/cancel` | Operator | Cancel a running query |
| `POST` | `/clusters/{name}/queries/{pid}/move` | Operator | Move query to resource group |
| `POST` | `/clusters/{name}/queries/export` | Operator Basic | Export active queries to CSV |

#### Cancel Running Query

Cancels a running query by PID. Executes `pg_cancel_backend()` on the coordinator. The session remains connected but the current query is interrupted.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/1234/cancel \
  -H "Content-Type: application/json" \
  -d '{"reason": "Query running too long"}'
```

**Request Body** (optional, `Content-Type: application/json`):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `reason` | string | No | Human-readable reason for cancellation (logged for audit) |

**Response (200 OK):**

```json
{
  "pid": 1234,
  "canceled": true,
  "reason": "Query running too long"
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `pid` | int | The backend process ID that was targeted |
| `canceled` | bool | `true` if `pg_cancel_backend()` returned true; `false` if PID not found or query already completed |
| `reason` | string | The cancellation reason (echoed back when provided) |

**Prometheus metric**: `cloudberry_query_cancel_total` counter is incremented on each cancel request.

**Error (400 Bad Request — invalid PID):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "PID must be a positive integer"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

**Error (500 Internal Server Error — cancel failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to cancel query"
  }
}
```

#### Move Query to Resource Group

Moves a running query to a different resource group by reassigning the user's resource group via `ALTER ROLE <user> RESOURCE GROUP <target_group>`.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/1234/move \
  -H "Content-Type: application/json" \
  -d '{"targetGroup": "etl"}'
```

**Request Body** (`Content-Type: application/json`):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetGroup` | string | Yes | Name of the target resource group |

**Response (200 OK):**

```json
{
  "pid": 1234,
  "moved": true,
  "targetGroup": "etl",
  "previousGroup": "default_group"
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `pid` | int | The backend process ID that was targeted |
| `moved` | bool | `true` if the resource group reassignment succeeded |
| `targetGroup` | string | The target resource group name |
| `previousGroup` | string | The resource group the query was previously in |

**Prometheus metric**: `cloudberry_query_move_total` counter is incremented on each move request.

**Error (400 Bad Request — missing target group):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "targetGroup is required"
  }
}
```

**Error (400 Bad Request — invalid PID):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "PID must be a positive integer"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (404 Not Found — resource group not found):**

```json
{
  "error": {
    "code": "RESOURCE_GROUP_NOT_FOUND",
    "message": "resource group \"etl\" not found"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

#### Export Active Queries to CSV

Exports all active queries from `pg_stat_activity` as a CSV file. The response streams rows directly from the database.

```bash
curl -u admin:password -X POST \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/queries/export?namespace=default" \
  -o active-queries.csv
```

**Response (200 OK):**

```
Content-Type: text/csv
Content-Disposition: attachment; filename="active-queries.csv"

pid,username,database,state,query,duration,wait_event_type,resource_group
1234,gpadmin,testdb,active,SELECT * FROM orders,,default_group
5678,analyst,mydb,idle,,,analytics
```

**CSV columns:**

| Column | Description |
|--------|-------------|
| `pid` | PostgreSQL backend process ID |
| `username` | Database user |
| `database` | Database name |
| `state` | Session state |
| `query` | Current or last query |
| `duration` | Query duration |
| `wait_event_type` | Wait event type (if any) |
| `resource_group` | Resource group name |

**Prometheus metric**: `cloudberry_active_query_export_total` counter is incremented on each export.

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (503 Service Unavailable — database not available):**

```json
{
  "error": {
    "code": "DB_NOT_AVAILABLE",
    "message": "database connection not configured"
  }
}
```

### Exporter Health

The exporter health endpoint provides a unified view of all Prometheus exporter sidecars deployed for a cluster.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/metrics/exporters` | Basic | List exporter health status |

#### List Exporter Health

Returns the health status of all Prometheus exporters deployed for the cluster, including their availability, last scrape time, and metric count.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/metrics/exporters"
```

**Response (200 OK):**

```json
{
  "exporters": [
    {
      "name": "postgres-exporter",
      "type": "postgres_exporter",
      "port": 9187,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 142,
      "errorMessage": ""
    },
    {
      "name": "cloudberry-query-exporter",
      "type": "cloudberry_query_exporter",
      "port": 9188,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 87,
      "errorMessage": ""
    },
    {
      "name": "node-exporter",
      "type": "node_exporter",
      "port": 9100,
      "healthy": true,
      "lastScrape": "2026-05-30T10:00:15Z",
      "scrapeInterval": "15s",
      "metricsCount": 256,
      "errorMessage": ""
    }
  ],
  "total": 3,
  "healthyCount": 3
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `exporters` | array | List of exporter health entries |
| `exporters[].name` | string | Exporter container name |
| `exporters[].type` | string | Exporter type identifier (`postgres_exporter`, `cloudberry_query_exporter`, `node_exporter`) |
| `exporters[].port` | int | Metrics port number |
| `exporters[].healthy` | bool | `true` if the exporter's `/metrics` endpoint is reachable and returns valid data |
| `exporters[].lastScrape` | string | ISO 8601 timestamp of the last successful scrape |
| `exporters[].scrapeInterval` | string | Configured scrape interval |
| `exporters[].metricsCount` | int | Number of metric families exposed |
| `exporters[].errorMessage` | string | Error message if the exporter is unhealthy (empty when healthy) |
| `total` | int | Total number of exporters |
| `healthyCount` | int | Number of healthy exporters |

**Prometheus metric**: `cloudberry_exporter_health_check_total` counter is incremented on each health check request.

**Response (200 OK — exporter unhealthy):**

```json
{
  "exporters": [
    {
      "name": "postgres-exporter",
      "type": "postgres_exporter",
      "port": 9187,
      "healthy": false,
      "lastScrape": "2026-05-30T09:55:00Z",
      "scrapeInterval": "15s",
      "metricsCount": 0,
      "errorMessage": "connection refused: dial tcp 127.0.0.1:9187: connect: connection refused"
    }
  ],
  "total": 1,
  "healthyCount": 0
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (500 Internal Server Error — health check failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to check exporter health"
  }
}
```

### Resource Groups

Resource group endpoints manage Cloudberry resource groups for workload isolation. Operations execute SQL commands on the coordinator via the `DBClientFactory`.

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/workload/resource-groups` | Basic | List resource groups |
| `POST` | `/clusters/{name}/workload/resource-groups` | Operator | Create a resource group |
| `DELETE` | `/clusters/{name}/workload/resource-groups/{groupName}` | Operator | Delete a resource group |
| `POST` | `/clusters/{name}/workload/resource-groups/{groupName}/assign` | Operator | Assign a role to a resource group |

#### List Resource Groups

Lists resource groups. When a database connection is available, groups are queried from `gp_toolkit.gp_resgroup_status`. Otherwise, the CRD spec is used as a fallback.

```bash
curl -u admin:password \
  "http://operator:8090/api/v1alpha1/clusters/my-cluster/workload/resource-groups"
```

**Response (200 OK):**

```json
{
  "resourceGroups": [
    {
      "name": "analytics",
      "concurrency": 10,
      "cpuMaxPercent": 50,
      "memoryLimit": 30
    }
  ],
  "total": 1
}
```

#### Create Resource Group

Creates a new resource group in the database by executing `CREATE RESOURCE GROUP`.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/workload/resource-groups \
  -H "Content-Type: application/json" \
  -d '{
    "name": "analytics",
    "concurrency": 10,
    "cpuMaxPercent": 50,
    "memoryLimit": 30
  }'
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Resource group name |
| `concurrency` | int | No | Maximum concurrent transactions |
| `cpuMaxPercent` | int | No | Maximum CPU usage percentage (0–100) |
| `memoryLimit` | int | No | Memory limit percentage (0–100) |

**Response (201 Created):**

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30,
  "status": "created"
}
```

**Graceful degradation (201 Created — no DB factory):**

When the `DBClientFactory` is not configured, the endpoint returns a success response with a pending message:

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30,
  "message": "resource group creation pending; database connection not available"
}
```

**Error (400 Bad Request — missing name):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "resource group name is required"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (500 Internal Server Error — database operation failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to create resource group"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

#### Delete Resource Group

Deletes a resource group from the database by executing `DROP RESOURCE GROUP`.

```bash
curl -u admin:password -X DELETE \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/workload/resource-groups/analytics
```

**Response (200 OK):**

```json
{
  "group": "analytics",
  "status": "deleted"
}
```

**Graceful degradation (200 OK — no DB factory):**

```json
{
  "group": "analytics",
  "status": "pending",
  "message": "database connection not available"
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (500 Internal Server Error — database operation failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to drop resource group"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

#### Assign Role to Resource Group

Assigns a database role to a resource group by executing `ALTER ROLE <role> RESOURCE GROUP <group>`.

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/workload/resource-groups/analytics/assign \
  -H "Content-Type: application/json" \
  -d '{"role": "analyst"}'
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `role` | string | Yes | Database role to assign to the resource group |

**Response (200 OK):**

```json
{
  "group": "analytics",
  "role": "analyst",
  "status": "assigned"
}
```

**Graceful degradation (200 OK — no DB factory):**

```json
{
  "group": "analytics",
  "role": "analyst",
  "status": "pending",
  "message": "database connection not available"
}
```

**Error (400 Bad Request — missing role):**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "role is required"
  }
}
```

**Error (404 Not Found — cluster not found):**

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "cluster \"my-cluster\" not found"
  }
}
```

**Error (500 Internal Server Error — database operation failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to assign role to resource group"
  }
}
```

**Error (503 Service Unavailable — database connection failed):**

```json
{
  "error": {
    "code": "DB_UNAVAILABLE",
    "message": "cannot connect to database"
  }
}
```

### Maintenance

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `POST` | `/clusters/{name}/maintenance/vacuum` | Operator | Run vacuum |
| `POST` | `/clusters/{name}/maintenance/analyze` | Operator | Run analyze |
| `POST` | `/clusters/{name}/maintenance/reindex` | Operator | Run reindex |
| `GET` | `/clusters/{name}/maintenance/jobs` | Operator Basic | List maintenance jobs |

#### Run Vacuum

```bash
curl -u admin:password -X POST \
  http://operator:8090/api/v1alpha1/clusters/my-cluster/maintenance/vacuum
```

**Response (202 Accepted):**

```json
{
  "status": "vacuum initiated"
}
```

### Authentication Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/auth/roles` | Admin | List roles |
| `POST` | `/clusters/{name}/auth/roles` | Admin | Create role |
| `PUT` | `/clusters/{name}/auth/roles/{role}` | Admin | Update role |
| `DELETE` | `/clusters/{name}/auth/roles/{role}` | Admin | Delete role |
| `POST` | `/auth/rotate-password` | Admin | Rotate admin password |

#### Rotate Admin Password

Generates a new cryptographically secure random password, updates the K8s Secret `cloudberry-operator-admin-password`, and refreshes the in-memory credential store immediately. No operator restart is required.

The new password is **not** returned in the response for security reasons. Retrieve it from the K8s Secret after rotation:

```bash
kubectl get secret cloudberry-operator-admin-password -n cloudberry-system \
  -o jsonpath='{.data.password}' | base64 -d
```

**Request:**

```bash
curl -u admin:current-password -X POST \
  http://operator:8090/api/v1alpha1/auth/rotate-password
```

No request body is required.

**Response (200 OK):**

```json
{
  "status": "rotated",
  "message": "Admin password rotated successfully"
}
```

**Prometheus metric**: `cloudberry_password_rotation_total` counter is incremented on each successful rotation.

**CLI equivalent:**

```bash
cloudberry-ctl auth rotate-password --cluster my-cluster
```

**Error (500 Internal Server Error — credential store not configured):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "credential store not configured"
  }
}
```

**Error (500 Internal Server Error — password generation failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to generate new password"
  }
}
```

**Error (500 Internal Server Error — K8s Secret update failed):**

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "failed to update admin password secret"
  }
}
```

**Error (401 Unauthorized — not authenticated):**

```json
{
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Missing or invalid Authorization header"
  }
}
```

**Error (403 Forbidden — insufficient permissions):**

```json
{
  "error": {
    "code": "FORBIDDEN",
    "message": "insufficient permissions: requires Admin"
  }
}
```

> **Note**: This endpoint is not cluster-scoped — it rotates the operator-level admin password used for API authentication. The path is `/api/v1alpha1/auth/rotate-password` (not under `/clusters/{name}/`).

## Request/Response Schemas

### Common Response Headers

All responses include security headers:

```
Cache-Control: no-store
Content-Security-Policy: default-src 'self'
Content-Type: application/json
Permissions-Policy: camera=(), microphone=()
Referrer-Policy: strict-origin-when-cross-origin
Strict-Transport-Security: max-age=31536000; includeSubDomains
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
```

## Error Handling

### Error Response Format

All errors follow a consistent JSON format:

```json
{
  "error": {
    "code": "CLUSTER_NOT_FOUND",
    "message": "Cluster 'my-cluster' not found in namespace 'cloudberry-test'",
    "details": {
      "cluster": "my-cluster",
      "namespace": "cloudberry-test"
    }
  }
}
```

### Error Codes

| HTTP Status | Code | Description |
|-------------|------|-------------|
| 400 | `INVALID_REQUEST` | Malformed request body or invalid parameters |
| 401 | `UNAUTHORIZED` | Missing or invalid credentials |
| 403 | `FORBIDDEN` | Insufficient permissions for the requested operation |
| 404 | `CLUSTER_NOT_FOUND` | Cluster does not exist |
| 404 | `SEGMENT_NOT_FOUND` | Segment does not exist |
| 404 | `QUERY_NOT_FOUND` | Historical query does not exist (query history detail) |
| 409 | `CONFLICT` | Operation conflicts with current cluster state |
| 422 | `VALIDATION_ERROR` | Request validation failed |
| 429 | `RATE_LIMITED` | Rate limit exceeded |
| 500 | `INTERNAL_ERROR` | Unexpected server error |
| 503 | `SERVICE_UNAVAILABLE` | Operator not ready |
| 503 | `DB_UNAVAILABLE` | Cannot connect to the cluster's database (session operations) |
| 503 | `DB_NOT_AVAILABLE` | Database connection not configured (query history) |
| 503 | `DB_CONNECTION_FAILED` | Failed to connect to coordinator database (query history) |

### Error Examples

**401 Unauthorized:**

```json
{
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Missing or invalid Authorization header"
  }
}
```

**403 Forbidden:**

```json
{
  "error": {
    "code": "FORBIDDEN",
    "message": "Permission 'Admin' required, current permission: 'Basic'"
  }
}
```

**409 Conflict:**

```json
{
  "error": {
    "code": "CONFLICT",
    "message": "Cannot start cluster: cluster is already running"
  }
}
```

## Pagination

List endpoints support pagination via query parameters:

```
GET /clusters/{name}/sessions?limit=50&offset=0
```

| Parameter | Type | Default | Max | Description |
|-----------|------|---------|-----|-------------|
| `limit` | int | 50 | 100 | Maximum items per page |
| `offset` | int | 0 | — | Number of items to skip |

**Paginated Response:**

```json
{
  "items": [...],
  "total": 150,
  "limit": 50,
  "offset": 0
}
```

## Monitoring Disabled Response

When `spec.queryMonitoring.enabled` is `false` (or the `queryMonitoring` section is omitted), all monitoring-specific endpoints return HTTP 200 with the following response body:

```json
{
  "monitoringEnabled": false,
  "message": "query monitoring is not enabled for this cluster"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `monitoringEnabled` | bool | Always `false` when monitoring is disabled |
| `message` | string | Human-readable explanation |

**Affected endpoints**: `GET /queries`, `GET /queries/active`, `GET /queries/history`, `GET /queries/history/{qid}`, `POST /queries/history/export`, `GET /queries/monitor/state`, `POST /queries/monitor/pause`, `GET /metrics/exporters`.

**Unaffected endpoints**: `GET /sessions`, `POST /queries/plan-check`, `POST /queries/{pid}/cancel`, `POST /queries/{pid}/move`, `GET /queries/{pid}` continue to function normally regardless of the monitoring enabled state.

The operator records a `cloudberry_monitoring_disabled_access_total` Prometheus counter metric (labeled by `cluster` and `namespace`) each time a monitoring endpoint is accessed while monitoring is disabled.

## Guest Access

When `guestAccess: true` is set in a cluster's `queryMonitoring` spec, certain read-only endpoints allow unauthenticated access. Guest users receive a `Basic` permission level identity.

### Guest-Enabled Endpoints

| Endpoint | Permission | Guest Result |
|----------|-----------|-------------|
| `GET /clusters/{name}/queries/active` | Basic | 200 OK |
| `GET /clusters/{name}/metrics/exporters` | Basic | 200 OK |
| `GET /clusters/{name}/queries` | Operator Basic | 403 Forbidden (guest has Basic) |

### Guest Access Rules

1. **GET/HEAD/OPTIONS only**: POST, PUT, and DELETE requests always return `401 Unauthorized` for unauthenticated users
2. **Per-cluster**: Guest access is checked against the cluster's `spec.queryMonitoring.guestAccess` field
3. **Auth header priority**: If an `Authorization` header is present, normal authentication is used
4. **Permission enforcement**: Guest identity has `Basic` permission — endpoints requiring higher permissions return `403 Forbidden`

### Permission Levels Per Endpoint

| Permission Level | Endpoints |
|-----------------|-----------|
| **Basic** | `GET /clusters`, `GET /clusters/{name}`, `GET /clusters/{name}/status`, `GET /clusters/{name}/queries/active`, `GET /clusters/{name}/metrics/exporters`, `GET /clusters/{name}/segments`, `GET /clusters/{name}/backups`, `GET /clusters/{name}/storage/*` |
| **Operator Basic** | `GET /clusters/{name}/config`, `GET /clusters/{name}/sessions`, `GET /clusters/{name}/queries`, `GET /clusters/{name}/queries/{pid}`, `GET /clusters/{name}/queries/history` |
| **Operator** | `POST /clusters/{name}/start`, `POST /clusters/{name}/stop`, `POST /clusters/{name}/queries/{pid}/cancel`, `POST /clusters/{name}/queries/{pid}/move`, `POST /clusters/{name}/maintenance/*` |
| **Admin** | `POST /clusters`, `DELETE /clusters/{name}`, `POST /clusters/{name}/standby/activate`, `POST /auth/rotate-password` |

## Rate Limiting

The API enforces per-IP token bucket rate limiting on all authenticated endpoints. Rate limiting is applied **before** authentication to protect against brute-force credential attacks.

- **Default limit**: 10 requests per minute per client IP. Configurable via the `api-rate-limit` config key / `--api-rate-limit` flag / `CLOUDBERRY_API_RATE_LIMIT` environment variable; set `0` to disable (useful for performance testing)
- **Observability**: rejections are counted on `cloudberry_api_rate_limit_rejections_total{route}` (route template label)
- **Algorithm**: Token bucket with automatic refill based on elapsed time
- **IP extraction**: Uses `RemoteAddr` by default. `X-Forwarded-For` and `X-Real-IP` headers are only trusted when the direct connection comes from a configured trusted proxy CIDR range. This prevents header spoofing attacks
- **Trusted proxies**: Configure trusted proxy CIDR ranges (e.g., `10.0.0.0/8`) to enable proxy header trust. When no trusted proxies are configured (the default), only `RemoteAddr` is used
- **Cleanup**: Inactive entries are removed every 5 minutes
- **Graceful shutdown**: The rate limiter's `Stop()` method terminates the background cleanup goroutine to prevent goroutine leaks

When the rate limit is exceeded, the API returns:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 7
```

```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "too many requests, please retry later"
  }
}
```

The `Retry-After` value is calculated from the token refill rate. Clients should wait at least this many seconds before retrying.

## HTTP Server Timeouts

The API server enforces the following timeouts to prevent resource exhaustion from slow or malicious clients:

| Timeout | Value | Description |
|---------|-------|-------------|
| `ReadTimeout` | 30s | Maximum duration for reading the entire request, including the body |
| `WriteTimeout` | 60s | Maximum duration before timing out writes of the response |
| `IdleTimeout` | 120s | Maximum time to wait for the next request when keep-alives are enabled |

## Request Body Limits

All endpoints that accept a request body enforce a maximum body size of **1 MiB** (1,048,576 bytes). Requests exceeding this limit receive a `400 Bad Request` response.

## Response Body Limits (CLI)

The `cloudberry-ctl` CLI enforces a maximum response body size of **10 MiB** (10,485,760 bytes) when reading API responses. This prevents the CLI from consuming excessive memory if the server returns an unexpectedly large response. Responses exceeding this limit are truncated.

## API Observability

Every API request is instrumented:

- **Metrics** — `cloudberry_api_requests_total{route,method,code}`, `cloudberry_api_request_duration_seconds{route,method}`, `cloudberry_api_requests_in_flight`, and `cloudberry_api_rate_limit_rejections_total{route}`. The `route` label is always the matched route **template** (e.g. `/api/v1alpha1/clusters/{name}`), never the raw path, so label cardinality stays bounded. Recording is **panic-safe**: the in-flight gauge is decremented and the request recorded via `defer`, so a panicking handler can never leak the gauge upward.
- **Tracing** — when telemetry is enabled, a server span is opened per request and renamed after routing to `<METHOD> <route template>` (raw path preserved in the `http.target` attribute); the span is marked `Error` on HTTP status `>= 400` and inbound W3C trace context is propagated. Handler-level child spans (`auth.*`, `db.*`) nest underneath.

See the [User Guide — Monitoring and Observability](user-guide.md#monitoring-and-observability) for the full metric and span reference.

## Data Loading Endpoints

The full data-loading REST surface (P.1–P.15) is **Implemented and serving real
data** (Scenario 107 flipped the final five job mutations + PXF servers CRUD +
job logs + external-tables to FULL; the PXF lifecycle routes landed in
Scenario 95). Status per route (`FULL` = serving real data):

| ID | Method | Path | Perm | Status |
|----|--------|------|------|--------|
| P.7 | GET | `/api/v1alpha1/clusters/{name}/data-loading/jobs` | Basic | **FULL** (lists `spec.dataLoading.jobs`) |
| P.9 | GET | `/api/v1alpha1/clusters/{name}/data-loading/jobs/{job}` | Basic | **FULL** (reads from spec) |
| P.8 | POST | `/api/v1alpha1/clusters/{name}/data-loading/jobs` | Operator | **FULL** (`201`; `409 JOB_EXISTS`; `400` unknown server) |
| P.10 | PUT | `/api/v1alpha1/clusters/{name}/data-loading/jobs/{job}` | Operator | **FULL** (`200`) |
| P.11 | DELETE | `/api/v1alpha1/clusters/{name}/data-loading/jobs/{job}` | Admin | **FULL** (best-effort deletes the spawned Job) |
| P.12 | POST | `/api/v1alpha1/clusters/{name}/data-loading/jobs/{job}/start` | Operator | **FULL** (creates a REAL one-off `batchv1.Job`; `202`; `409 JOB_ALREADY_RUNNING`) |
| P.13 | POST | `/api/v1alpha1/clusters/{name}/data-loading/jobs/{job}/stop` | Operator | **FULL** (deletes Job / suspends CronJob; `202`; idempotent `200`) |
| P.14 | GET | `/api/v1alpha1/clusters/{name}/data-loading/jobs/{job}/logs` | Basic | **FULL** (streams real Job pod logs; `?follow`/`?tailLines`; `501 LOGS_NOT_AVAILABLE` with no clientset) |
| P.1 | GET | `/api/v1alpha1/clusters/{name}/data-loading/pxf/status` | Basic | **FULL** (honest sidecar readiness) |
| P.6 | POST | `/api/v1alpha1/clusters/{name}/data-loading/pxf/sync` | Operator | **FULL** (`202`; ConfigMap refresh + roll) |
| P.2 | GET | `/api/v1alpha1/clusters/{name}/data-loading/pxf/servers[/{server}]` | Basic | **FULL** (REFERENCES only — no literal secrets; `404 SERVER_NOT_FOUND`) |
| P.3 | POST | `/api/v1alpha1/clusters/{name}/data-loading/pxf/servers` | Operator | **FULL** (`201` rendered `<server>__*.xml`; `409 SERVER_EXISTS`) |
| P.4 | PUT | `/api/v1alpha1/clusters/{name}/data-loading/pxf/servers/{server}` | Operator | **FULL** (`200` rendered config) |
| P.5 | DELETE | `/api/v1alpha1/clusters/{name}/data-loading/pxf/servers/{server}` | Admin | **FULL** (`409 SERVER_IN_USE` when referenced — mirrors W.9) |
| P.15 | GET | `/api/v1alpha1/clusters/{name}/data-loading/external-tables` | Basic | **FULL** (`{observed, observedAvailable, expected}` — live catalog or honest-absent) |

New error codes (Scenario 107): `SERVER_NOT_FOUND` (404), `SERVER_EXISTS` (409),
`SERVER_IN_USE` (409), `JOB_EXISTS` (409), `JOB_ALREADY_RUNNING` (409),
`LOGS_NOT_AVAILABLE` (501). Permissions: Basic (read/status/logs/external-tables),
Operator (create/update/start/stop/sync), Admin (delete).

**Honesty notes.** P.14 streams REAL Job pod logs (it returns the honest
`501 LOGS_NOT_AVAILABLE` only when no Kubernetes clientset is wired). P.15's
`observed` reflects a live `pg_exttable` + foreign-table probe
(`db.Client.ListExternalTables`) — `null` with `observedAvailable:false` when the
DB is unreachable, **never synthesized** — while `expected` (spec-derived would-be
tables) is kept separate and never claimed to "exist". See
[spec 12 §Implementation Status](../specifications/12-data-loading-spec.md#implementation-status).

The `spec.dataLoading` CRD model is the **PXF (Platform Extension Framework)**
model — `pxf` (servers, extensions, custom connectors), `gpfdist`, and
`jobs[]` of `type: pxf|gpload` (`pxfJob`/`gploadJob`). The old simplified model
(`streamingServer`; `jobs[].type: s3|kafka|rabbitmq` with
`s3Source`/`kafkaSource`/`rabbitmqSource`) was **replaced** and removed. CRs are
validated by the admission webhook rules `W.1`–`W.15` (gated on
`dataLoading.enabled: true`); see
[spec 12 §Webhook Validation](../specifications/12-data-loading-spec.md#webhook-validation)
and Scenario 89.

## Input Validation

The API validates all input parameters:

| Validation | Rule | Error Code |
|-----------|------|------------|
| Cluster name | Must be a valid DNS-1123 subdomain (lowercase alphanumeric, `-`, max 253 chars) | `INVALID_REQUEST` |
| Namespace | Must be a valid DNS-1123 subdomain (if provided) | `INVALID_REQUEST` |
| Path parameters | All path parameters (cluster name, namespace, resource group name, role name) are validated against a SQL identifier regex (`^[a-zA-Z_][a-zA-Z0-9_-]*$`). This prevents SQL injection and path traversal attacks | `INVALID_REQUEST` |
| Recovery type | Must be one of `incremental`, `full`, or `differential`. Other values are rejected | `INVALID_REQUEST` |
| PID (sessions) | Must be a valid positive integer (> 0). Zero, negative, and non-numeric values are rejected | `INVALID_REQUEST` |
| Request body | Must be valid JSON | `INVALID_REQUEST` |
| Body size | Must not exceed 1 MiB | `INVALID_REQUEST` |

## Annotations Reference

The operator uses annotations on `CloudberryCluster` resources to trigger actions and track state.

### Action Annotations (`avsoft.io/action`)

Set this annotation to trigger lifecycle operations. The operator processes and removes the annotation after handling.

| Value | Description | Resulting Phase |
|-------|-------------|-----------------|
| `start` | Normal start — all components | `Running` |
| `start-restricted` | Coordinator only, superuser connections | `Restricted` |
| `start-maintenance` | Coordinator only, utility mode | `Maintenance` |
| `stop` | Smart stop — wait for clients to disconnect | `Stopped` |
| `stop-fast` | Fast stop — rollback active transactions | `Stopped` |
| `stop-immediate` | Immediate stop — abort all connections | `Stopped` |
| `restart` | Stop then start all components | `Running` |
| `rebalance` | Rebalance segment data distribution | `Running` |
| `activate-standby` | Promote standby to coordinator | `Running` |

### Maintenance Annotations (`avsoft.io/maintenance`)

Set this annotation to trigger maintenance operations. The operator creates a Kubernetes Job and removes the annotation.

| Value | Description | SQL Command |
|-------|-------------|-------------|
| `vacuum` | Regular vacuum | `VACUUM` |
| `vacuum-analyze` | Vacuum with statistics refresh | `VACUUM ANALYZE` |
| `vacuum-full` | Full vacuum (exclusive lock) | `VACUUM FULL` |
| `analyze` | Refresh planner statistics | `ANALYZE` |
| `reindex` | Rebuild indexes | `REINDEX DATABASE` |
| `rebalance` | Segment data rebalance | `ANALYZE` (maps to `gpexpand` in production) |

### Rolling Restart Annotation (`avsoft.io/rolling-restart`)

This annotation is managed by the operator to track rolling restart progress. It contains a JSON payload:

```json
{
  "phase": "primaries",
  "startedAt": "2026-05-14T10:00:00Z",
  "restartParams": ["shared_buffers", "max_connections"]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current restart phase: `mirrors`, `primaries`, `standby`, `coordinator`, `completed` |
| `startedAt` | string | ISO 8601 timestamp when the rolling restart started |
| `restartParams` | string[] | List of parameter names that triggered the restart |

### Upgrade Annotation (`avsoft.io/upgrade`)

This annotation is managed by the operator to track in-progress cluster upgrade state. It contains a JSON payload:

```json
{
  "previousImage": "postgres:16",
  "previousVersion": "7.1.0",
  "phase": "primaries",
  "startedAt": "2026-05-15T10:00:00Z",
  "phaseStartedAt": "2026-05-15T10:01:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `previousImage` | string | Container image before the upgrade (used for rollback) |
| `previousVersion` | string | `status.clusterVersion` before the upgrade (used for rollback) |
| `phase` | string | Current upgrade phase: `mirrors`, `primaries`, `standby`, `coordinator`, `verify` |
| `startedAt` | string | ISO 8601 / RFC 3339 timestamp when the upgrade was initiated |
| `phaseStartedAt` | string | ISO 8601 / RFC 3339 timestamp when the current phase started. Reset on each phase transition. Used for 10-minute per-phase timeout detection |

The annotation is set when an upgrade begins and removed when the upgrade completes (success or rollback). The presence of this annotation indicates an upgrade is in progress.

### Other Annotations

| Annotation | Description |
|------------|-------------|
| `avsoft.io/config-hash` | Hash of the current configuration for change detection |
| `avsoft.io/restart-trigger` | Triggers a pod restart when changed |
| `avsoft.io/restart-pending` | Indicates a full cluster restart is in progress |
| `avsoft.io/recovery` | Triggers recovery operations (`incremental`, `full`, `differential`) |
| `avsoft.io/confirm-scale-in` | Set to `"true"` to confirm a scale-in of more than 50% of segments |
| `avsoft.io/scale-started` | Managed by operator — RFC 3339 timestamp tracking when a scale operation started. Used for timeout detection (10 minutes). Removed on success or failure |
| `avsoft.io/upgrade` | Managed by operator — JSON state tracking an in-progress cluster upgrade. Contains previousImage, previousVersion, phase, startedAt, and phaseStartedAt. Used for phase-by-phase upgrade progression and rollback. Removed on success or rollback |
| `avsoft.io/tls-cert-checksum` | Managed by operator — pod-template annotation on the coordinator/standby/segment StatefulSets carrying a checksum of the cluster TLS certificate Secret data (`auth.ssl.certSecret`). A certificate rotation changes the checksum and rolls the cluster pods exactly once; identical Secret data never triggers a rollout |

### Status Phases

The `status.phase` field reflects the current cluster lifecycle state:

| Phase | Description |
|-------|-------------|
| `Pending` | Cluster resource created, waiting for initialization |
| `Initializing` | StatefulSets and Services are being created |
| `Running` | All components are running and healthy |
| `Updating` | Cluster spec changed, resources are being updated |
| `Scaling` | Segment count is being changed |
| `Stopping` | Cluster is shutting down (scale-down in progress) |
| `Stopped` | All pods are scaled to zero |
| `Restricted` | Coordinator only, superuser connections only |
| `Maintenance` | Coordinator only, utility mode |
| `Failed` | An error occurred during reconciliation |
| `Deleting` | Cluster is being deleted |

## Webhook Endpoints

The operator registers Kubernetes admission webhooks for `CloudberryCluster` resources.

### Validating Webhook

```
POST /validate-cloudberry-example-com-v1alpha1-cloudberrycluster
```

Validates `CloudberryCluster` resources before admission. Enforces:

- **Cross-namespace name uniqueness**: Rejects creation if a `CloudberryCluster` with the same name already exists in any namespace. This prevents naming collisions across the entire Kubernetes cluster
- `segments.count >= 1`
- Spread mirroring requires hosts > `primariesPerHost`
- Enabling mirroring requires cluster to be in `Running` phase with sufficient nodes for the layout
- Changing mirroring layout while mirroring is enabled is rejected (disable first, then re-enable)
- OIDC enabled requires `issuerURL` and `clientID`
- Vault enabled requires `address`
- Valid parameter names in `config.parameters`
- `deletionPolicy` is `Retain` or `Delete`
- `backup.destination.s3.vaultSecret.path` (when set) must be non-empty and must not start with `/`; the explicit KV-v2 request form (`<mount>/data/<rest>`) is accepted with an admission **warning** suggesting the logical path — the operator injects the `data/` segment automatically for KV-v2 mounts

**Duplicate name rejection example:**

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "CloudberryCluster with name \"my-cluster\" already exists in namespace \"other-ns\""
  }
}
```

### Mutating Webhook

```
POST /mutate-cloudberry-example-com-v1alpha1-cloudberrycluster
```

Sets defaults on `CloudberryCluster` resources:

| Field | Default |
|-------|---------|
| `version` | `"2.1.0"` |
| `image` | `"cloudberrydb/cloudberry:2.1.0"` |
| `imagePullPolicy` | `IfNotPresent` |
| `coordinator.replicas` | `1` |
| `coordinator.port` | `5432` (validated: 1–65535) |
| `segments.primariesPerHost` | `2` |
| `segments.mirroring.enabled` | `true` |
| `segments.mirroring.layout` | `group` |
| `segments.antiAffinity` | `preferred` |
| `auth.basic.enabled` | `true` |
| `auth.basic.adminUser` | `gpadmin` |
| `backup.image` (when `backup.enabled`) | `"cloudberry-backup:2.1.0"` — the official backup toolchain image. A backup-capable image must contain `kubectl` (the backup/restore Jobs `kubectl exec` into the coordinator pod) and `gpbackup`/`gprestore`/`gpbackup_s3_plugin`; the base database image is **not** sufficient |
| `ha.ftsProbeInterval` | `60` |
| `ha.ftsProbeTimeout` | `20` |
| `ha.ftsProbeRetries` | `5` |
| `ha.checksums` | `true` |
| `monitoring.enabled` | `true` |
| `monitoring.metricsPort` | `9187` |
| `deletionPolicy` | `Retain` |

### Kubernetes Events Reference

The operator emits Kubernetes events for significant state changes. Events related to FTS and automatic failover:

| Event | Type | Description |
|-------|------|-------------|
| `SegmentFailover` | Warning | Primary segment failed and mirror promotion was triggered. Includes content ID, original primary hostname, and new primary hostname |
| `SegmentFailoverCompleted` | Normal | Segment failover completed successfully |
| `MirroringDegraded` | Warning | One or more segments are down; includes count of failed segments |

**Example `SegmentFailover` event message:**

```
Segment failover completed: contentID=0, original primary=my-cluster-segment-primary-0, new primary=my-cluster-segment-mirror-0
```

### FTS and Failover Metrics

The operator exposes the following Prometheus metrics related to FTS probing and automatic failover:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_fts_probe_total` | Counter | `cluster`, `namespace`, `result` | Total FTS probe executions. `result` is `success`, `failure`, or `degraded` |
| `cloudberry_fts_failover_total` | Counter | `cluster`, `namespace` | Total automatic failover events triggered. Incremented once per failover cycle (not per segment) |
| `cloudberry_segments_failed` | Gauge | `cluster`, `namespace` | Number of currently failed segments |
| `cloudberry_replication_lag_bytes` | Gauge | `cluster`, `namespace`, `segment` | Replication lag in bytes per mirror segment |

```promql
# Failover events in the last hour
increase(cloudberry_fts_failover_total{cluster="my-cluster"}[1h])

# FTS probe failure rate
rate(cloudberry_fts_probe_total{result="failure"}[5m])

# Currently failed segments
cloudberry_segments_failed{cluster="my-cluster"}
```

### Security, Admission, and Lifecycle Metrics

The operator exposes additional Prometheus metrics covering webhook/certificate rotation, Vault operations, admission decisions, and cluster lifecycle workflows (upgrades, rolling restarts, and recovery):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_cert_rotation_total` | Counter | `component`, `source`, `result` | Webhook/cert TLS rotation events. `source` is `vault-pki` or `self-signed`; `result` is `success` or `error` |
| `cloudberry_cert_expiry_seconds` | Gauge | `component` | Seconds until the certificate expires per component |
| `cloudberry_vault_operations_total` | Counter | `operation`, `result` | Vault auth/read/write operations. `operation` is `auth`, `read`, or `write`; `result` is `success` or `error` |
| `cloudberry_vault_operation_duration_seconds` | Histogram | `operation` | Duration of Vault operations in seconds |
| `cloudberry_webhook_admission_total` | Counter | `webhook`, `operation`, `result` | Validating/mutating admission outcomes. `webhook` is `validating` or `mutating`; `operation` is `create`, `update`, or `delete`; `result` is `allowed`, `denied`, or `error` |
| `cloudberry_upgrade_operations_total` | Counter | `cluster`, `namespace`, `result` | Cluster upgrade operations. `result` is `started`, `completed`, `rollback`, or `failed` |
| `cloudberry_rolling_restart_total` | Counter | `cluster`, `namespace`, `result` | Rolling restart operations. `result` is `started`, `completed`, or `failed` |
| `cloudberry_recovery_operations_total` | Counter | `cluster`, `namespace`, `type`, `result` | Segment recovery operations. `type` is `incremental`, `full`, or `differential`; controller-side `result` is `started`, `completed`, `failed`, or `noop`; the recovery API endpoint also records the **request-side** outcome with `result` `requested` (accepted) or `error` (rejected) |

```promql
# Certificates expiring within the next 7 days
cloudberry_cert_expiry_seconds < 604800

# Webhook denials over the last hour
increase(cloudberry_webhook_admission_total{result="denied"}[1h])

# Upgrade rollbacks per cluster (last 24 hours)
increase(cloudberry_upgrade_operations_total{result="rollback"}[24h])
```

### API Business and DDL Metrics

The REST API server records request-side counters for the cluster-lifecycle,
workload-management, and PXF-sync actions it serves. These are the **request-side**
view (was the action accepted/attempted?), complementary to the controller-side
outcome metrics (`cloudberry_maintenance_operations_total`,
`cloudberry_pxf_servers_changed_total`, etc.) that report what actually happened:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_api_cluster_lifecycle_requests_total` | Counter | `operation`, `result` | Cluster lifecycle/maintenance actions requested via the REST API, incremented at the `setClusterAnnotation`/`setMaintenanceAnnotation` choke points and `handleUpdateConfig`. `operation` is `start`, `stop`, `restart`, `reload`, `activate-standby`, `rebalance`, `vacuum`, `analyze`, `reindex`, or `config-update`; `result` is `accepted` or `error` |
| `cloudberry_api_workload_operations_total` | Counter | `kind`, `operation`, `result` | Workload-management DDL requested via the API. `kind` is `resource_group`, `resource_queue`, or `rule`; `operation` is `create`, `update`, `delete`, or `assign`; `result` is `success` or `error` |
| `cloudberry_pxf_sync_total` | Counter | `cluster`, `namespace`, `result` | PXF servers sync **request** outcomes (the request-side counterpart to the honest `cloudberry_pxf_servers_changed_total` force-pair counter, which only fires on a real ConfigMap diff). `result` is `success` or `error` |

```promql
# Cluster lifecycle requests rejected over the last hour
increase(cloudberry_api_cluster_lifecycle_requests_total{result="error"}[1h])

# Workload DDL operations by kind (last 24 hours)
sum by (kind) (increase(cloudberry_api_workload_operations_total{result="success"}[24h]))

# Failed PXF sync requests per cluster
increase(cloudberry_pxf_sync_total{result="error"}[1h])
```

### Control-Plane Setup Metrics

The control-plane sub-reconcilers record four honest control-plane outcome counters.
Each increments **only** at the real outcome (never on a no-op/skip), so a
persistently-failing provisioning, extension setup, or role setup is visible:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_gpfdist_reconcile_total` | Counter | `cluster`, `namespace`, `operation`, `result` | Operator-side gpfdist provisioning reconcile outcome. Incremented at the real Kubernetes create/update/delete outcomes in `reconcileGpfdist`/`ensureGpfdist*`. `operation` is `pvc`, `deployment`, `service`, or `delete`; `result` is `success` or `error` |
| `cloudberry_pxf_extension_setup_total` | Counter | `cluster`, `namespace`, `result` | PXF client-extension setup attempt outcome (`setupPXFExtensions` DB round-trip). `result` is `installed` (≥1 extension created), `absent` (DB reachable but 0 installed — e.g. `pxf` unavailable in the image / DB in recovery), or `error` (hard connectivity/setup failure). The honest `absent` value means "reachable, nothing installed" — it is **not** a failure |
| `cloudberry_dataloader_role_setup_total` | Counter | `cluster`, `namespace`, `result` | Outcome of the dedicated least-privilege data-loader role setup (`EnsureDataLoaderRole`, security control SE.6, in `dataload_controller.go`) — the sibling of `cloudberry_pxf_extension_setup_total`. `result` is `success` or `error`. This operation previously only logged a `Warn` on failure with no metric |
| `cloudberry_exporter_role_setup_total` | Counter | `cluster`, `namespace`, `result` | Outcome of the monitoring exporter role provisioning (`setupExporterRole` DB round-trip, in `admin_controller.go`) — the third sibling of `cloudberry_pxf_extension_setup_total` and `cloudberry_dataloader_role_setup_total`. `result` is `success` or `error`; recorder method `RecordExporterRoleSetup`. This operation previously only logged a `Warn` on failure with no metric, so all three best-effort role-setup DB round-trips are now uniformly observable |

```promql
# gpfdist provisioning error rate by operation (last 5 minutes)
sum by (operation) (rate(cloudberry_gpfdist_reconcile_total{result="error"}[5m]))

# PXF extension setups that found the DB reachable but installed nothing (last hour)
increase(cloudberry_pxf_extension_setup_total{result="absent"}[1h])

# Data-loader role setup failures (last hour)
increase(cloudberry_dataloader_role_setup_total{result="error"}[1h])

# Exporter role setup failures (last hour)
increase(cloudberry_exporter_role_setup_total{result="error"}[1h])
```

### Database Client Query Metrics

Both the mutating/DDL **and** the read-path `db.Client` methods record
`cloudberry_db_query_duration_seconds` with their method name as the `operation` label
(and emit a matching `db.<Method>` OTEL span), so the read and write sides are symmetric.

The 15 mutating/DDL operations are: `SetParameter`, `ReloadConfig`, `CreateRole`,
`AlterRole`, `DropRole`, `Vacuum`, `Analyze`, `Reindex`, `CreateResourceGroup`,
`AlterResourceGroup`, `DropResourceGroup`, `AssignRoleResourceGroup`,
`CreateResourceQueue`, `DropResourceQueue`, and `MoveQueryToResourceGroup`.

The 22 read-path operations are: `GetSegmentConfiguration`, `GetMirrorSyncStatus`,
`GetReplicationLag`, `GetActiveQueryCount`, `GetMaxConnections`, `GetResourceGroupUsage`,
`ListSessionsWithResourceGroup`, `ListSessions`, `GetDiskUsage`, `GetStorageDiskUsage`,
`ListResourceGroups`, `ListResourceQueues`, `CancelQuery`, `TriggerFTSProbe`,
`ShowParameter`, `GetBloatRecommendations`, `GetSkewRecommendations`,
`GetAgeRecommendations`, `GetIndexBloatRecommendations`, `GetTableDetails`,
`GetUsageReport`, and `GetRedistributionProgress`.

```promql
# p95 DB query latency by operation (last 5 minutes)
histogram_quantile(0.95, sum by (operation, le) (rate(cloudberry_db_query_duration_seconds_bucket[5m])))
```

### OpenAPI Specification

The operator serves an OpenAPI v3 specification at:

```
GET /openapi/v3
```
