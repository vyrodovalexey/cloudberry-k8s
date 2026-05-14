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
  - [High Availability](#high-availability)
  - [Sessions](#sessions)
  - [Resource Groups](#resource-groups)
  - [Maintenance](#maintenance)
  - [Authentication Management](#authentication-management)
- [Request/Response Schemas](#requestresponse-schemas)
- [Error Handling](#error-handling)
- [Pagination](#pagination)
- [Rate Limiting](#rate-limiting)
- [Request Body Limits](#request-body-limits)
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
      "version": "7.7"
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
    "clusterVersion": "7.7",
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

### High Availability

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/clusters/{name}/segments` | Basic | List segments |
| `GET` | `/clusters/{name}/segments/{id}` | Basic | Get segment details |
| `GET` | `/clusters/{name}/mirroring` | Basic | Get mirroring status |
| `POST` | `/clusters/{name}/recovery` | Operator | Start recovery |
| `POST` | `/clusters/{name}/rebalance` | Operator | Rebalance segments |
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
| `type` | string | Yes | `incremental`, `full`, or `differential` |
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
| `POST` | `/clusters/{name}/auth/rotate-password` | Admin | Rotate admin password |

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
| 409 | `CONFLICT` | Operation conflicts with current cluster state |
| 422 | `VALIDATION_ERROR` | Request validation failed |
| 429 | `RATE_LIMITED` | Rate limit exceeded |
| 500 | `INTERNAL_ERROR` | Unexpected server error |
| 503 | `SERVICE_UNAVAILABLE` | Operator not ready |
| 503 | `DB_UNAVAILABLE` | Cannot connect to the cluster's database (session operations) |

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

## Rate Limiting

The API enforces per-IP token bucket rate limiting on all authenticated endpoints. Rate limiting is applied **before** authentication to protect against brute-force credential attacks.

- **Default limit**: 10 requests per minute per client IP
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

## Request Body Limits

All endpoints that accept a request body enforce a maximum body size of **1 MiB** (1,048,576 bytes). Requests exceeding this limit receive a `400 Bad Request` response.

## Input Validation

The API validates all input parameters:

| Validation | Rule | Error Code |
|-----------|------|------------|
| Cluster name | Must be a valid DNS-1123 subdomain (lowercase alphanumeric, `-`, max 253 chars) | `INVALID_REQUEST` |
| Namespace | Must be a valid DNS-1123 subdomain (if provided) | `INVALID_REQUEST` |
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
| `rebalance` | Rebalance segment roles after recovery | `Running` |
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

### Other Annotations

| Annotation | Description |
|------------|-------------|
| `avsoft.io/config-hash` | Hash of the current configuration for change detection |
| `avsoft.io/restart-trigger` | Triggers a pod restart when changed |
| `avsoft.io/restart-pending` | Indicates a full cluster restart is in progress |
| `avsoft.io/recovery` | Triggers recovery operations (`incremental`, `full`, `differential`) |

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
- OIDC enabled requires `issuerURL` and `clientID`
- Vault enabled requires `address`
- Valid parameter names in `config.parameters`
- `deletionPolicy` is `Retain` or `Delete`

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
| `version` | `"7.7"` |
| `image` | `"cloudberrydb/cloudberry:7.7"` |
| `imagePullPolicy` | `IfNotPresent` |
| `coordinator.replicas` | `1` |
| `coordinator.port` | `5432` |
| `segments.primariesPerHost` | `2` |
| `segments.mirroring.enabled` | `true` |
| `segments.mirroring.layout` | `group` |
| `segments.antiAffinity` | `preferred` |
| `auth.basic.enabled` | `true` |
| `auth.basic.adminUser` | `gpadmin` |
| `ha.ftsProbeInterval` | `60` |
| `ha.ftsProbeTimeout` | `20` |
| `ha.ftsProbeRetries` | `5` |
| `ha.checksums` | `true` |
| `monitoring.enabled` | `true` |
| `monitoring.metricsPort` | `9187` |
| `deletionPolicy` | `Retain` |

### OpenAPI Specification

The operator serves an OpenAPI v3 specification at:

```
GET /openapi/v3
```
