# Cloudberry Operator - API Specification

**Version**: 1.0.0

---

## 1. Overview

The Cloudberry Operator exposes a REST API for programmatic access to cluster management operations. The API supports Basic and Bearer (JWT) authentication and follows RESTful conventions.

## 2. Base URL

```
https://{operator-service}:{port}/api/v1alpha1
```

Default port: 8090 (HTTP) or 8443 (HTTPS with TLS)

## 3. Authentication

### 3.1 Basic Authentication

```
Authorization: Basic base64(username:password)
```

### 3.2 Bearer Token (JWT)

```
Authorization: Bearer <JWT token>
```

### 3.3 Obtaining a Token

```bash
# Via Keycloak (password grant for testing)
curl -X POST http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=cloudberry-ctl" \
  -d "username=admin" \
  -d "password=adminpass"

# Via Keycloak (client_credentials for service accounts)
curl -X POST http://keycloak:8090/realms/cloudberry/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=cloudberry-operator" \
  -d "client_secret=<secret>"
```

## 4. API Endpoints

### 4.1 Cluster Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters | Basic | List all clusters |
| GET | /clusters/{name} | Basic | Get cluster details |
| POST | /clusters | Admin | Create cluster |
| PUT | /clusters/{name} | Operator | Update cluster |
| DELETE | /clusters/{name} | Admin | Delete cluster |
| GET | /clusters/{name}/status | Basic | Get cluster status |

### 4.2 Cluster Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/start | Operator | Start cluster (modes: normal, restricted, maintenance) |
| POST | /clusters/{name}/stop | Operator | Stop cluster (modes: smart, fast, immediate) |
| POST | /clusters/{name}/restart | Operator | Restart cluster |
| POST | /clusters/{name}/reload | Operator | Reload configuration |

### 4.2.1 Scaling Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/scale | Admin | Scale cluster segments |
| GET | /clusters/{name}/scale/status | Operator Basic | Get scale operation status |
| POST | /clusters/{name}/rebalance | Operator | Trigger data rebalancing |
| GET | /clusters/{name}/rebalance/status | Operator Basic | Get rebalance status |

### 4.2.2 Storage Operations

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/storage/expand | Admin | Expand PV storage |
| GET | /clusters/{name}/storage/disk-usage | Basic | Get disk usage |
| GET | /clusters/{name}/storage/pvcs | Basic | List PVC sizes and status |

### 4.3 Configuration

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/config | Operator Basic | Get configuration |
| PUT | /clusters/{name}/config | Operator | Update configuration |
| GET | /clusters/{name}/config/parameters | Operator Basic | List parameters |
| PUT | /clusters/{name}/config/parameters | Operator | Set parameters |
| GET | /clusters/{name}/config/hba | Operator Basic | Get HBA rules |
| PUT | /clusters/{name}/config/hba | Admin | Update HBA rules |

### 4.4 High Availability

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/segments | Basic | List segments |
| GET | /clusters/{name}/segments/{id} | Basic | Get segment details |
| GET | /clusters/{name}/mirroring | Basic | Get mirroring status |
| POST | /clusters/{name}/recovery | Operator | Start recovery |
| POST | /clusters/{name}/rebalance | Operator | Rebalance segments |
| GET | /clusters/{name}/standby | Basic | Get standby status |
| POST | /clusters/{name}/standby/activate | Admin | Activate standby |
| POST | /clusters/{name}/standby/reinitialize | Operator | Reinitialize standby |

### 4.5 Sessions

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/sessions | Operator Basic | List sessions |
| POST | /clusters/{name}/sessions/{pid}/cancel | Operator | Cancel query |
| DELETE | /clusters/{name}/sessions/{pid} | Operator | Terminate session |

### 4.6 Maintenance

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| POST | /clusters/{name}/maintenance/vacuum | Operator | Run vacuum |
| POST | /clusters/{name}/maintenance/analyze | Operator | Run analyze |
| POST | /clusters/{name}/maintenance/reindex | Operator | Run reindex |
| GET | /clusters/{name}/maintenance/jobs | Operator Basic | List maintenance jobs |

### 4.7 Authentication Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/auth/roles | Admin | List roles |
| POST | /clusters/{name}/auth/roles | Admin | Create role |
| PUT | /clusters/{name}/auth/roles/{role} | Admin | Update role |
| DELETE | /clusters/{name}/auth/roles/{role} | Admin | Delete role |
| POST | /clusters/{name}/auth/rotate-password | Admin | Rotate admin password |

### 4.8 Resource Group Management

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /clusters/{name}/workload/resource-groups | Basic | List resource groups |
| POST | /clusters/{name}/workload/resource-groups | Operator | Create resource group |
| DELETE | /clusters/{name}/workload/resource-groups/{group} | Operator | Delete resource group |
| POST | /clusters/{name}/workload/resource-groups/{group}/assign | Operator | Assign role to group |

### 4.9 Health and Metrics

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| GET | /healthz | None | Operator health check |
| GET | /readyz | None | Operator readiness check |
| GET | /metrics | None | Prometheus metrics |

## 5. Request/Response Schemas

### 5.1 Cluster Status Response

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

### 5.2 Start Cluster Request

```json
{
  "mode": "normal"  // "normal", "restricted", "maintenance"
}
```

### 5.3 Stop Cluster Request

```json
{
  "mode": "fast"  // "smart", "fast", "immediate"
}
```

### 5.4 Recovery Request

```json
{
  "type": "incremental",  // "incremental", "full", "differential"
  "targetSegments": [0, 1],  // optional, all failed if omitted
  "targetNode": "node-3",    // optional, recover in-place if omitted
  "parallelStreams": 4       // optional, for differential recovery
}
```

### 5.5 Configuration Update Request

```json
{
  "parameters": {
    "max_connections": "200",
    "work_mem": "128MB"
  },
  "coordinatorParameters": {
    "optimizer": "on"
  },
  "requiresRestart": false
}
```

### 5.6 HBA Rules Update Request

```json
{
  "rules": [
    {
      "type": "host",
      "database": "all",
      "user": "all",
      "address": "10.0.0.0/8",
      "method": "scram-sha-256"
    }
  ]
}
```

### 5.7 Scale Request

```json
{
  "segmentCount": 8,
  "confirmScaleIn": false
}
```

**Notes**:
- If `segmentCount` > current count: scale-out (add segments)
- If `segmentCount` < current count: scale-in (remove segments)
- `confirmScaleIn` must be `true` when reducing by more than 50%

### 5.8 Scale Status Response

```json
{
  "operation": "scale-out",
  "previousCount": 4,
  "targetCount": 8,
  "currentCount": 6,
  "phase": "redistributing",
  "redistributionProgress": 65,
  "startedAt": "2026-05-14T10:00:00Z",
  "estimatedCompletion": "2026-05-14T10:30:00Z"
}
```

### 5.9 Storage Expand Request

```json
{
  "component": "segments",
  "newSize": "500Gi"
}
```

**Valid `component` values**: `coordinator`, `standby`, `segments` (applies to all primary + mirror PVCs)

### 5.10 Storage Expand Response

```json
{
  "component": "segments",
  "previousSize": "200Gi",
  "newSize": "500Gi",
  "affectedPVCs": 8,
  "status": "expanding",
  "requiresRestart": false
}
```

### 5.11 Resource Group Create Request

```json
{
  "name": "analytics",
  "concurrency": 10,
  "cpuMaxPercent": 50,
  "memoryLimit": 30
}
```

### 5.12 Resource Group Assign Request

```json
{
  "role": "analyst"
}
```

### 5.13 Rebalance Request

```json
{
  "tables": ["public.orders", "public.customers"],
  "parallelism": 4,
  "excludeTables": ["audit_log", "temp_*"]
}
```

### 5.14 Session List Response

```json
{
  "sessions": [
    {
      "pid": 12345,
      "username": "analyst",
      "application": "psql",
      "clientAddress": "10.0.0.5",
      "state": "active",
      "query": "SELECT * FROM large_table",
      "queryStart": "2026-05-11T17:58:00Z",
      "duration": "2m30s"
    }
  ]
}
```

## 6. Error Handling

### 6.1 Error Response Format

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

### 6.2 Error Codes

| HTTP Status | Code | Description |
|-------------|------|-------------|
| 400 | INVALID_REQUEST | Malformed request body |
| 401 | UNAUTHORIZED | Missing or invalid credentials |
| 403 | FORBIDDEN | Insufficient permissions |
| 404 | CLUSTER_NOT_FOUND | Cluster does not exist |
| 404 | SEGMENT_NOT_FOUND | Segment does not exist |
| 409 | CONFLICT | Operation conflicts with current state |
| 422 | VALIDATION_ERROR | Request validation failed |
| 500 | INTERNAL_ERROR | Unexpected server error |
| 429 | RATE_LIMITED | Too many requests, retry after delay |
| 503 | SERVICE_UNAVAILABLE | Operator not ready |
| 503 | DB_UNAVAILABLE | Database connection unavailable |

### 6.3 Rate Limiting

- Default: 10 requests/minute per client IP (token bucket algorithm)
- Only trusts `X-Forwarded-For`/`X-Real-IP` from configured trusted proxies
- Configurable via operator configuration
- Returns `429 Too Many Requests` with `Retry-After` header
- Applied before authentication to prevent brute-force attacks

## 7. Pagination

For list endpoints:

```
GET /clusters/{name}/sessions?limit=50&offset=0
```

Response includes:
```json
{
  "items": [...],
  "total": 150,
  "limit": 50,
  "offset": 0
}
```

## 8. Webhook Endpoints

### 8.1 Validating Webhook

```
POST /validate-cloudberry-example-com-v1alpha1-cloudberrycluster
```

Validates CloudberryCluster CR before admission.

### 8.2 Mutating Webhook

```
POST /mutate-cloudberry-example-com-v1alpha1-cloudberrycluster
```

Sets defaults on CloudberryCluster CR.

## 9. OpenAPI/Swagger

The operator serves OpenAPI v3 specification at:
```
GET /openapi/v3
```
