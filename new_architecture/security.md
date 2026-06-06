# Security — Backup/Restore (Target Exec Model)

## 1. Auth & secrets handling

| Concern | Mechanism | Source |
|---|---|---|
| S3 credentials | K8s Secret (`backup-s3-credentials`) or Vault-materialized Secret | `backup_builder.go:822-903` |
| Injection | `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via env (never on disk) | `buildS3CredentialEnv` |
| Plugin config | rendered by `envsubst` at runtime into `/tmp/s3-config.yaml` | `renderToolScript` |
| DB auth | `gpadmin` via `<cluster>-admin-password` Secret; local trust in-pod | entrypoint `pg_hba` |
| API auth | REST basic-auth `admin` (PermissionAdmin) | `cmd/operator/main.go` |

## 2. Changes implied by coordinator-exec model

```mermaid
flowchart TD
  SEC["Secret backup-s3-credentials"] -->|env AWS_*| EXEC["exec session in coordinator-0"]
  CM["ConfigMap <cluster>-backup-s3-config"] -->|mount/stdin| EXEC
  EXEC -->|envsubst -> /tmp/s3-config.yaml| TOOL["gpbackup/gprestore"]
  TOOL -->|TLS (encryption=on)| MINIO[("MinIO/S3")]
```

- **Credential exposure surface moves from a Job pod to the coordinator exec
  session.** The credentials are still injected as env (never written to a
  ConfigMap), and `/tmp/s3-config.yaml` is ephemeral within the coordinator
  container. Prefer passing creds as exec-scoped env, not as persistent files.
- **Least privilege:** the operator's exec into the coordinator requires
  `pods/exec` RBAC on the cluster namespace for the operator SA. This is a **new
  RBAC grant** to add (scoped to the coordinator pod label selector where
  possible).
- **TLS:** keep `encryption: on` (default, `buildS3Env`) so all S3 traffic is
  TLS. `gpbackup_s3_plugin` 2.1.0 rejects the `aws_signature_version` option, so
  the generated config omits it; path-style addressing is auto-derived for custom
  MinIO endpoints via `forcePathStyle`.

## 3. RBAC delta

| Subject | Verb/Resource | Why |
|---|---|---|
| operator SA | `create` on `pods/exec` (coordinator pod) | run gpbackup/gprestore in coordinator (target model) |
| backup SA | `get` Secret/ConfigMap (existing) | unchanged for cleanup/exporter Jobs |

## 4. Residual risks

- Exec runs as `gpadmin` (UID 1000) inside the coordinator — same trust domain as
  the DB. Acceptable: gpbackup already needs full cluster access.
- `/data/backups` on the PVC may briefly hold metadata/TOC; ensure 0700 and
  cleanup so backup TOC isn't world-readable on shared nodes.
- No plaintext credentials in CR/Job specs (preserved from current design).
