# Specification 12: Data Loading

## Overview

This specification defines the data loading capabilities for Cloudberry Database clusters. It supports streaming data ingestion from S3, Kafka, and RabbitMQ sources with job management, scheduling, and monitoring.

## CRD Specification

### DataLoadingSpec

Added to `CloudberryClusterSpec`:

```yaml
dataLoading:
  enabled: true
  streamingServer:
    host: streaming.example.com
    port: 5432
    tlsMode: none                 # none, tls, skip-verify
    credentialSecret:
      name: streaming-credentials
  jobs:
    - name: s3-csv-loader
      type: s3
      enabled: true
      schedule: "*/30 * * * *"    # Every 30 minutes
      targetTable: public.events
      s3Source:
        bucket: data-lake
        path: /events/
        endpoint: "http://minio:9000"
        region: us-east-1
        format: csv
        credentialSecret:
          name: s3-credentials
        forcePathStyle: true
    - name: kafka-consumer
      type: kafka
      enabled: true
      targetTable: public.stream_data
      kafkaSource:
        brokers:
          - kafka:9092
        topic: cloudberry-data
        groupId: cloudberry-loader
        format: json
        startOffset: earliest
    - name: rabbitmq-consumer
      type: rabbitmq
      enabled: true
      targetTable: public.queue_data
      rabbitMQSource:
        host: rabbitmq
        port: 5672
        vhost: cloudberry
        queue: data-queue
        format: json
        credentialSecret:
          name: rabbitmq-credentials
```

### DataLoadingStatus

Added to `CloudberryClusterStatus`:

- `dataLoadingJobs` - Number of active data loading jobs

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/clusters/{name}/data-loading/jobs` | List all jobs |
| POST | `/clusters/{name}/data-loading/jobs` | Create a new job |
| GET | `/clusters/{name}/data-loading/jobs/{job}` | Get job details |
| PUT | `/clusters/{name}/data-loading/jobs/{job}` | Update a job |
| DELETE | `/clusters/{name}/data-loading/jobs/{job}` | Delete a job |
| POST | `/clusters/{name}/data-loading/jobs/{job}/start` | Start a job |
| POST | `/clusters/{name}/data-loading/jobs/{job}/stop` | Stop a job |

## CLI Commands

```bash
# Data loading operations
cloudberry-ctl data-loading jobs list
cloudberry-ctl data-loading jobs create --name my-job --type s3 --target-table public.data
cloudberry-ctl data-loading jobs start <job-name>
cloudberry-ctl data-loading jobs stop <job-name>
cloudberry-ctl data-loading jobs delete <job-name>
cloudberry-ctl data-loading status
```

## Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cloudberry_data_loading_jobs_active` | Gauge | cluster, namespace | Active data loading jobs |
| `cloudberry_data_loading_rows_total` | Counter | cluster, namespace, job, source_type | Total rows loaded |

## Source Types

### S3 Source
- Supports CSV, JSON, and Avro formats
- Compatible with MinIO (forcePathStyle)
- Credential-based authentication via Kubernetes secrets

### Kafka Source
- KRaft mode support (no ZooKeeper dependency)
- Consumer group management
- Configurable start offset (earliest/latest)
- Supports JSON, Avro, and CSV formats

### RabbitMQ Source
- Virtual host support
- Queue-based consumption
- Exchange binding support
- Credential-based authentication via Kubernetes secrets

## Webhook Validation

- `dataLoading.jobs[].name` is required
- `dataLoading.jobs[].type` is required (s3, kafka, rabbitmq)
- `dataLoading.jobs[].targetTable` is required

## Webhook Defaults

- `streamingServer.port`: 5432
- `streamingServer.tlsMode`: "none"
