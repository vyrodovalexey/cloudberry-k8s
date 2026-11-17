# U023 Performance Test Case Catalog — Cloudberry 2.2.0 campaign (2026-11-16)

Common fields for every PTC below unless stated otherwise:

- **Target / authorization**: local docker-desktop k8s, ns `cloudberry-test`, cluster
  `acceptance-test` (2.2.0, Running/InSync) + operator REST API (port-forward 8190→8090)
  + compose MinIO/HDFS + VictoriaMetrics :8428. Authorized by U023 delegation brief
  (exclusive use, host otherwise quiet). No other load permitted against the cluster
  during measurements.
- **Tool / load model**: `hey` (Go, open-model fixed-RPS `-q`/`-c`) for HTTP scenarios —
  same tool family as the 2026-07-10/12 baselines (macOS: Yandex Tank Docker `--net=host`
  is a no-op; the Tank `load.yaml`/`scenarios/*.yaml` remain the Linux/CI path with
  autostop as documented in load.yaml). Go benchmarks (`test/perf/`, build tag `e2e`) for
  data-path scenarios. SQL timings via `kubectl exec` psql (U015 documented path).
- **Execution**: SERIALIZED — one scenario at a time (U022 finding: parallel runs
  self-contend on the emulated DB), ≥3 runs per scenario, ≥30s idle gap between runs.
- **Autostop / safety**: every HTTP run is duration-capped (30–120 s) with per-request
  timeout; the campaign driver aborts the campaign on any 5xx or connection error
  (post-run stop condition; runs are short so exposure is bounded). Tank configs carry
  the full autostop block (load.yaml). Go benchmarks are exec-timeout bounded.
- **Load-generator check (every run)**: generator process CPU sampled mid-run via `ps`;
  a run is invalid if the generator exceeds ~80% of one core or (fixed-rate phases)
  achieved RPS < 95% of offered. hey's reported Requests/sec is the achieved-rate check.
- **Warmup**: A/B series: first 5 s of each hey run discarded from percentile
  computation is NOT needed — instead each scenario does one untimed warmup request /
  5-request warmup (per the suite's existing convention) before the timed run; DB
  queries: 1 untimed pre-pass per query per run (cache/JIT settle), then 3 timed passes.
- **Environment caveat (applies to ALL numbers)**: arm64 host, amd64-emulated DB pods,
  Rosetta off — wall-clock comparisons vs the 2026-07-12 baseline (same emulation
  asymmetry, different software stack) are RELATIVE only.
- **Baseline**: `test/performance/results/2026-07-12-perftest-report.md` (primary) and
  `2026-07-10` (secondary). Verdicts: no-regression / explained-delta / baseline-established
  (no prior number for the scenario).

| PTC | Scenario | Runs | Pass criteria |
|---|---|---|---|
| PTC-220-01 | Health endpoints (A1–A4) hey 100 RPS 60 s | 3 | p95 < 200 ms, p99 < 500 ms, 0 errors; health p50/p95/p99 within +50% of 07-12 (emulation + stack delta allowance) else explained |
| PTC-220-02 | API read path (B1/B2) 9 seq authed reqs | 3 | p99 < 500 ms, 0 non-2xx; bcrypt-dominated p50 compared to 07-12 (145–149 ms) |
| PTC-220-03 | Rate-limiter knee (B3-adapted) | 3 | exact enforcement of the CONFIGURED limit (600/min drift documented), fast 429 (<60 ms avg), accurate rejection metric |
| PTC-220-04 | Stepped load 5/10/25/50/100 RPS ×30 s | 3 | zero 5xx at every step; 200-class avg within baseline band (155 ms ± emulation); 429-class avg < 60 ms; achieved ≥95% offered RPS |
| PTC-220-05 | DB queries C1–C5 on mydb (U017 dataset) | 3×3 | read-only (mydb fingerprints unchanged); closed-form result correctness; timings recorded (07-12 dataset differs — shape comparison only) |
| PTC-220-06 | perf96 s3 read (text→CSV clause) / write (text→TEXT clause) | 3 | correct row counts; MinIO objects land non-zero-byte under timestamped prefix; rows/sec recorded |
| PTC-220-07 | perf97 hdfs read (text→CSV) / seqfile write (bridge) | 3 | correct row counts; part files land under timestamped dir; rows/sec recorded |
| PTC-220-08 | perf99 jdbc export 5000 rows (bridge clause) | 3 | export completes; exported_rows/sec recorded; pgsource delta cleanup |
| PTC-220-09 | perf105 PXF status API read | 3 | 200s only; avg_ms recorded (OIDC token path; skip documented if Keycloak creds unavailable) |
| PTC-220-10 | perf107 data-loading read endpoints | 3 | 200/429 only; avg_ms per endpoint recorded |
| PTC-220-11 | perf108 pxf/servers + test-read | 3 | 200/429 only; avg_ms recorded |
| PTC-220-12 | perf109 VM query latency | 3 | queries return; avg_ms recorded |
| PTC-220-13 | perf110 webhook admission latency (dry-run server: valid + W.1 invalid) | 3 | dry-run only (nothing persisted); avg_ms recorded |
| PTC-220-14 | perf111 netpol reconcile latency | 3 | no cluster mutation; avg_ms recorded |
| PTC-220-15 | perf113 storage-recommendation admission latency (dry-run) | 3 | dry-run only; avg_ms recorded |
| PTC-220-16 | perf119 storage API GETs (disabled-path — storage flags OFF per U022 ADV) | 3 | latency of the honest disabled/error path recorded; enabled-path excluded by constraint |
| PTC-220-17 | In-process suite `make test-perf` (builder micro-benchmarks) | 3 | exit 0 ×3; ns/op recorded per benchmark |

Excluded from this campaign (documented, not silent — see report §methodology):
perf101/103 live (job shapes absent from acceptance topology), perf102 live (no kafka
cluster), perf104 live-init (needs s3-load job shape), perf112/114/115/116/117/118/122
live (destructive CR-state flips — U022 exclusion class), perf120/121 enabled-path
(storage-observability flags must stay OFF — U022 58M01 contention, DEF-11), Tank
scenarios 87/88/113–122 YAML (Linux `--net=host` path only; macOS substitution = hey),
backup journeys (dedicated clusters; U018 proved the flows live).
