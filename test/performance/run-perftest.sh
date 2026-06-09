#!/usr/bin/env bash
# =============================================================================
# Cloudberry Operator - Performance Test Runner
# =============================================================================
#
# Runs Yandex Tank load tests against the Cloudberry Operator REST API.
#
# Usage:
#   ./run-perftest.sh [OPTIONS]
#
# Options:
#   --scenario <name>    Test scenario: smoke, baseline, stress, endurance (default: smoke)
#   --target <host:port> Target address (default: localhost:8090)
#   --ssl                Enable SSL/TLS for target connection
#   --rps <number>       Override max RPS (only for custom runs)
#   --duration <seconds> Override test duration (only for custom runs)
#   --ammo <file>        Override ammo file path
#   --docker             Run via Docker (default)
#   --native             Run via native yandex-tank installation
#   --generate-data      Generate the 100MB test dataset before running
#   --sql-bench          Run SQL benchmark queries against a target database
#   --db-host <host>     Database host for SQL benchmark (default: localhost)
#   --db-port <port>     Database port for SQL benchmark (default: 5432)
#   --db-name <name>     Database name for SQL benchmark (default: postgres)
#   --db-user <user>     Database user for SQL benchmark (default: gpadmin)
#   --analyze-only       Only analyze existing results (skip test execution)
#   --dry-run            Validate configuration without running the test
#   --help               Show this help message
#
# Examples:
#   ./run-perftest.sh --scenario smoke
#   ./run-perftest.sh --scenario baseline --target operator.example.com:8090 --ssl
#   ./run-perftest.sh --scenario stress --rps 500
#   ./run-perftest.sh --analyze-only
#
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration defaults
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="smoke"
TARGET="localhost:8190"
SSL="false"
RPS_OVERRIDE=""
DURATION_OVERRIDE=""
AMMO_OVERRIDE=""
USE_DOCKER=true
USE_HEY=false
GENERATE_DATA=false
SQL_BENCH=false
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="gpadmin"
ANALYZE_ONLY=false
DRY_RUN=false
PORT_FORWARD=false
PF_PID_COUNT=0
K8S_NAMESPACE="cloudberry-test"
K8S_OPERATOR_DEPLOY="cloudberry-operator"
OPERATOR_API_PORT="8090"
LOCAL_API_PORT="8190"
LOCAL_HEALTH_PORT="8081"
LOCAL_METRICS_PORT="8080"
RESULTS_DIR="${SCRIPT_DIR}/.yandextank"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TEST_RUN_DIR="${RESULTS_DIR}/${TIMESTAMP}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# =============================================================================
# Functions
# =============================================================================

usage() {
    head -35 "$0" | tail -30
    exit 0
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

log_header() {
    echo ""
    echo -e "${CYAN}=============================================================================${NC}"
    echo -e "${CYAN} $*${NC}"
    echo -e "${CYAN}=============================================================================${NC}"
    echo ""
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --scenario)
                SCENARIO="$2"
                shift 2
                ;;
            --target)
                TARGET="$2"
                shift 2
                ;;
            --ssl)
                SSL="true"
                shift
                ;;
            --rps)
                RPS_OVERRIDE="$2"
                shift 2
                ;;
            --duration)
                DURATION_OVERRIDE="$2"
                shift 2
                ;;
            --ammo)
                AMMO_OVERRIDE="$2"
                shift 2
                ;;
            --generate-data)
                GENERATE_DATA=true
                shift
                ;;
            --sql-bench)
                SQL_BENCH=true
                shift
                ;;
            --db-host)
                DB_HOST="$2"
                shift 2
                ;;
            --db-port)
                DB_PORT="$2"
                shift 2
                ;;
            --db-name)
                DB_NAME="$2"
                shift 2
                ;;
            --db-user)
                DB_USER="$2"
                shift 2
                ;;
            --docker)
                USE_DOCKER=true
                USE_HEY=false
                shift
                ;;
            --native)
                USE_DOCKER=false
                USE_HEY=false
                shift
                ;;
            --hey)
                USE_HEY=true
                USE_DOCKER=false
                shift
                ;;
            --port-forward)
                PORT_FORWARD=true
                shift
                ;;
            --namespace)
                K8S_NAMESPACE="$2"
                shift 2
                ;;
            --analyze-only)
                ANALYZE_ONLY=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help|-h)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done
}

# Validate the scenario name
validate_scenario() {
    local valid_scenarios=("smoke" "baseline" "stress" "endurance" "scenario87" "scenario88" "custom")
    local found=false

    for s in "${valid_scenarios[@]}"; do
        if [[ "$SCENARIO" == "$s" ]]; then
            found=true
            break
        fi
    done

    if [[ "$found" == "false" ]]; then
        log_error "Invalid scenario: ${SCENARIO}"
        log_info "Valid scenarios: ${valid_scenarios[*]}"
        exit 1
    fi
}

# Get the config file for the scenario
get_config_file() {
    case "$SCENARIO" in
        smoke|baseline|stress|endurance)
            echo "scenarios/${SCENARIO}.yaml"
            ;;
        scenario87)
            echo "scenarios/scenario87-migration.yaml"
            ;;
        scenario88)
            echo "scenarios/scenario88-backup-status.yaml"
            ;;
        custom)
            echo "load.yaml"
            ;;
    esac
}

# Check prerequisites
check_prerequisites() {
    log_header "Checking Prerequisites"

    # Check Docker if using Docker mode
    if [[ "$USE_DOCKER" == "true" ]]; then
        if command -v docker &>/dev/null; then
            log_success "Docker is available: $(docker --version)"
        else
            log_error "Docker is not installed. Install Docker or use --native flag."
            exit 1
        fi
    else
        if command -v yandex-tank &>/dev/null; then
            log_success "yandex-tank is available"
        else
            log_error "yandex-tank is not installed."
            log_info "Install: pip install yandextank"
            exit 1
        fi
    fi

    # Check config file exists
    local config_file
    config_file=$(get_config_file)
    if [[ -f "${SCRIPT_DIR}/${config_file}" ]]; then
        log_success "Config file found: ${config_file}"
    else
        log_error "Config file not found: ${SCRIPT_DIR}/${config_file}"
        exit 1
    fi

    # Check ammo file
    local ammo_file
    if [[ -n "$AMMO_OVERRIDE" ]]; then
        ammo_file="$AMMO_OVERRIDE"
    else
        ammo_file=$(sed -n 's/^[[:space:]]*ammofile:[[:space:]]*//p' "${SCRIPT_DIR}/${config_file}" 2>/dev/null | head -1)
        ammo_file="${ammo_file:-ammo/api-read.txt}"
        ammo_file=$(echo "$ammo_file" | tr -d ' ')
    fi

    if [[ -f "${SCRIPT_DIR}/${ammo_file}" ]]; then
        log_success "Ammo file found: ${ammo_file}"
    else
        log_error "Ammo file not found: ${SCRIPT_DIR}/${ammo_file}"
        exit 1
    fi
}

# Check target accessibility
check_target() {
    log_header "Checking Target Accessibility"

    local host port
    host=$(echo "$TARGET" | cut -d: -f1)
    port=$(echo "$TARGET" | cut -d: -f2)

    log_info "Target: ${TARGET} (SSL: ${SSL})"

    # Try to connect to the target
    if command -v nc &>/dev/null; then
        if nc -z -w 3 "$host" "$port" 2>/dev/null; then
            log_success "Target ${TARGET} is reachable"
        else
            log_warn "Target ${TARGET} is not reachable. Test may fail."
            log_info "Make sure the operator is running on ${TARGET}"
        fi
    elif command -v curl &>/dev/null; then
        local protocol="http"
        [[ "$SSL" == "true" ]] && protocol="https"
        if curl -sf -o /dev/null --connect-timeout 3 "${protocol}://${TARGET}/healthz" 2>/dev/null; then
            log_success "Target ${TARGET} is reachable (healthz OK)"
        else
            log_warn "Target ${TARGET} is not reachable or healthz failed."
            log_info "Make sure the operator is running on ${TARGET}"
        fi
    else
        log_warn "Cannot verify target connectivity (nc/curl not available)"
    fi

    # Quick health check with auth
    if command -v curl &>/dev/null; then
        local protocol="http"
        [[ "$SSL" == "true" ]] && protocol="https"

        log_info "Running health check..."
        local health_response
        health_response=$(curl -sf --connect-timeout 5 \
            -u admin:admin \
            "${protocol}://${TARGET}/healthz" 2>/dev/null || echo "FAILED")

        if [[ "$health_response" != "FAILED" ]]; then
            log_success "Health check passed: ${health_response}"
        else
            log_warn "Health check failed. Continuing anyway..."
        fi

        log_info "Running readiness check..."
        local ready_response
        ready_response=$(curl -sf --connect-timeout 5 \
            -u admin:admin \
            "${protocol}://${TARGET}/readyz" 2>/dev/null || echo "FAILED")

        if [[ "$ready_response" != "FAILED" ]]; then
            log_success "Readiness check passed: ${ready_response}"
        else
            log_warn "Readiness check failed. Continuing anyway..."
        fi
    fi
}

# Create a temporary config with overrides applied
create_runtime_config() {
    local config_file
    config_file=$(get_config_file)
    local runtime_config="${TEST_RUN_DIR}/runtime_config.yaml"

    mkdir -p "${TEST_RUN_DIR}"

    # Start with the base config
    cp "${SCRIPT_DIR}/${config_file}" "${runtime_config}"

    # Apply target override
    if [[ "$TARGET" != "localhost:8190" ]]; then
        sed -i.bak "s|address:.*|address: ${TARGET}|" "${runtime_config}"
        rm -f "${runtime_config}.bak"
    fi

    # Apply SSL override
    if [[ "$SSL" == "true" ]]; then
        sed -i.bak "s|ssl:.*|ssl: true|" "${runtime_config}"
        rm -f "${runtime_config}.bak"
    fi

    # Apply ammo override
    if [[ -n "$AMMO_OVERRIDE" ]]; then
        sed -i.bak "s|ammofile:.*|ammofile: ${AMMO_OVERRIDE}|" "${runtime_config}"
        rm -f "${runtime_config}.bak"
    fi

    # Apply RPS override (create custom schedule)
    if [[ -n "$RPS_OVERRIDE" ]]; then
        local duration="${DURATION_OVERRIDE:-300}"
        local ramp_rps=$((RPS_OVERRIDE / 5))
        [[ "$ramp_rps" -lt 1 ]] && ramp_rps=1
        sed -i.bak "s|schedule:.*|schedule: line(1, ${ramp_rps}, 30s) line(${ramp_rps}, ${RPS_OVERRIDE}, 30s) const(${RPS_OVERRIDE}, ${duration}s)|" "${runtime_config}"
        rm -f "${runtime_config}.bak"
    fi

    echo "${runtime_config}"
}

# Print test configuration summary
print_test_summary() {
    local config_file
    config_file=$(get_config_file)

    log_header "Test Configuration Summary"

    echo -e "  Scenario:     ${GREEN}${SCENARIO}${NC}"
    echo -e "  Config:       ${config_file}"
    echo -e "  Target:       ${TARGET}"
    echo -e "  SSL:          ${SSL}"
    echo -e "  Runner:       $(if [[ "$USE_DOCKER" == "true" ]]; then echo "Docker"; else echo "Native"; fi)"
    echo -e "  Results dir:  ${TEST_RUN_DIR}"
    echo ""

    if [[ -n "$RPS_OVERRIDE" ]]; then
        echo -e "  ${YELLOW}RPS Override:   ${RPS_OVERRIDE}${NC}"
    fi
    if [[ -n "$DURATION_OVERRIDE" ]]; then
        echo -e "  ${YELLOW}Duration Override: ${DURATION_OVERRIDE}s${NC}"
    fi
    if [[ -n "$AMMO_OVERRIDE" ]]; then
        echo -e "  ${YELLOW}Ammo Override:  ${AMMO_OVERRIDE}${NC}"
    fi

    echo ""

    case "$SCENARIO" in
        smoke)
            echo -e "  ${CYAN}Smoke Test: Quick validation at 10 RPS for 1 minute${NC}"
            echo -e "  Expected: All responses < 100ms, 0% errors"
            ;;
        baseline)
            echo -e "  ${CYAN}Baseline Test: Performance reference at 100 RPS for 5 minutes${NC}"
            echo -e "  Expected: p95 < 200ms, p99 < 500ms, errors < 0.1%"
            ;;
        stress)
            echo -e "  ${CYAN}Stress Test: Ramp to 1000 RPS to find breaking point${NC}"
            echo -e "  Expected: Identify max sustainable throughput"
            ;;
        endurance)
            echo -e "  ${CYAN}Endurance Test: 50 RPS sustained for 30 minutes${NC}"
            echo -e "  Expected: Stable latency, no memory leaks"
            ;;
        scenario87)
            echo -e "  ${CYAN}Scenario 87: Migration read-polling at 50 RPS for 2 minutes${NC}"
            echo -e "  Expected: p95 < 300ms, p99 < 1000ms, errors < 0.5%"
            ;;
        scenario88)
            echo -e "  ${CYAN}Scenario 88: Backup-status read-polling at 40 RPS for 2 minutes${NC}"
            echo -e "  Expected: p95 < 300ms, p99 < 1000ms, errors < 0.5%"
            ;;
    esac
    echo ""
}

# Run the test via Docker
run_docker() {
    local runtime_config="$1"

    log_header "Running Load Test (Docker)"

    # Use relative path for Docker volume mount
    local config_relative
    config_relative=$(realpath --relative-to="${SCRIPT_DIR}" "${runtime_config}" 2>/dev/null || echo "${runtime_config##*/}")

    docker run \
        --rm \
        --name "yandex-tank-${SCENARIO}-${TIMESTAMP}" \
        --net host \
        -v "${SCRIPT_DIR}:/var/loadtest:ro" \
        -v "${TEST_RUN_DIR}:/var/loadtest/.yandextank" \
        -v "${runtime_config}:/var/loadtest/active_config.yaml:ro" \
        -e "TERM=xterm-256color" \
        direvius/yandex-tank \
        -c active_config.yaml

    return $?
}

# Run the test natively
run_native() {
    local runtime_config="$1"

    log_header "Running Load Test (Native)"

    cd "${SCRIPT_DIR}"
    yandex-tank -c "${runtime_config}"

    return $?
}

# Analyze test results from phout.txt
analyze_results() {
    log_header "Analyzing Test Results"

    # Find the most recent results directory
    local results_dir="$TEST_RUN_DIR"
    if [[ "$ANALYZE_ONLY" == "true" ]]; then
        # Find the latest results directory
        results_dir=$(ls -td "${RESULTS_DIR}"/*/ 2>/dev/null | head -1)
        if [[ -z "$results_dir" ]]; then
            log_error "No results found in ${RESULTS_DIR}"
            exit 1
        fi
        log_info "Analyzing results from: ${results_dir}"
    fi

    # Look for phout.txt in the results directory
    local phout_file=""
    if [[ -f "${results_dir}/phout.txt" ]]; then
        phout_file="${results_dir}/phout.txt"
    elif [[ -f "${results_dir}/phout_*.txt" ]]; then
        phout_file=$(ls "${results_dir}"/phout_*.txt 2>/dev/null | head -1)
    fi

    if [[ -z "$phout_file" || ! -f "$phout_file" ]]; then
        log_warn "phout.txt not found in ${results_dir}"
        log_info "Results may be in a different format or the test did not complete."

        # List what we do have
        if [[ -d "$results_dir" ]]; then
            log_info "Available files in results directory:"
            ls -la "$results_dir" 2>/dev/null || true
        fi
        return 1
    fi

    log_info "Parsing results from: ${phout_file}"

    local total_requests=0
    local total_errors=0
    local total_2xx=0
    local total_4xx=0
    local total_5xx=0
    local total_other=0
    local total_latency=0
    local min_latency=999999999
    local max_latency=0
    local latencies=()

    # phout.txt format:
    # timestamp tag response_time connect_time send_time latency receive_time interval_event
    # size_out size_in http_code net_code
    while IFS=$'\t' read -r ts tag rt ct st lat recv ie so si http_code net_code rest; do
        total_requests=$((total_requests + 1))

        # Response time is in microseconds
        local rt_us="${rt:-0}"
        total_latency=$((total_latency + rt_us))
        latencies+=("$rt_us")

        if [[ "$rt_us" -lt "$min_latency" ]]; then
            min_latency=$rt_us
        fi
        if [[ "$rt_us" -gt "$max_latency" ]]; then
            max_latency=$rt_us
        fi

        # Count HTTP status codes
        case "${http_code:-0}" in
            2[0-9][0-9]) total_2xx=$((total_2xx + 1)) ;;
            4[0-9][0-9]) total_4xx=$((total_4xx + 1)) ;;
            5[0-9][0-9]) total_5xx=$((total_5xx + 1)); total_errors=$((total_errors + 1)) ;;
            0)           total_errors=$((total_errors + 1)); total_other=$((total_other + 1)) ;;
            *)           total_other=$((total_other + 1)) ;;
        esac
    done < "$phout_file"

    if [[ "$total_requests" -eq 0 ]]; then
        log_warn "No requests found in phout.txt"
        return 1
    fi

    # Calculate statistics
    local avg_latency=$((total_latency / total_requests))
    local error_rate
    if [[ "$total_requests" -gt 0 ]]; then
        error_rate=$(awk "BEGIN {printf \"%.2f\", ($total_errors / $total_requests) * 100}")
    else
        error_rate="0.00"
    fi

    # Sort latencies for percentile calculation
    IFS=$'\n' sorted_latencies=($(sort -n <<<"${latencies[*]}")); unset IFS

    local p50_idx=$(( (total_requests * 50) / 100 ))
    local p95_idx=$(( (total_requests * 95) / 100 ))
    local p99_idx=$(( (total_requests * 99) / 100 ))

    local p50=${sorted_latencies[$p50_idx]:-0}
    local p95=${sorted_latencies[$p95_idx]:-0}
    local p99=${sorted_latencies[$p99_idx]:-0}

    # Convert microseconds to milliseconds for display
    local avg_ms=$(awk "BEGIN {printf \"%.2f\", $avg_latency / 1000}")
    local min_ms=$(awk "BEGIN {printf \"%.2f\", $min_latency / 1000}")
    local max_ms=$(awk "BEGIN {printf \"%.2f\", $max_latency / 1000}")
    local p50_ms=$(awk "BEGIN {printf \"%.2f\", $p50 / 1000}")
    local p95_ms=$(awk "BEGIN {printf \"%.2f\", $p95 / 1000}")
    local p99_ms=$(awk "BEGIN {printf \"%.2f\", $p99 / 1000}")

    # Print results
    log_header "Test Results Summary"

    echo -e "  ${CYAN}Request Statistics${NC}"
    echo -e "  ─────────────────────────────────────────"
    echo -e "  Total Requests:    ${total_requests}"
    echo -e "  Successful (2xx):  ${GREEN}${total_2xx}${NC}"
    echo -e "  Client Errors (4xx): ${YELLOW}${total_4xx}${NC}"
    echo -e "  Server Errors (5xx): ${RED}${total_5xx}${NC}"
    echo -e "  Network Errors:    ${RED}${total_other}${NC}"
    echo -e "  Error Rate:        $(if (( $(echo "$error_rate < 1" | bc -l 2>/dev/null || echo 0) )); then echo -e "${GREEN}${error_rate}%${NC}"; else echo -e "${RED}${error_rate}%${NC}"; fi)"
    echo ""
    echo -e "  ${CYAN}Latency Statistics (ms)${NC}"
    echo -e "  ─────────────────────────────────────────"
    echo -e "  Min:               ${min_ms} ms"
    echo -e "  Avg:               ${avg_ms} ms"
    echo -e "  Max:               ${max_ms} ms"
    echo -e "  p50 (median):      ${p50_ms} ms"
    echo -e "  p95:               ${p95_ms} ms"
    echo -e "  p99:               ${p99_ms} ms"
    echo ""

    # SLO evaluation
    log_header "SLO Evaluation"

    local slo_pass=true

    case "$SCENARIO" in
        smoke)
            evaluate_slo "p95 Latency" "$p95" "1000000" "< 1000ms"
            evaluate_slo "Error Rate" "$total_errors" "0" "= 0%"
            ;;
        baseline)
            evaluate_slo "p50 Latency" "$p50" "50000" "< 50ms"
            evaluate_slo "p95 Latency" "$p95" "200000" "< 200ms"
            evaluate_slo "p99 Latency" "$p99" "500000" "< 500ms"
            evaluate_slo "Error Rate %" "$error_rate" "0.1" "< 0.1%"
            ;;
        stress)
            echo -e "  ${CYAN}Stress test - no fixed SLOs. Review results to find breaking point.${NC}"
            ;;
        endurance)
            evaluate_slo "p95 Latency" "$p95" "200000" "< 200ms"
            evaluate_slo "p99 Latency" "$p99" "500000" "< 500ms"
            evaluate_slo "Error Rate %" "$error_rate" "0.5" "< 0.5%"
            ;;
        scenario87)
            evaluate_slo "p50 Latency" "$p50" "100000" "< 100ms"
            evaluate_slo "p95 Latency" "$p95" "300000" "< 300ms"
            evaluate_slo "p99 Latency" "$p99" "1000000" "< 1000ms"
            evaluate_slo "Error Rate %" "$error_rate" "0.5" "< 0.5%"
            ;;
        scenario88)
            evaluate_slo "p50 Latency" "$p50" "100000" "< 100ms"
            evaluate_slo "p95 Latency" "$p95" "300000" "< 300ms"
            evaluate_slo "p99 Latency" "$p99" "1000000" "< 1000ms"
            evaluate_slo "Error Rate %" "$error_rate" "0.5" "< 0.5%"
            ;;
    esac

    # Save results to JSON
    local results_json="${results_dir}/results_summary.json"
    cat > "$results_json" <<EOF
{
  "scenario": "${SCENARIO}",
  "timestamp": "${TIMESTAMP}",
  "target": "${TARGET}",
  "total_requests": ${total_requests},
  "status_codes": {
    "2xx": ${total_2xx},
    "4xx": ${total_4xx},
    "5xx": ${total_5xx},
    "other": ${total_other}
  },
  "error_rate_percent": ${error_rate},
  "latency_ms": {
    "min": ${min_ms},
    "avg": ${avg_ms},
    "max": ${max_ms},
    "p50": ${p50_ms},
    "p95": ${p95_ms},
    "p99": ${p99_ms}
  }
}
EOF

    log_success "Results saved to: ${results_json}"
    echo ""

    return 0
}

# Evaluate a single SLO
evaluate_slo() {
    local name="$1"
    local actual="$2"
    local threshold="$3"
    local description="$4"

    local pass
    pass=$(awk "BEGIN {print ($actual <= $threshold) ? 1 : 0}")

    if [[ "$pass" -eq 1 ]]; then
        echo -e "  ${GREEN}PASS${NC}  ${name}: ${actual} (threshold: ${description})"
    else
        echo -e "  ${RED}FAIL${NC}  ${name}: ${actual} (threshold: ${description})"
    fi
}

# Generate ASCII latency distribution chart
generate_latency_chart() {
    local phout_file="$1"

    if [[ ! -f "$phout_file" ]]; then
        return
    fi

    log_header "Latency Distribution"

    # Create buckets: <1ms, 1-5ms, 5-10ms, 10-50ms, 50-100ms, 100-500ms, 500ms-1s, >1s
    local b1=0 b2=0 b3=0 b4=0 b5=0 b6=0 b7=0 b8=0
    local total=0

    while IFS=$'\t' read -r ts tag rt rest; do
        local rt_us="${rt:-0}"
        total=$((total + 1))

        if [[ "$rt_us" -lt 1000 ]]; then
            b1=$((b1 + 1))
        elif [[ "$rt_us" -lt 5000 ]]; then
            b2=$((b2 + 1))
        elif [[ "$rt_us" -lt 10000 ]]; then
            b3=$((b3 + 1))
        elif [[ "$rt_us" -lt 50000 ]]; then
            b4=$((b4 + 1))
        elif [[ "$rt_us" -lt 100000 ]]; then
            b5=$((b5 + 1))
        elif [[ "$rt_us" -lt 500000 ]]; then
            b6=$((b6 + 1))
        elif [[ "$rt_us" -lt 1000000 ]]; then
            b7=$((b7 + 1))
        else
            b8=$((b8 + 1))
        fi
    done < "$phout_file"

    if [[ "$total" -eq 0 ]]; then
        return
    fi

    # Find max for scaling
    local max_count=0
    for count in $b1 $b2 $b3 $b4 $b5 $b6 $b7 $b8; do
        if [[ "$count" -gt "$max_count" ]]; then
            max_count=$count
        fi
    done

    local bar_width=50

    print_bar() {
        local label="$1"
        local count="$2"
        local pct
        pct=$(awk "BEGIN {printf \"%.1f\", ($count / $total) * 100}")
        local bar_len=0
        if [[ "$max_count" -gt 0 ]]; then
            bar_len=$(( (count * bar_width) / max_count ))
        fi
        local bar=""
        for ((i=0; i<bar_len; i++)); do bar+="█"; done

        printf "  %-12s │ %-${bar_width}s │ %6d (%5s%%)\n" "$label" "$bar" "$count" "$pct"
    }

    echo "  Bucket       │ Distribution                                       │  Count"
    echo "  ─────────────┼────────────────────────────────────────────────────┼──────────────"
    print_bar "< 1ms" "$b1"
    print_bar "1-5ms" "$b2"
    print_bar "5-10ms" "$b3"
    print_bar "10-50ms" "$b4"
    print_bar "50-100ms" "$b5"
    print_bar "100-500ms" "$b6"
    print_bar "500ms-1s" "$b7"
    print_bar "> 1s" "$b8"
    echo ""
}

# Generate RPS over time chart
generate_rps_chart() {
    local phout_file="$1"

    if [[ ! -f "$phout_file" ]]; then
        return
    fi

    log_header "RPS Over Time"

    # Count requests per second
    declare -A rps_map
    local min_ts=999999999999
    local max_ts=0

    while IFS=$'\t' read -r ts rest; do
        local sec="${ts%%.*}"
        rps_map[$sec]=$(( ${rps_map[$sec]:-0} + 1 ))
        if [[ "$sec" -lt "$min_ts" ]]; then min_ts=$sec; fi
        if [[ "$sec" -gt "$max_ts" ]]; then max_ts=$sec; fi
    done < "$phout_file"

    local duration=$((max_ts - min_ts + 1))
    if [[ "$duration" -le 0 ]]; then
        return
    fi

    # Sample at most 60 data points for display
    local step=1
    if [[ "$duration" -gt 60 ]]; then
        step=$((duration / 60))
    fi

    local max_rps=0
    for ts in "${!rps_map[@]}"; do
        if [[ "${rps_map[$ts]}" -gt "$max_rps" ]]; then
            max_rps=${rps_map[$ts]}
        fi
    done

    local bar_width=50

    echo "  Time (s)  │ RPS                                                │ Value"
    echo "  ──────────┼────────────────────────────────────────────────────┼──────"

    local elapsed=0
    for ((ts=min_ts; ts<=max_ts; ts+=step)); do
        local rps=${rps_map[$ts]:-0}
        # Aggregate if step > 1
        if [[ "$step" -gt 1 ]]; then
            rps=0
            local count=0
            for ((s=ts; s<ts+step && s<=max_ts; s++)); do
                rps=$((rps + ${rps_map[$s]:-0}))
                count=$((count + 1))
            done
            if [[ "$count" -gt 0 ]]; then
                rps=$((rps / count))
            fi
        fi

        local bar_len=0
        if [[ "$max_rps" -gt 0 ]]; then
            bar_len=$(( (rps * bar_width) / max_rps ))
        fi
        local bar=""
        for ((i=0; i<bar_len; i++)); do bar+="▓"; done

        printf "  %-9s │ %-${bar_width}s │ %5d\n" "${elapsed}s" "$bar" "$rps"
        elapsed=$((elapsed + step))
    done
    echo ""
}

# Generate HTTP status code distribution chart
generate_status_chart() {
    local phout_file="$1"

    if [[ ! -f "$phout_file" ]]; then
        return
    fi

    log_header "HTTP Status Code Distribution Over Time"

    # Aggregate per-second status codes
    declare -A sec_2xx sec_4xx sec_5xx sec_other
    local min_ts=999999999999
    local max_ts=0

    while IFS=$'\t' read -r ts tag rt ct st lat recv ie so si http_code rest; do
        local sec="${ts%%.*}"
        if [[ "$sec" -lt "$min_ts" ]]; then min_ts=$sec; fi
        if [[ "$sec" -gt "$max_ts" ]]; then max_ts=$sec; fi

        case "${http_code:-0}" in
            2[0-9][0-9]) sec_2xx[$sec]=$(( ${sec_2xx[$sec]:-0} + 1 )) ;;
            4[0-9][0-9]) sec_4xx[$sec]=$(( ${sec_4xx[$sec]:-0} + 1 )) ;;
            5[0-9][0-9]) sec_5xx[$sec]=$(( ${sec_5xx[$sec]:-0} + 1 )) ;;
            *)           sec_other[$sec]=$(( ${sec_other[$sec]:-0} + 1 )) ;;
        esac
    done < "$phout_file"

    local duration=$((max_ts - min_ts + 1))
    if [[ "$duration" -le 0 ]]; then
        return
    fi

    local step=1
    if [[ "$duration" -gt 40 ]]; then
        step=$((duration / 40))
    fi

    echo -e "  ${GREEN}█ 2xx${NC}  ${YELLOW}█ 4xx${NC}  ${RED}█ 5xx${NC}  ░ other"
    echo ""
    echo "  Time (s)  │ Status Distribution"
    echo "  ──────────┼──────────────────────────────────────────────────────"

    local elapsed=0
    for ((ts=min_ts; ts<=max_ts; ts+=step)); do
        local s2=0 s4=0 s5=0 so=0
        for ((s=ts; s<ts+step && s<=max_ts; s++)); do
            s2=$((s2 + ${sec_2xx[$s]:-0}))
            s4=$((s4 + ${sec_4xx[$s]:-0}))
            s5=$((s5 + ${sec_5xx[$s]:-0}))
            so=$((so + ${sec_other[$s]:-0}))
        done

        local total=$((s2 + s4 + s5 + so))
        if [[ "$total" -eq 0 ]]; then
            elapsed=$((elapsed + step))
            continue
        fi

        local bar_width=50
        local b2=$(( (s2 * bar_width) / total ))
        local b4=$(( (s4 * bar_width) / total ))
        local b5=$(( (s5 * bar_width) / total ))
        local bo=$((bar_width - b2 - b4 - b5))
        [[ "$bo" -lt 0 ]] && bo=0

        local bar=""
        for ((i=0; i<b2; i++)); do bar+="${GREEN}█${NC}"; done
        for ((i=0; i<b4; i++)); do bar+="${YELLOW}█${NC}"; done
        for ((i=0; i<b5; i++)); do bar+="${RED}█${NC}"; done
        for ((i=0; i<bo; i++)); do bar+="░"; done

        printf "  %-9s │ " "${elapsed}s"
        echo -e "${bar} (2xx:${s2} 4xx:${s4} 5xx:${s5})"
        elapsed=$((elapsed + step))
    done
    echo ""
}

# Generate test dataset
generate_data() {
    log_header "Generating Test Dataset"

    local gen_script="${SCRIPT_DIR}/generate-dataset.sh"
    if [[ ! -x "$gen_script" ]]; then
        log_error "generate-dataset.sh not found or not executable"
        exit 1
    fi

    "$gen_script" --output-dir "${SCRIPT_DIR}/data"
    log_success "Dataset generation complete"
}

# Run SQL benchmark queries
run_sql_bench() {
    log_header "Running SQL Benchmark"

    if ! command -v psql &>/dev/null; then
        log_error "psql is not installed. Install PostgreSQL client tools."
        exit 1
    fi

    local psql_cmd="psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME}"
    local sql_dir="${SCRIPT_DIR}/sql"

    log_info "Target: ${DB_HOST}:${DB_PORT}/${DB_NAME} (user: ${DB_USER})"

    # Setup: create tables and load data
    log_info "Creating tables..."
    ${psql_cmd} -f "${sql_dir}/create-tables.sql" 2>&1 || {
        log_error "Failed to create tables"
        return 1
    }

    log_info "Loading data..."
    ${psql_cmd} -f "${sql_dir}/load-data.sql" 2>&1 || {
        log_error "Failed to load data"
        return 1
    }

    # Run benchmark queries
    local sql_files=("select-queries.sql" "join-queries.sql" "update-queries.sql")
    for sql_file in "${sql_files[@]}"; do
        log_info "Running ${sql_file}..."
        ${psql_cmd} -f "${sql_dir}/${sql_file}" 2>&1 || {
            log_warn "Some queries in ${sql_file} may have failed"
        }
    done

    log_success "SQL benchmark complete"
}

# Start kubectl port-forwards for the operator
start_port_forwards() {
    log_header "Starting Port Forwards"

    # Port-forward REST API (8090 -> LOCAL_API_PORT)
    kubectl port-forward -n "${K8S_NAMESPACE}" "deployment/${K8S_OPERATOR_DEPLOY}" \
        "${LOCAL_API_PORT}:${OPERATOR_API_PORT}" >/dev/null 2>&1 &
    eval "PF_PID_${PF_PID_COUNT}=$!"
    log_info "Port-forward: localhost:${LOCAL_API_PORT} -> operator:${OPERATOR_API_PORT} (REST API) [PID: $!]"
    PF_PID_COUNT=$((PF_PID_COUNT + 1))

    # Port-forward health probe (8081 -> LOCAL_HEALTH_PORT)
    kubectl port-forward -n "${K8S_NAMESPACE}" "deployment/${K8S_OPERATOR_DEPLOY}" \
        "${LOCAL_HEALTH_PORT}:8081" >/dev/null 2>&1 &
    eval "PF_PID_${PF_PID_COUNT}=$!"
    log_info "Port-forward: localhost:${LOCAL_HEALTH_PORT} -> operator:8081 (health) [PID: $!]"
    PF_PID_COUNT=$((PF_PID_COUNT + 1))

    # Port-forward metrics (8080 -> LOCAL_METRICS_PORT)
    kubectl port-forward -n "${K8S_NAMESPACE}" "deployment/${K8S_OPERATOR_DEPLOY}" \
        "${LOCAL_METRICS_PORT}:8080" >/dev/null 2>&1 &
    eval "PF_PID_${PF_PID_COUNT}=$!"
    log_info "Port-forward: localhost:${LOCAL_METRICS_PORT} -> operator:8080 (metrics) [PID: $!]"
    PF_PID_COUNT=$((PF_PID_COUNT + 1))

    # Wait for port-forwards to establish
    sleep 3

    # Verify port-forwards are alive
    local all_ok=true
    local i=0
    while [[ $i -lt $PF_PID_COUNT ]]; do
        local pid
        eval "pid=\$PF_PID_${i}"
        if ! kill -0 "$pid" 2>/dev/null; then
            log_error "Port-forward process $pid died"
            all_ok=false
        fi
        i=$((i + 1))
    done

    if [[ "$all_ok" == "true" ]]; then
        log_success "All port-forwards established"
    else
        log_error "Some port-forwards failed to start"
        stop_port_forwards
        exit 1
    fi
}

# Stop kubectl port-forwards
stop_port_forwards() {
    local i=0
    while [[ $i -lt $PF_PID_COUNT ]]; do
        local pid
        eval "pid=\$PF_PID_${i}"
        kill "$pid" 2>/dev/null || true
        i=$((i + 1))
    done
    # Wait briefly for processes to exit
    sleep 1
    PF_PID_COUNT=0
    log_info "Port-forwards stopped"
}

# Run the test using hey (Go HTTP load generator)
run_hey() {
    log_header "Running Load Test (hey)"

    if ! command -v hey &>/dev/null; then
        log_error "hey is not installed. Install: brew install hey"
        exit 1
    fi

    local protocol="http"
    [[ "$SSL" == "true" ]] && protocol="https"

    local host
    host=$(echo "$TARGET" | cut -d: -f1)

    # Determine endpoints and parameters based on scenario
    # Health endpoints use the health port (8081), REST API uses the API port (8190)
    local duration=60
    local rps=10
    local concurrency=5

    case "$SCENARIO" in
        smoke)
            duration=30
            rps=10
            concurrency=5
            ;;
        baseline)
            duration=120
            rps=100
            concurrency=20
            ;;
        stress)
            duration=60
            rps=500
            concurrency=50
            ;;
        endurance)
            duration=300
            rps=50
            concurrency=10
            ;;
        scenario87)
            duration=120
            rps=50
            concurrency=10
            ;;
        scenario88)
            duration=120
            rps=40
            concurrency=10
            ;;
        custom)
            duration="${DURATION_OVERRIDE:-60}"
            rps="${RPS_OVERRIDE:-10}"
            concurrency=$((rps / 2))
            [[ "$concurrency" -lt 1 ]] && concurrency=1
            ;;
    esac

    # Apply overrides
    [[ -n "$RPS_OVERRIDE" ]] && rps="$RPS_OVERRIDE"
    [[ -n "$DURATION_OVERRIDE" ]] && duration="$DURATION_OVERRIDE"

    # hey -q is per-worker rate. Calculate per-worker QPS for target total RPS.
    local qps_per_worker
    qps_per_worker=$(awk "BEGIN {v=int(${rps}/${concurrency}); if(v<1) v=1; print v}")

    log_info "Tool: hey"
    log_info "Health target: ${protocol}://${host}:${LOCAL_HEALTH_PORT}"
    log_info "API target: ${protocol}://${TARGET}"
    log_info "Target RPS: ${rps} (${concurrency} workers x ${qps_per_worker} QPS/worker), Duration: ${duration}s"
    echo ""

    local all_pass=true

    # Test 1: /healthz on health port
    local url="${protocol}://${host}:${LOCAL_HEALTH_PORT}/healthz"
    local output_file="${TEST_RUN_DIR}/hey_healthz.txt"
    log_info "Testing: /healthz on port ${LOCAL_HEALTH_PORT} (~${rps} RPS, ${duration}s)"

    hey -q "$qps_per_worker" \
        -c "$concurrency" \
        -z "${duration}s" \
        -H "User-Agent: yandex-tank/perftest" \
        "$url" > "$output_file" 2>&1 || {
        log_error "hey failed for /healthz"
        all_pass=false
    }
    if [[ -f "$output_file" ]] && grep -q "Requests/sec" "$output_file"; then
        log_success "Completed: /healthz"
        parse_hey_results "$output_file" "/healthz"
    fi

    # Test 2: /readyz on health port
    url="${protocol}://${host}:${LOCAL_HEALTH_PORT}/readyz"
    output_file="${TEST_RUN_DIR}/hey_readyz.txt"
    log_info "Testing: /readyz on port ${LOCAL_HEALTH_PORT} (~${rps} RPS, ${duration}s)"

    hey -q "$qps_per_worker" \
        -c "$concurrency" \
        -z "${duration}s" \
        -H "User-Agent: yandex-tank/perftest" \
        "$url" > "$output_file" 2>&1 || {
        log_error "hey failed for /readyz"
        all_pass=false
    }
    if [[ -f "$output_file" ]] && grep -q "Requests/sec" "$output_file"; then
        log_success "Completed: /readyz"
        parse_hey_results "$output_file" "/readyz"
    fi

    # Test 3: /healthz on REST API port (JSON response)
    url="${protocol}://${TARGET}/healthz"
    output_file="${TEST_RUN_DIR}/hey_api_healthz.txt"
    log_info "Testing: /healthz on REST API port ${LOCAL_API_PORT} (~${rps} RPS, ${duration}s)"

    hey -q "$qps_per_worker" \
        -c "$concurrency" \
        -z "${duration}s" \
        -H "User-Agent: yandex-tank/perftest" \
        "$url" > "$output_file" 2>&1 || {
        log_error "hey failed for API /healthz"
        all_pass=false
    }
    if [[ -f "$output_file" ]] && grep -q "Requests/sec" "$output_file"; then
        log_success "Completed: API /healthz"
        parse_hey_results "$output_file" "/api-healthz"
    fi

    # Generate combined summary
    generate_hey_summary

    if [[ "$all_pass" == "true" ]]; then
        return 0
    else
        return 1
    fi
}

# Parse hey output and extract metrics
parse_hey_results() {
    local output_file="$1"
    local endpoint="$2"

    if [[ ! -f "$output_file" ]]; then
        log_warn "No output file for ${endpoint}"
        return
    fi

    local rps_achieved avg_latency p50 p95 p99
    rps_achieved=$(grep "Requests/sec:" "$output_file" | awk '{print $2}')
    avg_latency=$(grep "Average:" "$output_file" | awk '{print $2}')

    # hey percentile format: "  50%% in 0.0029 secs"
    p50=$(grep "50%%" "$output_file" | awk '{print $3}')
    p95=$(grep "95%%" "$output_file" | awk '{print $3}')
    p99=$(grep "99%%" "$output_file" | awk '{print $3}')

    # Convert to ms for display
    local avg_ms p50_ms p95_ms p99_ms
    avg_ms=$(awk "BEGIN {printf \"%.2f\", ${avg_latency:-0} * 1000}")
    p50_ms=$(awk "BEGIN {printf \"%.2f\", ${p50:-0} * 1000}")
    p95_ms=$(awk "BEGIN {printf \"%.2f\", ${p95:-0} * 1000}")
    p99_ms=$(awk "BEGIN {printf \"%.2f\", ${p99:-0} * 1000}")

    # Count status codes
    local status_200 status_4xx status_5xx total_responses
    status_200=$(grep -E "^\s*\[200\]" "$output_file" | awk '{print $2}' || echo "0")
    status_4xx=$(grep -E "^\s*\[4[0-9][0-9]\]" "$output_file" | awk '{sum+=$2} END {print sum+0}')
    status_5xx=$(grep -E "^\s*\[5[0-9][0-9]\]" "$output_file" | awk '{sum+=$2} END {print sum+0}')
    total_responses=$(grep -E "^\s*\[[0-9]+\]" "$output_file" | awk '{sum+=$2} END {print sum+0}')
    [[ -z "$status_200" ]] && status_200=0
    [[ -z "$status_4xx" ]] && status_4xx=0
    [[ -z "$status_5xx" ]] && status_5xx=0
    [[ -z "$total_responses" || "$total_responses" == "0" ]] && total_responses=1

    echo ""
    echo -e "  ${CYAN}${endpoint}${NC}"
    echo -e "  ─────────────────────────────────────────"
    echo -e "  RPS Achieved:  ${rps_achieved}"
    echo -e "  Avg Latency:   ${avg_ms} ms"
    echo -e "  p50:           ${p50_ms} ms"
    echo -e "  p95:           ${p95_ms} ms"
    echo -e "  p99:           ${p99_ms} ms"
    echo -e "  Total:         ${total_responses} responses"
    echo -e "  Status 200:    ${GREEN}${status_200}${NC}"
    [[ "${status_4xx}" != "0" ]] && echo -e "  Status 4xx:    ${YELLOW}${status_4xx}${NC}"
    [[ "${status_5xx}" != "0" ]] && echo -e "  Status 5xx:    ${RED}${status_5xx}${NC}"
    echo ""
}

# Generate combined summary from hey results
generate_hey_summary() {
    log_header "Combined Results Summary"

    local summary_file="${TEST_RUN_DIR}/results_summary.json"
    local report_file="${TEST_RUN_DIR}/REPORT.md"

    # Collect all hey output files
    local hey_files=("${TEST_RUN_DIR}"/hey_*.txt)

    if [[ ${#hey_files[@]} -eq 0 ]]; then
        log_warn "No hey result files found"
        return
    fi

    # Generate JSON summary
    echo "{" > "$summary_file"
    echo "  \"scenario\": \"${SCENARIO}\"," >> "$summary_file"
    echo "  \"timestamp\": \"${TIMESTAMP}\"," >> "$summary_file"
    echo "  \"target\": \"${TARGET}\"," >> "$summary_file"
    echo "  \"tool\": \"hey\"," >> "$summary_file"
    echo "  \"endpoints\": {" >> "$summary_file"

    local first=true
    for f in "${hey_files[@]}"; do
        local endpoint_name
        endpoint_name=$(basename "$f" .txt | sed 's/hey_//' | tr '_' '/')
        local rps_val avg_val p50_val p95_val p99_val
        rps_val=$(grep "Requests/sec:" "$f" | awk '{print $2}' || echo "0")
        avg_val=$(grep "Average:" "$f" | awk '{print $2}' || echo "0")
        p50_val=$(grep "50%%" "$f" | awk '{print $3}' || echo "0")
        p95_val=$(grep "95%%" "$f" | awk '{print $3}' || echo "0")
        p99_val=$(grep "99%%" "$f" | awk '{print $3}' || echo "0")

        # Convert seconds to ms
        local avg_ms p50_ms p95_ms p99_ms
        avg_ms=$(awk "BEGIN {printf \"%.2f\", ${avg_val:-0} * 1000}")
        p50_ms=$(awk "BEGIN {printf \"%.2f\", ${p50_val:-0} * 1000}")
        p95_ms=$(awk "BEGIN {printf \"%.2f\", ${p95_val:-0} * 1000}")
        p99_ms=$(awk "BEGIN {printf \"%.2f\", ${p99_val:-0} * 1000}")

        [[ "$first" == "true" ]] && first=false || echo "," >> "$summary_file"
        cat >> "$summary_file" <<ENDPOINT
    "${endpoint_name}": {
      "rps": ${rps_val:-0},
      "latency_ms": {
        "avg": ${avg_ms},
        "p50": ${p50_ms},
        "p95": ${p95_ms},
        "p99": ${p99_ms}
      }
    }
ENDPOINT
    done

    echo "  }" >> "$summary_file"
    echo "}" >> "$summary_file"

    log_success "Results saved to: ${summary_file}"

    # SLO evaluation
    log_header "SLO Evaluation"

    for f in "${hey_files[@]}"; do
        local endpoint_name
        endpoint_name=$(basename "$f" .txt | sed 's/hey_//' | tr '_' '/')
        local p95_val p99_val error_pct
        p95_val=$(grep "95%%" "$f" | awk '{print $3}' || echo "0")
        p99_val=$(grep "99%%" "$f" | awk '{print $3}' || echo "0")

        local p95_ms p99_ms
        p95_ms=$(awk "BEGIN {printf \"%.2f\", ${p95_val:-0} * 1000}")
        p99_ms=$(awk "BEGIN {printf \"%.2f\", ${p99_val:-0} * 1000}")

        # Count errors
        local total_resp status_non200
        total_resp=$(grep -E "^\s*\[[0-9]+\]" "$f" | awk '{sum+=$2} END {print sum+0}')
        status_non200=$(grep -E "^\s*\[[^2][0-9][0-9]\]" "$f" | awk '{sum+=$2} END {print sum+0}')
        [[ -z "$total_resp" || "$total_resp" == "0" ]] && total_resp=1
        [[ -z "$status_non200" ]] && status_non200=0
        error_pct=$(awk "BEGIN {printf \"%.2f\", ($status_non200 / $total_resp) * 100}")

        echo -e "  ${CYAN}${endpoint_name}${NC}"

        case "$SCENARIO" in
            smoke)
                evaluate_slo "p95 Latency (ms)" "$p95_ms" "1000" "< 1000ms"
                evaluate_slo "Error Rate (%)" "$error_pct" "0" "= 0%"
                ;;
            baseline)
                evaluate_slo "p95 Latency (ms)" "$p95_ms" "200" "< 200ms"
                evaluate_slo "p99 Latency (ms)" "$p99_ms" "500" "< 500ms"
                evaluate_slo "Error Rate (%)" "$error_pct" "0.1" "< 0.1%"
                ;;
            *)
                evaluate_slo "p95 Latency (ms)" "$p95_ms" "1000" "< 1000ms"
                evaluate_slo "Error Rate (%)" "$error_pct" "1" "< 1%"
                ;;
        esac
        echo ""
    done
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    # Stop port-forwards
    if [[ $PF_PID_COUNT -gt 0 ]]; then
        stop_port_forwards
    fi
    # Stop any running Docker containers
    docker rm -f "yandex-tank-${SCENARIO}-${TIMESTAMP}" 2>/dev/null || true
}

# =============================================================================
# Main
# =============================================================================

main() {
    parse_args "$@"
    validate_scenario

    log_header "Cloudberry Operator - Performance Test"
    log_info "Scenario: ${SCENARIO}"
    log_info "Timestamp: ${TIMESTAMP}"

    # Create results directory
    mkdir -p "${TEST_RUN_DIR}"

    # Handle --generate-data flag
    if [[ "$GENERATE_DATA" == "true" ]]; then
        generate_data
        if [[ "$SQL_BENCH" != "true" && "$ANALYZE_ONLY" != "true" ]]; then
            # If only generating data, exit after generation
            if [[ "$SCENARIO" == "smoke" && "$RPS_OVERRIDE" == "" ]]; then
                log_success "Dataset generated. Use --sql-bench to run SQL benchmarks."
                exit 0
            fi
        fi
    fi

    # Handle --sql-bench flag
    if [[ "$SQL_BENCH" == "true" ]]; then
        run_sql_bench
        exit 0
    fi

    if [[ "$ANALYZE_ONLY" == "true" ]]; then
        analyze_results
        # Try to generate charts from the latest results
        local latest_dir
        latest_dir=$(ls -td "${RESULTS_DIR}"/*/ 2>/dev/null | head -1)
        if [[ -n "$latest_dir" ]]; then
            local phout="${latest_dir}/phout.txt"
            if [[ -f "$phout" ]]; then
                generate_latency_chart "$phout"
                generate_rps_chart "$phout"
                generate_status_chart "$phout"
            fi
        fi
        exit 0
    fi

    # Set up cleanup trap early
    trap cleanup EXIT

    # Start port-forwards if requested
    if [[ "$PORT_FORWARD" == "true" ]]; then
        start_port_forwards
    fi

    if [[ "$USE_HEY" == "true" ]]; then
        # hey-based mode (works on macOS without Docker --net host)
        check_target
        print_test_summary

        if [[ "$DRY_RUN" == "true" ]]; then
            log_success "Dry run complete. Configuration is valid."
            exit 0
        fi

        # Record test start
        echo "${TIMESTAMP}" > "${TEST_RUN_DIR}/test_start"
        echo "${SCENARIO}" > "${TEST_RUN_DIR}/scenario"
        echo "hey" > "${TEST_RUN_DIR}/tool"

        local exit_code=0
        run_hey || exit_code=$?

        # Record test end
        date +%Y%m%d_%H%M%S > "${TEST_RUN_DIR}/test_end"

        log_header "Test Complete"
        log_info "Results directory: ${TEST_RUN_DIR}"
        exit $exit_code
    fi

    # Yandex Tank mode (Docker or native)
    check_prerequisites
    check_target
    print_test_summary

    if [[ "$DRY_RUN" == "true" ]]; then
        log_success "Dry run complete. Configuration is valid."
        exit 0
    fi

    # Confirm before running stress/endurance tests
    if [[ "$SCENARIO" == "stress" || "$SCENARIO" == "endurance" ]]; then
        echo -e "${YELLOW}WARNING: This is a ${SCENARIO} test that may generate significant load.${NC}"
        echo -n "Continue? [y/N] "
        read -r confirm
        if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
            log_info "Test cancelled."
            exit 0
        fi
    fi

    # Create runtime config with overrides
    local runtime_config
    runtime_config=$(create_runtime_config)
    log_info "Runtime config: ${runtime_config}"

    # Record test start
    echo "${TIMESTAMP}" > "${TEST_RUN_DIR}/test_start"
    echo "${SCENARIO}" > "${TEST_RUN_DIR}/scenario"
    echo "yandex-tank" > "${TEST_RUN_DIR}/tool"

    # Run the test
    local exit_code=0
    if [[ "$USE_DOCKER" == "true" ]]; then
        run_docker "${runtime_config}" || exit_code=$?
    else
        run_native "${runtime_config}" || exit_code=$?
    fi

    # Record test end
    date +%Y%m%d_%H%M%S > "${TEST_RUN_DIR}/test_end"

    if [[ "$exit_code" -ne 0 ]]; then
        log_warn "Test exited with code ${exit_code} (may be autostop)"
    fi

    # Analyze results
    analyze_results || true

    # Generate charts
    local phout="${TEST_RUN_DIR}/phout.txt"
    if [[ -f "$phout" ]]; then
        generate_latency_chart "$phout"
        generate_rps_chart "$phout"
        generate_status_chart "$phout"
    fi

    log_header "Test Complete"
    log_info "Results directory: ${TEST_RUN_DIR}"
    log_info "To re-analyze: ./run-perftest.sh --analyze-only"

    exit $exit_code
}

main "$@"
