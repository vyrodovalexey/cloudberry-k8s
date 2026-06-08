#!/usr/bin/env bash
# =============================================================================
# Cloudberry Operator - Performance Test Dataset Generator
# =============================================================================
#
# Generates a ~100MB CSV dataset with realistic data for performance testing.
# Produces two tables: orders (~95MB) and customers (~5MB).
#
# Usage:
#   ./generate-dataset.sh [OPTIONS]
#
# Options:
#   --output-dir <dir>   Output directory (default: ./data)
#   --orders <count>     Number of order rows (default: 1000000)
#   --customers <count>  Number of customer rows (default: 50000)
#   --help               Show this help message
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/data"
ORDER_COUNT=1000000
CUSTOMER_COUNT=50000

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $*"
}

usage() {
    head -17 "$0" | tail -14
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        --orders) ORDER_COUNT="$2"; shift 2 ;;
        --customers) CUSTOMER_COUNT="$2"; shift 2 ;;
        --help|-h) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

mkdir -p "${OUTPUT_DIR}"

# ─────────────────────────────────────────────────────────────────────────────
# Generate customers table (~5MB)
# ─────────────────────────────────────────────────────────────────────────────
log_info "Generating ${CUSTOMER_COUNT} customers..."

CUSTOMERS_FILE="${OUTPUT_DIR}/customers.csv"

awk -v count="${CUSTOMER_COUNT}" '
BEGIN {
    srand(42)
    print "id,first_name,last_name,email,city,state,country,created_at"

    split("James,John,Robert,Michael,David,William,Richard,Joseph,Thomas,Charles," \
          "Mary,Patricia,Jennifer,Linda,Barbara,Elizabeth,Susan,Jessica,Sarah,Karen", first_names, ",")
    split("Smith,Johnson,Williams,Brown,Jones,Garcia,Miller,Davis,Rodriguez,Martinez," \
          "Hernandez,Lopez,Gonzalez,Wilson,Anderson,Thomas,Taylor,Moore,Jackson,Martin", last_names, ",")
    split("New York,Los Angeles,Chicago,Houston,Phoenix,Philadelphia,San Antonio," \
          "San Diego,Dallas,San Jose,Austin,Jacksonville,Fort Worth,Columbus,Charlotte", cities, ",")
    split("NY,CA,IL,TX,AZ,PA,TX,CA,TX,CA,TX,FL,TX,OH,NC", states, ",")
    split("US,US,US,US,US,US,US,US,US,US,US,US,US,US,US", countries, ",")

    fn_count = 20
    ln_count = 20
    city_count = 15

    for (i = 1; i <= count; i++) {
        fn_idx = int(rand() * fn_count) + 1
        ln_idx = int(rand() * ln_count) + 1
        city_idx = int(rand() * city_count) + 1

        fn = first_names[fn_idx]
        ln = last_names[ln_idx]
        email = tolower(fn) "." tolower(ln) i "@example.com"
        city = cities[city_idx]
        state = states[city_idx]
        country = countries[city_idx]

        year = 2020 + int(rand() * 6)
        month = int(rand() * 12) + 1
        day = int(rand() * 28) + 1
        printf "%d,%s,%s,%s,%s,%s,%s,%04d-%02d-%02d\n", \
            i, fn, ln, email, city, state, country, year, month, day
    }
}' > "${CUSTOMERS_FILE}"

CUST_SIZE=$(wc -c < "${CUSTOMERS_FILE}" | tr -d ' ')
log_success "Customers: ${CUSTOMERS_FILE} ($(( CUST_SIZE / 1024 / 1024 ))MB, ${CUSTOMER_COUNT} rows)"

# ─────────────────────────────────────────────────────────────────────────────
# Generate orders table (~95MB)
# ─────────────────────────────────────────────────────────────────────────────
log_info "Generating ${ORDER_COUNT} orders..."

ORDERS_FILE="${OUTPUT_DIR}/orders.csv"

awk -v count="${ORDER_COUNT}" -v cust_count="${CUSTOMER_COUNT}" '
BEGIN {
    srand(12345)
    print "id,customer_id,product_id,quantity,price,order_date,status,notes"

    split("pending,processing,shipped,delivered,cancelled,returned,refunded", statuses, ",")
    status_count = 7

    # Product IDs range from 1 to 10000
    product_max = 10000

    for (i = 1; i <= count; i++) {
        customer_id = int(rand() * cust_count) + 1
        product_id = int(rand() * product_max) + 1
        quantity = int(rand() * 20) + 1
        # Price between 1.00 and 999.99
        price = int(rand() * 99899 + 100) / 100.0
        status_idx = int(rand() * status_count) + 1
        status = statuses[status_idx]

        year = 2022 + int(rand() * 4)
        month = int(rand() * 12) + 1
        day = int(rand() * 28) + 1
        hour = int(rand() * 24)
        minute = int(rand() * 60)
        second = int(rand() * 60)

        # Generate a notes field with variable length to reach ~100 bytes per row
        notes_len = int(rand() * 40) + 10
        notes = ""
        for (j = 0; j < notes_len; j++) {
            c = int(rand() * 26) + 97
            notes = notes sprintf("%c", c)
        }

        printf "%d,%d,%d,%d,%.2f,%04d-%02d-%02d %02d:%02d:%02d,%s,%s\n", \
            i, customer_id, product_id, quantity, price, \
            year, month, day, hour, minute, second, status, notes
    }
}' > "${ORDERS_FILE}"

ORD_SIZE=$(wc -c < "${ORDERS_FILE}" | tr -d ' ')
log_success "Orders: ${ORDERS_FILE} ($(( ORD_SIZE / 1024 / 1024 ))MB, ${ORDER_COUNT} rows)"

TOTAL_SIZE=$(( (CUST_SIZE + ORD_SIZE) / 1024 / 1024 ))
log_success "Total dataset size: ${TOTAL_SIZE}MB"
log_success "Dataset generation complete."
