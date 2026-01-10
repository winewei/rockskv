#!/bin/bash
# Stress test for RocksKV
# Tests concurrent read/write performance

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="amd64" ;;
    arm64|aarch64) ARCH="arm64" ;;
esac

CLI="$PROJECT_DIR/bin/${OS}-${ARCH}/rockskv-cli"

if [ ! -f "$CLI" ]; then
    echo -e "${RED}Error: CLI binary not found. Please build first.${NC}"
    exit 1
fi

# Default parameters
NUM_OPS=${1:-1000}
NUM_CLIENTS=${2:-10}
VALUE_SIZE=${3:-64}
COMPUTE_ADDRS=("localhost:8000" "localhost:8001")

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RocksKV Stress Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Parameters:"
echo "  Operations per client: $NUM_OPS"
echo "  Concurrent clients:    $NUM_CLIENTS"
echo "  Value size:            $VALUE_SIZE bytes"
echo "  Compute nodes:         ${COMPUTE_ADDRS[*]}"
echo ""

# Generate random string
random_string() {
    local length=$1
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w "$length" | head -n 1
}

# Test function for a single client
run_client() {
    local client_id=$1
    local num_ops=$2
    local key_prefix="stress_c${client_id}_"
    local addr=${COMPUTE_ADDRS[$((client_id % ${#COMPUTE_ADDRS[@]}))]}
    local value=$(random_string $VALUE_SIZE)

    local start_time=$(python3 -c 'import time; print(time.time())')
    local put_count=0
    local get_count=0
    local errors=0

    # Write phase
    for ((i=0; i<num_ops; i++)); do
        key="${key_prefix}${i}"
        result=$($CLI -addr "$addr" put "$key" "$value" 2>/dev/null)
        if [[ "$result" == "OK" ]]; then
            ((put_count++))
        else
            ((errors++))
        fi
    done

    # Read phase
    for ((i=0; i<num_ops; i++)); do
        key="${key_prefix}${i}"
        result=$($CLI -addr "$addr" get "$key" 2>/dev/null)
        if [[ "$result" == "$value" ]]; then
            ((get_count++))
        else
            ((errors++))
        fi
    done

    # Cleanup phase (silent)
    for ((i=0; i<num_ops; i++)); do
        key="${key_prefix}${i}"
        $CLI -addr "$addr" delete "$key" >/dev/null 2>&1 || true
    done

    local end_time=$(python3 -c 'import time; print(time.time())')
    local duration=$(python3 -c "print(f'{$end_time - $start_time:.2f}')")
    local total_ops=$((put_count + get_count))
    local ops_per_sec=$(python3 -c "print(f'{$total_ops / ($end_time - $start_time):.2f}')")

    echo "CLIENT_RESULT:$client_id,$put_count,$get_count,$errors,$duration,$ops_per_sec"
}

# Check services
echo -e "${CYAN}Checking services...${NC}"
for addr in "${COMPUTE_ADDRS[@]}"; do
    if nc -z ${addr/:/ } 2>/dev/null; then
        echo -e "  $addr: ${GREEN}OK${NC}"
    else
        echo -e "  $addr: ${RED}NOT AVAILABLE${NC}"
    fi
done
echo ""

# Run stress test
echo -e "${CYAN}Running stress test with $NUM_CLIENTS concurrent clients...${NC}"
echo ""

RESULTS_FILE=$(mktemp)
START_TIME=$(python3 -c 'import time; print(time.time())')

# Launch clients in parallel
for ((c=0; c<NUM_CLIENTS; c++)); do
    run_client $c $NUM_OPS >> "$RESULTS_FILE" &
done

# Wait for all clients to complete
wait

END_TIME=$(python3 -c 'import time; print(time.time())')
TOTAL_DURATION=$(python3 -c "print(f'{$END_TIME - $START_TIME:.2f}')")

# Aggregate results
TOTAL_PUTS=0
TOTAL_GETS=0
TOTAL_ERRORS=0

echo -e "${CYAN}Results per client:${NC}"
printf "  %-10s %-10s %-10s %-10s %-12s %-12s\n" "Client" "Puts" "Gets" "Errors" "Duration(s)" "Ops/sec"
echo "  ------------------------------------------------------------------------"

while IFS= read -r line; do
    if [[ "$line" == CLIENT_RESULT:* ]]; then
        data="${line#CLIENT_RESULT:}"
        IFS=',' read -r client_id puts gets errors duration ops_sec <<< "$data"
        printf "  %-10s %-10s %-10s %-10s %-12s %-12s\n" "$client_id" "$puts" "$gets" "$errors" "$duration" "$ops_sec"
        TOTAL_PUTS=$((TOTAL_PUTS + puts))
        TOTAL_GETS=$((TOTAL_GETS + gets))
        TOTAL_ERRORS=$((TOTAL_ERRORS + errors))
    fi
done < "$RESULTS_FILE"

rm -f "$RESULTS_FILE"

TOTAL_OPS=$((TOTAL_PUTS + TOTAL_GETS))
TOTAL_OPS_PER_SEC=$(python3 -c "print(f'{$TOTAL_OPS / $TOTAL_DURATION:.2f}')")
PUT_OPS_PER_SEC=$(python3 -c "print(f'{$TOTAL_PUTS / $TOTAL_DURATION:.2f}')")
GET_OPS_PER_SEC=$(python3 -c "print(f'{$TOTAL_GETS / $TOTAL_DURATION:.2f}')")

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Summary${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Total duration:     ${TOTAL_DURATION}s"
echo "  Total operations:   $TOTAL_OPS"
echo "  Total puts:         $TOTAL_PUTS"
echo "  Total gets:         $TOTAL_GETS"
echo "  Total errors:       $TOTAL_ERRORS"
echo ""
echo -e "${CYAN}  Throughput:${NC}"
echo "    Combined:  $TOTAL_OPS_PER_SEC ops/sec"
echo "    PUT:       $PUT_OPS_PER_SEC ops/sec"
echo "    GET:       $GET_OPS_PER_SEC ops/sec"
echo ""

if [ "$TOTAL_ERRORS" -gt 0 ]; then
    echo -e "${YELLOW}Warning: $TOTAL_ERRORS errors occurred during the test${NC}"
fi

echo -e "${GREEN}Stress test completed!${NC}"
