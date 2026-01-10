#!/bin/bash
# Smoke test for RocksKV services
# This script tests basic put/get/delete operations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="amd64" ;;
    arm64|aarch64) ARCH="arm64" ;;
esac

CLI_BINARY="$PROJECT_DIR/bin/${OS}-${ARCH}/rockskv-cli"

# Fallback to default bin
if [ ! -f "$CLI_BINARY" ]; then
    CLI_BINARY="$PROJECT_DIR/bin/rockskv-cli"
fi

if [ ! -f "$CLI_BINARY" ]; then
    echo -e "${RED}Error: CLI binary not found. Please build first.${NC}"
    exit 1
fi

COMPUTE_ADDR="${COMPUTE_ADDR:-localhost:8000}"
TEST_PREFIX="smoke_test_$(date +%s)"
FAILED=0
PASSED=0

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RocksKV Smoke Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "CLI Binary: $CLI_BINARY"
echo "Compute Address: $COMPUTE_ADDR"
echo "Test Prefix: $TEST_PREFIX"
echo ""

# Helper functions
run_test() {
    local name=$1
    local cmd=$2
    local expected=$3

    printf "  %-40s" "$name"

    result=$(eval "$cmd" 2>/dev/null) || true

    if echo "$result" | grep -q "$expected"; then
        echo -e "${GREEN}PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "    Expected: $expected"
        echo "    Got: $result"
        ((FAILED++))
        return 1
    fi
}

run_test_exact() {
    local name=$1
    local cmd=$2
    local expected=$3

    printf "  %-40s" "$name"

    result=$(eval "$cmd" 2>/dev/null) || true

    if [ "$result" = "$expected" ]; then
        echo -e "${GREEN}PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "    Expected: '$expected'"
        echo "    Got: '$result'"
        ((FAILED++))
        return 1
    fi
}

# Wait for services to be ready
wait_for_service() {
    local addr=$1
    local max_attempts=${2:-30}
    local attempt=0

    echo -n "Waiting for $addr to be ready"
    while [ $attempt -lt $max_attempts ]; do
        if nc -z ${addr/:/ } 2>/dev/null; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        ((attempt++))
    done
    echo -e " ${RED}TIMEOUT${NC}"
    return 1
}

# Check if services are running
echo "Checking services..."
if ! wait_for_service "localhost:8000" 5; then
    echo -e "${RED}Compute service not running on localhost:8000${NC}"
    echo "Please start the services first:"
    echo "  ./scripts/start-all.sh"
    exit 1
fi

echo ""
echo "Running tests..."
echo ""

# Test 1: Basic Put
echo "Test Group: Basic Operations"
KEY1="${TEST_PREFIX}_key1"
VALUE1="hello_rockskv"

run_test "Put key-value" \
    "$CLI_BINARY -addr $COMPUTE_ADDR put $KEY1 $VALUE1" \
    "OK"

# Test 2: Basic Get
run_test_exact "Get existing key" \
    "$CLI_BINARY -addr $COMPUTE_ADDR get $KEY1" \
    "$VALUE1"

# Test 3: Get non-existent key
KEY_NONEXISTENT="${TEST_PREFIX}_nonexistent"
run_test "Get non-existent key" \
    "$CLI_BINARY -addr $COMPUTE_ADDR get $KEY_NONEXISTENT" \
    "(nil)"

# Test 4: Update existing key
VALUE2="updated_value"
run_test "Update existing key" \
    "$CLI_BINARY -addr $COMPUTE_ADDR put $KEY1 $VALUE2" \
    "OK"

run_test_exact "Verify updated value" \
    "$CLI_BINARY -addr $COMPUTE_ADDR get $KEY1" \
    "$VALUE2"

# Test 5: Delete key
run_test "Delete key" \
    "$CLI_BINARY -addr $COMPUTE_ADDR delete $KEY1" \
    "OK"

run_test "Verify key deleted" \
    "$CLI_BINARY -addr $COMPUTE_ADDR get $KEY1" \
    "(nil)"

# Test 6: Multiple keys (test routing to different partitions)
echo ""
echo "Test Group: Multiple Keys (Partition Routing)"

for i in {1..10}; do
    KEY="${TEST_PREFIX}_multi_$i"
    VALUE="value_$i"
    run_test "Put key $i" \
        "$CLI_BINARY -addr $COMPUTE_ADDR put $KEY $VALUE" \
        "OK"
done

for i in {1..10}; do
    KEY="${TEST_PREFIX}_multi_$i"
    VALUE="value_$i"
    run_test_exact "Get key $i" \
        "$CLI_BINARY -addr $COMPUTE_ADDR get $KEY" \
        "$VALUE"
done

# Cleanup
echo ""
echo "Cleaning up test keys..."
for i in {1..10}; do
    KEY="${TEST_PREFIX}_multi_$i"
    $CLI_BINARY -addr $COMPUTE_ADDR delete $KEY >/dev/null 2>&1 || true
done

# Summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Test Summary${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "  Passed: ${GREEN}$PASSED${NC}"
echo -e "  Failed: ${RED}$FAILED${NC}"
echo ""

if [ $FAILED -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
