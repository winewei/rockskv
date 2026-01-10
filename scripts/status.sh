#!/bin/bash
# Check status of RocksKV services

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PID_DIR="$PROJECT_DIR/.pids"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RocksKV Service Status${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

check_service() {
    local name=$1
    local port=$2
    local pid_file="$PID_DIR/${name}.pid"

    printf "%-12s" "$name:"

    # Check PID file
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${GREEN}RUNNING${NC} (PID: $pid, Port: $port)"
            return 0
        else
            echo -e "${RED}DEAD${NC} (stale PID file)"
            return 1
        fi
    else
        # Check if port is in use
        if lsof -i ":$port" > /dev/null 2>&1 || nc -z localhost "$port" 2>/dev/null; then
            echo -e "${YELLOW}RUNNING${NC} (external, Port: $port)"
            return 0
        else
            echo -e "${RED}STOPPED${NC}"
            return 1
        fi
    fi
}

# Check etcd
printf "%-12s" "etcd:"
if pgrep -x "etcd" > /dev/null; then
    echo -e "${GREEN}RUNNING${NC} (Port: 2379)"
else
    echo -e "${RED}STOPPED${NC}"
fi

# Check RocksKV services
check_service "metadata" 9000
check_service "storage" 9001
check_service "storage-2" 9002
check_service "storage-3" 9003
check_service "storage-4" 9004
check_service "compute" 8000
check_service "compute-2" 8001

echo ""
echo "Log files: $PROJECT_DIR/logs/"
echo "PID files: $PID_DIR/"
echo ""
