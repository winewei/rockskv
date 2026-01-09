#!/bin/bash
# Stop all RocksKV services

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PID_DIR="$PROJECT_DIR/.pids"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Stopping RocksKV Services${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

stop_service() {
    local name=$1
    local pid_file="$PID_DIR/${name}.pid"

    if [ ! -f "$pid_file" ]; then
        echo -e "${YELLOW}[SKIP]${NC} $name: no PID file found"
        return 0
    fi

    local pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
        echo -e "${GREEN}[STOP]${NC} Stopping $name (PID: $pid)..."
        kill "$pid" 2>/dev/null

        # Wait for graceful shutdown
        for i in {1..10}; do
            if ! kill -0 "$pid" 2>/dev/null; then
                break
            fi
            sleep 0.5
        done

        # Force kill if still running
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}[WARN]${NC} Force killing $name..."
            kill -9 "$pid" 2>/dev/null
        fi

        echo -e "${GREEN}[OK]${NC} $name stopped"
    else
        echo -e "${YELLOW}[SKIP]${NC} $name is not running"
    fi

    rm -f "$pid_file"
}

# Stop in reverse order
stop_service "compute"
stop_service "storage-2"
stop_service "storage"
stop_service "metadata"

echo ""
echo -e "${GREEN}All RocksKV services stopped.${NC}"
echo ""
echo "Note: etcd is not stopped automatically."
echo "To stop etcd: pkill etcd"
echo ""
