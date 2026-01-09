#!/bin/bash
# Start all RocksKV services for local development

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/.pids"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

mkdir -p "$LOG_DIR" "$PID_DIR"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  RocksKV Local Development Startup${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Function to start a service
start_service() {
    local name=$1
    local script=$2
    local pid_file="$PID_DIR/${name}.pid"
    local log_file="$LOG_DIR/${name}.log"

    if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo -e "${YELLOW}[SKIP]${NC} $name is already running (PID: $(cat "$pid_file"))"
        return 0
    fi

    echo -e "${GREEN}[START]${NC} Starting $name..."
    nohup "$script" > "$log_file" 2>&1 &
    echo $! > "$pid_file"
    sleep 1

    if kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo -e "${GREEN}[OK]${NC} $name started (PID: $(cat "$pid_file"), Log: $log_file)"
    else
        echo -e "${RED}[FAIL]${NC} $name failed to start. Check $log_file"
        return 1
    fi
}

# Check if etcd is running
if ! pgrep -x "etcd" > /dev/null; then
    echo -e "${YELLOW}[INFO]${NC} etcd is not running."
    echo "Please start etcd first in a separate terminal:"
    echo ""
    echo "  ./scripts/start-etcd.sh"
    echo ""
    echo "Or if you want to run etcd in background:"
    echo ""
    echo "  nohup ./scripts/start-etcd.sh > logs/etcd.log 2>&1 &"
    echo ""
    read -p "Press Enter after starting etcd, or Ctrl+C to cancel..."
fi

# Wait for etcd to be ready
echo -e "${GREEN}[CHECK]${NC} Checking etcd connectivity..."
for i in {1..10}; do
    if command -v etcdctl &> /dev/null; then
        if etcdctl endpoint health --endpoints=localhost:2379 2>/dev/null | grep -q "is healthy"; then
            echo -e "${GREEN}[OK]${NC} etcd is ready"
            break
        fi
    else
        # If etcdctl is not available, try a simple connection test
        if curl -s http://localhost:2379/health 2>/dev/null | grep -q "true"; then
            echo -e "${GREEN}[OK]${NC} etcd is ready"
            break
        fi
    fi

    if [ $i -eq 10 ]; then
        echo -e "${RED}[FAIL]${NC} Could not connect to etcd at localhost:2379"
        exit 1
    fi
    echo -e "${YELLOW}[WAIT]${NC} Waiting for etcd... ($i/10)"
    sleep 1
done

# Start services in order
echo ""
start_service "metadata" "$SCRIPT_DIR/start-metadata.sh"
sleep 2

start_service "storage" "$SCRIPT_DIR/start-storage.sh"
sleep 2

start_service "compute" "$SCRIPT_DIR/start-compute.sh"
sleep 1

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  All services started!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services:"
echo "  - etcd:     http://localhost:2379"
echo "  - metadata: localhost:9000"
echo "  - storage:  localhost:9001"
echo "  - compute:  localhost:8000 (client endpoint)"
echo ""
echo "Test with CLI:"
echo "  ./bin/darwin-arm64/rockskv-cli put foo bar"
echo "  ./bin/darwin-arm64/rockskv-cli get foo"
echo ""
echo "View logs:"
echo "  tail -f logs/metadata.log"
echo "  tail -f logs/storage.log"
echo "  tail -f logs/compute.log"
echo ""
echo "Stop all services:"
echo "  ./scripts/stop-all.sh"
echo ""
