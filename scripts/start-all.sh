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

# Start etcd first (if not running)
echo ""
ETCD_DATA_DIR="$PROJECT_DIR/data/etcd"
ETCD_PID_FILE="$PID_DIR/etcd.pid"
ETCD_LOG_FILE="$LOG_DIR/etcd.log"

if [ -f "$ETCD_PID_FILE" ] && kill -0 "$(cat "$ETCD_PID_FILE")" 2>/dev/null; then
    echo -e "${YELLOW}[SKIP]${NC} etcd is already running (PID: $(cat "$ETCD_PID_FILE"))"
elif pgrep -x etcd > /dev/null; then
    echo -e "${YELLOW}[SKIP]${NC} etcd is already running externally"
else
    if ! command -v etcd &> /dev/null; then
        echo -e "${RED}[FAIL]${NC} etcd is not installed. Please install it first (brew install etcd)"
        exit 1
    fi
    mkdir -p "$ETCD_DATA_DIR"
    echo -e "${GREEN}[START]${NC} Starting etcd..."
    nohup etcd --data-dir "$ETCD_DATA_DIR" \
        --listen-client-urls http://localhost:2379 \
        --advertise-client-urls http://localhost:2379 \
        --listen-peer-urls http://localhost:2380 > "$ETCD_LOG_FILE" 2>&1 &
    echo $! > "$ETCD_PID_FILE"
    sleep 2
    if kill -0 "$(cat "$ETCD_PID_FILE")" 2>/dev/null; then
        echo -e "${GREEN}[OK]${NC} etcd started (PID: $(cat "$ETCD_PID_FILE"))"
    else
        echo -e "${RED}[FAIL]${NC} etcd failed to start. Check $ETCD_LOG_FILE"
        exit 1
    fi
fi

# Start services in order
echo ""
start_service "metadata" "$SCRIPT_DIR/start-metadata.sh"
sleep 2

start_service "storage" "$SCRIPT_DIR/start-storage.sh"
sleep 1

start_service "storage-2" "$SCRIPT_DIR/start-storage-2.sh"
sleep 1

start_service "storage-3" "$SCRIPT_DIR/start-storage-3.sh"
sleep 1

start_service "storage-4" "$SCRIPT_DIR/start-storage-4.sh"
sleep 1

start_service "compute" "$SCRIPT_DIR/start-compute.sh"
sleep 1

start_service "compute-2" "$SCRIPT_DIR/start-compute-2.sh"
sleep 1

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  All services started!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services:"
echo "  - etcd:      localhost:2379"
echo "  - metadata:  localhost:9000"
echo "  - storage-1: localhost:9001"
echo "  - storage-2: localhost:9002"
echo "  - storage-3: localhost:9003"
echo "  - storage-4: localhost:9004"
echo "  - compute-1: localhost:8000 (client endpoint)"
echo "  - compute-2: localhost:8001 (client endpoint)"
echo ""
echo "Test with CLI:"
echo "  ./bin/darwin-arm64/rockskv-cli put foo bar"
echo "  ./bin/darwin-arm64/rockskv-cli get foo"
echo "  ./bin/darwin-arm64/rockskv-cli -addr localhost:8001 get foo"
echo ""
echo "View logs:"
echo "  tail -f logs/etcd.log"
echo "  tail -f logs/metadata.log"
echo "  tail -f logs/storage.log"
echo "  tail -f logs/storage-2.log"
echo "  tail -f logs/storage-3.log"
echo "  tail -f logs/storage-4.log"
echo "  tail -f logs/compute.log"
echo "  tail -f logs/compute-2.log"
echo ""
echo "Stop all services:"
echo "  ./scripts/stop-all.sh"
echo ""
