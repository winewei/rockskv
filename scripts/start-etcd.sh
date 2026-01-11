#!/bin/bash
# Start etcd for local development or testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Allow customization via environment variables
ETCD_DATA_DIR="${ETCD_DATA_DIR:-$PROJECT_DIR/data/etcd}"
ETCD_PID_FILE="${ETCD_PID_FILE:-$PROJECT_DIR/.pids/etcd.pid}"
ETCD_LOG_FILE="${ETCD_LOG_FILE:-$PROJECT_DIR/logs/etcd.log}"

# Check if etcd is installed
if ! command -v etcd &> /dev/null; then
    echo "etcd is not installed. Please install it first:"
    echo ""
    echo "  macOS:   brew install etcd"
    echo "  Ubuntu:  sudo apt-get install etcd"
    echo ""
    exit 1
fi

# Create directories
mkdir -p "$ETCD_DATA_DIR"
mkdir -p "$(dirname "$ETCD_PID_FILE")"
mkdir -p "$(dirname "$ETCD_LOG_FILE")"

# Check if etcd is already running
if [ -f "$ETCD_PID_FILE" ] && kill -0 "$(cat "$ETCD_PID_FILE")" 2>/dev/null; then
    echo "etcd is already running (PID: $(cat "$ETCD_PID_FILE"))"
    exit 0
fi

echo "Starting etcd..."
echo "  Data directory: $ETCD_DATA_DIR"
echo "  PID file: $ETCD_PID_FILE"
echo "  Log file: $ETCD_LOG_FILE"
echo "  Client URL: http://localhost:2379"
echo ""

# Save variable values before unsetting
DATA_DIR="$ETCD_DATA_DIR"
PID_FILE="$ETCD_PID_FILE"
LOG_FILE="$ETCD_LOG_FILE"

# Unset environment variables to avoid conflicts with command-line flags
unset ETCD_DATA_DIR ETCD_PID_FILE ETCD_LOG_FILE

# Start etcd in background and save PID
etcd \
    --data-dir "$DATA_DIR" \
    --listen-client-urls http://localhost:2379 \
    --advertise-client-urls http://localhost:2379 \
    --listen-peer-urls http://localhost:2380 \
    > "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"

# Wait a bit and verify it's running
sleep 1
if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "❌ etcd failed to start. Check $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi

echo "✅ etcd started successfully (PID: $(cat "$PID_FILE"))"
