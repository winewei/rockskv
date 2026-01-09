#!/bin/bash
# Start etcd for local development

set -e

ETCD_DATA_DIR="${ETCD_DATA_DIR:-./data/etcd}"

# Check if etcd is installed
if ! command -v etcd &> /dev/null; then
    echo "etcd is not installed. Please install it first:"
    echo ""
    echo "  macOS:   brew install etcd"
    echo "  Ubuntu:  sudo apt-get install etcd"
    echo ""
    exit 1
fi

# Create data directory
mkdir -p "$ETCD_DATA_DIR"

echo "Starting etcd..."
echo "  Data directory: $ETCD_DATA_DIR"
echo "  Client URL: http://localhost:2379"
echo ""

exec etcd \
    --data-dir "$ETCD_DATA_DIR" \
    --listen-client-urls http://localhost:2379 \
    --advertise-client-urls http://localhost:2379 \
    --listen-peer-urls http://localhost:2380
