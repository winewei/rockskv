#!/bin/bash
# Start RocksKV Compute Service

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="${CONFIG_FILE:-$PROJECT_DIR/config/local/compute.yaml}"

# Detect platform and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="amd64" ;;
    arm64|aarch64) ARCH="arm64" ;;
esac

BINARY="$PROJECT_DIR/bin/${OS}-${ARCH}/rockskv-compute"

# Fallback to default bin if platform-specific doesn't exist
if [ ! -f "$BINARY" ]; then
    BINARY="$PROJECT_DIR/bin/rockskv-compute"
fi

if [ ! -f "$BINARY" ]; then
    echo "Error: Binary not found. Please build first:"
    echo "  make build-${OS}-${ARCH}"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found: $CONFIG_FILE"
    exit 1
fi

echo "Starting RocksKV Compute Service..."
echo "  Binary: $BINARY"
echo "  Config: $CONFIG_FILE"
echo ""

exec "$BINARY" -c "$CONFIG_FILE"
