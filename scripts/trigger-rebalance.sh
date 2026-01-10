#!/bin/bash
# Trigger partition rebalance

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Use grpcurl to trigger rebalance
grpcurl -plaintext \
  -d '{
    "force": true,
    "max_concurrent": 5,
    "bandwidth_limit": 104857600
  }' \
  localhost:9000 \
  rockskv.MetadataService/TriggerRebalance

echo ""
echo "Rebalance triggered. Use the following command to check status:"
echo "  grpcurl -plaintext localhost:9000 rockskv.MetadataService/GetMigrationStatus"
