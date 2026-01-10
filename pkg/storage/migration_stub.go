//go:build !cgo || nocgo
// +build !cgo nocgo

package storage

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
)

// MigrationManager handles partition migration (stub implementation)
type MigrationManager struct {
	server *Server
	logger *zap.Logger
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(server *Server) *MigrationManager {
	return &MigrationManager{
		server: server,
		logger: common.NewLogger("migration"),
	}
}

// ExportPartition exports a partition to SST files (stub)
func (m *MigrationManager) ExportPartition(partitionID uint32, outputDir string) (string, int64, error) {
	return "", 0, fmt.Errorf("SST export not supported in stub implementation")
}

// IngestSST ingests an SST file into the database (stub)
func (m *MigrationManager) IngestSST(sstPath string) error {
	return fmt.Errorf("SST ingest not supported in stub implementation")
}

// MigratePartition migrates a partition from source to target node (stub)
func (m *MigrationManager) MigratePartition(ctx context.Context, partitionID uint32, sourceAddr, targetAddr string) error {
	return fmt.Errorf("partition migration not supported in stub implementation")
}

// DeletePartitionData deletes all data for a partition (stub)
func (m *MigrationManager) DeletePartitionData(partitionID uint32) error {
	return fmt.Errorf("partition data deletion not supported in stub implementation")
}

// GetPartitionSize returns the approximate size of a partition (stub)
func (m *MigrationManager) GetPartitionSize(partitionID uint32) (int64, int64, error) {
	return 0, 0, nil
}
