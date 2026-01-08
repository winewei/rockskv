//go:build cgo && !nocgo
// +build cgo,!nocgo

package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/linxGnu/grocksdb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/example/rockskv/pkg/common"
	pb "github.com/example/rockskv/pkg/proto"
)

// MigrationManager handles partition migration
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

// ExportPartition exports a partition to SST files
func (m *MigrationManager) ExportPartition(partitionID uint32, outputDir string) (string, int64, error) {
	pm := m.server.partitionManager
	db := m.server.db

	if !pm.HasPartition(partitionID) {
		return "", 0, fmt.Errorf("partition %d not found", partitionID)
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create SST file path
	sstPath := filepath.Join(outputDir, fmt.Sprintf("partition_%d_%d.sst", partitionID, time.Now().UnixNano()))

	// Create SST writer
	envOpts := grocksdb.NewDefaultEnvOptions()
	opts := grocksdb.NewDefaultOptions()
	sstWriter := grocksdb.NewSSTFileWriter(envOpts, opts)
	defer sstWriter.Destroy()

	if err := sstWriter.Open(sstPath); err != nil {
		return "", 0, fmt.Errorf("failed to open sst writer: %w", err)
	}

	// Create snapshot and iterate partition data
	snapshot := db.CreateSnapshot()
	defer db.ReleaseSnapshot(snapshot)

	iter := db.NewIteratorWithSnapshot(snapshot)
	defer iter.Close()

	startKey, endKey := GetPartitionRange(partitionID)

	keyCount := int64(0)
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		key := iter.Key()
		if compareBytes(key.Data(), endKey) >= 0 {
			key.Free()
			break
		}

		value := iter.Value()
		if err := sstWriter.Put(key.Data(), value.Data()); err != nil {
			key.Free()
			value.Free()
			return "", 0, fmt.Errorf("failed to write to sst: %w", err)
		}
		key.Free()
		value.Free()
		keyCount++
	}

	if err := iter.Err(); err != nil {
		return "", 0, fmt.Errorf("iterator error: %w", err)
	}

	if err := sstWriter.Finish(); err != nil {
		return "", 0, fmt.Errorf("failed to finish sst: %w", err)
	}

	m.logger.Info("Partition exported to SST",
		zap.Uint32("partition_id", partitionID),
		zap.String("path", sstPath),
		zap.Int64("keys", keyCount),
	)

	return sstPath, keyCount, nil
}

// IngestSST ingests an SST file into the database
func (m *MigrationManager) IngestSST(sstPath string) error {
	if err := m.server.db.IngestExternalFile([]string{sstPath}); err != nil {
		return fmt.Errorf("failed to ingest sst: %w", err)
	}

	m.logger.Info("SST file ingested", zap.String("path", sstPath))
	return nil
}

// MigratePartition migrates a partition from source to target node
func (m *MigrationManager) MigratePartition(ctx context.Context, partitionID uint32, sourceAddr, targetAddr string) error {
	m.logger.Info("Starting partition migration",
		zap.Uint32("partition_id", partitionID),
		zap.String("source", sourceAddr),
		zap.String("target", targetAddr),
	)

	// Connect to source node
	sourceConn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceConn.Close()
	sourceClient := pb.NewStorageServiceClient(sourceConn)

	// Connect to target node
	targetConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetConn.Close()
	targetClient := pb.NewStorageServiceClient(targetConn)

	// Export SST from source
	exportStream, err := sourceClient.ExportSST(ctx, &pb.ExportSSTRequest{
		PartitionId: partitionID,
	})
	if err != nil {
		return fmt.Errorf("failed to start export: %w", err)
	}

	// Ingest SST to target
	ingestStream, err := targetClient.IngestSST(ctx)
	if err != nil {
		return fmt.Errorf("failed to start ingest: %w", err)
	}

	// Stream data from source to target
	totalBytes := int64(0)
	for {
		chunk, err := exportStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("export stream error: %w", err)
		}

		if err := ingestStream.Send(chunk); err != nil {
			return fmt.Errorf("ingest stream error: %w", err)
		}

		totalBytes += int64(len(chunk.Data))

		if chunk.IsLast {
			break
		}
	}

	// Close ingest stream and get response
	resp, err := ingestStream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close ingest stream: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("ingest failed")
	}

	m.logger.Info("Partition migration completed",
		zap.Uint32("partition_id", partitionID),
		zap.Int64("bytes_transferred", totalBytes),
		zap.Int64("keys_ingested", resp.KeysIngested),
	)

	return nil
}

// DeletePartitionData deletes all data for a partition
func (m *MigrationManager) DeletePartitionData(partitionID uint32) error {
	pm := m.server.partitionManager
	db := m.server.db

	if !pm.HasPartition(partitionID) {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	startKey, endKey := GetPartitionRange(partitionID)

	// Create iterator to find all keys
	iter := db.NewIterator()
	defer iter.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	keyCount := int64(0)
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		key := iter.Key()
		if compareBytes(key.Data(), endKey) >= 0 {
			key.Free()
			break
		}

		batch.Delete(key.Data())
		key.Free()
		keyCount++
	}

	writeOpts := grocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()

	if err := m.server.db.db.Write(writeOpts, batch); err != nil {
		return fmt.Errorf("failed to delete partition data: %w", err)
	}

	// Trigger compaction to reclaim space
	db.CompactRange(startKey, endKey)

	m.logger.Info("Partition data deleted",
		zap.Uint32("partition_id", partitionID),
		zap.Int64("keys_deleted", keyCount),
	)

	return nil
}

// GetPartitionSize returns the approximate size of a partition
func (m *MigrationManager) GetPartitionSize(partitionID uint32) (int64, int64, error) {
	db := m.server.db

	startKey, endKey := GetPartitionRange(partitionID)

	iter := db.NewIterator()
	defer iter.Close()

	keyCount := int64(0)
	totalSize := int64(0)

	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		key := iter.Key()
		if compareBytes(key.Data(), endKey) >= 0 {
			key.Free()
			break
		}

		value := iter.Value()
		totalSize += int64(key.Size() + value.Size())
		key.Free()
		value.Free()
		keyCount++
	}

	return keyCount, totalSize, nil
}
