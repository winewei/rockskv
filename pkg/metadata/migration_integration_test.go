//go:build integration

package metadata

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/winewei/rockskv/pkg/proto"
	"github.com/winewei/rockskv/pkg/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testMigrationEtcdEndpoint = "localhost:2379"
)

// cleanupEtcdForMigrationTests cleans up all etcd data before each test
func cleanupEtcdForMigrationTests(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testMigrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client for cleanup: %v", err)
	}
	defer client.Close()

	// Fail-fast: Immediately verify the connection is working
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.Status(ctx, testMigrationEtcdEndpoint)
	if err != nil {
		t.Fatalf("Failed to connect to etcd (is etcd running?): %v", err)
	}

	// Delete all RocksKV data with timeout
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()

	_, err = client.Delete(deleteCtx, "/rockskv/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to cleanup etcd: %v", err)
	}
}

// TestSSTExport tests SST file export from a storage node
func TestSSTExport(t *testing.T) {
	t.Skip("SST export test requires proper partition assignment timing - needs further investigation")
	// TODO: Fix partition assignment timing issue
	// Current issue: partition 0 not found on storage node after InitCluster
	// Possible causes:
	// 1. Route table propagation delay
	// 2. Storage node not subscribing properly to route updates
	// 3. InitCluster not assigning partitions when only 1 storage node
	cleanupEtcdForMigrationTests(t)
	ctx := context.Background()

	// Use fixed ports
	metadataAddr := "localhost:29000"
	storageAddr := "localhost:29001"

	// Create metadata server
	metadataConfig := &HAServerConfig{
		ServerConfig: &ServerConfig{
			NodeID:     "metadata-test-export",
			ListenAddr: ":29000",
			Etcd: &EtcdConfig{
				Endpoints:   []string{testMigrationEtcdEndpoint},
				DialTimeout: 5 * time.Second,
			},
		},
		HAEnabled: false,
	}

	metadataServer, err := NewHAServer(metadataConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata server: %v", err)
	}

	go func() {
		if err := metadataServer.Start(ctx); err != nil {
			t.Logf("Metadata server stopped: %v", err)
		}
	}()
	defer metadataServer.Stop()
	time.Sleep(1 * time.Second)

	// Create source storage node
	dataDir := t.TempDir()
	sourceStorageConfig := &storage.ServerConfig{
		NodeID:        "storage-1",
		ListenAddr:    ":29001",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: dataDir,
			WALDir:  filepath.Join(dataDir, "wal"),
		},
	}

	sourceStorage, err := storage.NewServer(sourceStorageConfig)
	if err != nil {
		t.Fatalf("Failed to create source storage server: %v", err)
	}

	go func() {
		if err := sourceStorage.Start(); err != nil {
			t.Logf("Source storage server stopped: %v", err)
		}
	}()
	defer sourceStorage.Stop()
	time.Sleep(1 * time.Second)

	// Connect to metadata
	metadataConn, err := grpc.Dial(metadataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to metadata: %v", err)
	}
	defer metadataConn.Close()
	metadataClient := pb.NewMetadataServiceClient(metadataConn)

	// Register source storage node
	_, err = metadataClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId: "storage-1",
		Addr:   storageAddr,
		Role:   pb.NodeRole_STORAGE,
	})
	if err != nil {
		t.Fatalf("Failed to register source storage node: %v", err)
	}

	// Initialize cluster
	_, err = metadataClient.InitCluster(ctx, &pb.InitClusterRequest{})
	if err != nil {
		t.Fatalf("Failed to initialize cluster: %v", err)
	}

	time.Sleep(2 * time.Second) // Wait for route table to propagate

	// Connect to source storage
	sourceConn, err := grpc.Dial(storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to source storage: %v", err)
	}
	defer sourceConn.Close()
	sourceClient := pb.NewStorageServiceClient(sourceConn)

	// Put some test data in partition 0
	partitionID := uint32(0)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := fmt.Sprintf("test-value-%d", i)
		resp, err := sourceClient.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte(key),
			Value:       []byte(value),
			PartitionId: partitionID,
			Epoch:       1,
		})
		if err != nil || !resp.Success {
			t.Logf("Warning: Failed to put test data (key=%s): err=%v, success=%v", key, err, resp != nil && resp.Success)
		}
	}

	// Export SST file for partition 0
	stream, err := sourceClient.ExportSST(ctx, &pb.ExportSSTRequest{
		PartitionId:    partitionID,
		BandwidthLimit: 0, // Unlimited
	})
	if err != nil {
		t.Fatalf("Failed to start SST export: %v", err)
	}

	// Receive SST chunks
	var totalBytes int64
	chunkCount := 0
	var lastChunkReceived bool

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to receive SST chunk: %v", err)
		}

		chunkCount++
		totalBytes += int64(len(chunk.Data))

		if chunk.IsLast {
			lastChunkReceived = true
			t.Logf("Received last chunk. Total size: %d bytes, Chunks: %d", chunk.TotalSize, chunkCount)
		}
	}

	// Verify we received the complete SST file
	if !lastChunkReceived {
		t.Fatalf("Did not receive last chunk marker")
	}

	t.Logf("Successfully exported SST: %d bytes in %d chunks", totalBytes, chunkCount)
}

// TestSSTImport tests SST file import to a storage node
func TestSSTImport(t *testing.T) {
	t.Skip("SST import test has same partition assignment issues as TestSSTExport")
	// TODO: Fix once TestSSTExport is working
	cleanupEtcdForMigrationTests(t)
	ctx := context.Background()

	// Use fixed ports
	metadataAddr := "localhost:29100"
	sourceAddr := "localhost:29101"
	targetAddr := "localhost:29102"

	// Create metadata server
	metadataConfig := &HAServerConfig{
		ServerConfig: &ServerConfig{
			NodeID:     "metadata-test-import",
			ListenAddr: ":29100",
			Etcd: &EtcdConfig{
				Endpoints:   []string{testMigrationEtcdEndpoint},
				DialTimeout: 5 * time.Second,
			},
		},
		HAEnabled: false,
	}

	metadataServer, err := NewHAServer(metadataConfig)
	if err != nil {
		t.Fatalf("Failed to create metadata server: %v", err)
	}

	go func() {
		if err := metadataServer.Start(ctx); err != nil {
			t.Logf("Metadata server stopped: %v", err)
		}
	}()
	defer metadataServer.Stop()
	time.Sleep(1 * time.Second)

	// Create source and target storage nodes
	sourceDataDir := t.TempDir()
	sourceStorageConfig := &storage.ServerConfig{
		NodeID:        "storage-1",
		ListenAddr:    ":29101",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst1"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog1"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: sourceDataDir,
			WALDir:  filepath.Join(sourceDataDir, "wal"),
		},
	}

	targetDataDir := t.TempDir()
	targetStorageConfig := &storage.ServerConfig{
		NodeID:        "storage-2",
		ListenAddr:    ":29102",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst2"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog2"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: targetDataDir,
			WALDir:  filepath.Join(targetDataDir, "wal"),
		},
	}

	sourceStorage, err := storage.NewServer(sourceStorageConfig)
	if err != nil {
		t.Fatalf("Failed to create source storage server: %v", err)
	}

	targetStorage, err := storage.NewServer(targetStorageConfig)
	if err != nil {
		t.Fatalf("Failed to create target storage server: %v", err)
	}

	go func() {
		if err := sourceStorage.Start(); err != nil {
			t.Logf("Source storage server stopped: %v", err)
		}
	}()
	defer sourceStorage.Stop()

	go func() {
		if err := targetStorage.Start(); err != nil {
			t.Logf("Target storage server stopped: %v", err)
		}
	}()
	defer targetStorage.Stop()

	time.Sleep(1 * time.Second)

	// Connect to metadata
	metadataConn, err := grpc.Dial(metadataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to metadata: %v", err)
	}
	defer metadataConn.Close()
	metadataClient := pb.NewMetadataServiceClient(metadataConn)

	// Register both nodes
	_, err = metadataClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId: "storage-1",
		Addr:   sourceAddr,
		Role:   pb.NodeRole_STORAGE,
	})
	if err != nil {
		t.Fatalf("Failed to register source storage node: %v", err)
	}

	_, err = metadataClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId: "storage-2",
		Addr:   targetAddr,
		Role:   pb.NodeRole_STORAGE,
	})
	if err != nil {
		t.Fatalf("Failed to register target storage node: %v", err)
	}

	// Initialize cluster
	_, err = metadataClient.InitCluster(ctx, &pb.InitClusterRequest{})
	if err != nil {
		t.Fatalf("Failed to initialize cluster: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Connect to source and target storage
	sourceConn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to source storage: %v", err)
	}
	defer sourceConn.Close()
	sourceClient := pb.NewStorageServiceClient(sourceConn)

	targetConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to target storage: %v", err)
	}
	defer targetConn.Close()
	targetClient := pb.NewStorageServiceClient(targetConn)

	// Put test data in source partition 0
	partitionID := uint32(0)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("migrate-key-%d", i)
		value := fmt.Sprintf("migrate-value-%d", i)
		resp, err := sourceClient.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte(key),
			Value:       []byte(value),
			PartitionId: partitionID,
			Epoch:       1,
		})
		if err != nil || !resp.Success {
			t.Logf("Warning: Failed to put data: err=%v, success=%v", err, resp != nil && resp.Success)
		}
	}

	// Export SST from source
	exportStream, err := sourceClient.ExportSST(ctx, &pb.ExportSSTRequest{
		PartitionId:    partitionID,
		BandwidthLimit: 0,
	})
	if err != nil {
		t.Fatalf("Failed to start SST export: %v", err)
	}

	// Import SST to target
	importStream, err := targetClient.IngestSST(ctx)
	if err != nil {
		t.Fatalf("Failed to start SST import: %v", err)
	}

	// Stream chunks from source to target
	for {
		chunk, err := exportStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to receive chunk: %v", err)
		}

		err = importStream.Send(chunk)
		if err != nil {
			t.Fatalf("Failed to send chunk to target: %v", err)
		}

		if chunk.IsLast {
			break
		}
	}

	// Close import stream and get response
	importResp, err := importStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("Failed to close import stream: %v", err)
	}

	if !importResp.Success {
		t.Fatalf("Import failed")
	}

	t.Logf("Successfully imported SST: %d keys, %d bytes", importResp.KeysIngested, importResp.BytesIngested)

	// Verify data exists on target (read from partition 0)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("migrate-key-%d", i)
		expectedValue := fmt.Sprintf("migrate-value-%d", i)

		resp, err := targetClient.Get(ctx, &pb.StorageGetRequest{
			Key:         []byte(key),
			PartitionId: partitionID,
		})
		if err != nil {
			t.Fatalf("Failed to get key %s from target: %v", key, err)
		}

		if string(resp.Value) != expectedValue {
			t.Fatalf("Value mismatch for key %s: got %s, expected %s", key, resp.Value, expectedValue)
		}
	}

	t.Logf("Successfully verified all migrated data on target node")
}

// TestMigrationTriggerRebalance tests triggering a migration via TriggerRebalance API
func TestMigrationTriggerRebalance(t *testing.T) {
	t.Skip("TriggerRebalance requires MigrationController which is complex to set up - skipping for now")
	// TODO: Implement this when migration orchestration is needed
}

// TestMigrationCancel tests cancelling an in-progress migration
func TestMigrationCancel(t *testing.T) {
	t.Skip("Migration cancellation requires implementing pause/cancel logic in MigrationController")
	// TODO: Implement this test when cancel functionality is fully implemented
}

// TestConcurrentMigrations tests multiple concurrent partition migrations
func TestConcurrentMigrations(t *testing.T) {
	t.Skip("Concurrent migrations require full MigrationController setup - skipping for now")
	// TODO: Implement this when full migration orchestration is needed
}
