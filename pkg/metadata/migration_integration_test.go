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
	cleanupEtcdForMigrationTests(t)
	ctx := context.Background()

	// Use fixed ports
	metadataAddr := "localhost:29000"
	storage1Addr := "localhost:29001"
	storage2Addr := "localhost:29002"

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

	// Create two storage nodes (MinReplicaNodes = 2)
	dataDir1 := t.TempDir()
	storage1Config := &storage.ServerConfig{
		NodeID:        "storage-1",
		ListenAddr:    ":29001",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst1"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog1"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: dataDir1,
			WALDir:  filepath.Join(dataDir1, "wal"),
		},
	}

	dataDir2 := t.TempDir()
	storage2Config := &storage.ServerConfig{
		NodeID:        "storage-2",
		ListenAddr:    ":29002",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst2"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog2"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: dataDir2,
			WALDir:  filepath.Join(dataDir2, "wal"),
		},
	}

	storage1, err := storage.NewServer(storage1Config)
	if err != nil {
		t.Fatalf("Failed to create storage-1 server: %v", err)
	}

	storage2, err := storage.NewServer(storage2Config)
	if err != nil {
		t.Fatalf("Failed to create storage-2 server: %v", err)
	}

	go func() {
		if err := storage1.Start(); err != nil {
			t.Logf("Storage-1 server stopped: %v", err)
		}
	}()
	defer storage1.Stop()

	go func() {
		if err := storage2.Start(); err != nil {
			t.Logf("Storage-2 server stopped: %v", err)
		}
	}()
	defer storage2.Stop()

	// Wait for storage nodes to auto-register with metadata
	// Storage nodes register themselves in registerWithMetadata() goroutine
	time.Sleep(3 * time.Second)

	// Connect to metadata
	metadataConn, err := grpc.Dial(metadataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to metadata: %v", err)
	}
	defer metadataConn.Close()
	metadataClient := pb.NewMetadataServiceClient(metadataConn)

	// Wait for both nodes to be registered and online
	var nodes int
	for i := 0; i < 10; i++ {
		info, err := metadataClient.GetClusterInfo(ctx, &pb.GetClusterInfoRequest{})
		if err == nil {
			nodes = int(info.StorageNodeCount)
			if nodes >= 2 {
				t.Logf("Both storage nodes registered: %d nodes", nodes)
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if nodes < 2 {
		t.Fatalf("Expected 2 storage nodes, got %d", nodes)
	}

	// Initialize cluster
	initResp, err := metadataClient.InitCluster(ctx, &pb.InitClusterRequest{})
	if err != nil {
		t.Fatalf("Failed to initialize cluster: %v", err)
	}
	if !initResp.Success {
		t.Fatalf("InitCluster returned failure: %s", initResp.Message)
	}
	t.Logf("Cluster initialized with %d partitions", initResp.PartitionCount)

	// Wait for route table to propagate to storage nodes
	time.Sleep(2 * time.Second)

	// Get route table to find which node owns partition 0
	routeResp, err := metadataClient.GetRouteTable(ctx, &pb.GetRouteTableRequest{})
	if err != nil {
		t.Fatalf("Failed to get route table: %v", err)
	}

	var partition0 *pb.PartitionInfo
	for _, p := range routeResp.RouteTable.Partitions {
		if p.PartitionId == 0 {
			partition0 = p
			break
		}
	}
	if partition0 == nil {
		t.Fatalf("Partition 0 not found in route table")
	}
	t.Logf("Partition 0: Primary=%s, Replica=%s, Epoch=%d", partition0.Primary, partition0.Replica, partition0.Epoch)

	// Determine which storage node owns partition 0 as primary
	var targetAddr string
	if partition0.Primary == storage1Addr {
		targetAddr = storage1Addr
	} else if partition0.Primary == storage2Addr {
		targetAddr = storage2Addr
	} else {
		t.Fatalf("Partition 0 primary is unknown: %s", partition0.Primary)
	}

	// Connect to the storage node that owns partition 0
	storageConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to storage: %v", err)
	}
	defer storageConn.Close()
	storageClient := pb.NewStorageServiceClient(storageConn)

	// Wait for storage node to receive partition assignment
	// Poll until the first Put succeeds (indicating partition is ready)
	partitionID := uint32(0)
	var partitionReady bool
	for attempt := 0; attempt < 30; attempt++ {
		testResp, err := storageClient.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte("partition-ready-check"),
			Value:       []byte("test"),
			PartitionId: partitionID,
			Epoch:       partition0.Epoch,
		})
		if err == nil && testResp.Success {
			partitionReady = true
			t.Logf("Partition %d is ready (attempt %d)", partitionID, attempt+1)
			break
		}
		if testResp != nil && testResp.Error == pb.ErrorCode_PARTITION_NOT_FOUND {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// Other errors - log and continue
		if err != nil {
			t.Logf("Put check attempt %d: err=%v", attempt+1, err)
		} else {
			t.Logf("Put check attempt %d: error_code=%v", attempt+1, testResp.Error)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !partitionReady {
		t.Fatalf("Partition %d not ready after 15 seconds", partitionID)
	}

	// Put some test data in partition 0
	successCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := fmt.Sprintf("test-value-%d", i)
		resp, err := storageClient.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte(key),
			Value:       []byte(value),
			PartitionId: partitionID,
			Epoch:       partition0.Epoch,
		})
		if err != nil {
			t.Logf("Warning: Put failed (key=%s): %v", key, err)
			continue
		}
		if !resp.Success {
			t.Logf("Warning: Put returned failure (key=%s): %s", key, resp.Error)
			continue
		}
		successCount++
	}
	if successCount == 0 {
		t.Fatalf("Failed to put any test data")
	}
	t.Logf("Successfully put %d keys", successCount)

	// Export SST file for partition 0
	stream, err := storageClient.ExportSST(ctx, &pb.ExportSSTRequest{
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
	cleanupEtcdForMigrationTests(t)
	ctx := context.Background()

	// Use fixed ports
	metadataAddr := "localhost:29100"
	storage1Addr := "localhost:29101"
	storage2Addr := "localhost:29102"

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

	// Create two storage nodes (MinReplicaNodes = 2)
	dataDir1 := t.TempDir()
	storage1Config := &storage.ServerConfig{
		NodeID:        "storage-1",
		ListenAddr:    ":29101",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst1"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog1"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: dataDir1,
			WALDir:  filepath.Join(dataDir1, "wal"),
		},
	}

	dataDir2 := t.TempDir()
	storage2Config := &storage.ServerConfig{
		NodeID:        "storage-2",
		ListenAddr:    ":29102",
		MetadataAddr:  metadataAddr,
		EtcdEndpoints: []string{testMigrationEtcdEndpoint},
		SSTDir:        filepath.Join(t.TempDir(), "sst2"),
		CommandLogDir: filepath.Join(t.TempDir(), "cmdlog2"),
		RocksDB: &storage.RocksDBConfig{
			DataDir: dataDir2,
			WALDir:  filepath.Join(dataDir2, "wal"),
		},
	}

	storage1, err := storage.NewServer(storage1Config)
	if err != nil {
		t.Fatalf("Failed to create storage-1 server: %v", err)
	}

	storage2, err := storage.NewServer(storage2Config)
	if err != nil {
		t.Fatalf("Failed to create storage-2 server: %v", err)
	}

	go func() {
		if err := storage1.Start(); err != nil {
			t.Logf("Storage-1 server stopped: %v", err)
		}
	}()
	defer storage1.Stop()

	go func() {
		if err := storage2.Start(); err != nil {
			t.Logf("Storage-2 server stopped: %v", err)
		}
	}()
	defer storage2.Stop()

	// Wait for storage nodes to auto-register with metadata
	time.Sleep(3 * time.Second)

	// Connect to metadata
	metadataConn, err := grpc.Dial(metadataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to metadata: %v", err)
	}
	defer metadataConn.Close()
	metadataClient := pb.NewMetadataServiceClient(metadataConn)

	// Wait for both nodes to be registered and online
	var nodes int
	for i := 0; i < 10; i++ {
		info, err := metadataClient.GetClusterInfo(ctx, &pb.GetClusterInfoRequest{})
		if err == nil {
			nodes = int(info.StorageNodeCount)
			if nodes >= 2 {
				t.Logf("Both storage nodes registered: %d nodes", nodes)
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if nodes < 2 {
		t.Fatalf("Expected 2 storage nodes, got %d", nodes)
	}

	// Initialize cluster
	initResp, err := metadataClient.InitCluster(ctx, &pb.InitClusterRequest{})
	if err != nil {
		t.Fatalf("Failed to initialize cluster: %v", err)
	}
	if !initResp.Success {
		t.Fatalf("InitCluster returned failure: %s", initResp.Message)
	}
	t.Logf("Cluster initialized with %d partitions", initResp.PartitionCount)

	// Wait for route table to propagate to storage nodes
	// Storage nodes receive updates via SubscribeRouteUpdates stream
	// Need sufficient time for them to process the initial assignment
	time.Sleep(5 * time.Second)

	// Get route table to find which node owns partition 0
	routeResp, err := metadataClient.GetRouteTable(ctx, &pb.GetRouteTableRequest{})
	if err != nil {
		t.Fatalf("Failed to get route table: %v", err)
	}

	var partition0 *pb.PartitionInfo
	for _, p := range routeResp.RouteTable.Partitions {
		if p.PartitionId == 0 {
			partition0 = p
			break
		}
	}
	if partition0 == nil {
		t.Fatalf("Partition 0 not found in route table")
	}
	t.Logf("Partition 0: Primary=%s, Replica=%s, Epoch=%d", partition0.Primary, partition0.Replica, partition0.Epoch)

	// Determine source (primary) and target (replica) storage nodes
	var sourceAddr, targetAddr string
	if partition0.Primary == storage1Addr {
		sourceAddr = storage1Addr
		targetAddr = storage2Addr
	} else if partition0.Primary == storage2Addr {
		sourceAddr = storage2Addr
		targetAddr = storage1Addr
	} else {
		t.Fatalf("Partition 0 primary is unknown: %s", partition0.Primary)
	}
	t.Logf("Source (primary): %s, Target (replica): %s", sourceAddr, targetAddr)

	// Connect to source storage node (the primary owner of partition 0)
	sourceConn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to source storage: %v", err)
	}
	defer sourceConn.Close()
	sourceClient := pb.NewStorageServiceClient(sourceConn)

	// Connect to target storage node
	targetConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to target storage: %v", err)
	}
	defer targetConn.Close()
	targetClient := pb.NewStorageServiceClient(targetConn)

	// Wait for storage node to receive partition assignment
	// Poll until the first Put succeeds (indicating partition is ready)
	partitionID := uint32(0)
	var partitionReady bool
	for attempt := 0; attempt < 30; attempt++ {
		testResp, err := sourceClient.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte("partition-ready-check"),
			Value:       []byte("test"),
			PartitionId: partitionID,
			Epoch:       partition0.Epoch,
		})
		if err == nil && testResp.Success {
			partitionReady = true
			t.Logf("Partition %d is ready on source node (attempt %d)", partitionID, attempt+1)
			break
		}
		if testResp != nil && testResp.Error == pb.ErrorCode_PARTITION_NOT_FOUND {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// Other errors - log and continue
		if err != nil {
			t.Logf("Put check attempt %d: err=%v", attempt+1, err)
		} else {
			t.Logf("Put check attempt %d: error_code=%v", attempt+1, testResp.Error)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !partitionReady {
		t.Fatalf("Partition %d not ready on source node after 15 seconds", partitionID)
	}

	// Put test data in source partition 0
	successCount := 0
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("migrate-key-%d", i)
		value := fmt.Sprintf("migrate-value-%d", i)
		resp, err := sourceClient.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte(key),
			Value:       []byte(value),
			PartitionId: partitionID,
			Epoch:       partition0.Epoch,
		})
		if err != nil {
			t.Logf("Warning: Put failed (key=%s): %v", key, err)
			continue
		}
		if !resp.Success {
			t.Logf("Warning: Put returned failure (key=%s): %s", key, resp.Error)
			continue
		}
		successCount++
	}
	if successCount == 0 {
		t.Fatalf("Failed to put any test data")
	}
	t.Logf("Successfully put %d keys", successCount)

	// Verify data was written by reading back a key
	verifyResp, err := sourceClient.Get(ctx, &pb.StorageGetRequest{
		Key:         []byte("migrate-key-0"),
		PartitionId: partitionID,
	})
	if err != nil {
		t.Fatalf("Failed to verify written data: %v", err)
	}
	if !verifyResp.Found {
		t.Fatalf("Data verification failed: key 'migrate-key-0' not found after Put")
	}
	t.Logf("Verified data exists: migrate-key-0 = %s", string(verifyResp.Value))

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
	chunkCount := 0
	var totalExportBytes int64
	for {
		chunk, err := exportStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to receive chunk: %v", err)
		}

		// Log chunk details for debugging
		t.Logf("Received chunk: filename=%s, size=%d bytes, isLast=%v, totalSize=%d",
			chunk.Filename, len(chunk.Data), chunk.IsLast, chunk.TotalSize)

		err = importStream.Send(chunk)
		if err != nil {
			t.Fatalf("Failed to send chunk to target: %v", err)
		}
		chunkCount++
		totalExportBytes += int64(len(chunk.Data))

		if chunk.IsLast {
			t.Logf("Streamed %d chunks (%d bytes) to target", chunkCount, totalExportBytes)
			break
		}
	}

	// Check if export was empty
	if totalExportBytes == 0 {
		t.Logf("WARNING: ExportSST returned empty data despite successful Put and Get")
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
	// Note: The target may be the replica, so we need to verify it can read the imported data
	verifiedCount := 0
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("migrate-key-%d", i)
		expectedValue := fmt.Sprintf("migrate-value-%d", i)

		resp, err := targetClient.Get(ctx, &pb.StorageGetRequest{
			Key:         []byte(key),
			PartitionId: partitionID,
		})
		if err != nil {
			t.Logf("Warning: Failed to get key %s from target: %v", key, err)
			continue
		}

		if string(resp.Value) != expectedValue {
			t.Logf("Warning: Value mismatch for key %s: got %s, expected %s", key, resp.Value, expectedValue)
			continue
		}
		verifiedCount++
	}

	if verifiedCount == 0 {
		t.Fatalf("Failed to verify any migrated data on target")
	}
	t.Logf("Successfully verified %d/%d migrated keys on target node", verifiedCount, successCount)
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
