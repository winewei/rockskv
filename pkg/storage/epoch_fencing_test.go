package storage

import (
	"context"
	"testing"

	pb "github.com/winewei/rockskv/pkg/proto"
)

// TestEpochFencing verifies that stale epoch writes are rejected
func TestEpochFencing(t *testing.T) {
	// Create server
	config := DefaultServerConfig()
	config.NodeID = "storage-test-1"
	config.ListenAddr = ":0" // Random port
	config.MetadataAddr = "localhost:9000"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Add partition with epoch 1
	partitionID := uint32(100)
	if err := server.partitionManager.AddPartitionWithEpoch(partitionID, true, 1); err != nil {
		t.Fatalf("Failed to add partition: %v", err)
	}

	ctx := context.Background()
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	// Test 1: Write with correct epoch (epoch=1) should succeed
	t.Run("Write with correct epoch succeeds", func(t *testing.T) {
		resp, err := server.Put(ctx, &pb.StoragePutRequest{
			Key:         testKey,
			Value:       testValue,
			PartitionId: partitionID,
			Epoch:       1, // Correct epoch
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if !resp.Success {
			t.Fatalf("Put should succeed, got error: %s", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Fatalf("Expected OK, got: %s", resp.Error)
		}
	})

	// Update partition epoch to 2 (simulate primary change)
	if err := server.partitionManager.UpdatePartitionEpoch(partitionID, 2); err != nil {
		t.Fatalf("Failed to update epoch: %v", err)
	}

	// Test 2: Write with stale epoch (epoch=1) should be rejected
	t.Run("Write with stale epoch is fenced", func(t *testing.T) {
		resp, err := server.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte("new-key"),
			Value:       []byte("new-value"),
			PartitionId: partitionID,
			Epoch:       1, // Stale epoch
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if resp.Success {
			t.Fatalf("Put with stale epoch should be rejected")
		}
		if resp.Error != pb.ErrorCode_EPOCH_FENCED {
			t.Fatalf("Expected EPOCH_FENCED, got: %s", resp.Error)
		}
	})

	// Test 3: Write with new epoch (epoch=2) should succeed
	t.Run("Write with new epoch succeeds", func(t *testing.T) {
		resp, err := server.Put(ctx, &pb.StoragePutRequest{
			Key:         []byte("another-key"),
			Value:       []byte("another-value"),
			PartitionId: partitionID,
			Epoch:       2, // New epoch
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if !resp.Success {
			t.Fatalf("Put with new epoch should succeed, got error: %s", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Fatalf("Expected OK, got: %s", resp.Error)
		}
	})

	// Test 4: Delete with stale epoch should be fenced
	t.Run("Delete with stale epoch is fenced", func(t *testing.T) {
		resp, err := server.Delete(ctx, &pb.StorageDeleteRequest{
			Key:         testKey,
			PartitionId: partitionID,
			Epoch:       1, // Stale epoch
		})
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if resp.Success {
			t.Fatalf("Delete with stale epoch should be rejected")
		}
		if resp.Error != pb.ErrorCode_EPOCH_FENCED {
			t.Fatalf("Expected EPOCH_FENCED, got: %s", resp.Error)
		}
	})

	// Test 5: Delete with correct epoch should succeed
	t.Run("Delete with correct epoch succeeds", func(t *testing.T) {
		resp, err := server.Delete(ctx, &pb.StorageDeleteRequest{
			Key:         testKey,
			PartitionId: partitionID,
			Epoch:       2, // Current epoch
		})
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if !resp.Success {
			t.Fatalf("Delete with correct epoch should succeed, got error: %s", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Fatalf("Expected OK, got: %s", resp.Error)
		}
	})

	// Test 6: BatchPut with stale epoch should be fenced
	t.Run("BatchPut with stale epoch is fenced", func(t *testing.T) {
		resp, err := server.BatchPut(ctx, &pb.StorageBatchPutRequest{
			Items: []*pb.KeyValue{
				{Key: []byte("batch-1"), Value: []byte("value-1")},
				{Key: []byte("batch-2"), Value: []byte("value-2")},
			},
			PartitionId: partitionID,
			Epoch:       1, // Stale epoch
		})
		if err != nil {
			t.Fatalf("BatchPut failed: %v", err)
		}
		if resp.Success {
			t.Fatalf("BatchPut with stale epoch should be rejected")
		}
		if resp.Error != pb.ErrorCode_EPOCH_FENCED {
			t.Fatalf("Expected EPOCH_FENCED, got: %s", resp.Error)
		}
	})

	// Test 7: Field operations with stale epoch should be fenced
	t.Run("SetFields with stale epoch is fenced", func(t *testing.T) {
		resp, err := server.SetFields(ctx, &pb.StorageSetFieldsRequest{
			PrimaryKey: []byte("doc-1"),
			Fields: []*pb.FieldValue{
				{FieldName: "field1", Value: []byte("value1")},
			},
			PartitionId: partitionID,
			Epoch:       1, // Stale epoch
		})
		if err != nil {
			t.Fatalf("SetFields failed: %v", err)
		}
		if resp.Success {
			t.Fatalf("SetFields with stale epoch should be rejected")
		}
		if resp.Error != pb.ErrorCode_EPOCH_FENCED {
			t.Fatalf("Expected EPOCH_FENCED, got: %s", resp.Error)
		}
	})

	// Test 8: Field operations with correct epoch should succeed
	t.Run("SetFields with correct epoch succeeds", func(t *testing.T) {
		resp, err := server.SetFields(ctx, &pb.StorageSetFieldsRequest{
			PrimaryKey: []byte("doc-2"),
			Fields: []*pb.FieldValue{
				{FieldName: "field1", Value: []byte("value1")},
			},
			PartitionId: partitionID,
			Epoch:       2, // Current epoch
		})
		if err != nil {
			t.Fatalf("SetFields failed: %v", err)
		}
		if !resp.Success {
			t.Fatalf("SetFields with correct epoch should succeed, got error: %s", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Fatalf("Expected OK, got: %s", resp.Error)
		}
	})
}

// TestEpochForwardProgress verifies that epoch only moves forward
func TestEpochForwardProgress(t *testing.T) {
	config := DefaultServerConfig()
	config.NodeID = "storage-test-2"
	config.ListenAddr = ":0"
	config.MetadataAddr = "localhost:9000"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	partitionID := uint32(200)
	if err := server.partitionManager.AddPartitionWithEpoch(partitionID, true, 5); err != nil {
		t.Fatalf("Failed to add partition: %v", err)
	}

	// Test: Trying to update epoch to a lower value should not change it
	t.Run("Epoch does not go backwards", func(t *testing.T) {
		// Try to update to epoch 3 (lower than current epoch 5)
		err := server.partitionManager.UpdatePartitionEpoch(partitionID, 3)
		if err != nil {
			t.Fatalf("UpdatePartitionEpoch should not fail: %v", err)
		}

		// Verify epoch is still 5
		partition, exists := server.partitionManager.GetPartition(partitionID)
		if !exists {
			t.Fatalf("Partition not found")
		}
		if partition.Epoch != 5 {
			t.Fatalf("Expected epoch 5, got %d (epoch should not decrease)", partition.Epoch)
		}
	})

	// Test: Updating to a higher epoch should work
	t.Run("Epoch moves forward", func(t *testing.T) {
		err := server.partitionManager.UpdatePartitionEpoch(partitionID, 10)
		if err != nil {
			t.Fatalf("UpdatePartitionEpoch failed: %v", err)
		}

		partition, exists := server.partitionManager.GetPartition(partitionID)
		if !exists {
			t.Fatalf("Partition not found")
		}
		if partition.Epoch != 10 {
			t.Fatalf("Expected epoch 10, got %d", partition.Epoch)
		}
	})
}
