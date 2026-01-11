//go:build integration
// +build integration

package storage

import (
	"context"
	"testing"

	pb "github.com/winewei/rockskv/pkg/proto"
)

// TestIsPrimary tests the IsPrimary method of PartitionManager
func TestIsPrimary(t *testing.T) {
	// Create a temporary database (auto-cleanup on test end)
	dbPath := t.TempDir()

	config := &RocksDBConfig{
		DataDir:              dbPath,
		WALDir:               dbPath + "/wal",
		WriteBufferSize:      64 * 1024 * 1024,
		MaxWriteBufferNumber: 3,
	}

	db, err := NewRocksDB(config)
	if err != nil {
		t.Fatalf("Failed to create RocksDB: %v", err)
	}
	defer db.Close()

	pm := NewPartitionManager("test-node", db)

	tests := []struct {
		name        string
		partitionID uint32
		isPrimary   bool
		want        bool
	}{
		{
			name:        "primary partition",
			partitionID: 100,
			isPrimary:   true,
			want:        true,
		},
		{
			name:        "replica partition",
			partitionID: 200,
			isPrimary:   false,
			want:        false,
		},
		{
			name:        "non-existent partition",
			partitionID: 999,
			isPrimary:   false,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Add partition if not testing non-existent case
			if tt.partitionID != 999 {
				err := pm.AddPartition(tt.partitionID, tt.isPrimary)
				if err != nil {
					t.Fatalf("Failed to add partition: %v", err)
				}
			}

			got := pm.IsPrimary(tt.partitionID)
			if got != tt.want {
				t.Errorf("IsPrimary(%d) = %v, want %v", tt.partitionID, got, tt.want)
			}
		})
	}
}

// TestReplicaWriteRejection tests that write operations are rejected on replica partitions
func TestReplicaWriteRejection(t *testing.T) {
	// Create temporary directory for test (auto-cleanup on test end)
	dataDir := t.TempDir()

	// Create a test server
	serverConfig := &ServerConfig{
		NodeID:        "test-storage-1",
		ListenAddr:    ":0", // Use any available port
		MetadataAddr:  "localhost:9000",
		RocksDB:       DefaultRocksDBConfig(),
		SSTDir:        dataDir + "/sst",
		CommandLogDir: dataDir + "/cmdlog",
		EtcdEndpoints: []string{"localhost:2379"}, // Integration test requires etcd
	}
	serverConfig.RocksDB.DataDir = dataDir
	serverConfig.RocksDB.WALDir = dataDir + "/wal"

	server, err := NewServer(serverConfig)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	ctx := context.Background()

	// Add a partition as replica (not primary)
	partitionID := uint32(100)
	err = server.partitionManager.AddPartition(partitionID, false) // false = replica
	if err != nil {
		t.Fatalf("Failed to add replica partition: %v", err)
	}

	t.Run("Put on replica should fail", func(t *testing.T) {
		resp, err := server.Put(ctx, &pb.StoragePutRequest{
			PartitionId: partitionID,
			Key:         []byte("test-key"),
			Value:       []byte("test-value"),
		})
		if err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
		if resp.Success {
			t.Error("Put should have failed on replica partition")
		}
		if resp.Error != pb.ErrorCode_NOT_PRIMARY {
			t.Errorf("Expected error NOT_PRIMARY, got %v", resp.Error)
		}
	})

	t.Run("Delete on replica should fail", func(t *testing.T) {
		resp, err := server.Delete(ctx, &pb.StorageDeleteRequest{
			PartitionId: partitionID,
			Key:         []byte("test-key"),
		})
		if err != nil {
			t.Fatalf("Delete returned error: %v", err)
		}
		if resp.Success {
			t.Error("Delete should have failed on replica partition")
		}
		if resp.Error != pb.ErrorCode_NOT_PRIMARY {
			t.Errorf("Expected error NOT_PRIMARY, got %v", resp.Error)
		}
	})

	t.Run("SetFields on replica should fail", func(t *testing.T) {
		resp, err := server.SetFields(ctx, &pb.StorageSetFieldsRequest{
			PartitionId: partitionID,
			PrimaryKey:  []byte("doc-id"),
			Fields: []*pb.FieldValue{
				{FieldName: "name", Value: []byte("test")},
			},
		})
		if err != nil {
			t.Fatalf("SetFields returned error: %v", err)
		}
		if resp.Success {
			t.Error("SetFields should have failed on replica partition")
		}
		if resp.Error != pb.ErrorCode_NOT_PRIMARY {
			t.Errorf("Expected error NOT_PRIMARY, got %v", resp.Error)
		}
	})

	t.Run("DeleteField on replica should fail", func(t *testing.T) {
		resp, err := server.DeleteField(ctx, &pb.StorageDeleteFieldRequest{
			PartitionId: partitionID,
			PrimaryKey:  []byte("doc-id"),
			FieldName:   "name",
		})
		if err != nil {
			t.Fatalf("DeleteField returned error: %v", err)
		}
		if resp.Success {
			t.Error("DeleteField should have failed on replica partition")
		}
		if resp.Error != pb.ErrorCode_NOT_PRIMARY {
			t.Errorf("Expected error NOT_PRIMARY, got %v", resp.Error)
		}
	})

	t.Run("BatchPut on replica should fail", func(t *testing.T) {
		resp, err := server.BatchPut(ctx, &pb.StorageBatchPutRequest{
			PartitionId: partitionID,
			Items: []*pb.KeyValue{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
		})
		if err != nil {
			t.Fatalf("BatchPut returned error: %v", err)
		}
		if resp.Success {
			t.Error("BatchPut should have failed on replica partition")
		}
		if resp.Error != pb.ErrorCode_NOT_PRIMARY {
			t.Errorf("Expected error NOT_PRIMARY, got %v", resp.Error)
		}
	})
}

// TestPrimaryWriteSuccess tests that write operations succeed on primary partitions
func TestPrimaryWriteSuccess(t *testing.T) {
	// Create temporary directory for test (auto-cleanup on test end)
	dataDir := t.TempDir()

	// Create a test server
	serverConfig := &ServerConfig{
		NodeID:        "test-storage-2",
		ListenAddr:    ":0",
		MetadataAddr:  "localhost:9000",
		RocksDB:       DefaultRocksDBConfig(),
		SSTDir:        dataDir + "/sst",
		CommandLogDir: dataDir + "/cmdlog",
		EtcdEndpoints: []string{"localhost:2379"}, // Integration test requires etcd
	}
	serverConfig.RocksDB.DataDir = dataDir
	serverConfig.RocksDB.WALDir = dataDir + "/wal"

	server, err := NewServer(serverConfig)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	ctx := context.Background()

	// Add a partition as primary
	partitionID := uint32(200)
	err = server.partitionManager.AddPartition(partitionID, true) // true = primary
	if err != nil {
		t.Fatalf("Failed to add primary partition: %v", err)
	}

	t.Run("Put on primary should succeed", func(t *testing.T) {
		resp, err := server.Put(ctx, &pb.StoragePutRequest{
			PartitionId: partitionID,
			Key:         []byte("test-key"),
			Value:       []byte("test-value"),
		})
		if err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
		if !resp.Success {
			t.Errorf("Put should have succeeded on primary partition, error: %v", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Errorf("Expected error OK, got %v", resp.Error)
		}
	})

	t.Run("Delete on primary should succeed", func(t *testing.T) {
		resp, err := server.Delete(ctx, &pb.StorageDeleteRequest{
			PartitionId: partitionID,
			Key:         []byte("test-key"),
		})
		if err != nil {
			t.Fatalf("Delete returned error: %v", err)
		}
		if !resp.Success {
			t.Errorf("Delete should have succeeded on primary partition, error: %v", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Errorf("Expected error OK, got %v", resp.Error)
		}
	})

	t.Run("BatchPut on primary should succeed", func(t *testing.T) {
		resp, err := server.BatchPut(ctx, &pb.StorageBatchPutRequest{
			PartitionId: partitionID,
			Items: []*pb.KeyValue{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
		})
		if err != nil {
			t.Fatalf("BatchPut returned error: %v", err)
		}
		if !resp.Success {
			t.Errorf("BatchPut should have succeeded on primary partition, error: %v", resp.Error)
		}
		if resp.Error != pb.ErrorCode_OK {
			t.Errorf("Expected error OK, got %v", resp.Error)
		}
	})
}
