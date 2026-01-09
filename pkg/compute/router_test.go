package compute

import (
	"sync"
	"testing"
)

func TestCalculatePartition(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"empty key", []byte{}},
		{"simple key", []byte("hello")},
		{"numeric key", []byte("12345")},
		{"special chars", []byte("key:with:colons")},
		{"binary data", []byte{0x00, 0x01, 0x02, 0xff}},
		{"unicode", []byte("你好世界")},
		{"long key", []byte("this-is-a-very-long-key-that-should-still-work-correctly-in-partition-calculation")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partitionID := CalculatePartition(tt.key)

			// Partition ID should be in valid range
			if partitionID >= TotalPartitions {
				t.Errorf("CalculatePartition(%q) = %d, want < %d", tt.key, partitionID, TotalPartitions)
			}

			// Should be deterministic
			partitionID2 := CalculatePartition(tt.key)
			if partitionID != partitionID2 {
				t.Errorf("CalculatePartition not deterministic: %d != %d", partitionID, partitionID2)
			}
		})
	}
}

func TestCalculatePartitionDeterministic(t *testing.T) {
	// Run the same key calculation multiple times to ensure determinism
	key := []byte("test-deterministic-key")
	expected := CalculatePartition(key)

	for i := 0; i < 1000; i++ {
		result := CalculatePartition(key)
		if result != expected {
			t.Errorf("Iteration %d: got %d, want %d", i, result, expected)
		}
	}
}

func TestCalculatePartitionConcurrent(t *testing.T) {
	// Test thread safety of partition calculation
	key := []byte("concurrent-test-key")
	expected := CalculatePartition(key)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := CalculatePartition(key)
			if result != expected {
				errors <- nil // Signal error
			}
		}()
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		t.Error("Concurrent partition calculation produced inconsistent results")
	}
}

func TestCalculatePartitionConsistency(t *testing.T) {
	// Keys with similar prefixes should potentially map to different partitions
	keys := [][]byte{
		[]byte("user:1"),
		[]byte("user:2"),
		[]byte("user:3"),
		[]byte("user:10"),
		[]byte("user:100"),
	}

	partitions := make(map[uint32]bool)
	for _, key := range keys {
		partitionID := CalculatePartition(key)
		partitions[partitionID] = true
	}

	// We expect at least some spread for these different keys
	t.Logf("Keys mapped to %d unique partitions out of %d keys", len(partitions), len(keys))
}

func TestTotalPartitionsConstant(t *testing.T) {
	if TotalPartitions != 4096 {
		t.Errorf("TotalPartitions = %d, want 4096", TotalPartitions)
	}
}

func TestRouteRefreshIntervalConstant(t *testing.T) {
	if RouteRefreshInterval.Seconds() != 30 {
		t.Errorf("RouteRefreshInterval = %v, want 30s", RouteRefreshInterval)
	}
}

func TestNewRouter(t *testing.T) {
	router := NewRouter("test-node", "localhost:9000")

	if router == nil {
		t.Fatal("NewRouter returned nil")
	}

	if router.nodeID != "test-node" {
		t.Errorf("nodeID = %s, want test-node", router.nodeID)
	}

	if router.metadataAddr != "localhost:9000" {
		t.Errorf("metadataAddr = %s, want localhost:9000", router.metadataAddr)
	}

	if router.logger == nil {
		t.Error("logger should not be nil")
	}

	// Check route table is initialized
	table := router.GetRouteTable()
	if table == nil {
		t.Fatal("route table should not be nil")
	}

	if table.Partitions == nil {
		t.Fatal("route table partitions should not be nil")
	}
}

func TestRouteTableInitialization(t *testing.T) {
	router := NewRouter("test-node", "localhost:9000")
	table := router.GetRouteTable()

	if table.Version != 0 {
		t.Errorf("initial version = %d, want 0", table.Version)
	}

	if len(table.Partitions) != 0 {
		t.Errorf("initial partitions count = %d, want 0", len(table.Partitions))
	}
}

func TestPartitionInfo(t *testing.T) {
	info := &PartitionInfo{
		ID:      100,
		Primary: "node-1",
		Replica: "node-2",
	}

	if info.ID != 100 {
		t.Errorf("ID = %d, want 100", info.ID)
	}

	if info.Primary != "node-1" {
		t.Errorf("Primary = %s, want node-1", info.Primary)
	}

	if info.Replica != "node-2" {
		t.Errorf("Replica = %s, want node-2", info.Replica)
	}
}

func TestRouteTable(t *testing.T) {
	table := &RouteTable{
		Version:    10,
		Partitions: make(map[uint32]*PartitionInfo),
	}

	// Add some partitions
	table.Partitions[0] = &PartitionInfo{ID: 0, Primary: "node-1", Replica: "node-2"}
	table.Partitions[1] = &PartitionInfo{ID: 1, Primary: "node-2", Replica: "node-1"}

	if table.Version != 10 {
		t.Errorf("Version = %d, want 10", table.Version)
	}

	if len(table.Partitions) != 2 {
		t.Errorf("Partitions count = %d, want 2", len(table.Partitions))
	}

	p0, ok := table.Partitions[0]
	if !ok {
		t.Error("Partition 0 not found")
	}
	if p0.Primary != "node-1" {
		t.Errorf("Partition 0 Primary = %s, want node-1", p0.Primary)
	}
}

func BenchmarkCalculatePartition(b *testing.B) {
	key := []byte("benchmark-key-for-testing-partition-calculation")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculatePartition(key)
	}
}

func BenchmarkCalculatePartitionParallel(b *testing.B) {
	key := []byte("benchmark-key-for-testing-partition-calculation")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			CalculatePartition(key)
		}
	})
}

func BenchmarkNewRouter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewRouter("test-node", "localhost:9000")
	}
}
