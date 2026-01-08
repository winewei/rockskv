package storage

import (
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
		{"long key", []byte("this-is-a-very-long-key-that-should-still-work-correctly")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partitionID := CalculatePartition(tt.key)
			if partitionID >= TotalPartitions {
				t.Errorf("CalculatePartition(%q) = %d, want < %d", tt.key, partitionID, TotalPartitions)
			}

			// Verify consistency - same key should always return same partition
			partitionID2 := CalculatePartition(tt.key)
			if partitionID != partitionID2 {
				t.Errorf("CalculatePartition not consistent: %d != %d", partitionID, partitionID2)
			}
		})
	}
}

func TestMakePartitionKey(t *testing.T) {
	tests := []struct {
		name        string
		partitionID uint32
		key         []byte
	}{
		{"partition 0", 0, []byte("test")},
		{"partition 1000", 1000, []byte("test")},
		{"max partition", TotalPartitions - 1, []byte("test")},
		{"empty key", 100, []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storageKey := MakePartitionKey(tt.partitionID, tt.key)

			// Parse it back
			partitionID, key, err := ParsePartitionKey(storageKey)
			if err != nil {
				t.Fatalf("ParsePartitionKey failed: %v", err)
			}

			if partitionID != tt.partitionID {
				t.Errorf("partition ID mismatch: got %d, want %d", partitionID, tt.partitionID)
			}

			if string(key) != string(tt.key) {
				t.Errorf("key mismatch: got %q, want %q", key, tt.key)
			}
		})
	}
}

func TestGetPartitionRange(t *testing.T) {
	tests := []struct {
		name        string
		partitionID uint32
	}{
		{"partition 0", 0},
		{"partition 1", 1},
		{"partition 1000", 1000},
		{"last partition", TotalPartitions - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := GetPartitionRange(tt.partitionID)

			// Verify start < end
			if compareBytes(start, end) >= 0 {
				t.Errorf("start >= end for partition %d", tt.partitionID)
			}

			// Verify a key in the partition falls within range
			testKey := MakePartitionKey(tt.partitionID, []byte("test"))
			if compareBytes(testKey, start) < 0 || compareBytes(testKey, end) >= 0 {
				t.Errorf("test key outside range for partition %d", tt.partitionID)
			}
		})
	}
}

func TestPartitionDistribution(t *testing.T) {
	// Test that keys are reasonably distributed across partitions
	numKeys := 10000
	distribution := make(map[uint32]int)

	for i := 0; i < numKeys; i++ {
		key := []byte(string(rune('a'+i%26)) + string(rune(i)))
		partitionID := CalculatePartition(key)
		distribution[partitionID]++
	}

	// Check that we have a reasonable distribution
	// With 10000 keys and 4096 partitions, we expect ~2.4 keys per partition on average
	// But due to hash distribution, some variance is expected
	minCount := 0
	maxCount := 0
	for _, count := range distribution {
		if minCount == 0 || count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}

	t.Logf("Distribution stats: min=%d, max=%d, partitions=%d", minCount, maxCount, len(distribution))

	// This is a sanity check - if max is way too high, something's wrong
	if maxCount > numKeys/10 {
		t.Errorf("Poor distribution: max count %d is > %d", maxCount, numKeys/10)
	}
}

func BenchmarkCalculatePartition(b *testing.B) {
	key := []byte("benchmark-test-key-for-partition-calculation")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculatePartition(key)
	}
}

func BenchmarkMakePartitionKey(b *testing.B) {
	key := []byte("benchmark-test-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MakePartitionKey(1000, key)
	}
}
