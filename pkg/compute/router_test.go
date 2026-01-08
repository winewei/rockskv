package compute

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
		{"special chars", []byte("key:with:colons")},
		{"binary data", []byte{0x00, 0x01, 0x02, 0xff}},
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
	// This isn't a strict test, just a sanity check
	t.Logf("Keys mapped to %d unique partitions out of %d keys", len(partitions), len(keys))
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
