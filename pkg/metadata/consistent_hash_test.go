package metadata

import (
	"fmt"
	"testing"
)

func TestNewConsistentHash(t *testing.T) {
	ch := NewConsistentHash(100)
	if ch == nil {
		t.Fatal("NewConsistentHash returned nil")
	}
	if ch.virtualNodes != 100 {
		t.Errorf("virtualNodes = %d, want 100", ch.virtualNodes)
	}
}

func TestConsistentHashAddNode(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1:9001")
	ch.AddNode("node2:9002")

	if ch.NodeCount() != 2 {
		t.Errorf("NodeCount() = %d, want 2", ch.NodeCount())
	}

	// Adding same node again should not change count
	ch.AddNode("node1:9001")
	if ch.NodeCount() != 2 {
		t.Errorf("NodeCount() = %d after duplicate add, want 2", ch.NodeCount())
	}
}

func TestConsistentHashRemoveNode(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node1:9001")
	ch.AddNode("node2:9002")

	ch.RemoveNode("node1:9001")

	if ch.NodeCount() != 1 {
		t.Errorf("NodeCount() = %d, want 1", ch.NodeCount())
	}

	// Removing non-existent node should not panic
	ch.RemoveNode("node3:9003")
	if ch.NodeCount() != 1 {
		t.Errorf("NodeCount() = %d after removing non-existent, want 1", ch.NodeCount())
	}
}

func TestConsistentHashGetNode(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1:9001")
	ch.AddNode("node2:9002")
	ch.AddNode("node3:9003")

	// Same key should always return same node
	key := "test-key"
	node1 := ch.GetNode(key)
	node2 := ch.GetNode(key)

	if node1 != node2 {
		t.Errorf("GetNode returned different results for same key: %s vs %s", node1, node2)
	}

	// Empty ring should return empty string
	emptyRing := NewConsistentHash(10)
	if result := emptyRing.GetNode("key"); result != "" {
		t.Errorf("GetNode on empty ring = %s, want empty string", result)
	}
}

func TestConsistentHashGetNodes(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1:9001")
	ch.AddNode("node2:9002")
	ch.AddNode("node3:9003")

	primary, replica := ch.GetNodes(100)

	if primary == "" {
		t.Error("Primary should not be empty")
	}
	if replica == "" {
		t.Error("Replica should not be empty")
	}
	if primary == replica {
		t.Errorf("Primary and Replica should be different: %s", primary)
	}

	// Consistency check
	primary2, replica2 := ch.GetNodes(100)
	if primary != primary2 || replica != replica2 {
		t.Error("GetNodes should return consistent results")
	}
}

func TestConsistentHashGetNodesWithTwoNodes(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("node1:9001")
	ch.AddNode("node2:9002")

	for i := uint32(0); i < 100; i++ {
		primary, replica := ch.GetNodes(i)

		if primary == "" || replica == "" {
			t.Errorf("Partition %d: empty primary or replica", i)
		}
		if primary == replica {
			t.Errorf("Partition %d: primary == replica: %s", i, primary)
		}
	}
}

func TestConsistentHashDistribution(t *testing.T) {
	ch := NewConsistentHash(150)
	nodes := []string{"node1:9001", "node2:9002", "node3:9003", "node4:9004"}
	for _, node := range nodes {
		ch.AddNode(node)
	}

	distribution := ch.GetPartitionDistribution(4096)

	// Check that all nodes have some partitions
	for node, count := range distribution {
		if count == 0 {
			t.Errorf("Node %s has 0 partitions", node)
		}
		t.Logf("Node %s: %d partitions (%.1f%%)", node, count, float64(count)*100/4096)
	}

	// Check total
	total := 0
	for _, count := range distribution {
		total += count
	}
	if total != 4096 {
		t.Errorf("Total partitions = %d, want 4096", total)
	}

	// Check balance (no node should have more than 40% or less than 10% with 4 nodes)
	// Consistent hashing has some variance, but should be reasonably balanced
	for node, count := range distribution {
		percentage := float64(count) / 4096 * 100
		if percentage < 10 || percentage > 40 {
			t.Errorf("Node %s has unbalanced distribution: %.1f%%", node, percentage)
		}
	}
}

func TestConsistentHashMinimalMovement(t *testing.T) {
	ch := NewConsistentHash(150)
	ch.AddNode("node1:9001")
	ch.AddNode("node2:9002")
	ch.AddNode("node3:9003")

	// Record initial assignment
	initial := make(map[uint32]string)
	for i := uint32(0); i < 1000; i++ {
		primary, _ := ch.GetNodes(i)
		initial[i] = primary
	}

	// Add a new node
	ch.AddNode("node4:9004")

	// Count changes
	changes := 0
	for i := uint32(0); i < 1000; i++ {
		primary, _ := ch.GetNodes(i)
		if initial[i] != primary {
			changes++
		}
	}

	// With 4 nodes, roughly 25% of partitions should move to the new node
	// Allow 15-35% range
	changePercent := float64(changes) / 1000 * 100
	t.Logf("Partitions moved after adding node: %d (%.1f%%)", changes, changePercent)

	if changePercent < 15 || changePercent > 35 {
		t.Errorf("Partition movement = %.1f%%, expected 15-35%%", changePercent)
	}
}

func TestConsistentHashGetAllNodes(t *testing.T) {
	ch := NewConsistentHash(10)
	ch.AddNode("node2:9002")
	ch.AddNode("node1:9001")
	ch.AddNode("node3:9003")

	nodes := ch.GetAllNodes()

	if len(nodes) != 3 {
		t.Errorf("GetAllNodes() returned %d nodes, want 3", len(nodes))
	}

	// Should be sorted
	if nodes[0] != "node1:9001" || nodes[1] != "node2:9002" || nodes[2] != "node3:9003" {
		t.Errorf("GetAllNodes() not sorted: %v", nodes)
	}
}

func BenchmarkConsistentHashGetNodes(b *testing.B) {
	ch := NewConsistentHash(150)
	for i := 0; i < 10; i++ {
		ch.AddNode(fmt.Sprintf("node%d:900%d", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.GetNodes(uint32(i % 4096))
	}
}

func BenchmarkConsistentHashAddNode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ch := NewConsistentHash(150)
		for j := 0; j < 10; j++ {
			ch.AddNode(fmt.Sprintf("node%d:900%d", j, j))
		}
	}
}
