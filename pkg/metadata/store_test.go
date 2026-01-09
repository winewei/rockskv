package metadata

import (
	"testing"
	"time"
)

func TestNodeRoleConstants(t *testing.T) {
	tests := []struct {
		role     NodeRole
		expected string
	}{
		{NodeRoleStorage, "storage"},
		{NodeRoleCompute, "compute"},
		{NodeRoleMetadata, "metadata"},
	}

	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			if string(tt.role) != tt.expected {
				t.Errorf("NodeRole = %s, want %s", tt.role, tt.expected)
			}
		})
	}
}

func TestPartitionStatusConstants(t *testing.T) {
	tests := []struct {
		status   PartitionStatus
		expected string
	}{
		{PartitionStatusNormal, "normal"},
		{PartitionStatusMigratingOut, "migrating_out"},
		{PartitionStatusMigratingIn, "migrating_in"},
		{PartitionStatusOffline, "offline"},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if string(tt.status) != tt.expected {
				t.Errorf("PartitionStatus = %s, want %s", tt.status, tt.expected)
			}
		})
	}
}

func TestNodeEventTypeConstants(t *testing.T) {
	tests := []struct {
		eventType NodeEventType
		expected  string
	}{
		{NodeEventAdded, "added"},
		{NodeEventRemoved, "removed"},
		{NodeEventUpdated, "updated"},
	}

	for _, tt := range tests {
		t.Run(string(tt.eventType), func(t *testing.T) {
			if string(tt.eventType) != tt.expected {
				t.Errorf("NodeEventType = %s, want %s", tt.eventType, tt.expected)
			}
		})
	}
}

func TestNewRouteTable(t *testing.T) {
	table := NewRouteTable()

	if table == nil {
		t.Fatal("NewRouteTable returned nil")
	}

	if table.Version != 1 {
		t.Errorf("Version = %d, want 1", table.Version)
	}

	if table.Partitions == nil {
		t.Error("Partitions should not be nil")
	}

	if len(table.Partitions) != 0 {
		t.Errorf("Partitions length = %d, want 0", len(table.Partitions))
	}

	if table.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}

	// Should be recent
	if time.Since(table.UpdatedAt) > time.Second {
		t.Error("UpdatedAt should be recent")
	}
}

func TestNodeInfo(t *testing.T) {
	now := time.Now()
	node := &NodeInfo{
		ID:            "node-1",
		Addr:          "localhost:8080",
		Role:          NodeRoleStorage,
		Status:        "active",
		LastHeartbeat: now,
		RegisteredAt:  now,
		Partitions:    []uint32{1, 2, 3},
	}

	if node.ID != "node-1" {
		t.Errorf("ID = %s, want node-1", node.ID)
	}

	if node.Addr != "localhost:8080" {
		t.Errorf("Addr = %s, want localhost:8080", node.Addr)
	}

	if node.Role != NodeRoleStorage {
		t.Errorf("Role = %s, want storage", node.Role)
	}

	if node.Status != "active" {
		t.Errorf("Status = %s, want active", node.Status)
	}

	if len(node.Partitions) != 3 {
		t.Errorf("Partitions length = %d, want 3", len(node.Partitions))
	}
}

func TestNodeInfoEmptyPartitions(t *testing.T) {
	node := &NodeInfo{
		ID:   "node-1",
		Addr: "localhost:8080",
		Role: NodeRoleCompute,
	}

	if len(node.Partitions) != 0 {
		t.Error("Partitions should be empty for compute node")
	}
}

func TestPartitionInfo(t *testing.T) {
	partition := &PartitionInfo{
		ID:      100,
		Primary: "node-1",
		Replica: "node-2",
		Status:  PartitionStatusNormal,
	}

	if partition.ID != 100 {
		t.Errorf("ID = %d, want 100", partition.ID)
	}

	if partition.Primary != "node-1" {
		t.Errorf("Primary = %s, want node-1", partition.Primary)
	}

	if partition.Replica != "node-2" {
		t.Errorf("Replica = %s, want node-2", partition.Replica)
	}

	if partition.Status != PartitionStatusNormal {
		t.Errorf("Status = %s, want normal", partition.Status)
	}
}

func TestRouteTable(t *testing.T) {
	table := &RouteTable{
		Version:    10,
		Partitions: make(map[uint32]*PartitionInfo),
		UpdatedAt:  time.Now(),
	}

	// Add partitions
	table.Partitions[0] = &PartitionInfo{ID: 0, Primary: "node-1", Replica: "node-2", Status: PartitionStatusNormal}
	table.Partitions[1] = &PartitionInfo{ID: 1, Primary: "node-2", Replica: "node-1", Status: PartitionStatusNormal}

	if table.Version != 10 {
		t.Errorf("Version = %d, want 10", table.Version)
	}

	if len(table.Partitions) != 2 {
		t.Errorf("Partitions count = %d, want 2", len(table.Partitions))
	}

	p0 := table.Partitions[0]
	if p0.Primary != "node-1" {
		t.Errorf("Partition 0 Primary = %s, want node-1", p0.Primary)
	}
}

func TestNodeEvent(t *testing.T) {
	node := &NodeInfo{
		ID:   "node-1",
		Addr: "localhost:8080",
		Role: NodeRoleStorage,
	}

	event := &NodeEvent{
		Type:   NodeEventAdded,
		NodeID: "node-1",
		Node:   node,
	}

	if event.Type != NodeEventAdded {
		t.Errorf("Type = %s, want added", event.Type)
	}

	if event.NodeID != "node-1" {
		t.Errorf("NodeID = %s, want node-1", event.NodeID)
	}

	if event.Node != node {
		t.Error("Node reference mismatch")
	}
}

func BenchmarkNewRouteTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewRouteTable()
	}
}
