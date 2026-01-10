package metadata

import (
	"context"
	"time"

	pb "github.com/winewei/rockskv/pkg/proto"
)

// Store defines the interface for metadata storage
type Store interface {
	// Node operations
	RegisterNode(ctx context.Context, node *NodeInfo) error
	UnregisterNode(ctx context.Context, nodeID string) error
	GetNode(ctx context.Context, nodeID string) (*NodeInfo, error)
	ListNodes(ctx context.Context, role NodeRole) ([]*NodeInfo, error)
	UpdateNodeHeartbeat(ctx context.Context, nodeID string) error

	// Route table operations
	GetRouteTable(ctx context.Context) (*RouteTable, error)
	UpdateRouteTable(ctx context.Context, table *RouteTable) error
	GetPartition(ctx context.Context, partitionID uint32) (*PartitionInfo, error)
	UpdatePartition(ctx context.Context, partition *PartitionInfo) error

	// Watch operations
	WatchRouteTable(ctx context.Context) (<-chan *RouteTable, error)
	WatchNodes(ctx context.Context) (<-chan *NodeEvent, error)

	// Migration state operations (separate from route table)
	SetMigrationState(ctx context.Context, partitionID uint32, state *MigrationInfo) error
	GetMigrationState(ctx context.Context, partitionID uint32) (*MigrationInfo, error)
	DeleteMigrationState(ctx context.Context, partitionID uint32) error

	// Close closes the store
	Close() error
}

// NodeRole represents the role of a node
type NodeRole string

const (
	NodeRoleStorage  NodeRole = "storage"
	NodeRoleCompute  NodeRole = "compute"
	NodeRoleMetadata NodeRole = "metadata"
)

// NodeInfo contains information about a node
type NodeInfo struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"`
	Role          NodeRole  `json:"role"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	RegisteredAt  time.Time `json:"registered_at"`
	Partitions    []uint32  `json:"partitions,omitempty"`
}

// RouteTable contains routing information
type RouteTable struct {
	Version    uint64                    `json:"version"`
	Partitions map[uint32]*PartitionInfo `json:"partitions"`
	UpdatedAt  time.Time                 `json:"updated_at"`
}

// PartitionInfo contains information about a partition
type PartitionInfo struct {
	ID              uint32             `json:"id"`
	Primary         string             `json:"primary"`
	Replica         string             `json:"replica"`
	Status          PartitionStatus    `json:"status"`
	MigrationTarget string             `json:"migration_target,omitempty"`
	MigrationState  pb.MigrationState  `json:"migration_state,omitempty"`
}

// PartitionStatus represents the status of a partition
type PartitionStatus string

const (
	PartitionStatusNormal       PartitionStatus = "normal"
	PartitionStatusMigratingOut PartitionStatus = "migrating_out"
	PartitionStatusMigratingIn  PartitionStatus = "migrating_in"
	PartitionStatusOffline      PartitionStatus = "offline"
)

// NodeEvent represents a node change event
type NodeEvent struct {
	Type   NodeEventType
	NodeID string
	Node   *NodeInfo
}

// NodeEventType represents the type of node event
type NodeEventType string

const (
	NodeEventAdded   NodeEventType = "added"
	NodeEventRemoved NodeEventType = "removed"
	NodeEventUpdated NodeEventType = "updated"
)

// MigrationInfo contains temporary migration state (separate from route table)
type MigrationInfo struct {
	PartitionID     uint32            `json:"partition_id"`
	Target          string            `json:"target"`           // Target node address
	State           pb.MigrationState `json:"state"`            // Current migration state
	IsPrimaryMove   bool              `json:"is_primary_move"`  // true if migrating primary, false if replica
	StartedAt       time.Time         `json:"started_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

// NewRouteTable creates a new empty route table
func NewRouteTable() *RouteTable {
	return &RouteTable{
		Version:    1,
		Partitions: make(map[uint32]*PartitionInfo),
		UpdatedAt:  time.Now(),
	}
}
