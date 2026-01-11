package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
)

const (
	// Key prefixes
	nodePrefix          = "/rockskv/nodes/"
	heartbeatPrefix     = "/rockskv/heartbeats/"
	routeTableKey       = "/rockskv/route_table"
	partitionPrefix     = "/rockskv/partitions/"
	migrationPrefix     = "/rockskv/migrations/" // Separate storage for temporary migration state
	clusterInfoKey      = "/rockskv/cluster_info"
	partitionLeasePrefix = "/rockskv/partition_leases/" // Partition lease for split-brain prevention

	// Lock keys
	routeTableLockKey = "/rockskv/locks/route-table"

	// Lease TTL for node registration and heartbeat
	nodeLeaseTTL      = 30 // seconds
	heartbeatLeaseTTL = 15 // seconds - shorter TTL for faster failure detection

	// Session TTL for distributed lock
	lockSessionTTL = 30 // seconds

	// Default replica count
	defaultReplicaCount = 2
)

// EtcdStore implements Store interface using etcd
type EtcdStore struct {
	client          *clientv3.Client
	session         *concurrency.Session
	routeTableMutex *concurrency.Mutex
	logger          *zap.Logger
}

// EtcdConfig holds etcd configuration
type EtcdConfig struct {
	Endpoints   []string      `mapstructure:"endpoints"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	Username    string        `mapstructure:"username"`
	Password    string        `mapstructure:"password"`
}

// DefaultEtcdConfig returns default etcd configuration
func DefaultEtcdConfig() *EtcdConfig {
	return &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
}

// NewEtcdStore creates a new etcd store
func NewEtcdStore(config *EtcdConfig) (*EtcdStore, error) {
	if config == nil {
		config = DefaultEtcdConfig()
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
		Username:    config.Username,
		Password:    config.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	// Fail-fast: Immediately verify the connection is working
	// This catches etcd unavailability early instead of waiting for timeouts later
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Try to get status from any endpoint to verify connectivity
	_, err = client.Status(ctx, config.Endpoints[0])
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to verify etcd connection (is etcd running?): %w", err)
	}

	logger := common.NewLogger("etcd-store")
	logger.Info("Connected to etcd", zap.Strings("endpoints", config.Endpoints))

	// Create session for distributed lock
	session, err := concurrency.NewSession(client, concurrency.WithTTL(lockSessionTTL))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create etcd session: %w", err)
	}

	// Create mutex for route table updates
	routeTableMutex := concurrency.NewMutex(session, routeTableLockKey)

	store := &EtcdStore{
		client:          client,
		session:         session,
		routeTableMutex: routeTableMutex,
		logger:          logger,
	}

	// Start auto-compact in background (every 5 minutes)
	go store.StartAutoCompact(context.Background(), 5*time.Minute)

	return store, nil
}

// GetClusterInfo retrieves the cluster info
func (s *EtcdStore) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	resp, err := s.client.Get(ctx, clusterInfoKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}

	if len(resp.Kvs) == 0 {
		// Return default pending state if not initialized
		return &ClusterInfo{
			State:        ClusterStatePending,
			ReplicaCount: defaultReplicaCount,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}, nil
	}

	var info ClusterInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster info: %w", err)
	}

	return &info, nil
}

// SetClusterState updates the cluster state
func (s *EtcdStore) SetClusterState(ctx context.Context, state ClusterState) error {
	// Get current info or create new
	info, err := s.GetClusterInfo(ctx)
	if err != nil {
		return err
	}

	info.State = state
	info.UpdatedAt = time.Now()

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster info: %w", err)
	}

	_, err = s.client.Put(ctx, clusterInfoKey, string(data))
	if err != nil {
		return fmt.Errorf("failed to put cluster info: %w", err)
	}

	s.logger.Info("Cluster state updated", zap.String("state", string(state)))
	return nil
}

// RegisterNode registers a node with etcd
// Node info is persistent (no Lease), heartbeat uses separate key with Lease
func (s *EtcdStore) RegisterNode(ctx context.Context, node *NodeInfo) error {
	node.RegisteredAt = time.Now()
	node.LastHeartbeat = time.Now()
	node.Status = "online"

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	// Node registration is persistent (no Lease)
	// Heartbeat will be tracked separately with Lease
	key := nodePrefix + node.ID
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	s.logger.Info("Node registered",
		zap.String("node_id", node.ID),
		zap.String("addr", node.Addr),
		zap.String("role", string(node.Role)),
	)

	return nil
}

// UnregisterNode removes a node from etcd
func (s *EtcdStore) UnregisterNode(ctx context.Context, nodeID string) error {
	key := nodePrefix + nodeID
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to unregister node: %w", err)
	}

	s.logger.Info("Node unregistered", zap.String("node_id", nodeID))
	return nil
}

// GetNode retrieves a node by ID
func (s *EtcdStore) GetNode(ctx context.Context, nodeID string) (*NodeInfo, error) {
	key := nodePrefix + nodeID
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var node NodeInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node: %w", err)
	}

	return &node, nil
}

// ListNodes lists all nodes with optional role filter
func (s *EtcdStore) ListNodes(ctx context.Context, role NodeRole) ([]*NodeInfo, error) {
	resp, err := s.client.Get(ctx, nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	var nodes []*NodeInfo
	for _, kv := range resp.Kvs {
		var node NodeInfo
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			s.logger.Warn("Failed to unmarshal node", zap.Error(err))
			continue
		}

		if role == "" || node.Role == role {
			nodes = append(nodes, &node)
		}
	}

	return nodes, nil
}

// UpdateNodeHeartbeat updates the last heartbeat time of a node
// Now uses a separate heartbeat key with Lease for automatic expiration
func (s *EtcdStore) UpdateNodeHeartbeat(ctx context.Context, nodeID string) error {
	// Verify node exists first
	node, err := s.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Create a short-lived lease for heartbeat
	lease, err := s.client.Grant(ctx, heartbeatLeaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create heartbeat lease: %w", err)
	}

	// Update only the heartbeat key (not the entire node info)
	heartbeatKey := heartbeatPrefix + nodeID
	timestamp := time.Now().Format(time.RFC3339Nano)

	_, err = s.client.Put(ctx, heartbeatKey, timestamp, clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	return nil
}

// UpdateNodeStatus updates the status of a node
func (s *EtcdStore) UpdateNodeStatus(ctx context.Context, nodeID string, status NodeStatus) error {
	node, err := s.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.Status = string(status)

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := nodePrefix + nodeID
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	s.logger.Info("Node status updated",
		zap.String("node_id", nodeID),
		zap.String("status", string(status)),
	)
	return nil
}

// GetRouteTable retrieves the current route table
func (s *EtcdStore) GetRouteTable(ctx context.Context) (*RouteTable, error) {
	resp, err := s.client.Get(ctx, routeTableKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get route table: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return NewRouteTable(), nil
	}

	var table RouteTable
	if err := json.Unmarshal(resp.Kvs[0].Value, &table); err != nil {
		return nil, fmt.Errorf("failed to unmarshal route table: %w", err)
	}

	return &table, nil
}

// UpdateRouteTable updates the route table using distributed lock
func (s *EtcdStore) UpdateRouteTable(ctx context.Context, table *RouteTable) error {
	// Acquire distributed lock
	if err := s.routeTableMutex.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire route table lock: %w", err)
	}
	defer func() {
		if err := s.routeTableMutex.Unlock(ctx); err != nil {
			s.logger.Warn("Failed to release route table lock", zap.Error(err))
		}
	}()

	// Read latest version under lock
	current, err := s.GetRouteTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current route table: %w", err)
	}

	// Update version
	table.Version = current.Version + 1
	table.UpdatedAt = time.Now()

	data, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("failed to marshal route table: %w", err)
	}

	// Write to etcd (no CAS needed, we have the lock)
	_, err = s.client.Put(ctx, routeTableKey, string(data))
	if err != nil {
		return fmt.Errorf("failed to put route table: %w", err)
	}

	s.logger.Info("Route table updated",
		zap.Uint64("old_version", current.Version),
		zap.Uint64("new_version", table.Version))
	common.RouteTableVersion.Set(float64(table.Version))

	return nil
}

// GetPartition retrieves a partition by ID
func (s *EtcdStore) GetPartition(ctx context.Context, partitionID uint32) (*PartitionInfo, error) {
	key := fmt.Sprintf("%s%d", partitionPrefix, partitionID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var partition PartitionInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &partition); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition: %w", err)
	}

	return &partition, nil
}

// UpdatePartition updates a partition
func (s *EtcdStore) UpdatePartition(ctx context.Context, partition *PartitionInfo) error {
	data, err := json.Marshal(partition)
	if err != nil {
		return fmt.Errorf("failed to marshal partition: %w", err)
	}

	key := fmt.Sprintf("%s%d", partitionPrefix, partition.ID)
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update partition: %w", err)
	}

	return nil
}

// WatchRouteTable watches for route table changes
func (s *EtcdStore) WatchRouteTable(ctx context.Context) (<-chan *RouteTable, error) {
	ch := make(chan *RouteTable, 10)

	go func() {
		defer close(ch)

		watchCh := s.client.Watch(ctx, routeTableKey)
		for resp := range watchCh {
			for _, event := range resp.Events {
				if event.Type == clientv3.EventTypePut {
					var table RouteTable
					if err := json.Unmarshal(event.Kv.Value, &table); err != nil {
						s.logger.Warn("Failed to unmarshal route table event", zap.Error(err))
						continue
					}
					select {
					case ch <- &table:
					default:
						s.logger.Warn("Route table watch channel full")
					}
				}
			}
		}
	}()

	return ch, nil
}

// WatchNodes watches for node changes
func (s *EtcdStore) WatchNodes(ctx context.Context) (<-chan *NodeEvent, error) {
	ch := make(chan *NodeEvent, 10)

	go func() {
		defer close(ch)

		watchCh := s.client.Watch(ctx, nodePrefix, clientv3.WithPrefix())
		for resp := range watchCh {
			for _, event := range resp.Events {
				nodeEvent := &NodeEvent{}

				// Extract node ID from key
				key := string(event.Kv.Key)
				if len(key) > len(nodePrefix) {
					nodeEvent.NodeID = key[len(nodePrefix):]
				}

				switch event.Type {
				case clientv3.EventTypePut:
					var node NodeInfo
					if err := json.Unmarshal(event.Kv.Value, &node); err != nil {
						s.logger.Warn("Failed to unmarshal node event", zap.Error(err))
						continue
					}
					nodeEvent.Node = &node
					if event.IsCreate() {
						nodeEvent.Type = NodeEventAdded
					} else {
						nodeEvent.Type = NodeEventUpdated
					}
				case clientv3.EventTypeDelete:
					nodeEvent.Type = NodeEventRemoved
				}

				select {
				case ch <- nodeEvent:
				default:
					s.logger.Warn("Node watch channel full")
				}
			}
		}
	}()

	return ch, nil
}

// SetMigrationState sets the migration state for a partition (separate from route table)
func (s *EtcdStore) SetMigrationState(ctx context.Context, partitionID uint32, state *MigrationInfo) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal migration state: %w", err)
	}

	key := fmt.Sprintf("%s%d", migrationPrefix, partitionID)
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to set migration state: %w", err)
	}

	s.logger.Debug("Migration state set",
		zap.Uint32("partition_id", partitionID),
		zap.String("target", state.Target),
		zap.Int32("state", int32(state.State)),
	)

	return nil
}

// GetMigrationState gets the migration state for a partition
func (s *EtcdStore) GetMigrationState(ctx context.Context, partitionID uint32) (*MigrationInfo, error) {
	key := fmt.Sprintf("%s%d", migrationPrefix, partitionID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration state: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil // No migration in progress
	}

	var state MigrationInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal migration state: %w", err)
	}

	return &state, nil
}

// DeleteMigrationState deletes the migration state for a partition
func (s *EtcdStore) DeleteMigrationState(ctx context.Context, partitionID uint32) error {
	key := fmt.Sprintf("%s%d", migrationPrefix, partitionID)
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete migration state: %w", err)
	}

	s.logger.Debug("Migration state deleted", zap.Uint32("partition_id", partitionID))
	return nil
}

// Close closes the etcd client and session
func (s *EtcdStore) Close() error {
	if s.session != nil {
		if err := s.session.Close(); err != nil {
			s.logger.Warn("Failed to close etcd session", zap.Error(err))
		}
	}
	return s.client.Close()
}
