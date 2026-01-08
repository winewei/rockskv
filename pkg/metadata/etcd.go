package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/example/rockskv/pkg/common"
)

const (
	// Key prefixes
	nodePrefix      = "/rockskv/nodes/"
	routeTableKey   = "/rockskv/route_table"
	partitionPrefix = "/rockskv/partitions/"

	// Lease TTL for node registration
	nodeLeaseTTL = 30 // seconds
)

// EtcdStore implements Store interface using etcd
type EtcdStore struct {
	client *clientv3.Client
	logger *zap.Logger
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

	logger := common.NewLogger("etcd-store")
	logger.Info("Connected to etcd", zap.Strings("endpoints", config.Endpoints))

	return &EtcdStore{
		client: client,
		logger: logger,
	}, nil
}

// RegisterNode registers a node with etcd
func (s *EtcdStore) RegisterNode(ctx context.Context, node *NodeInfo) error {
	// Create lease
	lease, err := s.client.Grant(ctx, nodeLeaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	node.RegisteredAt = time.Now()
	node.LastHeartbeat = time.Now()
	node.Status = "online"

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := nodePrefix + node.ID
	_, err = s.client.Put(ctx, key, string(data), clientv3.WithLease(lease.ID))
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
func (s *EtcdStore) UpdateNodeHeartbeat(ctx context.Context, nodeID string) error {
	node, err := s.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.LastHeartbeat = time.Now()

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := nodePrefix + nodeID
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

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

// UpdateRouteTable updates the route table
func (s *EtcdStore) UpdateRouteTable(ctx context.Context, table *RouteTable) error {
	table.Version++
	table.UpdatedAt = time.Now()

	data, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("failed to marshal route table: %w", err)
	}

	_, err = s.client.Put(ctx, routeTableKey, string(data))
	if err != nil {
		return fmt.Errorf("failed to update route table: %w", err)
	}

	s.logger.Info("Route table updated", zap.Uint64("version", table.Version))
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

// Close closes the etcd client
func (s *EtcdStore) Close() error {
	return s.client.Close()
}
