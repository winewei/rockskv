package metadata

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
)

const (
	// DefaultHeartbeatInterval is the default interval for heartbeat checks
	DefaultHeartbeatInterval = 5 * time.Second

	// DefaultHeartbeatTimeout is the default timeout for considering a node dead
	DefaultHeartbeatTimeout = 30 * time.Second

	// DefaultFailoverDelay is the delay before triggering failover
	DefaultFailoverDelay = 10 * time.Second
)

// FailoverConfig holds failover configuration
type FailoverConfig struct {
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	HeartbeatTimeout  time.Duration `mapstructure:"heartbeat_timeout"`
	FailoverDelay     time.Duration `mapstructure:"failover_delay"`
}

// DefaultFailoverConfig returns default failover configuration
func DefaultFailoverConfig() *FailoverConfig {
	return &FailoverConfig{
		HeartbeatInterval: DefaultHeartbeatInterval,
		HeartbeatTimeout:  DefaultHeartbeatTimeout,
		FailoverDelay:     DefaultFailoverDelay,
	}
}

// FailoverManager manages automatic failover
type FailoverManager struct {
	config *FailoverConfig
	store  Store
	router *Router
	logger *zap.Logger

	// Track node health
	nodeHealth map[string]*nodeHealthInfo
	mu         sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

type nodeHealthInfo struct {
	nodeID        string
	lastHeartbeat time.Time
	failCount     int
	isHealthy     bool
}

// NewFailoverManager creates a new failover manager
func NewFailoverManager(config *FailoverConfig, store Store, router *Router) *FailoverManager {
	if config == nil {
		config = DefaultFailoverConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &FailoverManager{
		config:     config,
		store:      store,
		router:     router,
		logger:     common.NewLogger("failover"),
		nodeHealth: make(map[string]*nodeHealthInfo),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the failover manager
func (fm *FailoverManager) Start() {
	go fm.monitorNodes()
	fm.logger.Info("Failover manager started",
		zap.Duration("heartbeat_interval", fm.config.HeartbeatInterval),
		zap.Duration("heartbeat_timeout", fm.config.HeartbeatTimeout),
	)
}

// Stop stops the failover manager
func (fm *FailoverManager) Stop() {
	fm.cancel()
	fm.logger.Info("Failover manager stopped")
}

// monitorNodes periodically checks node health
func (fm *FailoverManager) monitorNodes() {
	ticker := time.NewTicker(fm.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fm.ctx.Done():
			return
		case <-ticker.C:
			fm.checkNodes()
		}
	}
}

// checkNodes checks the health of all storage nodes
func (fm *FailoverManager) checkNodes() {
	nodes, err := fm.store.ListNodes(fm.ctx, NodeRoleStorage)
	if err != nil {
		fm.logger.Error("Failed to list storage nodes", zap.Error(err))
		return
	}

	now := time.Now()
	failedNodes := make([]string, 0)

	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, node := range nodes {
		health, exists := fm.nodeHealth[node.ID]
		if !exists {
			health = &nodeHealthInfo{
				nodeID:        node.ID,
				lastHeartbeat: node.LastHeartbeat,
				isHealthy:     true,
			}
			fm.nodeHealth[node.ID] = health
		}

		// Update heartbeat time
		if node.LastHeartbeat.After(health.lastHeartbeat) {
			health.lastHeartbeat = node.LastHeartbeat
			health.failCount = 0
			health.isHealthy = true
		}

		// Check if node is unhealthy
		if now.Sub(health.lastHeartbeat) > fm.config.HeartbeatTimeout {
			health.failCount++
			if health.isHealthy {
				fm.logger.Warn("Node heartbeat timeout",
					zap.String("node_id", node.ID),
					zap.Time("last_heartbeat", health.lastHeartbeat),
				)
			}
			health.isHealthy = false
			failedNodes = append(failedNodes, node.ID)
		}
	}

	// Handle failed nodes
	for _, nodeID := range failedNodes {
		fm.handleNodeFailure(nodeID)
	}
}

// handleNodeFailure handles a node failure
func (fm *FailoverManager) handleNodeFailure(nodeID string) {
	fm.logger.Warn("Node failure detected, initiating failover",
		zap.String("node_id", nodeID),
	)

	// Find partitions affected by this node failure
	table := fm.router.GetRouteTable()
	affectedPartitions := make([]uint32, 0)

	for partitionID, partition := range table.Partitions {
		if partition.Primary == nodeID {
			affectedPartitions = append(affectedPartitions, partitionID)
		}
	}

	if len(affectedPartitions) == 0 {
		return
	}

	fm.logger.Info("Partitions affected by node failure",
		zap.String("node_id", nodeID),
		zap.Int("count", len(affectedPartitions)),
	)

	// Promote replicas for affected partitions
	for _, partitionID := range affectedPartitions {
		if err := fm.router.PromoteReplica(fm.ctx, partitionID); err != nil {
			fm.logger.Error("Failed to promote replica",
				zap.Uint32("partition_id", partitionID),
				zap.Error(err),
			)
			continue
		}

		fm.logger.Info("Replica promoted to primary",
			zap.Uint32("partition_id", partitionID),
		)
	}

	// Trigger rebalance to assign new replicas
	go func() {
		// Wait for failover delay to avoid thrashing
		time.Sleep(fm.config.FailoverDelay)

		if err := fm.router.RebalancePartitions(fm.ctx); err != nil {
			fm.logger.Error("Failed to rebalance after failover", zap.Error(err))
		}
	}()
}

// RecordHeartbeat records a heartbeat for a node
func (fm *FailoverManager) RecordHeartbeat(nodeID string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	health, exists := fm.nodeHealth[nodeID]
	if !exists {
		health = &nodeHealthInfo{
			nodeID:    nodeID,
			isHealthy: true,
		}
		fm.nodeHealth[nodeID] = health
	}

	health.lastHeartbeat = time.Now()
	health.failCount = 0

	if !health.isHealthy {
		fm.logger.Info("Node recovered", zap.String("node_id", nodeID))
		health.isHealthy = true
	}
}

// IsNodeHealthy checks if a node is healthy
func (fm *FailoverManager) IsNodeHealthy(nodeID string) bool {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	health, exists := fm.nodeHealth[nodeID]
	if !exists {
		return false
	}

	return health.isHealthy
}

// GetHealthyNodes returns a list of healthy nodes
func (fm *FailoverManager) GetHealthyNodes() []string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	healthy := make([]string, 0)
	for nodeID, health := range fm.nodeHealth {
		if health.isHealthy {
			healthy = append(healthy, nodeID)
		}
	}

	return healthy
}

// GetNodeHealth returns health information for a node
func (fm *FailoverManager) GetNodeHealth(nodeID string) (lastHeartbeat time.Time, isHealthy bool, failCount int) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	health, exists := fm.nodeHealth[nodeID]
	if !exists {
		return time.Time{}, false, 0
	}

	return health.lastHeartbeat, health.isHealthy, health.failCount
}
