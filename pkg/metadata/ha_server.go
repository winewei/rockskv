package metadata

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

// HAServerConfig holds HA metadata server configuration
type HAServerConfig struct {
	*ServerConfig

	// Enable HA mode with leader election
	HAEnabled bool `mapstructure:"ha_enabled"`
}

// DefaultHAServerConfig returns a default HA server configuration
func DefaultHAServerConfig() *HAServerConfig {
	return &HAServerConfig{
		ServerConfig: DefaultServerConfig(),
		HAEnabled:    false,
	}
}

// HAServer wraps Server with leader election for HA mode
type HAServer struct {
	*Server

	haEnabled      bool
	leaderElection *LeaderElection
	isLeader       atomic.Bool
	logger         *zap.Logger
}

// NewHAServer creates a new HA metadata server
func NewHAServer(config *HAServerConfig) (*HAServer, error) {
	if config == nil {
		config = DefaultHAServerConfig()
	}

	// Create base server
	server, err := NewServer(config.ServerConfig)
	if err != nil {
		return nil, err
	}

	logger := common.NewLogger("ha-metadata-server")

	haServer := &HAServer{
		Server:    server,
		haEnabled: config.HAEnabled,
		logger:    logger,
	}

	// Initialize leader election if HA is enabled
	if config.HAEnabled {
		leConfig := &LeaderElectionConfig{
			NodeID:     config.NodeID,
			EtcdConfig: config.Etcd,
		}
		le, err := NewLeaderElection(leConfig)
		if err != nil {
			server.Stop()
			return nil, fmt.Errorf("failed to create leader election: %w", err)
		}
		haServer.leaderElection = le

		// Register callback for leadership changes
		le.OnLeaderChange(func(isLeader bool) {
			haServer.onLeadershipChange(isLeader)
		})
	} else {
		// If HA is not enabled, this node is always the leader
		haServer.isLeader.Store(true)
	}

	return haServer, nil
}

// Start starts the HA server
func (s *HAServer) Start(ctx context.Context) error {
	// Start leader election if enabled
	if s.haEnabled && s.leaderElection != nil {
		if err := s.leaderElection.Start(); err != nil {
			return fmt.Errorf("failed to start leader election: %w", err)
		}

		// Wait for leader to be elected
		s.logger.Info("Waiting for leader election...")
		if err := s.leaderElection.WaitForLeader(ctx); err != nil {
			return fmt.Errorf("failed to wait for leader: %w", err)
		}

		leaderID := s.leaderElection.GetLeaderID()
		isLeader := s.leaderElection.IsLeader()
		s.logger.Info("Leader elected",
			zap.String("leader_id", leaderID),
			zap.Bool("is_self", isLeader),
		)
	}

	return s.Server.Start(ctx)
}

// Stop stops the HA server
func (s *HAServer) Stop() {
	// Stop leader election first
	if s.leaderElection != nil {
		if err := s.leaderElection.Stop(); err != nil {
			s.logger.Error("Failed to stop leader election", zap.Error(err))
		}
	}

	s.Server.Stop()
}

// onLeadershipChange is called when leadership status changes
func (s *HAServer) onLeadershipChange(isLeader bool) {
	s.isLeader.Store(isLeader)

	if isLeader {
		s.logger.Info("This node became the leader, enabling write operations")
		// Could trigger any leader-specific initialization here
	} else {
		s.logger.Warn("This node lost leadership, disabling write operations")
		// Could trigger any cleanup here
	}
}

// IsLeader returns true if this node is the leader
func (s *HAServer) IsLeader() bool {
	if !s.haEnabled {
		return true // Single node mode is always leader
	}
	return s.isLeader.Load()
}

// GetLeaderID returns the current leader's node ID
func (s *HAServer) GetLeaderID() string {
	if !s.haEnabled {
		return s.config.NodeID
	}
	if s.leaderElection == nil {
		return ""
	}
	return s.leaderElection.GetLeaderID()
}

// checkLeaderForWrite checks if this node is the leader for write operations
// Returns an error if not the leader
func (s *HAServer) checkLeaderForWrite() error {
	if !s.haEnabled {
		return nil // Single node mode
	}
	if s.IsLeader() {
		return nil
	}
	leaderID := s.GetLeaderID()
	return status.Errorf(codes.FailedPrecondition,
		"not the leader, current leader is: %s", leaderID)
}

// Override write operations to check for leader status

// RegisterNode overrides Server.RegisterNode with leader check
func (s *HAServer) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if err := s.checkLeaderForWrite(); err != nil {
		return &pb.RegisterNodeResponse{
			Success: false,
		}, err
	}
	return s.Server.RegisterNode(ctx, req)
}

// InitCluster overrides Server.InitCluster with leader check
func (s *HAServer) InitCluster(ctx context.Context, req *pb.InitClusterRequest) (*pb.InitClusterResponse, error) {
	if err := s.checkLeaderForWrite(); err != nil {
		return &pb.InitClusterResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}
	return s.Server.InitCluster(ctx, req)
}

// TriggerRebalance overrides Server.TriggerRebalance with leader check
func (s *HAServer) TriggerRebalance(ctx context.Context, req *pb.TriggerRebalanceRequest) (*pb.TriggerRebalanceResponse, error) {
	if err := s.checkLeaderForWrite(); err != nil {
		return &pb.TriggerRebalanceResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}
	return s.Server.TriggerRebalance(ctx, req)
}

// CancelMigration overrides Server.CancelMigration with leader check
func (s *HAServer) CancelMigration(ctx context.Context, req *pb.CancelMigrationRequest) (*pb.CancelMigrationResponse, error) {
	if err := s.checkLeaderForWrite(); err != nil {
		return &pb.CancelMigrationResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}
	return s.Server.CancelMigration(ctx, req)
}

// ShutdownNode overrides Server.ShutdownNode with leader check
func (s *HAServer) ShutdownNode(ctx context.Context, req *pb.ShutdownNodeRequest) (*pb.ShutdownNodeResponse, error) {
	if err := s.checkLeaderForWrite(); err != nil {
		return &pb.ShutdownNodeResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}
	return s.Server.ShutdownNode(ctx, req)
}

// Read operations don't require leader check - all nodes can serve reads

// GetRouteTable - read operation, all nodes can serve
// GetClusterInfo - read operation, all nodes can serve
// GetMigrationStatus - read operation, all nodes can serve
// GetNodeStatus - read operation, all nodes can serve
// Heartbeat - write to etcd, but should work on any node since etcd handles consistency
// SubscribeRouteUpdates - read operation, all nodes can serve

// GetLeaderInfo returns leader information for clients
func (s *HAServer) GetLeaderInfo(ctx context.Context, req *pb.GetLeaderInfoRequest) (*pb.GetLeaderInfoResponse, error) {
	leaderID := s.GetLeaderID()
	isLeader := s.IsLeader()

	// Try to get leader address from node registry
	var leaderAddr string
	if leaderID != "" {
		node, err := s.store.GetNode(ctx, leaderID)
		if err == nil && node != nil {
			leaderAddr = node.Addr
		}
	}

	return &pb.GetLeaderInfoResponse{
		LeaderId:   leaderID,
		LeaderAddr: leaderAddr,
		IsLeader:   isLeader,
		HaEnabled:  s.haEnabled,
	}, nil
}
