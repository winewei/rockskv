//go:build integration
// +build integration

package metadata

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/winewei/rockskv/pkg/proto"
)

const (
	testEtcdEndpoint = "localhost:2379"
)

// TestLeaderElection verifies that leader election works with 3 metadata nodes
func TestLeaderElection(t *testing.T) {
	ctx := context.Background()

	// Create configs for 3 metadata nodes
	configs := []*HAServerConfig{
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-test-1",
				ListenAddr: ":0", // Use any available port
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-test-2",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-test-3",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
	}

	// Create servers
	servers := make([]*HAServer, 0, 3)
	for _, config := range configs {
		server, err := NewHAServer(config)
		if err != nil {
			t.Fatalf("Failed to create server %s: %v", config.NodeID, err)
		}
		defer server.Stop()
		servers = append(servers, server)
	}

	// Start all servers
	for _, server := range servers {
		go func(s *HAServer) {
			if err := s.Start(ctx); err != nil {
				t.Logf("Server stopped: %v", err)
			}
		}(server)
	}

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Verify exactly one leader exists
	leaderCount := 0
	var leaderServer *HAServer
	for _, server := range servers {
		if server.IsLeader() {
			leaderCount++
			leaderServer = server
		}
	}

	if leaderCount != 1 {
		t.Fatalf("Expected exactly 1 leader, got %d", leaderCount)
	}

	// Verify all nodes agree on the same leader
	leaderID := leaderServer.GetLeaderID()
	for _, server := range servers {
		if server.GetLeaderID() != leaderID {
			t.Errorf("Server %s sees leader %s, expected %s",
				server.config.NodeID, server.GetLeaderID(), leaderID)
		}
	}

	// Verify leader entry exists in etcd (etcd concurrency.Election uses prefix-based keys)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	etcdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// etcd concurrency.Election stores keys with a prefix like "/rockskv/leader/metadata/{lease-id}"
	resp, err := etcdClient.Get(etcdCtx, "/rockskv/leader/metadata/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to query etcd: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatalf("Leader entry not found in etcd")
	}

	t.Logf("✅ Leader election successful: leader=%s", leaderID)
}

// TestLeaderFailover verifies that a new leader is elected when the current leader fails
func TestLeaderFailover(t *testing.T) {
	ctx := context.Background()

	// Create configs for 3 metadata nodes
	configs := []*HAServerConfig{
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-failover-1",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-failover-2",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-failover-3",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
	}

	// Create servers
	servers := make([]*HAServer, 0, 3)
	for _, config := range configs {
		server, err := NewHAServer(config)
		if err != nil {
			t.Fatalf("Failed to create server %s: %v", config.NodeID, err)
		}
		defer server.Stop()
		servers = append(servers, server)
	}

	// Start all servers
	for _, server := range servers {
		go func(s *HAServer) {
			if err := s.Start(ctx); err != nil {
				t.Logf("Server stopped: %v", err)
			}
		}(server)
	}

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Find the current leader
	var originalLeader *HAServer
	var followers []*HAServer
	for _, server := range servers {
		if server.IsLeader() {
			originalLeader = server
		} else {
			followers = append(followers, server)
		}
	}

	if originalLeader == nil {
		t.Fatalf("No leader found")
	}

	originalLeaderID := originalLeader.GetLeaderID()
	t.Logf("Original leader: %s", originalLeaderID)

	// Kill the leader
	originalLeader.Stop()

	// Wait for new leader election (TTL is 10 seconds)
	time.Sleep(12 * time.Second)

	// Verify a new leader was elected from the followers
	newLeaderCount := 0
	var newLeader *HAServer
	for _, server := range followers {
		if server.IsLeader() {
			newLeaderCount++
			newLeader = server
		}
	}

	if newLeaderCount != 1 {
		t.Fatalf("Expected exactly 1 new leader, got %d", newLeaderCount)
	}

	newLeaderID := newLeader.GetLeaderID()
	if newLeaderID == originalLeaderID {
		t.Fatalf("New leader ID should be different from original leader")
	}

	// Verify all remaining nodes agree on the new leader
	for _, server := range followers {
		if server.GetLeaderID() != newLeaderID {
			t.Errorf("Server %s sees leader %s, expected %s",
				server.config.NodeID, server.GetLeaderID(), newLeaderID)
		}
	}

	t.Logf("✅ Leader failover successful: %s -> %s", originalLeaderID, newLeaderID)
}

// TestWriteForwarding verifies that non-leader nodes reject write operations
func TestWriteForwarding(t *testing.T) {
	ctx := context.Background()

	// Create configs for 2 metadata nodes
	configs := []*HAServerConfig{
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-write-1",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-write-2",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
	}

	// Create servers
	servers := make([]*HAServer, 0, 2)
	for _, config := range configs {
		server, err := NewHAServer(config)
		if err != nil {
			t.Fatalf("Failed to create server %s: %v", config.NodeID, err)
		}
		defer server.Stop()
		servers = append(servers, server)
	}

	// Start all servers
	for _, server := range servers {
		go func(s *HAServer) {
			if err := s.Start(ctx); err != nil {
				t.Logf("Server stopped: %v", err)
			}
		}(server)
	}

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Find leader and follower
	var leader, follower *HAServer
	for _, server := range servers {
		if server.IsLeader() {
			leader = server
		} else {
			follower = server
		}
	}

	if leader == nil || follower == nil {
		t.Fatalf("Need both leader and follower")
	}

	// Test write operations
	t.Run("RegisterNode on follower should fail", func(t *testing.T) {
		resp, err := follower.RegisterNode(ctx, &pb.RegisterNodeRequest{
			NodeId: "test-storage-1",
			Addr:   "localhost:9001",
			Role:   pb.NodeRole_STORAGE,
		})

		if err == nil {
			t.Fatalf("Expected error, got success: %v", resp)
		}

		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("Expected gRPC status error, got: %v", err)
		}

		if st.Code() != codes.FailedPrecondition {
			t.Errorf("Expected FailedPrecondition error, got: %v", st.Code())
		}

		leaderID := leader.GetLeaderID()
		if st.Message() != "not the leader, current leader is: "+leaderID {
			t.Errorf("Expected leader ID in error message, got: %s", st.Message())
		}
	})

	t.Run("RegisterNode on leader should succeed", func(t *testing.T) {
		resp, err := leader.RegisterNode(ctx, &pb.RegisterNodeRequest{
			NodeId: "test-storage-2",
			Addr:   "localhost:9002",
			Role:   pb.NodeRole_STORAGE,
		})

		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}

		if !resp.Success {
			t.Errorf("Expected success=true, got: %v", resp)
		}
	})

	t.Run("InitCluster on follower should fail", func(t *testing.T) {
		_, err := follower.InitCluster(ctx, &pb.InitClusterRequest{})

		if err == nil {
			t.Fatalf("Expected error on follower")
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.FailedPrecondition {
			t.Errorf("Expected FailedPrecondition, got: %v", err)
		}
	})

	t.Logf("✅ Write forwarding correctly enforced")
}

// TestReadScaling verifies that all nodes can handle read operations
func TestReadScaling(t *testing.T) {
	ctx := context.Background()

	// Create configs for 3 metadata nodes
	configs := []*HAServerConfig{
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-read-1",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-read-2",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-read-3",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
	}

	// Create servers
	servers := make([]*HAServer, 0, 3)
	for _, config := range configs {
		server, err := NewHAServer(config)
		if err != nil {
			t.Fatalf("Failed to create server %s: %v", config.NodeID, err)
		}
		defer server.Stop()
		servers = append(servers, server)
	}

	// Start all servers
	for _, server := range servers {
		go func(s *HAServer) {
			if err := s.Start(ctx); err != nil {
				t.Logf("Server stopped: %v", err)
			}
		}(server)
	}

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Test read operations on all nodes
	for i, server := range servers {
		t.Run("GetRouteTable on node "+server.config.NodeID, func(t *testing.T) {
			resp, err := server.GetRouteTable(ctx, &pb.GetRouteTableRequest{})
			if err != nil {
				t.Fatalf("Node %d failed GetRouteTable: %v", i, err)
			}
			if resp.RouteTable == nil {
				t.Errorf("Expected non-nil route table")
			}
		})

		t.Run("GetClusterInfo on node "+server.config.NodeID, func(t *testing.T) {
			resp, err := server.GetClusterInfo(ctx, &pb.GetClusterInfoRequest{})
			if err != nil {
				t.Fatalf("Node %d failed GetClusterInfo: %v", i, err)
			}
			if resp == nil {
				t.Errorf("Expected non-nil cluster info")
			}
		})
	}

	t.Logf("✅ All nodes can serve read operations")
}

// TestGetLeaderInfo verifies that clients can discover the current leader
func TestGetLeaderInfo(t *testing.T) {
	ctx := context.Background()

	// Create config for a single HA-enabled node
	config := &HAServerConfig{
		ServerConfig: &ServerConfig{
			NodeID:     "metadata-leader-info-1",
			ListenAddr: ":0",
			Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
		},
		HAEnabled: true,
	}

	server, err := NewHAServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Start server
	go func() {
		if err := server.Start(ctx); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Test GetLeaderInfo
	resp, err := server.GetLeaderInfo(ctx, &pb.GetLeaderInfoRequest{})
	if err != nil {
		t.Fatalf("GetLeaderInfo failed: %v", err)
	}

	// Verify response
	if resp.LeaderId != config.NodeID {
		t.Errorf("Expected leader_id=%s, got %s", config.NodeID, resp.LeaderId)
	}

	if !resp.IsLeader {
		t.Errorf("Expected is_leader=true, got false")
	}

	if !resp.HaEnabled {
		t.Errorf("Expected ha_enabled=true, got false")
	}

	t.Logf("✅ GetLeaderInfo works correctly")
}

// TestSingleNodeMode verifies that HA disabled mode always makes node the leader
func TestSingleNodeMode(t *testing.T) {
	ctx := context.Background()

	config := &HAServerConfig{
		ServerConfig: &ServerConfig{
			NodeID:     "metadata-single-1",
			ListenAddr: ":0",
			Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
		},
		HAEnabled: false, // HA disabled
	}

	server, err := NewHAServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Start server
	go func() {
		if err := server.Start(ctx); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Give it a moment to start
	time.Sleep(1 * time.Second)

	// Verify node is always leader in single-node mode
	if !server.IsLeader() {
		t.Fatalf("Single-node mode should always be leader")
	}

	if server.GetLeaderID() != config.NodeID {
		t.Errorf("Expected leader_id=%s, got %s", config.NodeID, server.GetLeaderID())
	}

	// Test write operations succeed
	resp, err := server.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId: "test-storage-1",
		Addr:   "localhost:9001",
		Role:   pb.NodeRole_STORAGE,
	})

	if err != nil {
		t.Fatalf("RegisterNode should succeed in single-node mode: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success=true")
	}

	// GetLeaderInfo should reflect single-node mode
	leaderInfo, err := server.GetLeaderInfo(ctx, &pb.GetLeaderInfoRequest{})
	if err != nil {
		t.Fatalf("GetLeaderInfo failed: %v", err)
	}

	if leaderInfo.HaEnabled {
		t.Errorf("Expected ha_enabled=false in single-node mode")
	}

	if !leaderInfo.IsLeader {
		t.Errorf("Expected is_leader=true in single-node mode")
	}

	t.Logf("✅ Single-node mode works correctly")
}

// TestHeartbeatOnAllNodes verifies that heartbeat works on all nodes (not just leader)
func TestHeartbeatOnAllNodes(t *testing.T) {
	ctx := context.Background()

	// Create configs for 2 metadata nodes
	configs := []*HAServerConfig{
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-heartbeat-1",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-heartbeat-2",
				ListenAddr: ":0",
				Etcd:       &EtcdConfig{Endpoints: []string{testEtcdEndpoint}, DialTimeout: 5 * time.Second},
			},
			HAEnabled: true,
		},
	}

	// Create servers
	servers := make([]*HAServer, 0, 2)
	for _, config := range configs {
		server, err := NewHAServer(config)
		if err != nil {
			t.Fatalf("Failed to create server %s: %v", config.NodeID, err)
		}
		defer server.Stop()
		servers = append(servers, server)
	}

	// Start all servers
	for _, server := range servers {
		go func(s *HAServer) {
			if err := s.Start(ctx); err != nil {
				t.Logf("Server stopped: %v", err)
			}
		}(server)
	}

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Find the leader to register a test storage node first
	var leader *HAServer
	for _, server := range servers {
		if server.IsLeader() {
			leader = server
			break
		}
	}

	if leader == nil {
		t.Fatalf("No leader found")
	}

	// Register a test storage node on the leader
	nodeID := "test-storage-hb"
	_, err := leader.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId: nodeID,
		Addr:   "localhost:9999",
		Role:   pb.NodeRole_STORAGE,
	})
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	// Test heartbeat on all nodes (both leader and follower)
	// Heartbeat writes to etcd directly, so it should work on any node
	for _, server := range servers {
		t.Run("Heartbeat on "+server.config.NodeID, func(t *testing.T) {
			resp, err := server.Heartbeat(ctx, &pb.HeartbeatRequest{
				NodeId: nodeID,
			})

			if err != nil {
				t.Fatalf("Heartbeat failed: %v", err)
			}

			if !resp.Success {
				t.Errorf("Expected success=true")
			}
		})
	}

	t.Logf("✅ Heartbeat works on all nodes")
}
