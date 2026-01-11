package metadata

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
)

const (
	// Leader election key prefix
	leaderElectionPrefix = "/rockskv/leader/"

	// Election session TTL
	electionSessionTTL = 10 // seconds

	// Leader key for metadata service
	metadataLeaderKey = "/rockskv/leader/metadata"
)

// LeaderElection manages leader election for metadata service HA
type LeaderElection struct {
	client     *clientv3.Client
	session    *concurrency.Session
	election   *concurrency.Election
	nodeID     string
	isLeader   atomic.Bool
	leaderID   atomic.Value // stores current leader ID
	logger     *zap.Logger
	callbacks  []func(isLeader bool)
	callbackMu sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// LeaderElectionConfig holds leader election configuration
type LeaderElectionConfig struct {
	NodeID     string
	EtcdConfig *EtcdConfig
}

// NewLeaderElection creates a new leader election instance
func NewLeaderElection(config *LeaderElectionConfig) (*LeaderElection, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	etcdConfig := config.EtcdConfig
	if etcdConfig == nil {
		etcdConfig = DefaultEtcdConfig()
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdConfig.Endpoints,
		DialTimeout: etcdConfig.DialTimeout,
		Username:    etcdConfig.Username,
		Password:    etcdConfig.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	logger := common.NewLogger("leader-election")

	ctx, cancel := context.WithCancel(context.Background())

	le := &LeaderElection{
		client:    client,
		nodeID:    config.NodeID,
		logger:    logger,
		callbacks: make([]func(isLeader bool), 0),
		ctx:       ctx,
		cancel:    cancel,
	}
	le.leaderID.Store("")

	return le, nil
}

// Start begins the leader election process
func (le *LeaderElection) Start() error {
	// Create session for election
	session, err := concurrency.NewSession(le.client, concurrency.WithTTL(electionSessionTTL))
	if err != nil {
		return fmt.Errorf("failed to create election session: %w", err)
	}
	le.session = session

	// Create election instance
	le.election = concurrency.NewElection(session, metadataLeaderKey)

	// Start election goroutine
	le.wg.Add(1)
	go le.runElection()

	// Start leader watcher
	le.wg.Add(1)
	go le.watchLeader()

	le.logger.Info("Leader election started", zap.String("node_id", le.nodeID))
	return nil
}

// runElection continuously tries to become leader
func (le *LeaderElection) runElection() {
	defer le.wg.Done()

	for {
		select {
		case <-le.ctx.Done():
			return
		default:
		}

		le.logger.Info("Campaigning for leadership", zap.String("node_id", le.nodeID))

		// Campaign blocks until this node becomes leader or context is cancelled
		err := le.election.Campaign(le.ctx, le.nodeID)
		if err != nil {
			if le.ctx.Err() != nil {
				return // Context cancelled, shutting down
			}
			le.logger.Error("Campaign failed", zap.Error(err))
			time.Sleep(time.Second) // Wait before retrying
			continue
		}

		// This node is now the leader
		le.becomeLeader()

		// Wait until we lose leadership or context is cancelled
		select {
		case <-le.ctx.Done():
			le.resignLeadership()
			return
		case <-le.session.Done():
			// Session expired, we lost leadership
			le.loseLeadership()
			// Recreate session and try again
			session, err := concurrency.NewSession(le.client, concurrency.WithTTL(electionSessionTTL))
			if err != nil {
				le.logger.Error("Failed to recreate session", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			le.session = session
			le.election = concurrency.NewElection(session, metadataLeaderKey)
		}
	}
}

// watchLeader watches for leader changes
func (le *LeaderElection) watchLeader() {
	defer le.wg.Done()

	for {
		select {
		case <-le.ctx.Done():
			return
		default:
		}

		// Observe leader changes
		ch := le.election.Observe(le.ctx)
		for resp := range ch {
			if len(resp.Kvs) > 0 {
				leaderID := string(resp.Kvs[0].Value)
				le.leaderID.Store(leaderID)
				le.logger.Info("Leader changed",
					zap.String("leader_id", leaderID),
					zap.Bool("is_self", leaderID == le.nodeID))
			}
		}

		// Channel closed, wait a bit before retrying
		if le.ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
	}
}

// becomeLeader is called when this node becomes the leader
func (le *LeaderElection) becomeLeader() {
	le.isLeader.Store(true)
	le.leaderID.Store(le.nodeID)
	le.logger.Info("Became leader", zap.String("node_id", le.nodeID))

	// Notify callbacks
	le.callbackMu.RLock()
	callbacks := make([]func(bool), len(le.callbacks))
	copy(callbacks, le.callbacks)
	le.callbackMu.RUnlock()

	for _, cb := range callbacks {
		go cb(true)
	}
}

// loseLeadership is called when this node loses leadership
func (le *LeaderElection) loseLeadership() {
	le.isLeader.Store(false)
	le.logger.Warn("Lost leadership", zap.String("node_id", le.nodeID))

	// Notify callbacks
	le.callbackMu.RLock()
	callbacks := make([]func(bool), len(le.callbacks))
	copy(callbacks, le.callbacks)
	le.callbackMu.RUnlock()

	for _, cb := range callbacks {
		go cb(false)
	}
}

// resignLeadership voluntarily resigns leadership
func (le *LeaderElection) resignLeadership() {
	if !le.isLeader.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := le.election.Resign(ctx); err != nil {
		le.logger.Error("Failed to resign leadership", zap.Error(err))
	} else {
		le.logger.Info("Resigned leadership", zap.String("node_id", le.nodeID))
	}
	le.isLeader.Store(false)
}

// IsLeader returns true if this node is the current leader
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader.Load()
}

// GetLeaderID returns the current leader's node ID
func (le *LeaderElection) GetLeaderID() string {
	id := le.leaderID.Load()
	if id == nil {
		return ""
	}
	return id.(string)
}

// GetNodeID returns this node's ID
func (le *LeaderElection) GetNodeID() string {
	return le.nodeID
}

// OnLeaderChange registers a callback for leader changes
// The callback receives true when becoming leader, false when losing leadership
func (le *LeaderElection) OnLeaderChange(callback func(isLeader bool)) {
	le.callbackMu.Lock()
	le.callbacks = append(le.callbacks, callback)
	le.callbackMu.Unlock()
}

// Stop stops the leader election and resigns if leader
func (le *LeaderElection) Stop() error {
	le.logger.Info("Stopping leader election", zap.String("node_id", le.nodeID))

	// Cancel context to stop goroutines
	le.cancel()

	// Resign if we are leader
	if le.isLeader.Load() {
		le.resignLeadership()
	}

	// Wait for goroutines to finish
	le.wg.Wait()

	// Close session
	if le.session != nil {
		if err := le.session.Close(); err != nil {
			le.logger.Error("Failed to close session", zap.Error(err))
		}
	}

	// Close client
	if le.client != nil {
		if err := le.client.Close(); err != nil {
			le.logger.Error("Failed to close client", zap.Error(err))
		}
	}

	le.logger.Info("Leader election stopped", zap.String("node_id", le.nodeID))
	return nil
}

// WaitForLeader waits until a leader is elected or context is cancelled
func (le *LeaderElection) WaitForLeader(ctx context.Context) error {
	for {
		if le.GetLeaderID() != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
