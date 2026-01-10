package compute

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

// PoolConfig holds connection pool configuration
type PoolConfig struct {
	MaxConnsPerHost  int           `mapstructure:"max_conns_per_host"`
	IdleTimeout      time.Duration `mapstructure:"idle_timeout"`
	DialTimeout      time.Duration `mapstructure:"dial_timeout"`
	KeepAliveTime    time.Duration `mapstructure:"keep_alive_time"`
	KeepAliveTimeout time.Duration `mapstructure:"keep_alive_timeout"`
}

// DefaultPoolConfig returns a default pool configuration
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConnsPerHost:  10,
		IdleTimeout:      5 * time.Minute,
		DialTimeout:      5 * time.Second,
		KeepAliveTime:    10 * time.Second,
		KeepAliveTimeout: 5 * time.Second,
	}
}

// ConnectionPool manages gRPC connections to storage nodes
type ConnectionPool struct {
	config *PoolConfig
	conns  map[string]*grpc.ClientConn
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config *PoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultPoolConfig()
	}

	return &ConnectionPool{
		config: config,
		conns:  make(map[string]*grpc.ClientConn),
		logger: common.NewLogger("connection-pool"),
	}
}

// GetStorageClient gets or creates a storage service client for the given address
func (p *ConnectionPool) GetStorageClient(addr string) (pb.StorageServiceClient, error) {
	conn, err := p.getConn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewStorageServiceClient(conn), nil
}

// getConn gets or creates a connection for the given address
func (p *ConnectionPool) getConn(addr string) (*grpc.ClientConn, error) {
	// Try to get existing connection
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()

	if ok {
		// Check if connection is still usable
		state := conn.GetState()
		if state != 4 { // Not shutdown
			return conn, nil
		}
		// Connection is not usable, create a new one
		p.mu.Lock()
		delete(p.conns, addr)
		p.mu.Unlock()
	}

	// Create new connection
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double check after acquiring write lock
	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}

	conn, err := p.dial(addr)
	if err != nil {
		return nil, err
	}

	p.conns[addr] = conn
	common.ConnectionPoolSize.WithLabelValues(addr).Set(1)

	p.logger.Debug("Created connection to storage node",
		zap.String("addr", addr),
	)

	return conn, nil
}

// dial creates a new gRPC connection
func (p *ConnectionPool) dial(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                p.config.KeepAliveTime,
			Timeout:             p.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024), // 64MB
			grpc.MaxCallSendMsgSize(64*1024*1024), // 64MB
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	return conn, nil
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil {
			p.logger.Warn("Failed to close connection",
				zap.String("addr", addr),
				zap.Error(err),
			)
		}
		common.ConnectionPoolSize.WithLabelValues(addr).Set(0)
	}

	p.conns = make(map[string]*grpc.ClientConn)
	p.logger.Info("Connection pool closed")
}

// RemoveConn removes a connection from the pool
func (p *ConnectionPool) RemoveConn(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[addr]; ok {
		_ = conn.Close()
		delete(p.conns, addr)
		common.ConnectionPoolSize.WithLabelValues(addr).Set(0)
		p.logger.Debug("Removed connection from pool",
			zap.String("addr", addr),
		)
	}
}

// Size returns the number of connections in the pool
func (p *ConnectionPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.conns)
}

// NodeResolver resolves node IDs to addresses
type NodeResolver struct {
	router *Router
	cache  map[string]string
	mu     sync.RWMutex
}

// NewNodeResolver creates a new node resolver
func NewNodeResolver(router *Router) *NodeResolver {
	return &NodeResolver{
		router: router,
		cache:  make(map[string]string),
	}
}

// ResolveAddr resolves a node ID to an address
// For now, we assume node ID is in the format "host:port"
func (r *NodeResolver) ResolveAddr(nodeID string) (string, error) {
	// Check cache first
	r.mu.RLock()
	addr, ok := r.cache[nodeID]
	r.mu.RUnlock()

	if ok {
		return addr, nil
	}

	// For simplicity, assume node ID is the address
	// In production, this would query the metadata service
	addr = nodeID

	r.mu.Lock()
	r.cache[nodeID] = addr
	r.mu.Unlock()

	return addr, nil
}

// InvalidateCache invalidates the cache for a node
func (r *NodeResolver) InvalidateCache(nodeID string) {
	r.mu.Lock()
	delete(r.cache, nodeID)
	r.mu.Unlock()
}
