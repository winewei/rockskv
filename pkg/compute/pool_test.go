package compute

import (
	"testing"
	"time"
)

func TestDefaultPoolConfig(t *testing.T) {
	config := DefaultPoolConfig()

	if config == nil {
		t.Fatal("DefaultPoolConfig returned nil")
	}

	if config.MaxConnsPerHost != 10 {
		t.Errorf("MaxConnsPerHost = %d, want 10", config.MaxConnsPerHost)
	}

	if config.IdleTimeout != 5*time.Minute {
		t.Errorf("IdleTimeout = %v, want 5m", config.IdleTimeout)
	}

	if config.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want 5s", config.DialTimeout)
	}

	if config.KeepAliveTime != 10*time.Second {
		t.Errorf("KeepAliveTime = %v, want 10s", config.KeepAliveTime)
	}

	if config.KeepAliveTimeout != 5*time.Second {
		t.Errorf("KeepAliveTimeout = %v, want 5s", config.KeepAliveTimeout)
	}
}

func TestPoolConfigCustomValues(t *testing.T) {
	config := &PoolConfig{
		MaxConnsPerHost:  20,
		IdleTimeout:      10 * time.Minute,
		DialTimeout:      10 * time.Second,
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 15 * time.Second,
	}

	if config.MaxConnsPerHost != 20 {
		t.Errorf("MaxConnsPerHost = %d, want 20", config.MaxConnsPerHost)
	}

	if config.IdleTimeout != 10*time.Minute {
		t.Errorf("IdleTimeout = %v, want 10m", config.IdleTimeout)
	}

	if config.DialTimeout != 10*time.Second {
		t.Errorf("DialTimeout = %v, want 10s", config.DialTimeout)
	}

	if config.KeepAliveTime != 30*time.Second {
		t.Errorf("KeepAliveTime = %v, want 30s", config.KeepAliveTime)
	}
}

func TestNewConnectionPool(t *testing.T) {
	config := DefaultPoolConfig()
	pool := NewConnectionPool(config)

	if pool == nil {
		t.Fatal("NewConnectionPool returned nil")
	}

	if pool.config != config {
		t.Error("Pool config not set correctly")
	}

	if pool.conns == nil {
		t.Error("Pool conns map should not be nil")
	}

	if pool.logger == nil {
		t.Error("Pool logger should not be nil")
	}
}

func TestNewConnectionPoolWithNilConfig(t *testing.T) {
	pool := NewConnectionPool(nil)

	if pool == nil {
		t.Fatal("NewConnectionPool returned nil with nil config")
	}

	// Should use default config
	if pool.config == nil {
		t.Error("Pool should have default config")
	}

	if pool.config.MaxConnsPerHost != 10 {
		t.Errorf("Expected default MaxConnsPerHost=10, got %d", pool.config.MaxConnsPerHost)
	}
}

func TestConnectionPoolClose(t *testing.T) {
	config := DefaultPoolConfig()
	pool := NewConnectionPool(config)

	// Close should not panic even with no connections
	pool.Close()

	// Pool should be empty after close
	if pool.Size() != 0 {
		t.Errorf("Pool size after close = %d, want 0", pool.Size())
	}
}

func TestConnectionPoolSize(t *testing.T) {
	config := DefaultPoolConfig()
	pool := NewConnectionPool(config)

	// Initial size should be 0
	if pool.Size() != 0 {
		t.Errorf("Initial pool size = %d, want 0", pool.Size())
	}
}

func TestConnectionPoolRemoveConn(t *testing.T) {
	config := DefaultPoolConfig()
	pool := NewConnectionPool(config)

	// RemoveConn should not panic when address doesn't exist
	pool.RemoveConn("nonexistent:8080")

	// Size should still be 0
	if pool.Size() != 0 {
		t.Errorf("Pool size after removing nonexistent = %d, want 0", pool.Size())
	}
}

func TestNodeResolver(t *testing.T) {
	router := NewRouter("test-node", "localhost:2379")
	resolver := NewNodeResolver(router)

	if resolver == nil {
		t.Fatal("NewNodeResolver returned nil")
	}

	if resolver.router != router {
		t.Error("NodeResolver router not set correctly")
	}

	if resolver.cache == nil {
		t.Error("NodeResolver cache should not be nil")
	}
}

func TestNodeResolverResolveAddr(t *testing.T) {
	router := NewRouter("test-node", "localhost:2379")
	resolver := NewNodeResolver(router)

	// In the current implementation, nodeID is returned as-is
	nodeID := "storage-node-1:8080"
	addr, err := resolver.ResolveAddr(nodeID)

	if err != nil {
		t.Fatalf("ResolveAddr failed: %v", err)
	}

	if addr != nodeID {
		t.Errorf("ResolveAddr(%s) = %s, want %s", nodeID, addr, nodeID)
	}

	// Second call should use cache
	addr2, err := resolver.ResolveAddr(nodeID)
	if err != nil {
		t.Fatalf("ResolveAddr (cached) failed: %v", err)
	}

	if addr2 != nodeID {
		t.Errorf("ResolveAddr (cached) = %s, want %s", addr2, nodeID)
	}
}

func TestNodeResolverInvalidateCache(t *testing.T) {
	router := NewRouter("test-node", "localhost:2379")
	resolver := NewNodeResolver(router)

	nodeID := "storage-node-1:8080"

	// Populate cache
	_, _ = resolver.ResolveAddr(nodeID)

	// Invalidate
	resolver.InvalidateCache(nodeID)

	// The next ResolveAddr call should work (re-populate cache)
	addr, err := resolver.ResolveAddr(nodeID)
	if err != nil {
		t.Fatalf("ResolveAddr after invalidate failed: %v", err)
	}

	if addr != nodeID {
		t.Errorf("ResolveAddr after invalidate = %s, want %s", addr, nodeID)
	}
}

func BenchmarkNewConnectionPool(b *testing.B) {
	config := DefaultPoolConfig()
	for i := 0; i < b.N; i++ {
		pool := NewConnectionPool(config)
		pool.Close()
	}
}

func BenchmarkNodeResolverResolveAddr(b *testing.B) {
	router := NewRouter("test-node", "localhost:2379")
	resolver := NewNodeResolver(router)
	nodeID := "storage-node-1:8080"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = resolver.ResolveAddr(nodeID)
	}
}
