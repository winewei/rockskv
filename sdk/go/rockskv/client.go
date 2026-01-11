// Package rockskv provides a Go client for RocksKV distributed key-value store.
package rockskv

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	pb "github.com/winewei/rockskv/pkg/proto"
)

// Common errors
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrTimeout     = errors.New("request timed out")
	ErrNoAddrs     = errors.New("no addresses provided")
	ErrClosed      = errors.New("client is closed")
)

// Client is a RocksKV client.
type Client struct {
	mu      sync.RWMutex
	config  *Config
	conns   map[string]*grpc.ClientConn
	stubs   map[string]pb.KVServiceClient
	addrs   []string
	closed  bool
	rng     *rand.Rand
}

// Config holds client configuration.
type Config struct {
	Addrs           []string
	Timeout         time.Duration
	PoolSize        int
	RetryCount      int
	RetryDelay      time.Duration
	MaxRetryDelay   time.Duration
	KeepAliveTime   time.Duration
	KeepAliveTimeout time.Duration
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Timeout:          5 * time.Second,
		PoolSize:         10,
		RetryCount:       3,
		RetryDelay:       100 * time.Millisecond,
		MaxRetryDelay:    2 * time.Second,
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 5 * time.Second,
	}
}

// Option is a function that configures the client.
type Option func(*Config)

// WithAddrs sets the compute node addresses.
func WithAddrs(addrs ...string) Option {
	return func(c *Config) {
		c.Addrs = addrs
	}
}

// WithTimeout sets the request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

// WithPoolSize sets the connection pool size per host.
func WithPoolSize(size int) Option {
	return func(c *Config) {
		c.PoolSize = size
	}
}

// WithRetryCount sets the maximum retry attempts.
func WithRetryCount(count int) Option {
	return func(c *Config) {
		c.RetryCount = count
	}
}

// WithRetryDelay sets the initial retry delay.
func WithRetryDelay(delay time.Duration) Option {
	return func(c *Config) {
		c.RetryDelay = delay
	}
}

// NewClient creates a new RocksKV client.
func NewClient(opts ...Option) (*Client, error) {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Addrs) == 0 {
		return nil, ErrNoAddrs
	}

	client := &Client{
		config: config,
		conns:  make(map[string]*grpc.ClientConn),
		stubs:  make(map[string]pb.KVServiceClient),
		addrs:  config.Addrs,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Connect to all addresses
	for _, addr := range config.Addrs {
		if err := client.connect(addr); err != nil {
			client.Close()
			return nil, err
		}
	}

	return client, nil
}

func (c *Client) connect(addr string) error {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.config.KeepAliveTime,
			Timeout:             c.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conns[addr] = conn
	c.stubs[addr] = pb.NewKVServiceClient(conn)
	c.mu.Unlock()

	return nil
}

func (c *Client) getStub() (pb.KVServiceClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, ErrClosed
	}

	if len(c.stubs) == 0 {
		return nil, errors.New("no available connections")
	}

	// Random load balancing
	idx := c.rng.Intn(len(c.addrs))
	addr := c.addrs[idx]
	return c.stubs[addr], nil
}

func (c *Client) callWithRetry(ctx context.Context, fn func(context.Context, pb.KVServiceClient) error) error {
	var lastErr error
	delay := c.config.RetryDelay

	for attempt := 0; attempt <= c.config.RetryCount; attempt++ {
		stub, err := c.getStub()
		if err != nil {
			return err
		}

		// Create context with timeout
		callCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
		err = fn(callCtx, stub)
		cancel()

		if err == nil {
			return nil
		}

		lastErr = err
		st, ok := status.FromError(err)
		if !ok {
			return err
		}

		switch st.Code() {
		case codes.DeadlineExceeded:
			return ErrTimeout
		case codes.Unavailable:
			if attempt < c.config.RetryCount {
				time.Sleep(delay)
				delay = min(delay*2, c.config.MaxRetryDelay)
				continue
			}
		default:
			return err
		}
	}

	return lastErr
}

// Get retrieves a value by key.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	var value string
	var found bool

	err := c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		resp, err := stub.Get(callCtx, &pb.GetRequest{Key: []byte(key)})
		if err != nil {
			return err
		}
		found = resp.Found
		value = string(resp.Value)
		return nil
	})

	if err != nil {
		return "", err
	}

	if !found {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// GetBytes retrieves a value by key as bytes.
func (c *Client) GetBytes(ctx context.Context, key []byte) ([]byte, error) {
	var value []byte
	var found bool

	err := c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		resp, err := stub.Get(callCtx, &pb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		found = resp.Found
		value = resp.Value
		return nil
	})

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

// Put stores a key-value pair.
func (c *Client) Put(ctx context.Context, key, value string) error {
	return c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		_, err := stub.Put(callCtx, &pb.PutRequest{
			Key:   []byte(key),
			Value: []byte(value),
		})
		return err
	})
}

// PutBytes stores a key-value pair as bytes.
func (c *Client) PutBytes(ctx context.Context, key, value []byte) error {
	return c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		_, err := stub.Put(callCtx, &pb.PutRequest{
			Key:   key,
			Value: value,
		})
		return err
	})
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		_, err := stub.Delete(callCtx, &pb.DeleteRequest{
			Key: []byte(key),
		})
		return err
	})
}

// MGet retrieves multiple values.
func (c *Client) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	keyBytes := make([][]byte, len(keys))
	for i, k := range keys {
		keyBytes[i] = []byte(k)
	}

	result := make(map[string]string)

	err := c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		resp, err := stub.BatchGet(callCtx, &pb.BatchGetRequest{Keys: keyBytes})
		if err != nil {
			return err
		}
		for _, item := range resp.Items {
			if item.Found {
				result[string(item.Key)] = string(item.Value)
			}
		}
		return nil
	})

	return result, err
}

// MSet stores multiple key-value pairs.
func (c *Client) MSet(ctx context.Context, items map[string]string) (int, error) {
	kvItems := make([]*pb.KeyValue, 0, len(items))
	for k, v := range items {
		kvItems = append(kvItems, &pb.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	var count int

	err := c.callWithRetry(ctx, func(callCtx context.Context, stub pb.KVServiceClient) error {
		resp, err := stub.BatchPut(callCtx, &pb.BatchPutRequest{Items: kvItems})
		if err != nil {
			return err
		}
		count = int(resp.Count)
		return nil
	})

	return count, err
}

// Close closes all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var errs []error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	c.conns = nil
	c.stubs = nil

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
