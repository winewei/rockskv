package client

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/example/rockskv/pkg/common"
	pb "github.com/example/rockskv/pkg/proto"
)

// Config holds client configuration
type Config struct {
	// ComputeAddrs is a list of compute node addresses
	ComputeAddrs []string `mapstructure:"compute_addrs"`

	// DialTimeout is the connection timeout
	DialTimeout time.Duration `mapstructure:"dial_timeout"`

	// RequestTimeout is the default request timeout
	RequestTimeout time.Duration `mapstructure:"request_timeout"`

	// MaxRetries is the maximum number of retries
	MaxRetries int `mapstructure:"max_retries"`

	// RetryInterval is the interval between retries
	RetryInterval time.Duration `mapstructure:"retry_interval"`
}

// DefaultConfig returns a default client configuration
func DefaultConfig() *Config {
	return &Config{
		ComputeAddrs:   []string{"localhost:8000"},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Second,
		MaxRetries:     3,
		RetryInterval:  100 * time.Millisecond,
	}
}

// Client is the RocksKV client SDK
type Client struct {
	config *Config
	conn   *grpc.ClientConn
	client pb.KVServiceClient
	logger *zap.Logger
}

// New creates a new RocksKV client
func New(config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if len(config.ComputeAddrs) == 0 {
		return nil, fmt.Errorf("no compute addresses configured")
	}

	logger := common.NewLogger("rockskv-client")

	// Connect to the first available compute node
	var conn *grpc.ClientConn
	var err error

	for _, addr := range config.ComputeAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout)
		conn, err = grpc.DialContext(
			ctx,
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		cancel()

		if err == nil {
			logger.Info("Connected to compute node", zap.String("addr", addr))
			break
		}

		logger.Warn("Failed to connect to compute node",
			zap.String("addr", addr),
			zap.Error(err),
		)
	}

	if conn == nil {
		return nil, fmt.Errorf("failed to connect to any compute node: %w", err)
	}

	return &Client{
		config: config,
		conn:   conn,
		client: pb.NewKVServiceClient(conn),
		logger: logger,
	}, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Get retrieves a value by key
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.config.RequestTimeout)
		defer cancel()
	}

	var lastErr error
	for i := 0; i <= c.config.MaxRetries; i++ {
		resp, err := c.client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			lastErr = err
			if i < c.config.MaxRetries {
				time.Sleep(c.config.RetryInterval)
				continue
			}
			return nil, false, fmt.Errorf("get failed after %d retries: %w", c.config.MaxRetries, err)
		}

		return resp.Value, resp.Found, nil
	}

	return nil, false, lastErr
}

// Put stores a key-value pair
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.config.RequestTimeout)
		defer cancel()
	}

	var lastErr error
	for i := 0; i <= c.config.MaxRetries; i++ {
		resp, err := c.client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		if err != nil {
			lastErr = err
			if i < c.config.MaxRetries {
				time.Sleep(c.config.RetryInterval)
				continue
			}
			return fmt.Errorf("put failed after %d retries: %w", c.config.MaxRetries, err)
		}

		if !resp.Success {
			return fmt.Errorf("put failed: server returned failure")
		}

		return nil
	}

	return lastErr
}

// Delete removes a key
func (c *Client) Delete(ctx context.Context, key []byte) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.config.RequestTimeout)
		defer cancel()
	}

	var lastErr error
	for i := 0; i <= c.config.MaxRetries; i++ {
		resp, err := c.client.Delete(ctx, &pb.DeleteRequest{Key: key})
		if err != nil {
			lastErr = err
			if i < c.config.MaxRetries {
				time.Sleep(c.config.RetryInterval)
				continue
			}
			return fmt.Errorf("delete failed after %d retries: %w", c.config.MaxRetries, err)
		}

		if !resp.Success {
			return fmt.Errorf("delete failed: server returned failure")
		}

		return nil
	}

	return lastErr
}

// BatchGet retrieves multiple values by keys
func (c *Client) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.config.RequestTimeout)
		defer cancel()
	}

	resp, err := c.client.BatchGet(ctx, &pb.BatchGetRequest{Keys: keys})
	if err != nil {
		return nil, fmt.Errorf("batch get failed: %w", err)
	}

	result := make(map[string][]byte)
	for _, item := range resp.Items {
		if item.Found {
			result[string(item.Key)] = item.Value
		}
	}

	return result, nil
}

// BatchPut stores multiple key-value pairs
func (c *Client) BatchPut(ctx context.Context, items map[string][]byte) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.config.RequestTimeout)
		defer cancel()
	}

	pbItems := make([]*pb.KeyValue, 0, len(items))
	for key, value := range items {
		pbItems = append(pbItems, &pb.KeyValue{
			Key:   []byte(key),
			Value: value,
		})
	}

	resp, err := c.client.BatchPut(ctx, &pb.BatchPutRequest{Items: pbItems})
	if err != nil {
		return fmt.Errorf("batch put failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("batch put failed: server returned failure")
	}

	return nil
}

// GetString is a convenience method that gets a string value
func (c *Client) GetString(ctx context.Context, key string) (string, bool, error) {
	value, found, err := c.Get(ctx, []byte(key))
	if err != nil || !found {
		return "", found, err
	}
	return string(value), true, nil
}

// PutString is a convenience method that puts a string value
func (c *Client) PutString(ctx context.Context, key, value string) error {
	return c.Put(ctx, []byte(key), []byte(value))
}

// DeleteString is a convenience method that deletes a string key
func (c *Client) DeleteString(ctx context.Context, key string) error {
	return c.Delete(ctx, []byte(key))
}

// Exists checks if a key exists
func (c *Client) Exists(ctx context.Context, key []byte) (bool, error) {
	_, found, err := c.Get(ctx, key)
	return found, err
}

// SetNX sets a key only if it doesn't exist (Set if Not eXists)
func (c *Client) SetNX(ctx context.Context, key, value []byte) (bool, error) {
	// Check if exists
	exists, err := c.Exists(ctx, key)
	if err != nil {
		return false, err
	}

	if exists {
		return false, nil
	}

	// Try to set
	if err := c.Put(ctx, key, value); err != nil {
		return false, err
	}

	return true, nil
}

// Ping checks if the server is reachable
func (c *Client) Ping(ctx context.Context) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.config.RequestTimeout)
		defer cancel()
	}

	// Use a Get with an empty key as a ping
	_, _, err := c.Get(ctx, []byte("__ping__"))
	if err != nil {
		// Ignore "key not found" type errors
		return nil
	}
	return nil
}
