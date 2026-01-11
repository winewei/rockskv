// URI parsing for RocksKV connection strings.
package rockskv

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// URI format: rockskv://host1:port1,host2:port2[?options]
//
// Supported options:
//   - timeout: request timeout (e.g., "5s", "500ms")
//   - retryCount: max retry attempts (e.g., "3")
//   - retryDelay: initial retry delay (e.g., "100ms")
//   - maxRetryDelay: max retry delay (e.g., "2s")
//   - poolSize: connection pool size (e.g., "10")
//
// Examples:
//   rockskv://localhost:8000
//   rockskv://localhost:8000,localhost:8001
//   rockskv://node1:8000,node2:8000,node3:8000?timeout=5s&retryCount=3

// ParseURI parses a RocksKV connection URI and returns client options.
func ParseURI(uri string) ([]Option, error) {
	// Handle rockskv:// scheme
	if strings.HasPrefix(uri, "rockskv://") {
		uri = "http://" + uri[len("rockskv://"):]
	} else if !strings.Contains(uri, "://") {
		// Allow plain host:port,host:port format
		uri = "http://" + uri
	}

	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI: %w", err)
	}

	var opts []Option

	// Parse hosts
	hosts := u.Host
	if hosts == "" {
		return nil, fmt.Errorf("no hosts specified in URI")
	}

	// Split by comma for multiple hosts
	addrs := strings.Split(hosts, ",")
	for i, addr := range addrs {
		addrs[i] = strings.TrimSpace(addr)
		if addrs[i] == "" {
			return nil, fmt.Errorf("empty address at position %d", i)
		}
	}
	opts = append(opts, WithAddrs(addrs...))

	// Parse query options
	query := u.Query()

	if v := query.Get("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %w", err)
		}
		opts = append(opts, WithTimeout(d))
	}

	if v := query.Get("retryCount"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid retryCount: %w", err)
		}
		opts = append(opts, WithRetryCount(n))
	}

	if v := query.Get("retryDelay"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid retryDelay: %w", err)
		}
		opts = append(opts, WithRetryDelay(d))
	}

	if v := query.Get("maxRetryDelay"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid maxRetryDelay: %w", err)
		}
		opts = append(opts, func(c *Config) {
			c.MaxRetryDelay = d
		})
	}

	if v := query.Get("poolSize"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid poolSize: %w", err)
		}
		opts = append(opts, WithPoolSize(n))
	}

	return opts, nil
}

// NewClientFromURI creates a new client from a connection URI.
//
// Examples:
//
//	client, err := rockskv.NewClientFromURI("rockskv://localhost:8000,localhost:8001")
//	client, err := rockskv.NewClientFromURI("rockskv://node1:8000?timeout=5s&retryCount=3")
func NewClientFromURI(uri string, extraOpts ...Option) (*Client, error) {
	opts, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	opts = append(opts, extraOpts...)
	return NewClient(opts...)
}
