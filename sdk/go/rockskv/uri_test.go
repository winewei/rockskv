package rockskv

import (
	"testing"
	"time"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		wantErr bool
		check   func(*Config) bool
	}{
		{
			name:    "single host",
			uri:     "rockskv://localhost:8000",
			wantErr: false,
			check: func(c *Config) bool {
				return len(c.Addrs) == 1 && c.Addrs[0] == "localhost:8000"
			},
		},
		{
			name:    "multiple hosts",
			uri:     "rockskv://localhost:8000,localhost:8001,localhost:8002",
			wantErr: false,
			check: func(c *Config) bool {
				return len(c.Addrs) == 3
			},
		},
		{
			name:    "with timeout option",
			uri:     "rockskv://localhost:8000?timeout=10s",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Timeout == 10*time.Second
			},
		},
		{
			name:    "with retry options",
			uri:     "rockskv://localhost:8000?retryCount=5&retryDelay=200ms",
			wantErr: false,
			check: func(c *Config) bool {
				return c.RetryCount == 5 && c.RetryDelay == 200*time.Millisecond
			},
		},
		{
			name:    "with all options",
			uri:     "rockskv://host1:8000,host2:8001?timeout=5s&retryCount=3&poolSize=20",
			wantErr: false,
			check: func(c *Config) bool {
				return len(c.Addrs) == 2 &&
					c.Timeout == 5*time.Second &&
					c.RetryCount == 3 &&
					c.PoolSize == 20
			},
		},
		{
			name:    "plain host format",
			uri:     "localhost:8000,localhost:8001",
			wantErr: false,
			check: func(c *Config) bool {
				return len(c.Addrs) == 2
			},
		},
		{
			name:    "empty URI",
			uri:     "",
			wantErr: true,
		},
		{
			name:    "invalid timeout",
			uri:     "rockskv://localhost:8000?timeout=invalid",
			wantErr: true,
		},
		{
			name:    "invalid retryCount",
			uri:     "rockskv://localhost:8000?retryCount=abc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := ParseURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseURI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Apply options to config
			config := DefaultConfig()
			for _, opt := range opts {
				opt(config)
			}

			if tt.check != nil && !tt.check(config) {
				t.Errorf("ParseURI() config check failed for %s", tt.uri)
			}
		})
	}
}

func TestNewClientFromURI(t *testing.T) {
	// Test that we can create a client from URI (will fail to connect, but should parse)
	_, err := NewClientFromURI("rockskv://localhost:18000?timeout=1s&retryCount=0")
	// Expect connection error, not parse error
	if err == nil {
		t.Skip("Unexpectedly connected - skipping")
	}
	// The error should be a connection error, not a URI parse error
	if err.Error() == "no hosts specified in URI" {
		t.Errorf("Got parse error instead of connection error: %v", err)
	}
}
