package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/example/rockskv/pkg/common"
	"github.com/example/rockskv/pkg/compute"
)

func main() {
	// Parse command line flags
	configPath := flag.String("c", "config/compute.yaml", "path to config file")
	flag.Parse()

	// Initialize logger
	if err := common.InitLogger("info", true); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer common.Sync()

	logger := common.GetLogger()

	// Load configuration
	config, err := loadConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// Create server
	server, err := compute.NewServer(config)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	// Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		metricsAddr := ":8090"
		logger.Info("Starting metrics server", zap.String("addr", metricsAddr))
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			logger.Error("Metrics server failed", zap.Error(err))
		}
	}()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	// Wait for signal or error
	select {
	case sig := <-sigCh:
		logger.Info("Received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("Server error", zap.Error(err))
	}

	// Graceful shutdown
	server.Stop()
	logger.Info("Server stopped")
}

func loadConfig(path string) (*compute.ServerConfig, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// Set defaults
	viper.SetDefault("node_id", "compute-1")
	viper.SetDefault("listen_addr", ":8000")
	viper.SetDefault("metadata_addr", "localhost:9000")
	viper.SetDefault("pool.max_conns_per_host", 10)
	viper.SetDefault("pool.idle_timeout", "5m")
	viper.SetDefault("pool.dial_timeout", "5s")
	viper.SetDefault("pool.keep_alive_time", "10s")
	viper.SetDefault("pool.keep_alive_timeout", "5s")

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found, use defaults
	}

	// Environment variable overrides
	viper.AutomaticEnv()

	config := &compute.ServerConfig{
		NodeID:       viper.GetString("node_id"),
		ListenAddr:   viper.GetString("listen_addr"),
		MetadataAddr: viper.GetString("metadata_addr"),
		Pool: &compute.PoolConfig{
			MaxConnsPerHost:  viper.GetInt("pool.max_conns_per_host"),
			IdleTimeout:      viper.GetDuration("pool.idle_timeout"),
			DialTimeout:      viper.GetDuration("pool.dial_timeout"),
			KeepAliveTime:    viper.GetDuration("pool.keep_alive_time"),
			KeepAliveTimeout: viper.GetDuration("pool.keep_alive_timeout"),
		},
	}

	// Parse durations properly if they're strings
	if config.Pool.IdleTimeout == 0 {
		config.Pool.IdleTimeout = 5 * time.Minute
	}
	if config.Pool.DialTimeout == 0 {
		config.Pool.DialTimeout = 5 * time.Second
	}
	if config.Pool.KeepAliveTime == 0 {
		config.Pool.KeepAliveTime = 10 * time.Second
	}
	if config.Pool.KeepAliveTimeout == 0 {
		config.Pool.KeepAliveTimeout = 5 * time.Second
	}

	return config, nil
}
