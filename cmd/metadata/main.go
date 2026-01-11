package main

import (
	"context"
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

	"github.com/winewei/rockskv/pkg/common"
	"github.com/winewei/rockskv/pkg/metadata"
)

func main() {
	// Parse command line flags
	configPath := flag.String("c", "config/metadata.yaml", "path to config file")
	initCluster := flag.Bool("init", false, "initialize cluster partitions")
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

	// Create HA server (supports both single-node and HA mode)
	server, err := metadata.NewHAServer(config)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize cluster if requested
	if *initCluster {
		logger.Info("Waiting for storage nodes to register...")
		time.Sleep(5 * time.Second) // Give nodes time to register

		if err := server.InitializeCluster(ctx); err != nil {
			logger.Fatal("Failed to initialize cluster", zap.Error(err))
		}
		logger.Info("Cluster initialized successfully")
	}

	// Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		metricsAddr := ":9090"
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
		errCh <- server.Start(ctx)
	}()

	// Wait for signal or error
	select {
	case sig := <-sigCh:
		logger.Info("Received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	case err := <-errCh:
		logger.Error("Server error", zap.Error(err))
	}

	// Graceful shutdown
	server.Stop()
	logger.Info("Server stopped")
}

func loadConfig(path string) (*metadata.HAServerConfig, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// Set defaults
	viper.SetDefault("node_id", "metadata-1")
	viper.SetDefault("listen_addr", ":9000")
	viper.SetDefault("etcd.endpoints", []string{"localhost:2379"})
	viper.SetDefault("etcd.dial_timeout", "5s")
	viper.SetDefault("ha_enabled", false)

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found, use defaults
	}

	// Environment variable overrides
	viper.AutomaticEnv()

	config := &metadata.HAServerConfig{
		ServerConfig: &metadata.ServerConfig{
			NodeID:     viper.GetString("node_id"),
			ListenAddr: viper.GetString("listen_addr"),
			Etcd: &metadata.EtcdConfig{
				Endpoints:   viper.GetStringSlice("etcd.endpoints"),
				DialTimeout: viper.GetDuration("etcd.dial_timeout"),
				Username:    viper.GetString("etcd.username"),
				Password:    viper.GetString("etcd.password"),
			},
		},
		HAEnabled: viper.GetBool("ha_enabled"),
	}

	return config, nil
}
