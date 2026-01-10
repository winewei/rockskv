package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
	"github.com/winewei/rockskv/pkg/storage"
)

func main() {
	// Parse command line flags
	configPath := flag.String("c", "config/storage.yaml", "path to config file")
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
	server, err := storage.NewServer(config)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	// Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		metricsAddr := viper.GetString("metrics_addr")
		if metricsAddr == "" {
			metricsAddr = ":9091"
		}
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

func loadConfig(path string) (*storage.ServerConfig, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// Set defaults
	viper.SetDefault("node_id", "storage-1")
	viper.SetDefault("listen_addr", ":9001")
	viper.SetDefault("metadata_addr", "localhost:9000")
	viper.SetDefault("sst_dir", "/tmp/rockskv/sst")
	viper.SetDefault("rocksdb.data_dir", "/data/rockskv")
	viper.SetDefault("rocksdb.wal_dir", "/data/rockskv/wal")
	viper.SetDefault("rocksdb.block_cache_size", 512*1024*1024)
	viper.SetDefault("rocksdb.write_buffer_size", 64*1024*1024)
	viper.SetDefault("rocksdb.max_write_buffer_number", 4)
	viper.SetDefault("rocksdb.max_background_compactions", 4)
	viper.SetDefault("rocksdb.max_background_flushes", 2)
	viper.SetDefault("rocksdb.compression_type", "lz4")

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found, use defaults
	}

	// Environment variable overrides
	viper.AutomaticEnv()

	config := &storage.ServerConfig{
		NodeID:       viper.GetString("node_id"),
		ListenAddr:   viper.GetString("listen_addr"),
		MetadataAddr: viper.GetString("metadata_addr"),
		SSTDir:       viper.GetString("sst_dir"),
		RocksDB: &storage.RocksDBConfig{
			DataDir:                  viper.GetString("rocksdb.data_dir"),
			WALDir:                   viper.GetString("rocksdb.wal_dir"),
			BlockCacheSize:           viper.GetInt64("rocksdb.block_cache_size"),
			WriteBufferSize:          viper.GetUint64("rocksdb.write_buffer_size"),
			MaxWriteBufferNumber:     viper.GetInt("rocksdb.max_write_buffer_number"),
			MaxBackgroundCompactions: viper.GetInt("rocksdb.max_background_compactions"),
			MaxBackgroundFlushes:     viper.GetInt("rocksdb.max_background_flushes"),
			CompressionType:          viper.GetString("rocksdb.compression_type"),
		},
	}

	return config, nil
}
