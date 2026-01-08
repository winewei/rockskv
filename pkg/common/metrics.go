package common

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestCounter counts the number of requests
	RequestCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rockskv",
			Name:      "requests_total",
			Help:      "Total number of requests",
		},
		[]string{"service", "method", "status"},
	)

	// RequestLatency measures request latency
	RequestLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rockskv",
			Name:      "request_duration_seconds",
			Help:      "Request latency in seconds",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"service", "method"},
	)

	// StorageOperations counts storage operations
	StorageOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rockskv",
			Subsystem: "storage",
			Name:      "operations_total",
			Help:      "Total number of storage operations",
		},
		[]string{"operation", "status"},
	)

	// StorageLatency measures storage operation latency
	StorageLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rockskv",
			Subsystem: "storage",
			Name:      "operation_duration_seconds",
			Help:      "Storage operation latency in seconds",
			Buckets:   []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
		[]string{"operation"},
	)

	// PartitionGauge tracks the number of partitions
	PartitionGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rockskv",
			Subsystem: "storage",
			Name:      "partitions",
			Help:      "Number of partitions by status",
		},
		[]string{"status"},
	)

	// ConnectionPoolSize tracks connection pool size
	ConnectionPoolSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rockskv",
			Subsystem: "compute",
			Name:      "connection_pool_size",
			Help:      "Number of connections in the pool",
		},
		[]string{"target"},
	)

	// RouteTableVersion tracks the current route table version
	RouteTableVersion = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "rockskv",
			Subsystem: "metadata",
			Name:      "route_table_version",
			Help:      "Current route table version",
		},
	)

	// NodeCount tracks the number of registered nodes
	NodeCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rockskv",
			Subsystem: "metadata",
			Name:      "nodes_total",
			Help:      "Number of registered nodes by role",
		},
		[]string{"role"},
	)
)
