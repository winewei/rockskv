package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/winewei/rockskv/pkg/client"
	pb "github.com/winewei/rockskv/pkg/proto"
)

const TotalPartitions = 4096

func main() {
	// Parse command line flags
	addr := flag.String("addr", "localhost:8000", "Compute node address")
	timeout := flag.Duration("timeout", 5*time.Second, "Request timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Create client
	cfg := &client.Config{
		ComputeAddrs:   []string{*addr},
		DialTimeout:    5 * time.Second,
		RequestTimeout: *timeout,
		MaxRetries:     3,
		RetryInterval:  100 * time.Millisecond,
	}

	c, err := client.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()

	// Execute command
	cmd := strings.ToLower(args[0])
	switch cmd {
	case "get":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: get <key>")
			os.Exit(1)
		}
		value, found, err := c.GetString(ctx, args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if !found {
			fmt.Println("(nil)")
		} else {
			fmt.Println(value)
		}

	case "put", "set":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: put <key> <value>")
			os.Exit(1)
		}
		err := c.PutString(ctx, args[1], args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "delete", "del":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: delete <key>")
			os.Exit(1)
		}
		err := c.DeleteString(ctx, args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "exists":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: exists <key>")
			os.Exit(1)
		}
		exists, err := c.Exists(ctx, []byte(args[1]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if exists {
			fmt.Println("1")
		} else {
			fmt.Println("0")
		}

	case "mget":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: mget <key1> [key2] ...")
			os.Exit(1)
		}
		keys := make([][]byte, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = []byte(args[i])
		}
		results, err := c.BatchGet(ctx, keys)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		for _, key := range args[1:] {
			if value, ok := results[key]; ok {
				fmt.Printf("%s: %s\n", key, string(value))
			} else {
				fmt.Printf("%s: (nil)\n", key)
			}
		}

	case "mset":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			fmt.Fprintln(os.Stderr, "Usage: mset <key1> <value1> [key2] [value2] ...")
			os.Exit(1)
		}
		items := make(map[string][]byte)
		for i := 1; i < len(args); i += 2 {
			items[args[i]] = []byte(args[i+1])
		}
		err := c.BatchPut(ctx, items)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "setnx":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: setnx <key> <value>")
			os.Exit(1)
		}
		set, err := c.SetNX(ctx, []byte(args[1]), []byte(args[2]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if set {
			fmt.Println("1")
		} else {
			fmt.Println("0")
		}

	case "ping":
		err := c.Ping(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("PONG")

	case "benchmark", "bench":
		runBenchmark(c, args[1:])

	case "locate":
		runLocate(args[1:])

	case "partitions":
		runPartitions(args[1:])

	case "rebalance":
		runRebalance(args[1:])

	case "migration-status", "mstatus":
		runMigrationStatus(args[1:])

	case "migration-cancel", "mcancel":
		runMigrationCancel(args[1:])

	case "cluster-info", "info":
		runClusterInfo(args[1:])

	case "init-cluster", "init":
		runInitCluster(args[1:])

	case "shutdown-node", "shutdown":
		runShutdownNode(args[1:])

	case "node-status", "nstatus":
		runNodeStatus(args[1:])

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("RocksKV CLI")
	fmt.Println()
	fmt.Println("Usage: rockskv-cli [options] <command> [arguments]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  -addr string    Compute node address (default \"localhost:8000\")")
	fmt.Println("  -timeout duration  Request timeout (default 5s)")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  get <key>                    Get value by key")
	fmt.Println("  put <key> <value>            Set key-value pair")
	fmt.Println("  delete <key>                 Delete key")
	fmt.Println("  exists <key>                 Check if key exists")
	fmt.Println("  mget <key1> [key2] ...       Get multiple keys")
	fmt.Println("  mset <k1> <v1> [k2] [v2] ... Set multiple key-value pairs")
	fmt.Println("  setnx <key> <value>          Set if not exists")
	fmt.Println("  ping                         Ping the server")
	fmt.Println("  benchmark [options]          Run benchmark")
	fmt.Println()
	fmt.Println("Cluster Commands:")
	fmt.Println("  cluster-info                 Show cluster state and info")
	fmt.Println("  init-cluster                 Initialize cluster partitions")
	fmt.Println("  locate <key>                 Show partition and nodes for a key")
	fmt.Println("  partitions [options]         Show partition distribution")
	fmt.Println()
	fmt.Println("Migration Commands:")
	fmt.Println("  rebalance [options]          Trigger cluster rebalance")
	fmt.Println("  migration-status             Show migration status")
	fmt.Println("  migration-cancel             Cancel ongoing migration")
	fmt.Println()
	fmt.Println("Node Lifecycle Commands:")
	fmt.Println("  shutdown-node <node_id>      Gracefully shutdown a storage node")
	fmt.Println("  node-status <node_id>        Show node status and partition counts")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  rockskv-cli put foo bar")
	fmt.Println("  rockskv-cli get foo")
	fmt.Println("  rockskv-cli -addr 10.0.0.1:8000 get foo")
	fmt.Println("  rockskv-cli rebalance -metadata localhost:9000")
}

func runBenchmark(c *client.Client, args []string) {
	// Parse benchmark options
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	n := fs.Int("n", 10000, "Number of operations")
	keySize := fs.Int("keysize", 16, "Key size in bytes")
	valueSize := fs.Int("valuesize", 64, "Value size in bytes")
	parallel := fs.Int("p", 10, "Number of parallel clients")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse benchmark flags: %v\n", err)
		return
	}

	fmt.Printf("Running benchmark: %d operations, %d parallel\n", *n, *parallel)
	fmt.Printf("Key size: %d bytes, Value size: %d bytes\n", *keySize, *valueSize)

	ctx := context.Background()

	// Generate test data
	key := make([]byte, *keySize)
	value := make([]byte, *valueSize)
	for i := range key {
		key[i] = byte('a' + i%26)
	}
	for i := range value {
		value[i] = byte('x')
	}

	// Benchmark PUT
	start := time.Now()
	for i := 0; i < *n; i++ {
		k := append(key, []byte(fmt.Sprintf("%d", i))...)
		if err := c.Put(ctx, k, value); err != nil {
			fmt.Printf("PUT error: %v\n", err)
		}
	}
	putDuration := time.Since(start)
	putQPS := float64(*n) / putDuration.Seconds()
	fmt.Printf("PUT: %d ops in %v (%.2f ops/sec)\n", *n, putDuration, putQPS)

	// Benchmark GET
	start = time.Now()
	for i := 0; i < *n; i++ {
		k := append(key, []byte(fmt.Sprintf("%d", i))...)
		if _, _, err := c.Get(ctx, k); err != nil {
			fmt.Printf("GET error: %v\n", err)
		}
	}
	getDuration := time.Since(start)
	getQPS := float64(*n) / getDuration.Seconds()
	fmt.Printf("GET: %d ops in %v (%.2f ops/sec)\n", *n, getDuration, getQPS)

	// Cleanup
	for i := 0; i < *n; i++ {
		k := append(key, []byte(fmt.Sprintf("%d", i))...)
		_ = c.Delete(ctx, k)
	}
}

func getMetadataClient(metadataAddr string) (pb.MetadataServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(metadataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to metadata service: %w", err)
	}
	return pb.NewMetadataServiceClient(conn), conn, nil
}

func runLocate(args []string) {
	fs := flag.NewFlagSet("locate", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: locate <key> [key2] ...")
		os.Exit(1)
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetRouteTable(ctx, &pb.GetRouteTableRequest{Version: 0})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Build partition map
	partitionMap := make(map[uint32]*pb.PartitionInfo)
	for _, p := range resp.RouteTable.Partitions {
		partitionMap[p.PartitionId] = p
	}

	fmt.Printf("%-20s %-12s %-20s %-20s %-10s\n", "Key", "Partition", "Primary", "Replica", "Status")
	fmt.Println(strings.Repeat("-", 85))

	for _, key := range fs.Args() {
		partitionID := uint32(xxhash.Sum64([]byte(key)) % TotalPartitions)
		partition := partitionMap[partitionID]

		if partition == nil {
			fmt.Printf("%-20s %-12d %-20s %-20s %-10s\n", key, partitionID, "(unknown)", "(unknown)", "N/A")
		} else {
			status := "normal"
			if partition.MigrationState != pb.MigrationState_MIGRATION_NONE {
				status = getMigrationStateString(partition.MigrationState)
			}
			fmt.Printf("%-20s %-12d %-20s %-20s %-10s\n",
				truncate(key, 20), partitionID, partition.Primary, partition.Replica, status)

			if partition.MigrationTarget != "" {
				fmt.Printf("%-20s %-12s -> %-20s\n", "", "(migrating)", partition.MigrationTarget)
			}
		}
	}
}

func runPartitions(args []string) {
	fs := flag.NewFlagSet("partitions", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	showAll := fs.Bool("all", false, "Show all partitions")
	node := fs.String("node", "", "Filter by node address")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetRouteTable(ctx, &pb.GetRouteTableRequest{Version: 0})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.RouteTable == nil || len(resp.RouteTable.Partitions) == 0 {
		fmt.Println("No partitions found. Cluster may not be initialized.")
		return
	}

	// Count partitions per node
	nodeStats := make(map[string]*nodePartitionStats)
	migratingCount := 0

	for _, p := range resp.RouteTable.Partitions {
		// Primary stats
		if _, ok := nodeStats[p.Primary]; !ok {
			nodeStats[p.Primary] = &nodePartitionStats{}
		}
		nodeStats[p.Primary].primaryCount++

		// Replica stats
		if _, ok := nodeStats[p.Replica]; !ok {
			nodeStats[p.Replica] = &nodePartitionStats{}
		}
		nodeStats[p.Replica].replicaCount++

		if p.MigrationState != pb.MigrationState_MIGRATION_NONE {
			migratingCount++
		}
	}

	fmt.Printf("Route Table Version: %d\n", resp.RouteTable.Version)
	fmt.Printf("Total Partitions: %d\n", len(resp.RouteTable.Partitions))
	fmt.Printf("Migrating: %d\n", migratingCount)
	fmt.Println()

	// Sort nodes
	var nodes []string
	for node := range nodeStats {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	fmt.Println("Node Distribution:")
	fmt.Printf("  %-25s %-15s %-15s %-15s\n", "Node", "Primary", "Replica", "Total")
	fmt.Println("  " + strings.Repeat("-", 70))

	for _, n := range nodes {
		stats := nodeStats[n]
		total := stats.primaryCount + stats.replicaCount
		fmt.Printf("  %-25s %-15d %-15d %-15d\n", n, stats.primaryCount, stats.replicaCount, total)
	}

	// Show individual partitions if requested
	if *showAll || *node != "" {
		fmt.Println()
		fmt.Println("Partition Details:")
		fmt.Printf("  %-12s %-25s %-25s %-15s\n", "Partition", "Primary", "Replica", "Status")
		fmt.Println("  " + strings.Repeat("-", 80))

		// Sort partitions
		var partitions []*pb.PartitionInfo
		for _, p := range resp.RouteTable.Partitions {
			partitions = append(partitions, p)
		}
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].PartitionId < partitions[j].PartitionId
		})

		count := 0
		for _, p := range partitions {
			if *node != "" && p.Primary != *node && p.Replica != *node {
				continue
			}

			status := "normal"
			if p.MigrationState != pb.MigrationState_MIGRATION_NONE {
				status = getMigrationStateString(p.MigrationState)
			}

			fmt.Printf("  %-12d %-25s %-25s %-15s\n",
				p.PartitionId, p.Primary, p.Replica, status)

			count++
			if !*showAll && count >= 20 {
				fmt.Printf("  ... showing first 20, use -all to show all\n")
				break
			}
		}
	}
}

type nodePartitionStats struct {
	primaryCount int
	replicaCount int
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func runRebalance(args []string) {
	fs := flag.NewFlagSet("rebalance", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	force := fs.Bool("force", false, "Force rebalance even if already balanced")
	bandwidth := fs.Int64("bandwidth", 0, "Bandwidth limit in bytes/second (0 for default)")
	concurrent := fs.Int("concurrent", 0, "Max concurrent partition migrations (0 for default)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.TriggerRebalance(ctx, &pb.TriggerRebalanceRequest{
		Force:         *force,
		BandwidthLimit: *bandwidth,
		MaxConcurrent:  int32(*concurrent),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Printf("Rebalance started: migration_id=%s, partitions=%d\n", resp.MigrationId, resp.PartitionsToMigrate)
		fmt.Println("Use 'migration-status' to monitor progress")
	} else {
		fmt.Printf("Rebalance failed: %s\n", resp.Message)
	}
}

func runMigrationStatus(args []string) {
	fs := flag.NewFlagSet("migration-status", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	watch := fs.Bool("watch", false, "Watch migration progress")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.GetMigrationStatus(ctx, &pb.GetMigrationStatusRequest{})
		cancel()

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if !resp.InProgress && resp.MigrationId == "" {
			fmt.Println("No migration in progress")
			return
		}

		// Clear screen if watching
		if *watch {
			fmt.Print("\033[H\033[2J")
		}

		fmt.Printf("Migration ID: %s\n", resp.MigrationId)
		fmt.Printf("Status: %s\n", getMigrationStatusString(resp))
		fmt.Printf("Progress: %.1f%%\n", resp.ProgressPercent)
		fmt.Printf("Partitions: %d/%d completed, %d failed\n",
			resp.CompletedPartitions, resp.TotalPartitions, resp.FailedPartitions)
		fmt.Printf("Data: %s / %s\n", formatBytes(resp.BytesCopied), formatBytes(resp.BytesTotal))

		if resp.EstimatedRemainingSeconds > 0 {
			fmt.Printf("ETA: %s\n", formatDuration(time.Duration(resp.EstimatedRemainingSeconds)*time.Second))
		}

		if len(resp.PartitionStatus) > 0 {
			fmt.Println("\nPartition Details:")
			fmt.Printf("  %-12s %-20s %-20s %-15s %-10s\n", "Partition", "Source", "Target", "State", "Progress")
			fmt.Println("  " + strings.Repeat("-", 80))

			// Show first 10 in-progress partitions
			count := 0
			for _, p := range resp.PartitionStatus {
				if p.State != pb.MigrationState_MIGRATION_COMPLETE && count < 10 {
					fmt.Printf("  %-12d %-20s %-20s %-15s %.1f%%\n",
						p.PartitionId, p.SourceNode, p.TargetNode,
						getMigrationStateString(p.State), p.ProgressPercent)
					count++
				}
			}
			if len(resp.PartitionStatus) > count {
				fmt.Printf("  ... and %d more\n", len(resp.PartitionStatus)-count)
			}
		}

		if !*watch || !resp.InProgress {
			break
		}

		time.Sleep(2 * time.Second)
	}
}

func runMigrationCancel(args []string) {
	fs := flag.NewFlagSet("migration-cancel", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CancelMigration(ctx, &pb.CancelMigrationRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Println("Migration cancelled")
	} else {
		fmt.Printf("Cancel failed: %s\n", resp.Message)
	}
}

func getMigrationStatusString(resp *pb.GetMigrationStatusResponse) string {
	if !resp.InProgress {
		if resp.FailedPartitions > 0 {
			return "completed_with_errors"
		}
		return "completed"
	}
	return "running"
}

func getMigrationStateString(state pb.MigrationState) string {
	switch state {
	case pb.MigrationState_MIGRATION_NONE:
		return "none"
	case pb.MigrationState_MIGRATION_PREPARING:
		return "preparing"
	case pb.MigrationState_MIGRATION_COPYING:
		return "copying"
	case pb.MigrationState_MIGRATION_CATCHUP:
		return "catchup"
	case pb.MigrationState_MIGRATION_SWITCHING:
		return "switching"
	case pb.MigrationState_MIGRATION_COMPLETE:
		return "complete"
	case pb.MigrationState_MIGRATION_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func runClusterInfo(args []string) {
	fs := flag.NewFlagSet("cluster-info", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetClusterInfo(ctx, &pb.GetClusterInfoRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Cluster Information:")
	fmt.Printf("  State:          %s\n", getClusterStateString(resp.State))
	fmt.Printf("  Replica Count:  %d\n", resp.ReplicaCount)
	fmt.Printf("  Storage Nodes:  %d\n", resp.StorageNodeCount)
	fmt.Printf("  Partitions:     %d\n", resp.PartitionCount)
}

func runInitCluster(args []string) {
	fs := flag.NewFlagSet("init-cluster", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("Initializing cluster...")

	resp, err := client.InitCluster(ctx, &pb.InitClusterRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Printf("Cluster initialized successfully!\n")
		fmt.Printf("  Partitions: %d\n", resp.PartitionCount)
		fmt.Printf("  Nodes:      %d\n", resp.NodeCount)
	} else {
		fmt.Fprintf(os.Stderr, "Initialization failed: %s\n", resp.Message)
		os.Exit(1)
	}
}

func getClusterStateString(state pb.ClusterState) string {
	switch state {
	case pb.ClusterState_CLUSTER_PENDING:
		return "PENDING (waiting for init-cluster)"
	case pb.ClusterState_CLUSTER_INITIALIZING:
		return "INITIALIZING"
	case pb.ClusterState_CLUSTER_RUNNING:
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}

func runShutdownNode(args []string) {
	fs := flag.NewFlagSet("shutdown-node", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	force := fs.Bool("force", false, "Force shutdown without migration")
	timeout := fs.Int64("timeout", 0, "Migration timeout in milliseconds (0 for default 5 minutes)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: shutdown-node <node_id>")
		os.Exit(1)
	}

	nodeID := fs.Arg(0)

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Initiating controlled shutdown for node: %s\n", nodeID)
	if *force {
		fmt.Println("WARNING: Force mode enabled - partitions will not be migrated!")
	}

	// Use a longer timeout for shutdown operations
	timeoutDuration := 10 * time.Minute
	if *timeout > 0 {
		timeoutDuration = time.Duration(*timeout) * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	resp, err := client.ShutdownNode(ctx, &pb.ShutdownNodeRequest{
		NodeId:             nodeID,
		Force:              *force,
		MigrationTimeoutMs: *timeout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Printf("Node shutdown completed successfully!\n")
		fmt.Printf("  Partitions migrated: %d\n", resp.PartitionsMigrated)
		fmt.Printf("  Partitions failed:   %d\n", resp.PartitionsFailed)
	} else {
		fmt.Fprintf(os.Stderr, "Shutdown failed: %s\n", resp.Message)
		fmt.Printf("  Partitions migrated: %d\n", resp.PartitionsMigrated)
		fmt.Printf("  Partitions failed:   %d\n", resp.PartitionsFailed)
		os.Exit(1)
	}
}

func runNodeStatus(args []string) {
	fs := flag.NewFlagSet("node-status", flag.ExitOnError)
	metadataAddr := fs.String("metadata", "localhost:9000", "Metadata service address")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		return
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: node-status <node_id>")
		os.Exit(1)
	}

	nodeID := fs.Arg(0)

	client, conn, err := getMetadataClient(*metadataAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetNodeStatus(ctx, &pb.GetNodeStatusRequest{
		NodeId: nodeID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node Status: %s\n", nodeID)
	fmt.Printf("  Address:            %s\n", resp.Addr)
	fmt.Printf("  Status:             %s\n", getNodeStatusString(resp.Status))
	fmt.Printf("  Primary Partitions: %d\n", resp.PrimaryPartitionCount)
	fmt.Printf("  Replica Partitions: %d\n", resp.ReplicaPartitionCount)
	fmt.Printf("  Total Partitions:   %d\n", resp.PrimaryPartitionCount+resp.ReplicaPartitionCount)

	if resp.LastHeartbeat > 0 {
		lastHB := time.Unix(0, resp.LastHeartbeat)
		fmt.Printf("  Last Heartbeat:     %s (%s ago)\n",
			lastHB.Format("2006-01-02 15:04:05"),
			time.Since(lastHB).Truncate(time.Second))
	}

	if resp.IsDraining {
		fmt.Printf("  Draining:           YES\n")
		fmt.Printf("  Remaining:          %d partitions\n", resp.DrainingPartitionsRemaining)
	}
}

func getNodeStatusString(status pb.NodeStatus) string {
	switch status {
	case pb.NodeStatus_NODE_ONLINE:
		return "ONLINE"
	case pb.NodeStatus_NODE_OFFLINE:
		return "OFFLINE"
	case pb.NodeStatus_NODE_DRAINING:
		return "DRAINING"
	case pb.NodeStatus_NODE_REMOVED:
		return "REMOVED"
	default:
		return "UNKNOWN"
	}
}
