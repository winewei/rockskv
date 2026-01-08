package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/example/rockskv/pkg/client"
)

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
	defer c.Close()

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
	fmt.Println("Examples:")
	fmt.Println("  rockskv-cli put foo bar")
	fmt.Println("  rockskv-cli get foo")
	fmt.Println("  rockskv-cli -addr 10.0.0.1:8000 get foo")
}

func runBenchmark(c *client.Client, args []string) {
	// Parse benchmark options
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	n := fs.Int("n", 10000, "Number of operations")
	keySize := fs.Int("keysize", 16, "Key size in bytes")
	valueSize := fs.Int("valuesize", 64, "Value size in bytes")
	parallel := fs.Int("p", 10, "Number of parallel clients")
	fs.Parse(args)

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
		c.Delete(ctx, k)
	}
}
