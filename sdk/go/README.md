# RocksKV Go SDK

A high-performance Go client for RocksKV distributed key-value store.

## Installation

```bash
go get github.com/winewei/rockskv/sdk/go/rockskv
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/winewei/rockskv/sdk/go/rockskv"
)

func main() {
    // Create client
    client, err := rockskv.NewClient(
        rockskv.WithAddrs("localhost:8000", "localhost:8001"),
        rockskv.WithTimeout(5 * time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Put
    if err := client.Put(ctx, "hello", "world"); err != nil {
        log.Fatal(err)
    }

    // Get
    value, err := client.Get(ctx, "hello")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("hello = %s\n", value)

    // Delete
    if err := client.Delete(ctx, "hello"); err != nil {
        log.Fatal(err)
    }
}
```

## Features

- Connection pooling with automatic reconnection
- Load balancing across multiple compute nodes
- Retry with exponential backoff
- Context-based timeout and cancellation
- Batch operations (MGet, MSet)
- Thread-safe

## Configuration Options

```go
client, err := rockskv.NewClient(
    rockskv.WithAddrs("host1:8000", "host2:8001"),  // Compute node addresses
    rockskv.WithTimeout(5 * time.Second),            // Request timeout
    rockskv.WithPoolSize(10),                        // Connection pool size
    rockskv.WithRetryCount(3),                       // Max retry attempts
    rockskv.WithRetryDelay(100 * time.Millisecond),  // Initial retry delay
)
```

## Batch Operations

```go
// Batch Put
items := map[string]string{
    "key1": "value1",
    "key2": "value2",
    "key3": "value3",
}
count, err := client.MSet(ctx, items)

// Batch Get
keys := []string{"key1", "key2", "key3"}
results, err := client.MGet(ctx, keys)
for key, value := range results {
    fmt.Printf("%s = %s\n", key, value)
}
```

## Error Handling

```go
value, err := client.Get(ctx, "key")
if err != nil {
    if errors.Is(err, rockskv.ErrKeyNotFound) {
        fmt.Println("Key not found")
    } else if errors.Is(err, rockskv.ErrTimeout) {
        fmt.Println("Request timed out")
    } else {
        fmt.Printf("Error: %v\n", err)
    }
}
```
