// Example: Basic usage of RocksKV Go SDK
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/winewei/rockskv/sdk/go/rockskv"
)

func main() {
	// Create client with options
	client, err := rockskv.NewClient(
		rockskv.WithAddrs("localhost:8000", "localhost:8001"),
		rockskv.WithTimeout(5*time.Second),
		rockskv.WithRetryCount(3),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// ========== Basic Operations ==========
	fmt.Println("--- Basic Operations ---")

	// Put
	if err := client.Put(ctx, "greeting", "Hello, RocksKV!"); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Println("PUT greeting = 'Hello, RocksKV!'")

	// Get
	value, err := client.Get(ctx, "greeting")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("GET greeting = %s\n", value)

	// Delete
	if err := client.Delete(ctx, "greeting"); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Println("DELETE greeting")

	// Get after delete
	_, err = client.Get(ctx, "greeting")
	if err == rockskv.ErrKeyNotFound {
		fmt.Println("GET greeting (after delete) = (nil)")
	}

	// ========== JSON Data ==========
	fmt.Println("\n--- JSON Data ---")

	type User struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Email string `json:"email"`
		Age   int    `json:"age"`
	}

	user := User{
		ID:    1,
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}

	// Store JSON
	userJSON, _ := json.Marshal(user)
	if err := client.Put(ctx, "user:1", string(userJSON)); err != nil {
		log.Fatalf("Put JSON failed: %v", err)
	}
	fmt.Printf("PUT user:1 = %s\n", string(userJSON))

	// Retrieve JSON
	value, err = client.Get(ctx, "user:1")
	if err != nil {
		log.Fatalf("Get JSON failed: %v", err)
	}

	var retrievedUser User
	if err := json.Unmarshal([]byte(value), &retrievedUser); err != nil {
		log.Fatalf("Unmarshal failed: %v", err)
	}
	fmt.Printf("GET user:1 = %+v\n", retrievedUser)

	// ========== Batch Operations ==========
	fmt.Println("\n--- Batch Operations ---")

	// MSet
	items := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	count, err := client.MSet(ctx, items)
	if err != nil {
		log.Fatalf("MSet failed: %v", err)
	}
	fmt.Printf("MSET %d keys, stored %d\n", len(items), count)

	// MGet
	keys := []string{"key1", "key2", "key3", "key4", "key5", "nonexistent"}
	results, err := client.MGet(ctx, keys)
	if err != nil {
		log.Fatalf("MGet failed: %v", err)
	}
	fmt.Printf("MGET %d keys:\n", len(keys))
	for _, k := range keys {
		if v, ok := results[k]; ok {
			fmt.Printf("  %s = %s\n", k, v)
		} else {
			fmt.Printf("  %s = (nil)\n", k)
		}
	}

	// ========== Cleanup ==========
	fmt.Println("\n--- Cleanup ---")
	for key := range items {
		_ = client.Delete(ctx, key)
	}
	_ = client.Delete(ctx, "user:1")
	fmt.Println("Cleaned up test keys")

	fmt.Println("\nDone!")
}
