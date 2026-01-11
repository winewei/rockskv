/*
 * Basic usage example for RocksKV Java SDK.
 *
 * Before running:
 * 1. Generate proto files using protoc
 * 2. Compile the SDK
 * 3. Start RocksKV cluster
 */
package com.rockskv.examples;

import com.rockskv.RocksKVClient;
import com.rockskv.ClientConfig;
import com.google.gson.Gson;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class BasicUsage {
    public static void main(String[] args) {
        // Create client with configuration
        ClientConfig config = ClientConfig.builder()
            .timeout(Duration.ofSeconds(5))
            .retryCount(3)
            .poolSize(10)
            .build();

        try (RocksKVClient client = RocksKVClient.builder()
                .addAddress("localhost:8000")
                .addAddress("localhost:8001")
                .config(config)
                .build()) {

            System.out.println("Connected to RocksKV cluster");

            // ========== Basic Operations ==========
            System.out.println("\n--- Basic Operations ---");

            // Put
            client.put("greeting", "Hello, RocksKV!");
            System.out.println("PUT greeting = 'Hello, RocksKV!'");

            // Get
            Optional<String> value = client.get("greeting");
            value.ifPresent(v -> System.out.println("GET greeting = " + v));

            // Delete
            client.delete("greeting");
            System.out.println("DELETE greeting");

            // Get after delete
            value = client.get("greeting");
            System.out.println("GET greeting (after delete) = " +
                (value.isPresent() ? value.get() : "(nil)"));

            // ========== JSON Data ==========
            System.out.println("\n--- JSON Data ---");

            User user = new User(1, "Alice", "alice@example.com", 30);
            Gson gson = new Gson();

            // Store JSON
            String userJson = gson.toJson(user);
            client.put("user:1", userJson);
            System.out.println("PUT user:1 = " + userJson);

            // Retrieve JSON
            value = client.get("user:1");
            if (value.isPresent()) {
                User retrievedUser = gson.fromJson(value.get(), User.class);
                System.out.println("GET user:1 = " + retrievedUser);
            }

            // ========== Batch Operations ==========
            System.out.println("\n--- Batch Operations ---");

            // MSet
            Map<String, String> items = new HashMap<>();
            items.put("key1", "value1");
            items.put("key2", "value2");
            items.put("key3", "value3");
            items.put("key4", "value4");
            items.put("key5", "value5");

            int count = client.mset(items);
            System.out.println("MSET " + items.size() + " keys, stored " + count);

            // MGet
            List<String> keys = Arrays.asList("key1", "key2", "key3", "key4", "key5", "nonexistent");
            Map<String, String> results = client.mget(keys);
            System.out.println("MGET " + keys.size() + " keys:");
            for (String key : keys) {
                String v = results.get(key);
                System.out.println("  " + key + " = " + (v != null ? v : "(nil)"));
            }

            // ========== Async Operations ==========
            System.out.println("\n--- Async Operations ---");

            // Write 100 keys concurrently
            List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                final int idx = i;
                writeFutures.add(client.putAsync("async_key_" + idx, "value_" + idx));
            }
            CompletableFuture.allOf(writeFutures.toArray(new CompletableFuture[0])).join();
            System.out.println("Written 100 keys concurrently");

            // Read all keys concurrently
            List<CompletableFuture<Optional<String>>> readFutures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                readFutures.add(client.getAsync("async_key_" + i));
            }
            CompletableFuture.allOf(readFutures.toArray(new CompletableFuture[0])).join();

            long successCount = readFutures.stream()
                .map(CompletableFuture::join)
                .filter(Optional::isPresent)
                .count();
            System.out.println("Read 100 keys, verified " + successCount);

            // ========== Cleanup ==========
            System.out.println("\n--- Cleanup ---");
            for (String key : items.keySet()) {
                client.delete(key);
            }
            client.delete("user:1");
            for (int i = 0; i < 100; i++) {
                client.delete("async_key_" + i);
            }
            System.out.println("Cleaned up test keys");

            System.out.println("\nDone!");
        }
    }

    // User model
    static class User {
        int id;
        String name;
        String email;
        int age;

        User(int id, String name, String email, int age) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{id=" + id + ", name='" + name + "', email='" + email + "', age=" + age + "}";
        }
    }
}
