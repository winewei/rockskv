# RocksKV Java SDK

A high-performance Java client for RocksKV distributed key-value store.

## Installation

### Maven

```xml
<dependency>
    <groupId>com.rockskv</groupId>
    <artifactId>rockskv-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.rockskv:rockskv-client:0.1.0'
```

## Quick Start

```java
import com.rockskv.RocksKVClient;
import com.rockskv.ClientConfig;

public class Example {
    public static void main(String[] args) {
        // Create client
        RocksKVClient client = RocksKVClient.builder()
            .addAddress("localhost:8000")
            .addAddress("localhost:8001")
            .timeout(Duration.ofSeconds(5))
            .build();

        try {
            // Put
            client.put("hello", "world");

            // Get
            Optional<String> value = client.get("hello");
            value.ifPresent(v -> System.out.println("hello = " + v));

            // Delete
            client.delete("hello");
        } finally {
            client.close();
        }
    }
}
```

## Features

- Connection pooling with automatic reconnection
- Load balancing across multiple compute nodes
- Retry with exponential backoff
- Async operations with CompletableFuture
- Batch operations (mget/mset)
- Thread-safe

## Configuration

```java
ClientConfig config = ClientConfig.builder()
    .timeout(Duration.ofSeconds(5))       // Request timeout
    .poolSize(10)                          // Connection pool size
    .retryCount(3)                         // Max retry attempts
    .retryDelay(Duration.ofMillis(100))   // Initial retry delay
    .build();

RocksKVClient client = RocksKVClient.builder()
    .addAddress("localhost:8000")
    .config(config)
    .build();
```

## Async Operations

```java
import java.util.concurrent.CompletableFuture;

// Async put
CompletableFuture<Void> putFuture = client.putAsync("key", "value");

// Async get
CompletableFuture<Optional<String>> getFuture = client.getAsync("key");
getFuture.thenAccept(value -> {
    value.ifPresent(v -> System.out.println("Value: " + v));
});

// Wait for all operations
CompletableFuture.allOf(putFuture, getFuture).join();
```

## Batch Operations

```java
// Batch put
Map<String, String> items = Map.of(
    "key1", "value1",
    "key2", "value2",
    "key3", "value3"
);
int count = client.mset(items);

// Batch get
List<String> keys = List.of("key1", "key2", "key3");
Map<String, String> results = client.mget(keys);
```

## Error Handling

```java
try {
    String value = client.get("key").orElseThrow(() ->
        new KeyNotFoundException("Key not found"));
} catch (KeyNotFoundException e) {
    System.out.println("Key not found");
} catch (TimeoutException e) {
    System.out.println("Request timed out");
} catch (RocksKVException e) {
    System.out.println("Error: " + e.getMessage());
}
```

## Building from Source

```bash
# Build with Maven (generates proto files automatically)
cd sdk/java
mvn clean compile

# Run the example
mvn exec:java -Dexec.mainClass="com.rockskv.examples.BasicUsage"
```

## Generate Proto Files Manually

If you prefer to generate proto files manually:

```bash
# Using protoc (requires protoc and grpc-java plugin)
protoc -I../../proto \
    --java_out=src/main/java \
    --grpc-java_out=src/main/java \
    ../../proto/rockskv.proto
```

## Project Structure

```
sdk/java/
├── pom.xml                                    # Maven configuration
└── src/main/java/com/rockskv/
    ├── ClientConfig.java                      # Client configuration
    ├── RocksKVClient.java                     # Main client implementation
    └── examples/
        └── BasicUsage.java                    # Usage example
```
