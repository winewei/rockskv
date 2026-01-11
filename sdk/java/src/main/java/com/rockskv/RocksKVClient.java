package com.rockskv;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import rockskv.Rockskv.*;
import rockskv.KVServiceGrpc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RocksKV client for Java.
 */
public class RocksKVClient implements AutoCloseable {
    private final List<String> addresses;
    private final ClientConfig config;
    private final List<ManagedChannel> channels;
    private final List<KVServiceGrpc.KVServiceBlockingStub> stubs;
    private final List<KVServiceGrpc.KVServiceFutureStub> futureStubs;
    private final AtomicInteger roundRobin;
    private final Random random;
    private volatile boolean closed;

    private RocksKVClient(List<String> addresses, ClientConfig config) {
        this.addresses = new ArrayList<>(addresses);
        this.config = config;
        this.channels = new ArrayList<>();
        this.stubs = new ArrayList<>();
        this.futureStubs = new ArrayList<>();
        this.roundRobin = new AtomicInteger(0);
        this.random = new Random();
        this.closed = false;

        for (String addr : addresses) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(addr)
                    .usePlaintext()
                    .build();
            channels.add(channel);
            stubs.add(KVServiceGrpc.newBlockingStub(channel));
            futureStubs.add(KVServiceGrpc.newFutureStub(channel));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private KVServiceGrpc.KVServiceBlockingStub getStub() {
        int idx = random.nextInt(stubs.size());
        return stubs.get(idx);
    }

    private KVServiceGrpc.KVServiceFutureStub getFutureStub() {
        int idx = random.nextInt(futureStubs.size());
        return futureStubs.get(idx);
    }

    /**
     * Get a value by key.
     */
    public Optional<String> get(String key) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                .build();

        int retries = config.getRetryCount();
        long delay = config.getRetryDelay().toMillis();
        Exception lastError = null;

        for (int i = 0; i <= retries; i++) {
            try {
                GetResponse response = getStub().get(request);
                if (response.getFound()) {
                    return Optional.of(response.getValue().toStringUtf8());
                }
                return Optional.empty();
            } catch (StatusRuntimeException e) {
                lastError = e;
                if (i < retries) {
                    try {
                        Thread.sleep(delay);
                        delay = Math.min(delay * 2, config.getMaxRetryDelay().toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        }
        throw new RuntimeException("Get failed after retries", lastError);
    }

    /**
     * Put a key-value pair.
     */
    public void put(String key, String value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                .setValue(com.google.protobuf.ByteString.copyFromUtf8(value))
                .build();

        int retries = config.getRetryCount();
        long delay = config.getRetryDelay().toMillis();
        Exception lastError = null;

        for (int i = 0; i <= retries; i++) {
            try {
                getStub().put(request);
                return;
            } catch (StatusRuntimeException e) {
                lastError = e;
                if (i < retries) {
                    try {
                        Thread.sleep(delay);
                        delay = Math.min(delay * 2, config.getMaxRetryDelay().toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        }
        throw new RuntimeException("Put failed after retries", lastError);
    }

    /**
     * Delete a key.
     */
    public void delete(String key) {
        DeleteRequest request = DeleteRequest.newBuilder()
                .setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                .build();

        int retries = config.getRetryCount();
        long delay = config.getRetryDelay().toMillis();
        Exception lastError = null;

        for (int i = 0; i <= retries; i++) {
            try {
                getStub().delete(request);
                return;
            } catch (StatusRuntimeException e) {
                lastError = e;
                if (i < retries) {
                    try {
                        Thread.sleep(delay);
                        delay = Math.min(delay * 2, config.getMaxRetryDelay().toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        }
        throw new RuntimeException("Delete failed after retries", lastError);
    }

    /**
     * Get multiple values.
     */
    public Map<String, String> mget(List<String> keys) {
        BatchGetRequest.Builder builder = BatchGetRequest.newBuilder();
        for (String key : keys) {
            builder.addKeys(com.google.protobuf.ByteString.copyFromUtf8(key));
        }

        BatchGetResponse response = getStub().batchGet(builder.build());
        Map<String, String> result = new HashMap<>();
        for (KeyValueResult item : response.getItemsList()) {
            if (item.getFound()) {
                result.put(item.getKey().toStringUtf8(), item.getValue().toStringUtf8());
            }
        }
        return result;
    }

    /**
     * Set multiple key-value pairs.
     */
    public int mset(Map<String, String> items) {
        BatchPutRequest.Builder builder = BatchPutRequest.newBuilder();
        for (Map.Entry<String, String> entry : items.entrySet()) {
            builder.addItems(KeyValue.newBuilder()
                    .setKey(com.google.protobuf.ByteString.copyFromUtf8(entry.getKey()))
                    .setValue(com.google.protobuf.ByteString.copyFromUtf8(entry.getValue()))
                    .build());
        }

        BatchPutResponse response = getStub().batchPut(builder.build());
        return (int) response.getCount();
    }

    /**
     * Async put operation.
     */
    public CompletableFuture<Void> putAsync(String key, String value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                .setValue(com.google.protobuf.ByteString.copyFromUtf8(value))
                .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        com.google.common.util.concurrent.ListenableFuture<PutResponse> grpcFuture =
                getFutureStub().put(request);

        com.google.common.util.concurrent.Futures.addCallback(
                grpcFuture,
                new com.google.common.util.concurrent.FutureCallback<PutResponse>() {
                    @Override
                    public void onSuccess(PutResponse result) {
                        future.complete(null);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        future.completeExceptionally(t);
                    }
                },
                java.util.concurrent.Executors.newSingleThreadExecutor()
        );

        return future;
    }

    /**
     * Async get operation.
     */
    public CompletableFuture<Optional<String>> getAsync(String key) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                .build();

        CompletableFuture<Optional<String>> future = new CompletableFuture<>();
        com.google.common.util.concurrent.ListenableFuture<GetResponse> grpcFuture =
                getFutureStub().get(request);

        com.google.common.util.concurrent.Futures.addCallback(
                grpcFuture,
                new com.google.common.util.concurrent.FutureCallback<GetResponse>() {
                    @Override
                    public void onSuccess(GetResponse result) {
                        if (result.getFound()) {
                            future.complete(Optional.of(result.getValue().toStringUtf8()));
                        } else {
                            future.complete(Optional.empty());
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        future.completeExceptionally(t);
                    }
                },
                java.util.concurrent.Executors.newSingleThreadExecutor()
        );

        return future;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (ManagedChannel channel : channels) {
            channel.shutdown();
        }
    }

    public static class Builder {
        private final List<String> addresses = new ArrayList<>();
        private ClientConfig config = ClientConfig.builder().build();

        public Builder addAddress(String address) {
            addresses.add(address);
            return this;
        }

        public Builder config(ClientConfig config) {
            this.config = config;
            return this;
        }

        public RocksKVClient build() {
            if (addresses.isEmpty()) {
                throw new IllegalArgumentException("At least one address is required");
            }
            return new RocksKVClient(addresses, config);
        }
    }
}
