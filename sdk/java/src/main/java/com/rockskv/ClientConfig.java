package com.rockskv;

import java.time.Duration;

/**
 * Configuration for RocksKV client.
 */
public class ClientConfig {
    private Duration timeout = Duration.ofSeconds(5);
    private int poolSize = 10;
    private int retryCount = 3;
    private Duration retryDelay = Duration.ofMillis(100);
    private Duration maxRetryDelay = Duration.ofSeconds(2);

    private ClientConfig() {}

    public static Builder builder() {
        return new Builder();
    }

    public Duration getTimeout() {
        return timeout;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public Duration getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public static class Builder {
        private final ClientConfig config = new ClientConfig();

        public Builder timeout(Duration timeout) {
            config.timeout = timeout;
            return this;
        }

        public Builder poolSize(int poolSize) {
            config.poolSize = poolSize;
            return this;
        }

        public Builder retryCount(int retryCount) {
            config.retryCount = retryCount;
            return this;
        }

        public Builder retryDelay(Duration retryDelay) {
            config.retryDelay = retryDelay;
            return this;
        }

        public Builder maxRetryDelay(Duration maxRetryDelay) {
            config.maxRetryDelay = maxRetryDelay;
            return this;
        }

        public ClientConfig build() {
            return config;
        }
    }
}
