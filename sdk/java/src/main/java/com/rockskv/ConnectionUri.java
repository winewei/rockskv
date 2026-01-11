package com.rockskv;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for RocksKV connection URIs.
 *
 * <p>URI format: {@code rockskv://host1:port1,host2:port2[?options]}
 *
 * <p>Supported options:
 * <ul>
 *   <li>timeout: request timeout in milliseconds (e.g., "5000")</li>
 *   <li>retryCount: max retry attempts (e.g., "3")</li>
 *   <li>retryDelay: initial retry delay in milliseconds (e.g., "100")</li>
 *   <li>poolSize: connection pool size (e.g., "10")</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 * rockskv://localhost:8000
 * rockskv://localhost:8000,localhost:8001
 * rockskv://node1:8000,node2:8000?timeout=5000&amp;retryCount=3
 * </pre>
 */
public class ConnectionUri {
    private final List<String> addresses;
    private final ClientConfig config;

    private ConnectionUri(List<String> addresses, ClientConfig config) {
        this.addresses = addresses;
        this.config = config;
    }

    /**
     * Parse a RocksKV connection URI.
     *
     * @param uri the connection URI string
     * @return ConnectionUri containing parsed addresses and config
     * @throws IllegalArgumentException if the URI is invalid
     */
    public static ConnectionUri parse(String uri) {
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException("URI cannot be null or empty");
        }

        // Handle rockskv:// scheme
        String normalizedUri = uri;
        if (uri.startsWith("rockskv://")) {
            normalizedUri = "http://" + uri.substring("rockskv://".length());
        } else if (!uri.contains("://")) {
            // Plain host:port format
            normalizedUri = "http://" + uri;
        }

        URI parsed;
        try {
            parsed = new URI(normalizedUri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI: " + uri, e);
        }

        // Parse hosts
        String host = parsed.getHost();
        int port = parsed.getPort();
        String authority = parsed.getAuthority();

        List<String> addresses = new ArrayList<>();

        // Handle comma-separated hosts in authority
        if (authority != null && authority.contains(",")) {
            String[] hosts = authority.split(",");
            for (String h : hosts) {
                h = h.trim();
                if (!h.isEmpty()) {
                    addresses.add(h);
                }
            }
        } else if (host != null) {
            if (port > 0) {
                addresses.add(host + ":" + port);
            } else {
                addresses.add(host);
            }
        }

        if (addresses.isEmpty()) {
            throw new IllegalArgumentException("No hosts specified in URI");
        }

        // Parse query options
        ClientConfig.Builder configBuilder = ClientConfig.builder();
        String query = parsed.getQuery();

        if (query != null && !query.isEmpty()) {
            String[] params = query.split("&");
            for (String param : params) {
                String[] kv = param.split("=", 2);
                if (kv.length != 2) continue;

                String key = kv[0].trim();
                String value = kv[1].trim();

                switch (key) {
                    case "timeout":
                        configBuilder.timeout(Duration.ofMillis(Long.parseLong(value)));
                        break;
                    case "retryCount":
                        configBuilder.retryCount(Integer.parseInt(value));
                        break;
                    case "retryDelay":
                        configBuilder.retryDelay(Duration.ofMillis(Long.parseLong(value)));
                        break;
                    case "maxRetryDelay":
                        configBuilder.maxRetryDelay(Duration.ofMillis(Long.parseLong(value)));
                        break;
                    case "poolSize":
                        configBuilder.poolSize(Integer.parseInt(value));
                        break;
                }
            }
        }

        return new ConnectionUri(addresses, configBuilder.build());
    }

    /**
     * Create a RocksKV client from this connection URI.
     *
     * @return RocksKVClient instance
     */
    public RocksKVClient createClient() {
        RocksKVClient.Builder builder = RocksKVClient.builder().config(config);
        for (String addr : addresses) {
            builder.addAddress(addr);
        }
        return builder.build();
    }

    /**
     * Create a RocksKV client directly from a connection URI string.
     *
     * @param uri the connection URI string
     * @return RocksKVClient instance
     */
    public static RocksKVClient createClient(String uri) {
        return parse(uri).createClient();
    }

    /**
     * Get the parsed addresses.
     *
     * @return list of server addresses
     */
    public List<String> getAddresses() {
        return new ArrayList<>(addresses);
    }

    /**
     * Get the parsed config.
     *
     * @return client configuration
     */
    public ClientConfig getConfig() {
        return config;
    }
}
