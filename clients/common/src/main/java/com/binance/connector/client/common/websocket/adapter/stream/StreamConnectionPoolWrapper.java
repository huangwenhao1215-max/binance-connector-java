package com.binance.connector.client.common.websocket.adapter.stream;

import com.binance.connector.client.common.SystemUtil;
import com.binance.connector.client.common.websocket.adapter.ConnectionWrapper;
import com.binance.connector.client.common.websocket.configuration.WebSocketClientConfiguration;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueue;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueueWrapper;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class StreamConnectionPoolWrapper implements StreamConnectionInterface {

    // All stream operations are synchronized: the shared round-robin ListIterator
    // `pool` and the `connectionStreamMap` HashMap were previously mutated by
    // concurrent caller threads (subscribe/unsubscribe from many consumers),
    // corrupting iterator state (NoSuchElementException / NPE / IndexOutOfBounds
    // thrown into callers). These operations are low-frequency, so lock
    // contention is a non-issue.
    private static final long WAIT_FOR_READY_TIMEOUT_MS = 30_000;

    private final LinkedList<StreamConnectionWrapper> connectionList = new LinkedList<>();
    private ListIterator<StreamConnectionWrapper> pool;
    private Boolean isConnected = false;
    private String userAgent =
            String.format(
                    "binance-connector-java/1.0.0 (Java/%s; %s; %s)",
                    SystemUtil.getJavaVersion(), SystemUtil.getOs(), SystemUtil.getArch());

    private final Map<String, StreamConnectionWrapper> connectionStreamMap = new HashMap<>();

    public StreamConnectionPoolWrapper(WebSocketClientConfiguration clientConfiguration) {
        this(clientConfiguration, null);
    }

    public StreamConnectionPoolWrapper(
            WebSocketClientConfiguration clientConfiguration, Gson gson) {
        Integer poolSize = clientConfiguration.getPoolSize();
        Integer batchSize = clientConfiguration.getReconnectBatchSize();
        Integer reconnectAfter = clientConfiguration.getReconnectIntervalTime();
        for (int i = 0; i < poolSize; i++) {
            StreamConnectionWrapper connectionWrapper =
                    new StreamConnectionWrapper(clientConfiguration, gson);
            connectionList.add(connectionWrapper);
        }

        // Scheduler to reconnect by batch every 23h (configurable)
        new Timer()
                .scheduleAtFixedRate(
                        new TimerTask() {
                            @Override
                            public void run() {
                                try {
                                    for (int i = 0; i < batchSize; i++) {
                                        int currentBatchSize = (poolSize / batchSize);
                                        if (i == batchSize - 1) {
                                            currentBatchSize += poolSize % batchSize;
                                        }

                                        for (int j = 0; j < currentBatchSize; j++) {
                                            int connectionIndex = ((poolSize / batchSize) * i) + j;
                                            ConnectionWrapper connectionWrapper =
                                                    connectionList.get(connectionIndex);
                                            if (connectionWrapper.canReconnect()) {
                                                connectionWrapper.connect();
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        },
                        reconnectAfter,
                        reconnectAfter);

        pool = connectionList.listIterator();
    }

    @Override
    public void connect() {
        for (ConnectionWrapper connectionWrapper : connectionList) {
            connectionWrapper.setUserAgent(userAgent);
            connectionWrapper.connect();
        }
        isConnected = true;
    }

    @Override
    public void disconnect() {
        for (ConnectionWrapper connectionWrapper : connectionList) {
            connectionWrapper.disconnect();
        }
        isConnected = false;
    }

    public synchronized Map<String, StreamBlockingQueue<String>> subscribe(
            RequestWrapperDTO<Set<String>, Object> requestWrapperDTO) {
        Set<String> params = requestWrapperDTO.getParams();
        Map<String, StreamBlockingQueue<String>> queueMap = new HashMap<>();
        for (String identifier : params) {
            StreamConnectionWrapper connection = getConnection(identifier);
            queueMap.putAll(connection.subscribe(requestWrapperDTO));
        }
        return queueMap;
    }

    @Override
    public void unsubscribe(StreamBlockingQueueWrapper queue) {
        unsubscribe(queue.getInnerQueue());
    }

    @Override
    public synchronized void unsubscribe(StreamBlockingQueue queue) {
        StreamConnectionWrapper connection = getConnection(queue.getOperationId());
        connection.unsubscribe(queue);
        connectionStreamMap.remove(queue.getOperationId());
    }

    public synchronized StreamConnectionWrapper getConnection(String identifier) {
        if (connectionStreamMap.containsKey(identifier)) {
            return connectionStreamMap.get(identifier);
        }

        StreamConnectionWrapper connection = getConnection();
        connectionStreamMap.put(identifier, connection);

        return connection;
    }

    /**
     * @return the next connection from the pool, using round-robin.
     *     If every connection is currently down (e.g. mid-reconnect after a close),
     *     waits up to {@link #WAIT_FOR_READY_TIMEOUT_MS} for one to become ready
     *     instead of spinning forever like the original implementation, then
     *     throws IllegalStateException so callers can retry on their own schedule.
     */
    public synchronized StreamConnectionWrapper getConnection() {
        long deadline = System.currentTimeMillis() + WAIT_FOR_READY_TIMEOUT_MS;
        while (true) {
            if (!pool.hasNext()) {
                pool = connectionList.listIterator();
            }
            StreamConnectionWrapper connection = pool.next();
            int probed = 1;
            while (!connection.isReady()) {
                if (probed >= connectionList.size()) {
                    break; // every connection probed, none ready
                }
                if (!pool.hasNext()) {
                    pool = connectionList.listIterator();
                } else {
                    connection = pool.next();
                }
                probed++;
            }
            if (connection.isReady()) {
                return connection;
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                        "no ready connection in pool after " + WAIT_FOR_READY_TIMEOUT_MS + "ms");
            }
            try {
                wait(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted while waiting for a ready connection", e);
            }
        }
    }

    @Override
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public void stop() throws Exception {
        for (ConnectionWrapper connectionWrapper : connectionList) {
            connectionWrapper.stop();
        }
        isConnected = false;
    }
}
