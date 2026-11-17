package com.binance.connector.client.common.websocket.adapter;

import com.binance.connector.client.common.websocket.dtos.ApiRequestWrapperDTO;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;

public interface ConnectionInterface {
    void connect();

    void disconnect();

    void send(ApiRequestWrapperDTO request) throws InterruptedException;

    BlockingQueue<String> sendForStream(ApiRequestWrapperDTO request) throws InterruptedException;

    /**
     * Stop feeding a queue returned by {@link #sendForStream}. Every inbound stream event is
     * offered to every registered queue, so a caller that re-subscribes (getting a new queue)
     * must drop the old one, or the abandoned queue keeps growing with every event.
     *
     * @return true if the queue was registered and is now removed
     */
    default boolean removeStreamQueue(BlockingQueue<String> queue) {
        return false;
    }

    void send(RequestWrapperDTO request) throws InterruptedException;

    void setUserAgent(String userAgent);

    boolean isConnected();

    void setLogonMethods(List<String> logonMethods);

    void setLogoutMethods(List<String> logoutMethods);

    void stop() throws Exception;

    /**
     * Register a callback fired on abnormal WebSocket close (any code that is not
     * a clean shutdown). Invoked before auto-reconnect so callers can pause trading
     * or surface alerts while the SDK is mid-reconnect. Implementations must be
     * safe to call from Jetty's event thread.
     */
    default void setOnCloseHook(BiConsumer<Integer, String> hook) {
        // default no-op so existing implementations stay compatible
    }
}
