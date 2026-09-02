package com.binance.connector.client.common.websocket.adapter.stream;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.JSON;
import com.binance.connector.client.common.websocket.adapter.ConnectionWrapper;
import com.binance.connector.client.common.websocket.configuration.WebSocketClientConfiguration;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueue;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueueWrapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.lang.reflect.Type;
import java.util.logging.Level;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public class StreamConnectionWrapper extends ConnectionWrapper
        implements StreamConnectionInterface {
    // NOTE: the former `subcriptionsListResult` field (populated from the
    // server's LIST_SUBSCRIPTIONS reply) is gone -- afterConnect now restores
    // straight from `subscriptions`, which is correct even after an abrupt
    // server-side close. See beforeConnect/afterConnect below.
    // Mutated by consumer threads via subscribe/unsubscribe while the Jetty WS
    // thread iterates the per-stream list in onWebSocketText. With the previous
    // HashMap/ArrayList an unsubscribe racing the iterator threw
    // NoSuchElementException out of the Jetty callback, and Jetty then closed
    // the whole connection with code 1008. Concurrent structures remove that race.
    private final Map<String, List<StreamBlockingQueue<String>>> subscriptions =
            new ConcurrentHashMap<>();

    public StreamConnectionWrapper(WebSocketClientConfiguration configuration, Gson json) {
        super(configuration, json);
    }

    public StreamConnectionWrapper(WebSocketClientConfiguration configuration) {
        super(configuration);
    }

    public StreamConnectionWrapper(
            WebSocketClientConfiguration configuration, WebSocketClient webSocketClient) {
        super(configuration, webSocketClient);
    }

    public Map<String, StreamBlockingQueue<String>> subscribe(
            RequestWrapperDTO<Set<String>, Object> requestWrapperDTO) {
        HashMap<String, StreamBlockingQueue<String>> queueMap = new HashMap<>();
        for (String subscription : requestWrapperDTO.getParams()) {
            List<StreamBlockingQueue<String>> blockingQueueList =
                    this.subscriptions.computeIfAbsent(
                            subscription, k -> new CopyOnWriteArrayList<>());
            BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>();

            StreamBlockingQueue blockingQueueWrapper =
                    new StreamBlockingQueue(blockingQueue, subscription);

            blockingQueueList.add(blockingQueueWrapper);
            try {
                this.innerSend(requestWrapperDTO);
            } catch (RuntimeException e) {
                // Send failed (typically a dead session). Undo the local
                // registration: leaving it behind strands a queue nobody will
                // ever poll and, worse, leaves a key that afterConnect would
                // treat as a live subscription and resurrect on every reconnect.
                blockingQueueList.remove(blockingQueueWrapper);
                if (blockingQueueList.isEmpty()) {
                    this.subscriptions.remove(subscription, blockingQueueList);
                }
                throw e;
            }
            queueMap.put(subscription, blockingQueueWrapper);
        }

        return queueMap;
    }

    public void unsubscribe(StreamBlockingQueueWrapper blockingQueueWrapper) {
        unsubscribe(blockingQueueWrapper.getInnerQueue());
    }

    public void unsubscribe(StreamBlockingQueue queue) {
        String operationId = queue.getOperationId();
        List<StreamBlockingQueue<String>> blockingQueues = subscriptions.get(operationId);
        if (blockingQueues == null) {
            return;
        }
        // remove() == false means another thread already removed this queue —
        // skip so the server-side UNSUBSCRIBE is never sent twice.
        if (!blockingQueues.remove(queue)) {
            return;
        }

        // unsubscribe from the stream if no more queue
        if (blockingQueues.isEmpty()) {
            // Drop the now-empty entry too, so it cannot be mistaken for a live
            // subscription later (afterConnect restores from this map). remove
            // (key, value) so a concurrent re-subscribe that just installed a
            // fresh list for this stream is not clobbered.
            subscriptions.remove(operationId, blockingQueues);
            RequestWrapperDTO listSubscriptions =
                    new RequestWrapperDTO.Builder<>()
                            .id(UUID.randomUUID().toString())
                            .method("UNSUBSCRIBE")
                            .params(Collections.singletonList(operationId))
                            .build();

            this.send(listSubscriptions);
        }
    }

    @Override
    public void onWebSocketText(String message) {
        try {
            JsonElement root = JsonParser.parseString(message);
            JsonObject obj = root.getAsJsonObject();

            if (handleShutdownMessage(obj)) {
                return;
            }

            // Response to subscribe
            JsonElement id = obj.get("id");
            if (id != null && !id.isJsonNull()) {
                JsonElement result = obj.get("result");
                RequestWrapperDTO requestWrapperDTO = pendingRequest.get(id.getAsString());
                Type responseType = requestWrapperDTO.getResponseType();
                Object fromJson =
                        result.isJsonNull() ? null : JSON.getGson().fromJson(result, responseType);
                pendingRequest.remove(id.getAsString());
                requestWrapperDTO.getResponseCallback().complete(fromJson);
                return;
            }

            JsonElement stream = obj.get("stream");
            // no stream, we couldn't detect which queue to use.
            if (stream == null) {
                return;
            }
            List<StreamBlockingQueue<String>> blockingQueues =
                    subscriptions.get(stream.getAsString());
            if (blockingQueues == null || blockingQueues.isEmpty()) {
                return;
            }
            for (StreamBlockingQueue<String> queue : blockingQueues) {
                JsonElement data = obj.get("data");
                queue.offer(data.toString());
            }
        } catch (Exception e) {
            // Swallow per-message failures: rethrowing propagates out of the Jetty
            // callback and Jetty then closes the whole connection (1008). One bad
            // message must not take down every stream multiplexed on this socket.
            log.log(
                    Level.SEVERE,
                    "Exception while receiving message from WS connection",
                    e);
        }
    }

    protected void beforeConnect() {
        // Previously this asked the SERVER for the subscription list via
        // LIST_SUBSCRIPTIONS, but it bailed out whenever the old session was
        // already closed -- which is precisely the case after a server-side
        // close (1006/1008), i.e. the only situation reconnect-resubscribe
        // exists for. The `subscriptions` map below is the authoritative local
        // record of what this wrapper subscribed to (its keys ARE the stream
        // names passed to SUBSCRIBE, see subscribe()), so afterConnect now
        // restores from that and no round trip is needed here at all.
        super.beforeConnect();
    }

    protected void afterConnect(Session session) {
        // super FIRST: send() targets this.session, and the base method is what
        // swaps this.session over to the freshly established one. Sending before
        // it would put the restore frame on the old, already-dead session.
        super.afterConnect(session);

        // Skip empty lists. unsubscribe()/subscribe() now drop the entry when
        // its last queue goes away, so this should not trigger -- it is kept as
        // a cheap guard because anything that did leak through would be
        // resurrected on every single reconnect, with the server pushing a
        // stream nobody consumes for the rest of the connection's life.
        Set<String> streams = new HashSet<>();
        subscriptions.forEach(
                (stream, queues) -> {
                    if (queues != null && !queues.isEmpty()) {
                        streams.add(stream);
                    }
                });
        if (!streams.isEmpty()) {
            RequestWrapperDTO subscribe =
                    new RequestWrapperDTO.Builder<>()
                            .id(UUID.randomUUID().toString())
                            .method("SUBSCRIBE")
                            .params(streams)
                            .build();

            this.send(subscribe);
        }
    }
}
