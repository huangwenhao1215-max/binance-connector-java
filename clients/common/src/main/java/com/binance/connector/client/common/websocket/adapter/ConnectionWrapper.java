package com.binance.connector.client.common.websocket.adapter;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.DecimalFormatter;
import com.binance.connector.client.common.SystemUtil;
import com.binance.connector.client.common.auth.SignatureGeneratorFactory;
import com.binance.connector.client.common.sign.SignatureGenerator;
import com.binance.connector.client.common.websocket.SignatureUtil;
import com.binance.connector.client.common.websocket.configuration.WebSocketClientConfiguration;
import com.binance.connector.client.common.websocket.dtos.ApiRequestWrapperDTO;
import com.binance.connector.client.common.websocket.dtos.BaseRequestDTO;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;
import com.binance.connector.client.common.websocket.dtos.SessionResponse;
import com.binance.connector.client.common.websocket.service.DeserializeExclusionStrategy;
import com.binance.connector.client.common.websocket.service.SerializeExclusionStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bouncycastle.crypto.CryptoException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public class ConnectionWrapper implements WebSocketListener, ConnectionInterface {
    // protected so subclasses (StreamConnectionWrapper) can log per-message
    // processing failures instead of rethrowing them out of Jetty callbacks
    // (Jetty closes the whole connection with 1008 when a frame handler throws).
    protected static final Logger log = Logger.getLogger(ConnectionWrapper.class.getName());

    public static final int WAIT_TIME = 5;
    private static final String QUERY_PARAM_TIME_UNIT = "timeUnit";
    // -2015 for wrong API Keys, -1022 for wrong signature
    private static final List<Integer> ERROR_CODE_WRONG_CREDENTIALS = Arrays.asList(-2015, -1022);

    // ConcurrentHashMap: requests are put from Jetty write callbacks and
    // matched/removed from the Jetty read thread concurrently; the previous
    // plain HashMap could drop entries under concurrency, leaving request
    // futures to hang until caller-side timeouts.
    protected Map<String, RequestWrapperDTO> pendingRequest = new ConcurrentHashMap<>();
    protected volatile Session session;
    // volatile: written by the 23h reconnect Timer / connect callers, read from
    // the Jetty read thread (onWebSocketText reconnect trigger, onWebSocketClose
    // old-session check) with no other synchronization in between.
    protected volatile Session oldSession;

    protected List<String> logonMethods = new ArrayList<>();
    protected List<String> logoutMethods = new ArrayList<>();

    private String userAgent =
            String.format(
                    "binance-connector-java/1.0.0 (Java/%s; %s; %s)",
                    SystemUtil.getJavaVersion(), SystemUtil.getOs(), SystemUtil.getArch());

    // volatile: these are written by the Jetty connect/close callbacks but read
    // from caller threads (e.g. pool getConnection loops). Without volatile the
    // reader may never observe setReady(true) after a reconnect — a tight loop
    // on isReady can even be JIT-hoisted into an infinite loop.
    private volatile boolean isLoggedOn = false;
    private volatile boolean isReady = false;
    private volatile boolean canReconnect = true;
    private final WebSocketClient webSocketClient;
    private final WebSocketClientConfiguration configuration;
    private final SignatureGeneratorFactory signatureGeneratorFactory =
            new SignatureGeneratorFactory();
    private Gson gson;
    private SignatureGenerator signatureGenerator;

    private CountDownLatch countDownLatch;

    // volatile: set by the reconnect Timer / shutdown handler on their threads,
    // checked by the Jetty read thread when in-flight requests drain.
    private volatile boolean pendingReconnect = false;

    // CopyOnWriteArrayList: subscriptions add from caller threads while the
    // Jetty read thread iterates to offer events — same race pattern the plain
    // ArrayList had as the StreamConnectionWrapper subscriptions map.
    private List<BlockingQueue<String>> streamQueues = new CopyOnWriteArrayList<>();

    private Timer timer;

    // Far below typical NAT/firewall idle timeout (60s) to keep the TCP flow alive
    // when no application frames are flowing (e.g. illiquid bookTicker silence).
    private static final long PING_INTERVAL_SECONDS = 30L;
    // sendPing only reads the backing array; position/limit are untouched, so a
    // single shared zero-length payload is safe across connections and threads.
    private static final ByteBuffer EMPTY_PING_PAYLOAD = ByteBuffer.allocate(0);
    private final ScheduledExecutorService pingScheduler;

    // Reconnect backoff. onWebSocketClose used to call connect() inline, which
    // (a) ran on the Jetty callback thread and blocked it for the whole TCP+TLS
    // handshake (connect() joins on the session promise), and (b) retried with
    // zero delay, so a server that keeps rejecting us -- e.g. Binance closing
    // with 1008 "Too many requests" -- turned into a tight reconnect loop that
    // burns the 300-attempts-per-5min-per-IP budget and risks an IP ban.
    // Reconnects now go on this scheduler with exponential backoff + jitter.
    private static final long RECONNECT_BASE_DELAY_MS = 1_000L;
    private static final long RECONNECT_MAX_DELAY_MS = 60_000L;
    // How long a connection must survive to count as "stable", i.e. for the
    // next drop to restart the backoff from scratch instead of continuing to
    // escalate. Below this it is treated as flapping.
    private static final long STABLE_CONNECTION_MS = 60_000L;
    private final ScheduledExecutorService reconnectScheduler;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    // volatile: written on the connect path, read on the Jetty close callback.
    private volatile long connectedAtMs = 0L;

    // Optional application-level hook fired on abnormal close (anything that is
    // not NORMAL/NO_CLOSE-expected). Invoked before the auto-reconnect so the
    // caller can pause trading / surface an alert while the SDK is mid-reconnect.
    // statusCode and reason are forwarded as-is from Jetty.
    private volatile BiConsumer<Integer, String> onCloseHook;

    public ConnectionWrapper(WebSocketClientConfiguration configuration, Gson gson) {
        this(configuration, null, gson);
    }

    public ConnectionWrapper(WebSocketClientConfiguration configuration) {
        this(configuration, null, null);
    }

    public ConnectionWrapper(
            WebSocketClientConfiguration configuration, WebSocketClient webSocketClient) {
        this(configuration, webSocketClient, null);
    }

    public ConnectionWrapper(
            WebSocketClientConfiguration configuration,
            WebSocketClient webSocketClient,
            Gson gson) {
        if (webSocketClient == null) {
            HttpClient httpClient = new HttpClient();
            httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, userAgent));
            if (configuration.getConnectTimeout() != null) {
                httpClient.setConnectTimeout(configuration.getConnectTimeout());
            }

            if (configuration.getWebSocketProxy() != null) {
                httpClient.getProxyConfiguration().addProxy(configuration.getWebSocketProxy());
                if (configuration.getWebSocketProxyAuthentication() != null) {
                    httpClient
                            .getAuthenticationStore()
                            .addAuthentication(configuration.getWebSocketProxyAuthentication());
                }
            }
            webSocketClient = new WebSocketClient(httpClient);
        }

        webSocketClient.setIdleTimeout(Duration.ZERO);
        if (configuration.getMessageMaxSize() != null) {
            webSocketClient.setMaxTextMessageSize(configuration.getMessageMaxSize());
        }

        if (!webSocketClient.isStarted() && !webSocketClient.isStarting()) {
            try {
                webSocketClient.start();
            } catch (Exception e) {
                throw new ApiException(e);
            }
        }
        this.configuration = configuration;
        this.webSocketClient = webSocketClient;

        if (gson == null) {
            this.gson =
                    new GsonBuilder()
                            .registerTypeAdapter(
                                    Double.class,
                                    (JsonSerializer<Double>)
                                            (src, typeOfSrc, context) -> {
                                                DecimalFormat df = DecimalFormatter.getFormatter();
                                                return new JsonPrimitive(df.format(src));
                                            })
                            .addSerializationExclusionStrategy(new SerializeExclusionStrategy())
                            .addDeserializationExclusionStrategy(new DeserializeExclusionStrategy())
                            .create();
        } else {
            this.gson = gson;
        }

        Integer reconnectAfter = configuration.getReconnectIntervalTime();
        this.timer = new Timer();
        this.timer.scheduleAtFixedRate(
                        new TimerTask() {
                            @Override
                            public void run() {
                                try {
                                    if (canReconnect()) {
                                        connect();
                                    } else {
                                        pendingReconnect = true;
                                    }

                                } catch (Exception e) {
                                    // Must not propagate: an exception out of a
                                    // TimerTask kills the Timer thread for good,
                                    // and this connection would then lose its
                                    // periodic pre-emptive reconnect until the
                                    // server force-closes it at the 24h mark.
                                    log.log(
                                            Level.SEVERE,
                                            "periodic reconnect failed, retrying with backoff",
                                            e);
                                    scheduleReconnect();
                                }
                            }
                        },
                        reconnectAfter,
                        reconnectAfter);

        // Proactive WebSocket PING frame keepalive. The SDK only reacts to inbound
        // pings (onWebSocketPing) and never sends one, so when a shared market-data
        // stream goes silent (e.g. bookTicker with no best-bid/ask change), NAT or
        // firewall idle timers (commonly 60s) cut the TCP and we see code 1006
        // "Session Closed" repeated every ~60s. Sending a protocol-level PING every
        // 30s keeps the flow alive without consuming any business quota.
        this.pingScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "binance-ws-ping-" + configuration.getUrl());
            t.setDaemon(true);
            return t;
        });
        this.pingScheduler.scheduleAtFixedRate(() -> {
            Session s = this.session;
            if (s == null || !s.isOpen()) {
                return;
            }
            try {
                s.getRemote().sendPing(EMPTY_PING_PAYLOAD);
            } catch (Exception e) {
                log.log(Level.WARNING, "failed to send websocket ping", e);
            }
        }, PING_INTERVAL_SECONDS, PING_INTERVAL_SECONDS, TimeUnit.SECONDS);

        // Separate executor from pingScheduler on purpose: connect() blocks for
        // the whole handshake, and sharing the single-threaded ping executor
        // would stall keepalive pings for the duration of every reconnect.
        this.reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "binance-ws-reconnect-" + configuration.getUrl());
            t.setDaemon(true);
            return t;
        });
    }

    public void connect() {
        try {
            connect(null);
        } catch (Exception e) {
            throw new ApiException(e);
        }
    }

    public void connect(Consumer<Session> customCallback)
            throws IOException, URISyntaxException, InterruptedException {
        pendingReconnect = false;
        boolean autoLogon = !logonMethods.isEmpty() && (configuration.getAutoLogon() || isLoggedOn);

        // For LogOn mode, we need the logon to be completed before changing the session
        countDownLatch = new CountDownLatch(autoLogon ? 1 : 0);

        Consumer<Session> callback = autoLogon ? getLogonConsumer() : null;
        // The server URI to connect to.
        URI serverURI = null;
        serverURI = getUri(configuration.getUrl());

        if (configuration.getSignatureConfiguration() != null) {
            this.signatureGenerator =
                    signatureGeneratorFactory.getSignatureGenerator(
                            configuration.getSignatureConfiguration());
        }

        beforeConnect();

        ClientUpgradeRequest clientUpgradeRequest = null;
        if (configuration.getCompression()) {
            clientUpgradeRequest = new ClientUpgradeRequest();
            clientUpgradeRequest.addExtensions("permessage-deflate");
        }

        // Connect the client EndPoint to the server.
        CompletableFuture<Session> clientSessionPromise =
                webSocketClient.connect(this, serverURI, clientUpgradeRequest);
        Session session = clientSessionPromise.join();

        if (callback != null) {
            callback.accept(session);
        }
        if (customCallback != null) {
            customCallback.accept(session);
        }

        // Wait up to 5s for the logon to be completed
        boolean await = countDownLatch.await(WAIT_TIME, TimeUnit.SECONDS);
        if (!await) {
            log.log(Level.WARNING, "countDownLatch wait timeout");
        }
        afterConnect(session);
    }

    public Consumer<Session> getLogonConsumer() {
        return (Session session) -> {
            try {
                this.logOn(session);
            } catch (Exception e) {
                throw new ApiException(e);
            }
        };
    }

    public void onWebSocketPing(ByteBuffer payload) {
        Session s = this.session;
        if (s != null) {
            s.getRemote().sendPong(payload, WriteCallback.NOOP);
        }
    }

    public void setOnCloseHook(BiConsumer<Integer, String> hook) {
        this.onCloseHook = hook;
    }

    public Map<String, RequestWrapperDTO> getPendingRequest() {
        return pendingRequest;
    }

    public boolean isPendingReconnect() {
        return pendingReconnect;
    }

    public void setPendingReconnect(boolean pendingReconnect) {
        this.pendingReconnect = pendingReconnect;
    }

    public boolean isReady() {
        return isReady && session.isOpen();
    }

    public void setReady(boolean ready) {
        isReady = ready;
    }

    public String getUserAgent() {
        return userAgent;
    }

    @Override
    public void setUserAgent(String userAgent) {
        if (webSocketClient != null) {
            webSocketClient
                    .getHttpClient()
                    .setUserAgentField(new HttpField(HttpHeader.USER_AGENT, userAgent));
        }
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        if (cause instanceof ClosedChannelException) {
            log.info("channel was closed: " + cause.getMessage());
        } else {
            log.log(Level.SEVERE, "received webSocket error", cause);
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        if (statusCode == StatusCode.NORMAL) {
            log.info("received websocket close, code: " + statusCode + " reason: " + reason);
            return;
        }
        if (statusCode == StatusCode.NO_CLOSE) {
            Collection<Session> openSessions = webSocketClient.getOpenSessions();
            for (Session openSession : openSessions) {
                // old session being closed, it's expected
                if (oldSession != null && !oldSession.isOpen() && openSession.equals(oldSession)) {
                    log.info("old session closing, code: " + statusCode + " reason: " + reason);
                    return;
                }
            }
        }
        log.log(
                Level.SEVERE,
                "received websocket close, code: " + statusCode + " reason: " + reason);
        BiConsumer<Integer, String> hook = this.onCloseHook;
        if (hook != null) {
            try {
                hook.accept(statusCode, reason);
            } catch (Throwable t) {
                log.log(Level.WARNING, "onClose hook threw", t);
            }
        }
        // Nothing in flight can ever be answered on a socket that just closed:
        // pendingRequest is only drained in onWebSocketText. Leaving entries
        // behind strands their callers' futures forever AND wedges every code
        // path gated on canReconnect() (which requires pendingRequest to be
        // empty) -- including the periodic reconnect timer.
        failPendingRequests(statusCode + "/" + reason);

        if (canReconnect) {
            scheduleReconnect();
        }
    }

    public void send(RequestWrapperDTO request) {
        send(request, this.session);
    }

    public void send(ApiRequestWrapperDTO request) {
        BaseRequestDTO baseRequest = request.getParams();
        if (baseRequest == null) {
            baseRequest = new BaseRequestDTO();
        }

        if (request.isApiKeyOnly() && baseRequest.getApiKey() == null) {
            baseRequest.setApiKey(configuration.getSignatureConfiguration().getApiKey());
        }

        if (request.isSigned()) {
            baseRequest.setTimestamp(getTimestamp().toString());

            if (!isLoggedOn) {
                if (baseRequest.getApiKey() == null) {
                    baseRequest.setApiKey(configuration.getSignatureConfiguration().getApiKey());
                }

                try {
                    baseRequest.setSignature(
                            signatureGenerator.signAsString(baseRequest.toUrlQueryString()));
                } catch (CryptoException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        innerSend(request);
    }

    @Override
    public BlockingQueue<String> sendForStream(ApiRequestWrapperDTO request)
            throws InterruptedException {
        LinkedBlockingDeque<String> streamQueue = new LinkedBlockingDeque<>();
        streamQueues.add(streamQueue);
        send(request);
        return streamQueue;
    }

    public void innerSend(RequestWrapperDTO requestWrapperDTO) {
        send(requestWrapperDTO);
    }

    public void send(RequestWrapperDTO request, Session session) {
        session.getRemote()
                .sendString(
                        gson.toJson(request),
                        new WriteCallback() {
                            @Override
                            public void writeFailed(Throwable x) {
                                throw new ApiException(x);
                            }

                            @Override
                            public void writeSuccess() {
                                ConnectionWrapper.this.pendingRequest.put(request.getId(), request);
                            }
                        });
    }

    public Long getTimestamp() {
        return System.currentTimeMillis();
    }

    public void logOn(Session session) throws CryptoException {
        SignatureUtil signatureUtil = new SignatureUtil();
        String timestamp = signatureUtil.buildTimestamp();

        BaseRequestDTO build =
                new BaseRequestDTO.Builder()
                        .apiKey(configuration.getSignatureConfiguration().getApiKey())
                        .timestamp(timestamp)
                        .build();

        build.setSignature(signatureGenerator.signAsString(build.toString()));
        RequestWrapperDTO<? extends BaseRequestDTO, SessionResponse> request =
                new RequestWrapperDTO.Builder<BaseRequestDTO, SessionResponse>()
                        .id(UUID.randomUUID().toString())
                        .method(logonMethods.get(0))
                        .params(build)
                        .responseType(SessionResponse.class)
                        .build();

        request.getResponseCallback()
                .handle(
                        (resp, error) -> {
                            if (error != null) {
                                log.log(Level.SEVERE, "Logon request failed", error);
                            }
                            if (resp.getError() != null) {
                                if (ERROR_CODE_WRONG_CREDENTIALS.contains(
                                        resp.getError().getCode())) {
                                    canReconnect = false;
                                }
                                log.log(
                                        Level.SEVERE,
                                        "Logon request failed",
                                        new ApiException(
                                                resp.getError().getCode(),
                                                resp.getError().getMsg()));
                            }
                            countDownLatch.countDown();
                            return resp;
                        });

        this.send(request, session);
    }

    @Override
    public void onWebSocketConnect(Session session) {
        this.session = session;
        // No need to demand here, because this endpoint is auto-demanding.
    }

    @Override
    public void onWebSocketText(String message) {
        try {
            JsonElement root = JsonParser.parseString(message);
            JsonObject obj = root.getAsJsonObject();

            if (handleShutdownMessage(obj)) {
                return;
            }

            JsonElement idElem = obj.get("id");
            String id = idElem == null ? null : idElem.getAsString();
            RequestWrapperDTO requestWrapperDTO = null;
            if (id != null) {
                requestWrapperDTO = pendingRequest.get(id);
            }

            if (requestWrapperDTO == null) {
                JsonElement eventElem = obj.get("event");
                for (BlockingQueue<String> streamQueue : streamQueues) {
                    streamQueue.offer(eventElem != null ? eventElem.toString() : message);
                }
                return;
            }
            Type responseType = requestWrapperDTO.getResponseType();

            Object responseResult = gson.fromJson(root, responseType);
            pendingRequest.remove(id);

            if (this.logonMethods.contains(requestWrapperDTO.getMethod())) {
                if (obj.get("status").getAsInt() == 200) {
                    isLoggedOn = true;
                }
            }

            if (this.logoutMethods.contains(requestWrapperDTO.getMethod())) {
                if (obj.get("status").getAsInt() == 200) {
                    isLoggedOn = false;
                }
            }

            requestWrapperDTO.getResponseCallback().complete(responseResult);

            if (isPendingReconnect() && pendingRequest.isEmpty()) {
                connect();
            }
        } catch (Exception e) {
            // Swallow per-message failures: rethrowing propagates out of the Jetty
            // callback and Jetty then closes the whole connection (1008). One bad
            // message must not take down the shared connection.
            log.log(Level.SEVERE, "Exception while receiving message from WS connection", e);
        }
    }

    public boolean canReconnect() {
        return isReady && pendingRequest.isEmpty();
    }

    protected boolean handleShutdownMessage(JsonObject jsonObj) {
        JsonElement eventElem = jsonObj.get("event");
        if (eventElem != null) {
            JsonObject eventElemObj = eventElem.getAsJsonObject();
            JsonElement eElement = eventElemObj.get("e");
            if (eElement != null && "serverShutdown".equals(eElement.getAsString())) {
                if (canReconnect()) {
                    // We are on the Jetty read thread here; connect() blocks for
                    // the whole handshake, so hand it off. No backoff: this is a
                    // planned server rotation announced on a still-live socket,
                    // not a failure, and reconnecting promptly is the point.
                    try {
                        reconnectScheduler.execute(
                                () -> {
                                    try {
                                        connect();
                                    } catch (Exception e) {
                                        log.log(
                                                Level.SEVERE,
                                                "Error while reconnecting after serverShutdown",
                                                e);
                                        scheduleReconnect();
                                    }
                                });
                    } catch (RejectedExecutionException e) {
                        log.info("reconnect scheduler is shut down, skipping serverShutdown reconnect");
                    }
                } else {
                    pendingReconnect = true;
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Complete every in-flight request exceptionally and drop it.
     *
     * <p>Removes entry-by-entry rather than iterate-then-clear(): a request
     * registered concurrently would otherwise be discarded by the clear()
     * without ever being completed, stranding its caller.
     */
    private void failPendingRequests(String reason) {
        if (pendingRequest.isEmpty()) {
            return;
        }
        int failed = 0;
        for (String id : pendingRequest.keySet()) {
            RequestWrapperDTO request = pendingRequest.remove(id);
            if (request == null) {
                continue;
            }
            failed++;
            try {
                request.getResponseCallback()
                        .completeExceptionally(
                                new ApiException("websocket closed before response: " + reason));
            } catch (Exception e) {
                log.log(Level.WARNING, "failed to complete pending request exceptionally", e);
            }
        }
        if (failed > 0) {
            log.info("failed " + failed + " in-flight request(s) after websocket close: " + reason);
        }
    }

    /**
     * Queue a reconnect on the dedicated scheduler after an exponentially
     * backed-off, jittered delay. Never call connect() straight from a Jetty
     * callback: it blocks on the session promise and would hold up the WS read
     * thread. The jitter matters when many wrappers/processes are knocked off
     * by the same server-side event -- without it they all retry in lockstep
     * and recreate the overload that closed them.
     */
    private void scheduleReconnect() {
        // A connection that stayed up a while and then dropped is a fresh
        // incident, not an escalating failure -- restart the backoff. Resetting
        // on every successful handshake instead (the obvious place) is wrong:
        // when Binance accepts the TCP upgrade and only then closes with 1008,
        // every cycle would look "successful" and the delay would never grow
        // past the first step, which is exactly when backoff is needed most.
        long connectedAt = connectedAtMs;
        if (connectedAt > 0 && System.currentTimeMillis() - connectedAt >= STABLE_CONNECTION_MS) {
            reconnectAttempts.set(0);
        }
        // Consume it so the recursive retry below cannot re-trigger the reset.
        connectedAtMs = 0L;

        int attempt = reconnectAttempts.getAndIncrement();
        long backoffMs =
                Math.min(RECONNECT_MAX_DELAY_MS, RECONNECT_BASE_DELAY_MS << Math.min(attempt, 6));
        long delayMs = backoffMs / 2 + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1);
        log.info(
                "scheduling websocket reconnect attempt "
                        + (attempt + 1)
                        + " in "
                        + delayMs
                        + "ms");
        try {
            reconnectScheduler.schedule(
                    () -> {
                        // Deliberately NOT gated on canReconnect(): that predicate
                        // (isReady && pendingRequest.isEmpty()) exists to stop the
                        // periodic 24h timer from cutting a HEALTHY connection out
                        // from under in-flight work. On a socket that already died
                        // it is meaningless, and bailing here would leave the
                        // connection down for good. The caller checked the
                        // canReconnect flag before scheduling us.
                        try {
                            connect();
                        } catch (Exception e) {
                            log.log(Level.SEVERE, "Error while trying to reconnect", e);
                            // Failed attempt: queue the next one, otherwise a
                            // connect() that throws would end reconnection for good.
                            scheduleReconnect();
                        }
                    },
                    delayMs,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler already shut down (disconnect/close path) — nothing to do.
            log.info("reconnect scheduler is shut down, skipping reconnect");
        }
    }

    protected void beforeConnect() {
        // session exists and is open, it's a reconnect
        if (this.session != null && this.session.isOpen()) {
            this.oldSession = this.session;
        }
    }

    protected void afterConnect(Session session) {
        this.session = session;
        if (this.oldSession != null) {
            this.oldSession.close(StatusCode.NORMAL, "close after reconnect", WriteCallback.NOOP);
        }
        canReconnect = true;
        // Only stamp the time here; whether this counts as a recovery that
        // resets the backoff is decided at the NEXT close, based on how long
        // the connection actually survived. See scheduleReconnect().
        connectedAtMs = System.currentTimeMillis();
        setReady(true);
    }

    @Override
    public boolean isConnected() {
        return session != null && session.isOpen();
    }

    @Override
    public void setLogonMethods(List<String> logonMethods) {
        this.logonMethods = logonMethods;
    }

    @Override
    public void setLogoutMethods(List<String> logoutMethods) {
        this.logoutMethods = logoutMethods;
    }

    public URI getUri(String uri) throws URISyntaxException {
        URI oldUri = new URI(uri);

        // timeUnit not configured or already specified in query
        if (configuration.getTimeUnit() == null
                || (oldUri.getQuery() != null
                        && oldUri.getQuery().contains(QUERY_PARAM_TIME_UNIT + "="))) {
            return oldUri;
        }

        String newQuery = oldUri.getQuery();
        String appendQuery = QUERY_PARAM_TIME_UNIT + "=" + configuration.getTimeUnit();
        if (newQuery == null) {
            newQuery = appendQuery;
        } else {
            newQuery += "&" + appendQuery;
        }

        return new URI(
                oldUri.getScheme(),
                oldUri.getAuthority(),
                oldUri.getPath(),
                newQuery,
                oldUri.getFragment());
    }

    public void disconnect() {
        if (this.session != null) {
            this.session.disconnect();
        }
        setReady(false);
        canReconnect = false;
    }

    public void stop() throws Exception {
        if (this.webSocketClient != null) {
            setReady(false);
            canReconnect = false;
            timer.cancel();
            pingScheduler.shutdownNow();
            reconnectScheduler.shutdownNow();
            webSocketClient.stop();
        }
    }
}
