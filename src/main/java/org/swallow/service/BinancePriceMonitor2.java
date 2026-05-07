package org.swallow.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.swallow.entity.MonitorRules;
import org.swallow.record.BinanceTickerDTO;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BinancePriceMonitor2 {
    /**
     * 使用组合流入口
     */
    private static final String BINANCE_WS_URL = "wss://fstream.binance.me/market/ws"; //
    private final Sinks.Many<String> subscribeSink = Sinks.many().unicast().onBackpressureBuffer();
    /**
     * 记录当前已经订阅了哪些 Stream，防止重复订阅
     */
    private final Set<String> subscribedStreams = ConcurrentHashMap.newKeySet();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong lastMessageTime = new AtomicLong(System.currentTimeMillis());
    private final Map<String, AtomicLong> messageCount = new HashMap<>();
    private WebSocketSession currentSession;
    @Resource
    private PriceRuleEngineService ruleEngineService;
    @Resource
    private MonitorRulesService monitorRulesService;
    @Resource
    private PushPlusService pushPlusService;

    @PostConstruct
    public void init() {
        connect();
    }

    private void connect() {
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

        client.execute(URI.create(BINANCE_WS_URL), session -> {
                    this.currentSession = session;
                    log.info("✅ 币安连接已建立");

                    // 【核心修改 1】：每次连接成功，清空内存已订阅列表，触发重新订阅
                    subscribedStreams.clear();

                    // 【核心修改 2】：连接建立后，立即触发一次数据库查询并订阅
                    // 这里不需要手动 .subscribe()，而是将其转换为信号发给 subscribeSink
                    reSubscribeAllFromDb();

                    Mono<Void> receive = session.receive()
                            .timeout(Duration.ofSeconds(60))
                            .doOnNext(msg -> {
                                // 1. 检查是否是协议层面的 PING 消息
                                if (WebSocketMessage.Type.PING.equals(msg.getType())) {
                                    session.send(Mono.just(session.pongMessage(factory ->
                                            msg.getPayload().factory().wrap(msg.getPayload().asByteBuffer().duplicate())
                                    ))).subscribe();
                                    log.info("✅ 已响应协议层 PONG");
                                }
                                lastMessageTime.set(System.currentTimeMillis());
                            }) // 每次有消息，更新时间戳
                            .flatMap(msg -> handleMessage(msg.getPayloadAsText()))
                            .onErrorResume(java.util.concurrent.TimeoutException.class, e -> {
                                // 触发报警
                                return sendDeadStreamAlert("WebSocket 数据停滞", "超过 60s 未收到行情数据，尝试重连...");
                            })

                            .doOnError(e -> log.error("❌ 接收消息异常", e))
                            .then();

                    Mono<Void> send = session.send(
                            subscribeSink.asFlux()
                                    .map(session::textMessage)
                                    .doOnNext(m -> log.info("➡️ 指令已发出: {}", m.getPayloadAsText()))
                    );

                    return Mono.zip(receive, send).then();
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofMinutes(2))
                        .doBeforeRetry(signal -> {
                            log.info("⚠️ 正在进行第 {} 次重连...", signal.totalRetries() + 1);
                            // 重连前确保状态清理
                            this.currentSession = null;
                        }))
                .subscribe();
    }

    /**
     * 发送停流告警并触发重连
     */
    private Mono<Void> sendDeadStreamAlert(String title, String content) {
        log.error("🚨 {} : {}", title, content);

        // 1. 异步调用 PushPlus 发送消息 (不阻塞流)
        pushPlusService.send(title, content).subscribe();

        // 2. 主动关闭当前 Session 触发 retryWhen 重连
        if (this.currentSession != null) {
            return this.currentSession.close()
                    .then(Mono.error(new RuntimeException("心跳超时，主动触发重连")));
        }
        return Mono.error(new RuntimeException("心跳超时且无可用Session"));
    }

    /**
     * 批量订阅多个币种
     *
     * @param symbols 币种列表，如 ["SOL", "BTC", "ETH"]
     */
    public void batchSubscribe(List<String> symbols) {
        if (symbols == null || symbols.isEmpty()) return;

        // 1. 过滤掉已经订阅过的，并转换成小写的 stream 名称
        List<String> streamsToSubscribe = symbols.stream()
                .map(s -> s.toLowerCase() + "@ticker")
                .filter(stream -> !subscribedStreams.contains(stream))
                .collect(Collectors.toList());

        if (streamsToSubscribe.isEmpty()) {
            log.info("ℹ️ 所有请求的币种已在订阅列表中，跳过批量订阅。");
            return;
        }

        // 2. 构造批量订阅 JSON
        // 币安限制：单个 params 数组建议不要超过 50 个，如果更多建议分批
        long id = System.currentTimeMillis();
        Map<String, Object> command = new HashMap<>();
        command.put("method", "SUBSCRIBE");
        command.put("params", streamsToSubscribe);
        command.put("id", id);

        try {
            String json = objectMapper.writeValueAsString(command);

            // 3. 发送指令到 Sink
            Sinks.EmitResult result = subscribeSink.tryEmitNext(json);

            if (result.isSuccess()) {
                subscribedStreams.addAll(streamsToSubscribe);
                log.info("🚀 批量订阅指令已发送 (ID: {}), 包含: {}", id, streamsToSubscribe);
            }
        } catch (JsonProcessingException e) {
            log.error("构造批量订阅 JSON 失败", e);
        }
    }

    /**
     * 从数据库加载所有规则并触发批量订阅
     */
    private void reSubscribeAllFromDb() {
        monitorRulesService.list(null)
                .map(MonitorRules::getSymbol)
                .distinct()
                .collectList()
                .subscribe(this::batchSubscribe, e -> log.error("❌ 重新加载订阅规则失败", e));
    }

    public void subscribeIfAbsent(String symbol) {
        String streamName = symbol.toLowerCase() + "@ticker";

        // 检查内存中是否已订阅
        if (subscribedStreams.contains(streamName)) {
            log.info("ℹ️ {} 已经订阅，跳过指令发送", streamName);
            return;
        }

        // 构造币安标准的 SUBSCRIBE JSON
        String subscribeJson = String.format(
                "{\"method\": \"SUBSCRIBE\", \"params\": [\"%s\"], \"id\": %d}",
                streamName, System.currentTimeMillis()
        );

        // 发送信号到 Sink
        Sinks.EmitResult result = subscribeSink.tryEmitNext(subscribeJson);
        if (result.isSuccess()) {
            subscribedStreams.add(streamName);
        } else {
            log.error("❌ 订阅指令发送失败: {}", result);
        }
    }

    private Mono<Void> handleMessage(String rawJson) {
        try {
            BinanceTickerDTO ticker = objectMapper.readValue(
                    rawJson,
                    BinanceTickerDTO.class
            );


            if (ticker != null && ticker.getSymbol() != null) {
                String symbol = ticker.getSymbol();
                double currentPrice = Double.parseDouble(ticker.getLastPrice());
                long count = getCount(symbol);
                if (count % 60 == 0) {
                    // 每 100 条打印一次，防止刷屏
                    log.info("解析成功 - 币种: {}, 价格: {}", symbol, currentPrice);
                }
                return ruleEngineService.checkRules(ticker);
            }
        } catch (Exception e) {
            if (!rawJson.contains("result")) {
                log.error("行情解析异常: {}, 原始数据: {}", e.getMessage(), rawJson);
            }
        }
        return Mono.empty();
    }

    private long getCount(String symbol) {
        AtomicLong orDefault = messageCount.getOrDefault(symbol, new AtomicLong(0));
        long l = orDefault.incrementAndGet();
        messageCount.put(symbol, orDefault);
        return l;
    }
}
