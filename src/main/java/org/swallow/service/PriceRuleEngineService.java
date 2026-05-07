package org.swallow.service;

import cn.hutool.core.collection.CollectionUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.swallow.entity.MonitorRules;
import org.swallow.record.BinanceTickerDTO;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PriceRuleEngineService {
    @Resource
    private PushPlusService pushPlusService;
    @Resource
    private MonitorRulesService monitorRulesService;

    // 本地规则缓存，每隔一段时间（如 1 分钟）或在微信端更新规则后刷新
    private final Map<String, List<MonitorRules>> ruleCache = new ConcurrentHashMap<>();


    @PostConstruct
    public void init() {
        refreshRulesTask();
    }

    // 【核心】限流缓存：Key 为 "规则ID", Value 为 "上次触发时间"
    // 设置 5 分钟内同一条规则不重复推送
    private final Cache<Long, LocalDateTime> alertCache = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .build();

    public Mono<Void> processPrice(List<MonitorRules> monitorRules, BinanceTickerDTO ticker) {
        // 遍历内存中的规则
        return Flux.fromIterable(monitorRules)
                .filter(rule -> rule.getSymbol().equalsIgnoreCase(ticker.getSymbol()))
                .filter(rule -> checkCondition(rule.getConditionExpression(), ticker))
                .flatMap(rule -> triggerAlert(rule, ticker))
                .then();
    }

    private boolean checkCondition(String expression, Object rootObject) {
        try {
            org.springframework.expression.ExpressionParser parser =
                    new org.springframework.expression.spel.standard.SpelExpressionParser();

            // 直接以 trade 对象为根节点进行解析
            // 这样表达式里的 "price" 会自动映射到 trade.getPrice() 或 trade.price 字段
            return Boolean.TRUE.equals(parser.parseExpression(expression)
                    .getValue(rootObject, Boolean.class));
        } catch (Exception e) {
            // 打印更详细的日志，方便你调试表达式写错的情况
            log.error("❌ 规则表达式错误: [" + expression + "] -> {}", e.getMessage());
            return false;
        }
    }

    /**
     * 触发告警动作（带限流逻辑）
     */
    private Mono<String> triggerAlert(MonitorRules rule, BinanceTickerDTO trade) {
        Long ruleId = rule.getId();

        // 检查限流锁：如果缓存中存在，说明最近 5 分钟刚发过
        if (alertCache.getIfPresent(ruleId) != null) {
            return Mono.empty();
        }

        // 更新限流锁
        alertCache.put(ruleId, LocalDateTime.now());

        // 构建推送内容
        String title = "🚨 价格触发告警: " + trade.getSymbol();
        String timeStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // 解析模板并替换占位符
        String htmlContent = rule.getTemplate()
                .replace("{price}", trade.getLastPrice())
                .replace("{time}", timeStr)
                .replace("{symbol}", trade.getHighPrice());

        // 执行推送

        Mono<String> stringMono = pushPlusService.sendWechatAlert(title, htmlContent, rule);
        return stringMono.flatMap(s -> {
            if ("success".equals(s)) {
                rule.setIsEnabled(false);
                return monitorRulesService.saveOrUpdate(rule);
            }
            return Mono.just("ok");
        });


    }


    /**
     * 刷新规则的方法
     *
     * @param newRules 数据库规则
     */
    public void refreshRules(Map<String, List<MonitorRules>> newRules) {
        this.ruleCache.clear();
        this.ruleCache.putAll(newRules);
    }

    /**
     * 定时从数据库刷新规则：每 60 秒执行一次
     */
    @Scheduled(fixedRate = 60000)
    public void refreshRulesTask() {
        try {
            // 只查询已启用的规则
            Flux<MonitorRules> list = monitorRulesService.list(null);
            Mono<Map<String, List<MonitorRules>>> collect = list.collect(Collectors.groupingBy(MonitorRules::getSymbol));
            collect.flatMap(stringListMap -> {
                refreshRules(stringListMap);
                log.info("🔄 规则库同步成功");
                ruleCache.forEach((key, value) -> {
                    for (MonitorRules monitorRules : value) {
                        log.info("{} 订阅，告警规则:{}", monitorRules.getSymbol(), monitorRules.getConditionExpression());
                    }
                });
                return Mono.empty();
            }).subscribe();
        } catch (Exception e) {
            log.error("❌ 刷新规则失败: {}", e.getMessage());
        }
    }

    public Mono<Void> checkRules(BinanceTickerDTO ticker) {
        List<MonitorRules> monitorRules = ruleCache.get(ticker.getSymbol());
        if (CollectionUtil.isEmpty(monitorRules)) return Mono.empty();
        return processPrice(monitorRules, ticker);
    }
}
