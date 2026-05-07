package org.swallow.service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.swallow.dto.WechatMsg;
import org.swallow.entity.MonitorRules;
import org.swallow.repository.MonitorRulesRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Administrator
 * @description 针对表【monitor_rules】的数据库操作Service实现
 * @createDate 2026-04-15 12:02:28
 */
@Service
@Slf4j
public class MonitorRulesService {
    @Lazy
    @Resource
    private BinancePriceMonitor2 binancePriceMonitor2;

    private final MonitorRulesRepository monitorRulesRepository;

    private static final Pattern RULE_PATTERN =
            Pattern.compile("(?i)^([A-Z0-9]+)\\s*([><])\\s*([0-9]+(?:\\.[0-9]+)?)$");

    public MonitorRulesService(MonitorRulesRepository monitorRulesRepository) {
        this.monitorRulesRepository = monitorRulesRepository;
    }

    public Mono<String> saveOrUpdate(MonitorRules rules){
        return monitorRulesRepository.save(rules).flatMap(rules1 ->  Mono.just("ok"));
    }

    public Flux<MonitorRules> list(String openId){
        return monitorRulesRepository.listByOpenId(openId);
    }

    public Mono<String> addRule(WechatMsg msg, String content) {
        Matcher matcher = RULE_PATTERN.matcher(content);

        if (!matcher.matches()) {
            return Mono.just("❌ 格式错误。请发送: SOL > 90");
        }
        //解析命令
        String symbol = matcher.group(1).toUpperCase();
        String operator = matcher.group(2);
        String price = matcher.group(3);

        //构造 SpEL 表达式
        String spelExpression = String.format("price %s %s", operator, price);

        MonitorRules rule = new MonitorRules();
        rule.setOpenId(msg.getFromUserName());
        rule.setSymbol(symbol + "USDT");
        rule.setRuleName(symbol + " 价格预警");
        rule.setMarketType("SPOT");
        rule.setConditionExpression(spelExpression);
        rule.setActionType("PUSH_PLUS");
        rule.setTemplate(String.format("【行情预警】%s 当前价格已 %s %s", symbol, operator, price));
        rule.setIsEnabled(true);
        return monitorRulesRepository.save(rule).flatMap(monitorRules -> {
            log.info("成功为用户 {} 创建规则: {} {} {}", msg.getFromUserName(), symbol, content, price);
            binancePriceMonitor2.subscribeIfAbsent(rule.getSymbol());
            return Mono.just(String.format("✅ 监控已开启,id: %s\n币种: %s\n触发条件: %s %s\n价格达到后将通过公众号通知您。", rule.getId(), symbol, content, price));
        });
    }

    public Mono<String> delRule(long ruleId, String openid) {
        Mono<MonitorRules> monitorRulesMono = monitorRulesRepository.findById(ruleId);
        return monitorRulesMono.flatMap(monitorRules -> {
            if (!openid.equals(monitorRules.getOpenId())) {
                return Mono.just("无效id");
            }
            monitorRules.setIsEnabled(false);
            return monitorRulesRepository.save(monitorRules).flatMap(result->Mono.just("删除规则【" + ruleId + "】成功"));
        }).switchIfEmpty(Mono.just("无效id"));
    }
}




