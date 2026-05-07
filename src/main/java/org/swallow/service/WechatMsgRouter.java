package org.swallow.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.swallow.dto.WechatMsg;
import org.swallow.entity.MonitorRules;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
public class WechatMsgRouter {
    private final UsersService userService;
    private final MonitorRulesService monitorRulesService;

    // 正则表达式：解析 [币种] [>|<] [价格]
    private static final Pattern PRICE_PATTERN =
            Pattern.compile("(?i)^([A-Z0-9]+)\\s*([><])\\s*([0-9]+(?:\\.[0-9]+)?)$");

    private static final Pattern DEL_PATTERN = Pattern.compile("(?i)^del\\s+(\\d+)$");

    public WechatMsgRouter(UsersService userService, MonitorRulesService monitorRulesService) {
        this.userService = userService;
        this.monitorRulesService = monitorRulesService;
    }

    /**
     * 核心路由方法
     */
    public Mono<String> route(WechatMsg msg) {
        String content = msg.getContent().trim();
        String openid = msg.getFromUserName();

        // 1. 路由逻辑：匹配绑定 Token 指令 (格式如 bind:PPTK...)
        if (content.toLowerCase().startsWith("bind:")) {
            String token = content.substring(5).trim();
            return userService.bindPushToken(openid, token);
        }

        // 2. 路由逻辑：匹配价格监控规则 (格式如 BTC > 60000)
        if (PRICE_PATTERN.matcher(content).matches()) {
            return monitorRulesService.addRule(msg, content);
        }

        if ("list".equalsIgnoreCase(content)) {
            Flux<MonitorRules> list = monitorRulesService.list(openid);
            return list.map(o -> o.getId() + "   " + o.getConditionExpression().replace("price", o.getSymbol()))
                    .collect(Collectors.joining("\n"));
        }
        Matcher matcher = DEL_PATTERN.matcher(content);
        if (matcher.matches()) {
            long ruleId = Long.parseLong(matcher.group(1));
            return monitorRulesService.delRule(ruleId, openid);
        }

        // 3. 兜底逻辑：无法识别的指令
        return Mono.just("""
                💡 指令帮助：
                1️⃣ 绑定通知：发送 bind:你的Token
                2️⃣ 添加监控：发送 币种 > 价格 (如 SOL > 95)
                3️⃣ 查询规则：发送 list""");
    }
}
