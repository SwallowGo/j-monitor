package org.swallow.service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.swallow.entity.MonitorRules;
import org.swallow.record.PushPlusToRequest;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class PushPlusService {
    private final WebClient webClient = WebClient.create("http://www.pushplus.plus");
    @Value("${push-plus.token}")
    private String pushPlusToken;

    private static final String TOPIC = "conchbay";

    @Resource
    private UsersService usersService;


    /**
     * 发送微信告警
     *
     * @param title   告警标题（如：BTC 价格波动）
     * @param content 告警内容（支持 HTML）
     */
    public Mono<String> sendWechatAlert(String title, String content, MonitorRules rule) {
        return usersService.getToken(rule)
                .flatMap(token -> {
                    log.info("触发预警规则：{},token:{}",rule.getConditionExpression(),token);
                    PushPlusToRequest request = new PushPlusToRequest(
                            pushPlusToken,
                            title,
                            content,
                            token,
                            "html"
                    );

                    return send(request);
                })
                .switchIfEmpty(Mono.defer(() -> {
                    log.error("未找到 Token，取消推送");
                    return Mono.empty();
                }));


    }
    /**
     * 发送推送给固定token
     * @param title   标题
     * @param content 内容（支持 HTML 格式）
     * @return Mono<String> 返回 PushPlus 的响应结果
     */
    public Mono<String> send(String title, String content) {
        PushPlusToRequest request = new PushPlusToRequest(
                pushPlusToken,
                title,
                content,
                null,
                "html"
        );

        return send(request);
    }

    public Mono<String> send(PushPlusToRequest request){
        return webClient.post()
                .uri("/send")
                .bodyValue(request)
                .retrieve()
                .bodyToMono(String.class)
                .doOnSuccess(response -> log.info("PushPlus 响应: {}", response))
                .doOnError(e -> log.info("推送失败: {}", e.getMessage()))
                .map(response -> "success")
                .onErrorReturn("fail");
    }
}
