package org.swallow.controller;

import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.json.JSONUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.swallow.dto.WechatMsg;
import org.swallow.service.MonitorRulesService;
import org.swallow.service.WechatMsgRouter;
import org.swallow.util.XmlUtils;
import reactor.core.publisher.Mono;

import java.util.Arrays;

@RestController
@RequestMapping("/api/v1/wc")
@Slf4j
public class WechatCheckController {
    @Value("${wechat.token}")
    private String token;
    @Resource
    private WechatMsgRouter wechatMsgRouter;

    @GetMapping("/check")
    public Mono<String> create(@RequestParam String signature,
                               @RequestParam String timestamp,
                               @RequestParam String nonce,
                               @RequestParam String echostr) {
        log.info("接收到参数,signature:{}", signature);
        log.info("接收到参数,timestamp:{}", timestamp);
        log.info("接收到参数,nonce:{}", nonce);
        log.info("接收到参数,echostr:{}", echostr);
        String[] strings = {timestamp, nonce, token};
        Arrays.sort(strings);
        StringBuilder builder = new StringBuilder();
        for (String string : strings) {
            builder.append(string);
        }
        String toCompare = builder.toString();
        log.info("拼接得到：{}", toCompare);
        String s = DigestUtil.sha1Hex(toCompare);

        boolean result = s.equals(signature);
        log.info(s);
        log.info(signature);
        log.info("比较结果：{}", result);
        if (result) {
            return Mono.just(echostr);
        }
        return Mono.just("Success");
    }

    @PostMapping("/check")
    public Mono<String> create(@RequestBody String body) {
        try {
            WechatMsg msg = XmlUtils.toBean(body, WechatMsg.class);
            log.info("手动解析成功，内容: {}", JSONUtil.toJsonStr(msg));
            return wechatMsgRouter.route(msg).flatMap(s -> Mono.just(buildReplyXml(msg, s)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 构造被动回复 XML
     */
    public static String buildReplyXml(WechatMsg msg, String text) {
        return String.format(
                "<xml>" +
                        "<ToUserName><![CDATA[%s]]></ToUserName>" +
                        "<FromUserName><![CDATA[%s]]></FromUserName>" +
                        "<CreateTime>%d</CreateTime>" +
                        "<MsgType><![CDATA[text]]></MsgType>" +
                        "<Content><![CDATA[%s]]></Content>" +
                        "</xml>",
                // 接收者是刚才的发起人
                msg.getFromUserName(),
                // 发送者是公众号原始ID
                msg.getToUserName(),
                System.currentTimeMillis() / 1000,
                text
        );
    }
}
