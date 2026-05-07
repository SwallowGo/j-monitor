package org.swallow.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/rules")
@Slf4j
public class RuleController {

    @PostMapping("/add")
    public Mono<String> create(@RequestBody String body) {
        log.info("接收到规则设置请求:{}", body);
        return Mono.just("Success");
    }

}
