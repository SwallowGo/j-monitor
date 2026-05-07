package org.swallow.service;

import org.springframework.stereotype.Service;
import org.swallow.entity.MonitorRules;
import org.swallow.repository.UsersRepository;
import reactor.core.publisher.Mono;

/**
 * @author lzy
 */
@Service
public class UsersService {

    private final UsersRepository usersRepository;

    public UsersService(UsersRepository usersRepository) {
        this.usersRepository = usersRepository;
    }

    public Mono<String> bindPushToken(String openid, String token) {
        return usersRepository.findByPushToken(token)
                // 1. 如果用户存在 (Mono 不为空)，进入 flatMap 执行更新
                .flatMap(user -> {
                    user.setOpenid(openid);

                    return usersRepository.save(user)
                            .map(savedUser -> "绑定成功！用户：" + savedUser.getOpenid());
                })
                // 2. 如果用户不存在 (Mono 为空)，执行 switchIfEmpty 里的逻辑
                .switchIfEmpty(Mono.just("用户不存在，请先联系管理员。"));
    }

    public Mono<String> getToken(MonitorRules rule) {
        return usersRepository.findByOpenid(rule.getOpenId())
                .flatMap(users -> Mono.just(users.getPushToken())).
                switchIfEmpty(Mono.just(""));
    }
}




