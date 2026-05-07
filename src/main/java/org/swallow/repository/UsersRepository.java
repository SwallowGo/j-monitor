package org.swallow.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.swallow.entity.Users;
import reactor.core.publisher.Mono;

public interface UsersRepository extends ReactiveCrudRepository<Users, Long> {
    // 响应式查询：根据 openid 查找用户
    Mono<Users> findByOpenid(String openid);
    Mono<Users> findByPushToken(String pushToken);
}
