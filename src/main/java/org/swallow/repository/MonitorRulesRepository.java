package org.swallow.repository;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.swallow.entity.MonitorRules;
import reactor.core.publisher.Flux;

public interface MonitorRulesRepository extends ReactiveCrudRepository<MonitorRules, Long> {

    @Query("SELECT * FROM monitor_rules WHERE (:openId IS NULL OR open_id = :openId) and is_enabled = 1")
    Flux<MonitorRules> listByOpenId(String openId);
}
