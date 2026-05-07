package org.swallow.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * 监控规则
 */
@Table(value ="monitor_rules")
@Data
public class MonitorRules implements Serializable {
    @Id
    private Long id;

    private String ruleName;

    private String symbol;

    private Object marketType;

    private String conditionExpression;

    private String actionType;

    private String template;

    private Boolean isEnabled;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    private String openId;


}