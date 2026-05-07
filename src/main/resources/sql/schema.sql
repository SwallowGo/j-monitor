-- 用户绑定信息表
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    openid VARCHAR(64) NULL UNIQUE COMMENT '微信用户唯一标识',
    push_token VARCHAR(128) DEFAULT NULL COMMENT 'PushPlus推送Token',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 监控规则表
CREATE TABLE IF NOT EXISTS monitor_rules (
                                             id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                             openid VARCHAR(64) NOT NULL COMMENT '所属用户OpenID',
    symbol VARCHAR(20) NOT NULL COMMENT '币种, 如SOL, BTC',
    price_condition VARCHAR(10) NOT NULL COMMENT '条件, > 或 <',
    target_price DECIMAL(20, 8) NOT NULL COMMENT '目标价格',
    status TINYINT DEFAULT 1 COMMENT '状态: 1-监控中, 0-已触发/删除',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_openid (openid),
    INDEX idx_status_symbol (status, symbol)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;