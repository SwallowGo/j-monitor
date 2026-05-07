package org.swallow.entity;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.Date;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * 
 * @TableName users
 */
@Table(value ="users")
@Data
public class Users implements Serializable {
    /**
     * 
     */
    @Id
    private Long id;

    /**
     * 微信用户唯一标识
     */
    private String openid;

    /**
     * PushPlus推送Token
     */
    private String pushToken;

    /**
     * 
     */
    private LocalDateTime updateTime;

    /**
     * 
     */
    private LocalDateTime createTime;

}