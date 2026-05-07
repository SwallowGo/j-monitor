package org.swallow.record;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceTickerDTO {
    /**
     * 事件类型: 24hrTicker
     */
    @JsonProperty("e")
    private String eventType;

    /**
     * 事件时间
     */
    @JsonProperty("E")
    private Long eventTime;

    /**
     * 交易对: SOLUSDT
     */
    @JsonProperty("s")
    private String symbol;

    /**
     * 24小时价格变化
     */
    @JsonProperty("p")
    private String priceChange;

    /**
     * 24小时价格变化百分比
     */
    @JsonProperty("P")
    private String priceChangePercent;

    /**
     * 最新成交价格（核心监控字段）
     */
    @JsonProperty("c")
    private String lastPrice;  

    /**
     * 24小时最高价
     */
    @JsonProperty("h")
    private String highPrice;  

    /**
     * 24小时最低价
     */
    @JsonProperty("l")
    private String lowPrice;   

    /**
     * 24小时成交量
     */
    @JsonProperty("v")
    private String totalVolume;

}
