# j-monitor

**monitor** 是一個基于 **Java 21** 和 **Spring Boot 3.x** 構建的高性能響應式（Reactive）加密貨幣行情監控系統。

[![Java Version](https://img.shields.io/badge/Java-21-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-brightgreen.svg)](https://spring.io/projects/spring-boot)

## 🌟 項目亮點
本項目不僅是一個行情監控工具，更是對 **Project Reactor** 響應式編程在長連接場景下的深度實踐。
- **異步非阻塞架構**：全鏈路基於 Spring WebFlux，在高併發行情推送下保持低內存佔用。
- **自癒機制 (Self-Healing)**：利用 `.timeout()` 與 `.retryWhen()` 算子實現心跳監控，在 WebSocket 斷流時自動重連並重新訂閱。
- **背壓控制 (Backpressure)**：有效處理行情波動時的數據積壓問題，確保系統穩定。
- **自動化告警**：集成 PushPlus 微信通知插件，實時推送異常監控狀態與價格預警。

## 🏗️ 技術架構
- **核心框架**: Spring Boot 3.2.5 (WebFlux)
- **響應式庫**: Project Reactor
- **網絡通信**: Reactor Netty (WebSocketClient)
- **數據處理**: 流式處理管道 (Stream Pipeline)
- **日誌管理**: Logback (支持 Docker 掛載與滾動清理)



## 🚀 快速開始

### 環境變量配置
為了安全起見，本項目不硬編碼任何密鑰。請在運行環境中配置以下變量：
- `PUSHPLUS_TOKEN`: 你的 PushPlus 令牌
- `BINANCE_WS_URL`: 幣安 WebSocket 入口 (默認: `wss://stream.binance.com:9443/ws`)

### 使用 Docker 部署
1. 編譯項目：
   ```bash
   mvn clean package -DskipTests