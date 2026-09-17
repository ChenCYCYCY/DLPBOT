# DLP Bot 429 狀態機制

當 `/api/bot-health` 回 HTTP 429，或 JSON 回報 Discord OAuth 為 429 / rate_limited / circuit_open 時：

1. Bot 不關閉。
2. Discord 狀態立即改成：`🟠 DLP系統限流受限｜等待60分`
3. 進入 60 分鐘冷卻，冷卻期間停止一般網站健康檢查。
4. 每 60 秒更新一次 Discord 狀態：59、58、57...1 分。
5. 冷卻結束後立即恢復網站健康檢查。
6. 若仍偵測到 429，重新開始新的 60 分鐘冷卻。
7. 若已恢復正常，狀態自動回到 DLP 正常狀態。
8. 手動維護模式仍具有最高優先權。

## Bot 如何得知「有人收到 429」

Bot 不會自行呼叫 Discord OAuth，以免增加限流。網站的 `/api/bot-health` 必須在 OAuth Circuit Breaker 開啟時回報 429 狀態，例如：

```json
{
  "status": "online",
  "oauth": "rate_limited",
  "oauth_status": 429
}
```

也支援健康端點本身直接回 HTTP 429。
