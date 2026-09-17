# DLP Bot - Discord OAuth 429 狀態

Bot 會持續監控 `DLP_WEBSITE_STATUS_URL`（預設 `/api/bot-health`）。

## 狀態

- 正常：`🟢DLP正常｜X人在線🟢`
- Discord OAuth 429：`🟠DLP登入受限｜429🟠`
- 網站異常：`🔴DLP系統異常🔴`
- 手動維護：`🟡DLP系統維護中🟡`

429 不會關閉 Bot、不會停止 PostgreSQL 工作佇列，也不會停止維護指令。

## 網站建議回傳格式

正常：

```json
{
  "status": "online",
  "oauth": "ok",
  "online": 10,
  "offline": 5
}
```

Discord OAuth 被限流：

```json
{
  "status": "online",
  "oauth": "rate_limited",
  "oauth_status": 429,
  "retry_after": 3600,
  "online": 10,
  "offline": 5
}
```

Bot 也支援 nested `oauth` 物件，以及 `circuit_open` 等相容狀態名稱。
