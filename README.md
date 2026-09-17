# DLP Discord Bot Worker

這個 Bot 與網站分開執行，兩邊透過同一個 PostgreSQL / Neon `DATABASE_URL` 交換工作。

Bot 目前負責：
1. 表單送出：移除市民身分（若有）、給予待面試身分，並私訊「已收到申請，請耐心等候」。
2. 入幫審核通過：移除待面試與市民身分、給予門生、Discord 暱稱改成 `DLP.原始Discord名稱`，並私訊錄取通知。
3. 入幫審核駁回：移除待面試、給予市民，並私訊駁回原因。
4. 改名審核通過：Discord 暱稱同步成 `DLP.改後名稱`。
5. 階級調整：移除舊 DLP 階級身分組，只保留新階級身分組。
6. 手動踢出 / 違規自動踢出 / 黑名單除名：移除全部 DLP 階級與待面試身分、給予市民，並私訊實際除名原因。

所有 Token / Guild ID / Role ID / DB URL / 輪詢設定都只從 `.env` / Render Environment 讀取。

## 網站狀態顯示

Bot 會獨立偵測 DLP 網站，不是偵測 Bot 自己。預設每 60 秒請求：

`https://dlpweb.onrender.com/api/bot-health`

網站正常時建議回傳：

```json
{"status":"online","online":12,"offline":8}
```

Discord Bot 狀態顯示：
- 正常：`DLP正常｜12人在線`
- 維護：`DLP維護中`
- 429 / 4xx / 5xx / timeout / 無法連線：`DLP異常｜稍後再試`

### 維護模式指令

維護模式不再由網站 API 自動切換，也不需要去 Render 修改環境變數。

使用 Discord Slash Command：

`/maintenance`

可選：
- `🟡 開啟維護模式`
- `🟢 關閉維護模式`
- `🔎 查看目前狀態`

只有具備「管理伺服器（Manage Server）」權限的人可操作。維護狀態會寫入 PostgreSQL 的 `discord_bot_runtime_settings`，所以 Bot 重啟或 Render 重新部署後仍會保留。

關閉維護模式後，Bot 會立即重新檢查網站，不需等待下一個 60 秒輪詢。

可用 Render Environment 調整：
- `DLP_WEBSITE_STATUS_URL`
- `DLP_WEBSITE_CHECK_INTERVAL`
- `DLP_WEBSITE_TIMEOUT`

`DLP_MAINTENANCE_MODE` 僅保留做第一次建立設定時的初始 fallback，平常請直接使用 `/maintenance`。
