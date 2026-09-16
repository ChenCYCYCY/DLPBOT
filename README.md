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
