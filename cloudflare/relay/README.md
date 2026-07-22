# Cloudflare weekly journal + relay

這個 Worker 同時提供 `docs/` 週報靜態資產與 `/api`，並用 D1 儲存回饋與評讀請求。整個 workers.dev 網址由 Cloudflare Access 保護，因此評讀確認頁不需要 passphrase。

評讀請求的 D1 lifecycle 為：

```text
requested → processing → done
                       ↘ failed → 使用者重新送出 → requested
                       ↘ deferred → 本機排程自動重試
```

週報會向 `GET /api?view=appraisal_statuses&week=<YYYY-Wxx>` 批次取得狀態：

- `requested` / `processing`：顯示「評讀中」。
- `deferred`：顯示「評讀中（等待重試）」。
- `failed`：顯示錯誤訊息與「重試請求評讀」。
- `done`：提示重新整理頁面查看已完成評讀。

本機 `weekly.process_appraisal_requests` 使用同步 token 呼叫
`action=update_appraisal_request` 回報 lifecycle。重新送出同一篇會更新 D1 的
request timestamp，本機據此辨識它是新 retry，不會被舊的 failed state 擋住。

## 一次性部署

```bash
cd cloudflare/relay
npm install
npx wrangler login
npx wrangler d1 create journal-fetcher
```

把輸出的 `database_id` 填入 `wrangler.toml`，並建立 schema 與同步 token：

```bash
npx wrangler d1 migrations apply journal-fetcher --remote
npx wrangler secret put SYNC_TOKEN
npx wrangler deploy
```

在 Cloudflare Dashboard 將 Production Worker URL 設為 Restricted，Access 應用程式設定兩條政策：

- Allow：只允許本人的 email。
- Service Auth：只允許本機同步用的 service token。

`wrangler.toml` 的 `TEAM_DOMAIN` 與 `POLICY_AUD` 用來在 Worker 內再次驗證 `Cf-Access-Jwt-Assertion`。

## 本機設定

```env
FEEDBACK_ENDPOINT_URL=https://<worker>.workers.dev/api
FEEDBACK_SYNC_TOKEN=<與 Worker SYNC_TOKEN 相同>
CF_ACCESS_CLIENT_ID=<service token client ID>
CF_ACCESS_CLIENT_SECRET=<service token client secret>
JOURNAL_FETCHER_PAGES_BASE_URL=https://<worker>.workers.dev
JOURNAL_FETCHER_CLOUDFLARE_DEPLOY=1
```

既有 HTML 已把舊 Apps Script URL 寫死。先預覽，再精確替換成 `/api`：

```bash
python3 scripts/migrate_feedback_endpoint.py \
  'https://script.google.com/macros/s/<id>/exec' \
  'https://<worker>.workers.dev/api'

python3 scripts/migrate_feedback_endpoint.py \
  'https://script.google.com/macros/s/<id>/exec' \
  'https://<worker>.workers.dev/api' --apply
```

確認新網址與 Discord 連結都正確後，才停用 Apps Script deployment。不要把 `SYNC_TOKEN`、service-token secret 或 Cloudflare API token 寫進 repository。

## 測試

```bash
cd cloudflare/relay
npm test
node --check src/index.js
npx wrangler deploy --dry-run
```
