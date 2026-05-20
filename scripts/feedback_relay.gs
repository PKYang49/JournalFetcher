/**
 * JournalFetcher — 週報精選回饋中繼 (Google Apps Script web app)
 * ============================================================
 * 週報 HTML 的「本週精選評讀」卡片上有 👍/👎 按鈕,點擊會 POST 到這支
 * web app,把一列回饋寫進綁定的 Google 試算表。你的 Mac 每週跑
 * run_weekly 前會用 token 驗證的 GET 把資料拉回 interest_feedback.jsonl。
 *
 * 部署步驟(一次性):
 *   1. 開一份新的 Google 試算表。
 *   2. 擴充功能 → Apps Script,把這整個檔案內容貼進 Code.gs。
 *   3. 把下面的 SYNC_TOKEN 改成一串自訂亂碼(例如用密碼產生器產 32 字元)。
 *   4. 部署 → 新增部署作業 → 類型選「網頁應用程式」:
 *        - 執行身分:我
 *        - 具有存取權的使用者:所有人
 *   5. 複製產生的 /exec 網址。
 *   6. 在專案 .env 填:
 *        FEEDBACK_ENDPOINT_URL=<剛剛複製的 /exec 網址>
 *        FEEDBACK_SYNC_TOKEN=<與下方 SYNC_TOKEN 完全相同的字串>
 *
 * 安全性:
 *   - /exec 網址會被嵌進公開的 GitHub Pages HTML,任何人都看得到 → 寫入
 *     (doPost) 無法靠 token 保護。sync_feedback.py 會用「曾被選為精選的
 *     PMID 白名單」過濾掉亂寫的資料,降低被灌垃圾的影響。
 *   - 讀取 (doGet) 需要 token,token 只放在你 Mac 的 .env,不會出現在
 *     HTML,所以外人無法把你的回饋讀走。
 */

const SHEET_NAME = 'feedback';
const SYNC_TOKEN = 'CHANGE_ME';  // 必須與 .env 的 FEEDBACK_SYNC_TOKEN 一致

function _sheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
  }
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(['ts', 'week', 'pmid', 'doi', 'journal', 'title', 'verdict', 'note']);
  }
  return sheet;
}

function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

/** 接收一筆回饋並 append 到試算表。瀏覽器以 text/plain 送 JSON 字串。 */
function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(20000);
  try {
    const d = JSON.parse(e.postData.contents);
    const verdict = (d.verdict === 'up' || d.verdict === 'down') ? d.verdict : '';
    if (!verdict || !d.pmid) {
      return _json({ ok: false, error: 'bad payload' });
    }
    _sheet().appendRow([
      new Date().toISOString(),
      String(d.week || ''),
      String(d.pmid || ''),
      String(d.doi || ''),
      String(d.journal || ''),
      String(d.title || ''),
      verdict,
      String(d.note || ''),
    ]);
    return _json({ ok: true });
  } catch (err) {
    return _json({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

/** 回傳整張回饋表(需 token)。供 weekly/sync_feedback.py 拉資料。 */
function doGet(e) {
  if (!e || !e.parameter || e.parameter.token !== SYNC_TOKEN) {
    return _json({ ok: false, error: 'unauthorized' });
  }
  const values = _sheet().getDataRange().getValues();
  const header = values.shift();
  const rows = values.map(function (r) {
    const obj = {};
    header.forEach(function (h, i) { obj[h] = r[i]; });
    return obj;
  });
  return _json({ ok: true, rows: rows });
}
