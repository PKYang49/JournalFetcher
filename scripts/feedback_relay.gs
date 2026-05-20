/**
 * JournalFetcher — 週報精選回饋中繼 (Google Apps Script web app)
 * ============================================================
 * 週報 HTML 的「本週精選評讀」卡片上有 👍/👎 按鈕,點擊會 POST 到這支
 * web app,把一列回饋寫進綁定的 Google 試算表。你的 Mac 每週跑
 * run_weekly 前會用 token 驗證的 POST 把資料拉回 interest_feedback.jsonl。
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
 *     (doPost) 無法靠 token 保護。本 relay 會驗證 payload、用 week+pmid
 *     upsert、限制總列數；sync_feedback.py 也會用 PMID 白名單過濾掉亂寫
 *     的資料,降低被灌垃圾的影響。
 *   - 同步讀取使用 POST body 傳 token,token 只放在你 Mac 的 .env,不會
 *     出現在 HTML 或 URL query,所以外人無法把你的回饋讀走。
 */

const SHEET_NAME = 'feedback';
const SYNC_TOKEN = 'CHANGE_ME';  // 必須與 .env 的 FEEDBACK_SYNC_TOKEN 一致
const MAX_ROWS = 2000;
const KEEP_ROWS = 1000;
const MAX_DAILY_WRITES = 300;
const FEEDBACK_COLUMNS = ['ts', 'week', 'pmid', 'doi', 'journal', 'title', 'verdict', 'note'];

function _sheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
  }
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(FEEDBACK_COLUMNS);
  }
  return sheet;
}

function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function _text(value, maxLen) {
  return String(value || '').trim().slice(0, maxLen);
}

function _validateFeedback(d) {
  const week = _text(d.week, 12);
  const pmid = _text(d.pmid, 16);
  const verdict = _text(d.verdict, 8);
  if (!/^20\d{2}-W\d{2}$/.test(week)) {
    throw new Error('bad week');
  }
  if (!/^\d{6,12}$/.test(pmid)) {
    throw new Error('bad pmid');
  }
  if (verdict !== 'up' && verdict !== 'down') {
    throw new Error('bad verdict');
  }
  return {
    week: week,
    pmid: pmid,
    doi: _text(d.doi, 120),
    journal: _text(d.journal, 40),
    title: _text(d.title, 500),
    verdict: verdict,
    note: _text(d.note, 500),
  };
}

function _feedbackIndexByKey(sheet, week, pmid) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return 0;
  }
  const values = sheet.getRange(2, 2, lastRow - 1, 2).getValues();
  for (let i = values.length - 1; i >= 0; i--) {
    if (String(values[i][0]) === week && String(values[i][1]) === pmid) {
      return i + 2;
    }
  }
  return 0;
}

function _trimRows(sheet) {
  const lastRow = sheet.getLastRow();
  const maxLastRow = MAX_ROWS + 1;  // header + data rows
  if (lastRow <= maxLastRow) {
    return;
  }
  const keepLastRow = KEEP_ROWS + 1;
  const deleteCount = lastRow - keepLastRow;
  if (deleteCount > 0) {
    sheet.deleteRows(2, deleteCount);
  }
}

function _dailyWriteCount() {
  const props = PropertiesService.getScriptProperties();
  const today = Utilities.formatDate(new Date(), 'Etc/UTC', 'yyyy-MM-dd');
  const key = 'writes_' + today;
  const count = Number(props.getProperty(key) || '0') + 1;
  props.setProperty(key, String(count));
  return count;
}

function _upsertFeedback(d) {
  if (_dailyWriteCount() > MAX_DAILY_WRITES) {
    throw new Error('daily write limit exceeded');
  }
  const row = [
    new Date().toISOString(),
    d.week,
    d.pmid,
    d.doi,
    d.journal,
    d.title,
    d.verdict,
    d.note,
  ];
  const sheet = _sheet();
  const existingRow = _feedbackIndexByKey(sheet, d.week, d.pmid);
  if (existingRow) {
    sheet.getRange(existingRow, 1, 1, row.length).setValues([row]);
    return 'updated';
  }
  sheet.appendRow(row);
  _trimRows(sheet);
  return 'created';
}

/** 接收一筆回饋並 upsert 到試算表。瀏覽器以 text/plain 送 JSON 字串。 */
function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(20000);
  try {
    const d = JSON.parse(e.postData.contents);
    if (d.action === 'sync') {
      if (d.token !== SYNC_TOKEN) {
        return _json({ ok: false, error: 'unauthorized' });
      }
      return _feedbackRows();
    }

    const feedback = _validateFeedback(d);
    const status = _upsertFeedback(feedback);
    return _json({ ok: true, status: status });
  } catch (err) {
    return _json({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

/** 回傳整張回饋表。呼叫端必須已完成 token 驗證。 */
function _feedbackRows() {
  const values = _sheet().getDataRange().getValues();
  const header = values.shift();
  const rows = values.map(function (r) {
    const obj = {};
    header.forEach(function (h, i) { obj[h] = r[i]; });
    return obj;
  });
  return _json({ ok: true, rows: rows });
}

/** 不再用 GET 傳 token，避免 token 落在 URL query / access logs。 */
function doGet() {
  return _json({ ok: false, error: 'use POST' });
}
