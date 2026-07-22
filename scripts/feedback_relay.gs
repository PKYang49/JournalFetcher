/**
 * JournalFetcher — 舊版週報精選回饋中繼 (Google Apps Script web app)
 * 正式服務已移至 Cloudflare Worker + D1；此檔僅保留遷移參考。
 * ============================================================
 * 週報 HTML 的回饋按鈕會 POST 到這支 web app。「文獻評讀」連結會
 * 開啟這支 web app 產生的 passphrase 驗證頁；驗證通過後才把評讀
 * 請求寫進綁定的 Google 試算表。你的 Mac 會用 token 驗證的 POST
 * 把資料拉回本地處理。
 *
 * 部署步驟(一次性):
 *   1. 開一份新的 Google 試算表。
 *   2. 擴充功能 → Apps Script,把這整個檔案內容貼進 Code.gs。
 *   3. 把下面的 SYNC_TOKEN 改成一串自訂亂碼(例如用密碼產生器產 32 字元)。
 *   4. 專案設定 → 指令碼屬性,新增:
 *        APPRAISAL_PASSPHRASE=<手機要求評讀時輸入的長密語>
 *   5. 部署 → 新增部署作業 → 類型選「網頁應用程式」:
 *        - 執行身分:我
 *        - 具有存取權的使用者:所有人
 *   6. 複製產生的 /exec 網址。
 *   7. 在專案 .env 填:
 *        FEEDBACK_ENDPOINT_URL=<剛剛複製的 /exec 網址>
 *        FEEDBACK_SYNC_TOKEN=<與下方 SYNC_TOKEN 完全相同的字串>
 *
 * 安全性:
 *   - /exec 網址若被嵌進公開 HTML，任何人都看得到。回饋
 *     寫入仍不驗證,但評讀請求必須輸入 server-side passphrase 才能寫入。
 *   - 本 relay 會驗證 payload、用 week+pmid 或 week+doi upsert、限制總列數；
 *     sync_feedback.py 也會用 PMID/DOI 白名單過濾掉亂寫的資料。
 *   - 同步讀取使用 POST body 傳 token,token 只放在你 Mac 的 .env,不會
 *     出現在 HTML 或 URL query,所以外人無法把你的回饋讀走。
 */

const FEEDBACK_SHEET_NAME = 'feedback';
const APPRAISAL_SHEET_NAME = 'appraisal_requests';
const SYNC_TOKEN = 'CHANGE_ME';  // 必須與 .env 的 FEEDBACK_SYNC_TOKEN 一致
const APPRAISAL_PASSPHRASE_PROPERTY = 'APPRAISAL_PASSPHRASE';
const MAX_ROWS = 2000;
const KEEP_ROWS = 1000;
const MAX_DAILY_WRITES = 300;
const FEEDBACK_COLUMNS = ['ts', 'week', 'pmid', 'doi', 'journal', 'title', 'verdict', 'note'];
const APPRAISAL_COLUMNS = ['ts', 'week', 'pmid', 'doi', 'journal', 'title', 'status', 'note'];

function _sheetByName(name, columns) {
  if (!name) {
    throw new Error('_sheetByName requires a sheet name. Run setup() instead of helper functions.');
  }
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new Error('_sheetByName requires nonempty columns for sheet: ' + name);
  }
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
  }
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(columns);
  }
  return sheet;
}

function setup() {
  _feedbackSheet();
  _appraisalSheet();
  return 'ok';
}

function _feedbackSheet() {
  return _sheetByName(FEEDBACK_SHEET_NAME, FEEDBACK_COLUMNS);
}

function _appraisalSheet() {
  return _sheetByName(APPRAISAL_SHEET_NAME, APPRAISAL_COLUMNS);
}

function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function _html(title, body) {
  const page = '<!doctype html><html lang="zh-Hant"><head>'
    + '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    + '<title>' + _escapeHtml(title) + '</title>'
    + '<style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang TC","Microsoft JhengHei",sans-serif;'
    + 'background:#fafafa;color:#1a1a1a;margin:0;padding:24px 16px;line-height:1.65}'
    + 'main{max-width:640px;margin:0 auto;background:#fff;border:1px solid #e5e5e5;border-radius:8px;padding:22px}'
    + 'h1{font-size:22px;margin:0 0 12px}.meta{color:#666;font-size:14px;margin:8px 0 16px}'
    + 'label{display:block;font-weight:600;margin:14px 0 6px}input{box-sizing:border-box;width:100%;font:inherit;'
    + 'padding:10px;border:1px solid #ccc;border-radius:6px}button{font:inherit;background:#0b5fff;color:#fff;'
    + 'border:0;border-radius:6px;padding:9px 14px;margin-top:16px}a{color:#0b5fff;text-decoration:none}'
    + '@media(prefers-color-scheme:dark){body{background:#1a1a1a;color:#e8e8e8}main{background:#242424;border-color:#333}'
    + '.meta{color:#999}input{background:#1a1a1a;color:#e8e8e8;border-color:#444}}</style></head><body><main>'
    + body + '</main></body></html>';
  return HtmlService.createHtmlOutput(page).setTitle(title);
}

function _escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function _text(value, maxLen) {
  return String(value || '').trim().slice(0, maxLen);
}

function _appraisalPassphrase() {
  return PropertiesService.getScriptProperties().getProperty(APPRAISAL_PASSPHRASE_PROPERTY) || '';
}

function _verifyAppraisalPassphrase(value) {
  const expected = _appraisalPassphrase();
  if (!expected || expected === 'CHANGE_ME') {
    throw new Error('appraisal passphrase not configured');
  }
  if (String(value || '') !== expected) {
    throw new Error('unauthorized');
  }
}

function _validateFeedback(d) {
  const week = _text(d.week, 12);
  const pmid = _text(d.pmid, 16);
  const doi = _text(d.doi, 120);
  const verdict = _text(d.verdict, 8);
  if (!/^20\d{2}-W\d{2}$/.test(week)) {
    throw new Error('bad week');
  }
  if (!pmid && !doi) {
    throw new Error('missing identifier');
  }
  if (pmid && !/^\d{6,12}$/.test(pmid)) {
    throw new Error('bad pmid');
  }
  if (doi && !/^10\.\S+\/\S+/.test(doi)) {
    throw new Error('bad doi');
  }
  if (verdict !== 'up' && verdict !== 'down') {
    throw new Error('bad verdict');
  }
  return {
    week: week,
    pmid: pmid,
    doi: doi,
    journal: _text(d.journal, 40),
    title: _text(d.title, 500),
    verdict: verdict,
    note: _text(d.note, 500),
  };
}

function _validateAppraisalRequest(d) {
  const week = _text(d.week, 12);
  const pmid = _text(d.pmid, 16);
  const doi = _text(d.doi, 120);
  if (!/^20\d{2}-W\d{2}$/.test(week)) {
    throw new Error('bad week');
  }
  if (!pmid && !doi) {
    throw new Error('missing identifier');
  }
  if (pmid && !/^\d{6,12}$/.test(pmid)) {
    throw new Error('bad pmid');
  }
  if (doi && !/^10\.\S+\/\S+/.test(doi)) {
    throw new Error('bad doi');
  }
  return {
    week: week,
    pmid: pmid,
    doi: doi,
    journal: _text(d.journal, 40),
    title: _text(d.title, 500),
    status: 'requested',
    note: _text(d.note, 500),
  };
}

function _indexByKey(sheet, week, pmid) {
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

function _feedbackIndexByKey(sheet, week, pmid, doi) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return 0;
  }
  const values = sheet.getRange(2, 2, lastRow - 1, 3).getValues();
  const keyPmid = String(pmid || '');
  const keyDoi = String(doi || '').toLowerCase();
  for (let i = values.length - 1; i >= 0; i--) {
    const rowWeek = String(values[i][0]);
    const rowPmid = String(values[i][1] || '');
    const rowDoi = String(values[i][2] || '').toLowerCase();
    if (rowWeek !== week) {
      continue;
    }
    if (keyPmid && rowPmid === keyPmid) {
      return i + 2;
    }
    if (!keyPmid && keyDoi && rowDoi === keyDoi) {
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
  const sheet = _feedbackSheet();
  const existingRow = _feedbackIndexByKey(sheet, d.week, d.pmid, d.doi);
  if (existingRow) {
    sheet.getRange(existingRow, 1, 1, row.length).setValues([row]);
    return 'updated';
  }
  sheet.appendRow(row);
  _trimRows(sheet);
  return 'created';
}

function _upsertAppraisalRequest(d) {
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
    d.status,
    d.note,
  ];
  const sheet = _appraisalSheet();
  const existingRow = _appraisalIndexByKey(sheet, d.week, d.pmid, d.doi);
  if (existingRow) {
    sheet.getRange(existingRow, 1, 1, row.length).setValues([row]);
    return 'updated';
  }
  sheet.appendRow(row);
  _trimRows(sheet);
  return 'created';
}

function _appraisalIndexByKey(sheet, week, pmid, doi) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return 0;
  }
  const values = sheet.getRange(2, 2, lastRow - 1, 3).getValues();
  const keyPmid = String(pmid || '');
  const keyDoi = String(doi || '').toLowerCase();
  for (let i = values.length - 1; i >= 0; i--) {
    const rowWeek = String(values[i][0]);
    const rowPmid = String(values[i][1] || '');
    const rowDoi = String(values[i][2] || '').toLowerCase();
    if (rowWeek !== week) {
      continue;
    }
    if (keyPmid && rowPmid === keyPmid) {
      return i + 2;
    }
    if (!keyPmid && keyDoi && rowDoi === keyDoi) {
      return i + 2;
    }
  }
  return 0;
}

function _postPayload(e) {
  const raw = e && e.postData && e.postData.contents ? e.postData.contents : '';
  const trimmed = String(raw).trim();
  if (trimmed.charAt(0) === '{') {
    return JSON.parse(trimmed);
  }
  const payload = e.parameter || {};
  const actions = e && e.parameters && e.parameters.action ? e.parameters.action : [];
  if (Array.isArray(actions) && actions.indexOf('request_appraisal') !== -1) {
    payload.action = 'request_appraisal';
  }
  return payload;
}

function _appraisalForm(p) {
  const request = _validateAppraisalRequest(p);
  const postUrl = ScriptApp.getService().getUrl();
  const identifier = request.pmid ? 'PMID ' + request.pmid : request.doi;
  const body = ''
    + '<h1>要求文獻評讀</h1>'
    + '<div class="meta">' + _escapeHtml(request.week) + ' · ' + _escapeHtml(identifier) + '</div>'
    + '<p>' + _escapeHtml(request.title || '(no title)') + '</p>'
    + '<form method="post" action="' + _escapeHtml(postUrl) + '" target="_top">'
    + '<input type="hidden" name="action" value="request_appraisal">'
    + '<input type="hidden" name="week" value="' + _escapeHtml(request.week) + '">'
    + '<input type="hidden" name="pmid" value="' + _escapeHtml(request.pmid) + '">'
    + '<input type="hidden" name="doi" value="' + _escapeHtml(request.doi) + '">'
    + '<input type="hidden" name="journal" value="' + _escapeHtml(request.journal) + '">'
    + '<input type="hidden" name="title" value="' + _escapeHtml(request.title) + '">'
    + '<label for="passphrase">Passphrase</label>'
    + '<input id="passphrase" name="passphrase" type="password" autocomplete="current-password" autofocus required>'
    + '<button type="submit">送出評讀請求</button>'
    + '</form>';
  return _html('要求文獻評讀', body);
}

function _appraisalResultPage(request, status) {
  const identifier = request.pmid ? 'PMID ' + request.pmid : request.doi;
  const body = ''
    + '<h1>已送出評讀請求</h1>'
    + '<div class="meta">' + _escapeHtml(request.week) + ' · ' + _escapeHtml(identifier) + ' · ' + _escapeHtml(status) + '</div>'
    + '<p>' + _escapeHtml(request.title || '(no title)') + '</p>'
    + '<p>本機完成下載與文獻評讀後，會用 Discord 傳送完整評讀連結。</p>';
  return _html('已送出評讀請求', body);
}

/** 接收一筆回饋或已驗證的評讀請求。 */
function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(20000);
  try {
    const d = _postPayload(e);
    if (d.action === 'sync') {
      if (d.token !== SYNC_TOKEN) {
        return _json({ ok: false, error: 'unauthorized' });
      }
      return _feedbackRows();
    }
    if (d.action === 'sync_appraisal_requests') {
      if (d.token !== SYNC_TOKEN) {
        return _json({ ok: false, error: 'unauthorized' });
      }
      return _appraisalRows();
    }
    if (d.action === 'request_appraisal') {
      _verifyAppraisalPassphrase(d.passphrase);
      const request = _validateAppraisalRequest(d);
      const status = _upsertAppraisalRequest(request);
      return _appraisalResultPage(request, status);
    }

    const feedback = _validateFeedback(d);
    const status = _upsertFeedback(feedback);
    return _json({ ok: true, status: status });
  } catch (err) {
    const p = e && e.parameter ? e.parameter : {};
    const actions = e && e.parameters && e.parameters.action ? e.parameters.action : [];
    if (p.action === 'request_appraisal' || (Array.isArray(actions) && actions.indexOf('request_appraisal') !== -1)) {
      return _html(
        '評讀請求失敗',
        '<h1>評讀請求失敗</h1><p>' + _escapeHtml(String(err)) + '</p>'
          + '<p><a href="javascript:history.back()">返回重試</a></p>'
      );
    }
    return _json({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

/** 回傳整張回饋表。呼叫端必須已完成 token 驗證。 */
function _feedbackRows() {
  const values = _feedbackSheet().getDataRange().getValues();
  const header = values.shift();
  const rows = values.map(function (r) {
    const obj = {};
    header.forEach(function (h, i) { obj[h] = r[i]; });
    return obj;
  });
  return _json({ ok: true, rows: rows });
}

/** 回傳整張評讀請求表。呼叫端必須已完成 token 驗證。 */
function _appraisalRows() {
  const values = _appraisalSheet().getDataRange().getValues();
  const header = values.shift();
  const rows = values.map(function (r) {
    const obj = {};
    header.forEach(function (h, i) { obj[h] = r[i]; });
    return obj;
  });
  return _json({ ok: true, rows: rows });
}

/** 不再用 GET 傳 token，避免 token 落在 URL query / access logs。 */
function doGet(e) {
  try {
    const p = e && e.parameter ? e.parameter : {};
    if (p.view === 'appraise' || p.action === 'appraise') {
      return _appraisalForm(p);
    }
  } catch (err) {
    return _html('無法建立評讀請求', '<h1>無法建立評讀請求</h1><p>' + _escapeHtml(String(err)) + '</p>');
  }
  return _json({ ok: false, error: 'use POST' });
}
