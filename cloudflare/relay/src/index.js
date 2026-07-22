import { createRemoteJWKSet, jwtVerify } from "jose";

const WEEK_RE = /^20\d{2}-W\d{2}$/;
const PMID_RE = /^\d{6,12}$/;
const DOI_RE = /^10\.\S+\/\S+/;
const APPRAISAL_STATUSES = new Set([
  "requested", "processing", "deferred", "failed", "done",
]);

class HttpError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}

let accessKeySet;
let accessKeySetUrl;

function text(value, maxLength) {
  return String(value ?? "").trim().slice(0, maxLength);
}

function normalizeDoi(value) {
  return text(value, 120)
    .replace(/^https?:\/\/(?:dx\.)?doi\.org\//i, "")
    .replace(/\.$/, "")
    .toLowerCase();
}

function validateIdentifier(payload) {
  const week = text(payload.week, 12);
  const pmid = text(payload.pmid, 16);
  const doi = normalizeDoi(payload.doi);
  if (!WEEK_RE.test(week)) {
    throw new HttpError("bad week");
  }
  if (!pmid && !doi) {
    throw new HttpError("missing identifier");
  }
  if (pmid && !PMID_RE.test(pmid)) {
    throw new HttpError("bad pmid");
  }
  if (doi && !DOI_RE.test(doi)) {
    throw new HttpError("bad doi");
  }
  return { week, pmid, doi, identifier: pmid ? `pmid:${pmid}` : `doi:${doi}` };
}

function validateFeedback(payload) {
  const base = validateIdentifier(payload);
  const verdict = text(payload.verdict, 8);
  if (verdict !== "up" && verdict !== "down") {
    throw new HttpError("bad verdict");
  }
  return {
    ...base,
    journal: text(payload.journal, 40),
    title: text(payload.title, 500),
    verdict,
    note: text(payload.note, 500),
  };
}

function validateAppraisalRequest(payload) {
  return {
    ...validateIdentifier(payload),
    journal: text(payload.journal, 40),
    title: text(payload.title, 500),
    status: "requested",
    note: text(payload.note, 500),
  };
}

function validateAppraisalStatusUpdate(payload) {
  const base = validateIdentifier(payload);
  const status = text(payload.status, 20);
  if (!APPRAISAL_STATUSES.has(status)) {
    throw new HttpError("bad appraisal status");
  }
  return {
    ...base,
    status,
    note: text(payload.note, 500),
  };
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function htmlPage(title, body, status = 200) {
  const page = `<!doctype html><html lang="zh-Hant"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHtml(title)}</title>
<style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang TC","Microsoft JhengHei",sans-serif;background:#fafafa;color:#1a1a1a;margin:0;padding:24px 16px;line-height:1.65}main{max-width:640px;margin:0 auto;background:#fff;border:1px solid #e5e5e5;border-radius:8px;padding:22px}h1{font-size:22px;margin:0 0 12px}.meta{color:#666;font-size:14px;margin:8px 0 16px}label{display:block;font-weight:600;margin:14px 0 6px}input{box-sizing:border-box;width:100%;font:inherit;padding:10px;border:1px solid #ccc;border-radius:6px}button{font:inherit;background:#0b5fff;color:#fff;border:0;border-radius:6px;padding:9px 14px;margin-top:16px}a{color:#0b5fff;text-decoration:none}@media(prefers-color-scheme:dark){body{background:#1a1a1a;color:#e8e8e8}main{background:#242424;border-color:#333}.meta{color:#999}input{background:#1a1a1a;color:#e8e8e8;border-color:#444}}</style>
</head><body><main>${body}</main></body></html>`;
  return new Response(page, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function jsonResponse(payload, status = 200) {
  return Response.json(payload, { status });
}

async function parsePayload(request) {
  const raw = (await request.text()).trim();
  if (!raw) {
    return {};
  }
  if (raw.startsWith("{")) {
    try {
      return JSON.parse(raw);
    } catch {
      throw new HttpError("bad json");
    }
  }
  return Object.fromEntries(new URLSearchParams(raw));
}

async function secretsEqual(actual, expected) {
  if (!expected || expected === "CHANGE_ME") {
    return false;
  }
  const encoder = new TextEncoder();
  const [left, right] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(String(actual ?? ""))),
    crypto.subtle.digest("SHA-256", encoder.encode(String(expected))),
  ]);
  const a = new Uint8Array(left);
  const b = new Uint8Array(right);
  let difference = a.length ^ b.length;
  for (let index = 0; index < Math.min(a.length, b.length); index += 1) {
    difference |= a[index] ^ b[index];
  }
  return difference === 0;
}

async function requireSecret(actual, expected, label) {
  if (!expected || expected === "CHANGE_ME") {
    throw new HttpError(`${label} not configured`, 503);
  }
  if (!(await secretsEqual(actual, expected))) {
    throw new HttpError("unauthorized", 401);
  }
}

async function requireAccess(request, env) {
  const teamDomain = text(env.TEAM_DOMAIN, 255);
  const audience = text(env.POLICY_AUD, 128);
  if (!teamDomain || !audience) {
    throw new HttpError("Cloudflare Access verification not configured", 503);
  }
  const token = request.headers.get("Cf-Access-Jwt-Assertion") || "";
  if (!token) {
    throw new HttpError("Cloudflare Access assertion missing", 401);
  }

  const issuer = `https://${teamDomain}`;
  const jwksUrl = `${issuer}/cdn-cgi/access/certs`;
  if (!accessKeySet || accessKeySetUrl !== jwksUrl) {
    accessKeySet = createRemoteJWKSet(new URL(jwksUrl));
    accessKeySetUrl = jwksUrl;
  }
  try {
    await jwtVerify(token, accessKeySet, { issuer, audience });
  } catch {
    throw new HttpError("invalid Cloudflare Access assertion", 401);
  }
}

async function reserveWrite(env) {
  const limit = Number.parseInt(env.MAX_DAILY_WRITES || "300", 10);
  if (!Number.isFinite(limit) || limit < 1) {
    throw new HttpError("invalid MAX_DAILY_WRITES", 503);
  }
  const day = new Date().toISOString().slice(0, 10);
  const result = await env.DB.prepare(
    `INSERT INTO daily_write_counters (day, count) VALUES (?1, 1)
     ON CONFLICT(day) DO UPDATE SET count = count + 1
     RETURNING count`,
  ).bind(day).first();
  if (!result || Number(result.count) > limit) {
    throw new HttpError("daily write limit exceeded", 429);
  }
}

async function upsertFeedback(env, row) {
  await reserveWrite(env);
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO feedback
       (ts, week, identifier, pmid, doi, journal, title, verdict, note)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
     ON CONFLICT(week, identifier) DO UPDATE SET
       ts = excluded.ts,
       pmid = excluded.pmid,
       doi = excluded.doi,
       journal = excluded.journal,
       title = excluded.title,
       verdict = excluded.verdict,
       note = excluded.note`,
  ).bind(
    now, row.week, row.identifier, row.pmid, row.doi,
    row.journal, row.title, row.verdict, row.note,
  ).run();
  return "stored";
}

async function upsertAppraisalRequest(env, row) {
  await reserveWrite(env);
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO appraisal_requests
       (ts, week, identifier, pmid, doi, journal, title, status, note)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
     ON CONFLICT(week, identifier) DO UPDATE SET
       ts = excluded.ts,
       pmid = excluded.pmid,
       doi = excluded.doi,
       journal = excluded.journal,
       title = excluded.title,
       status = excluded.status,
       note = excluded.note`,
  ).bind(
    now, row.week, row.identifier, row.pmid, row.doi,
    row.journal, row.title, row.status, row.note,
  ).run();
  return "stored";
}

async function updateAppraisalRequestStatus(env, row) {
  await reserveWrite(env);
  const result = await env.DB.prepare(
    `UPDATE appraisal_requests
     SET status = ?1, note = ?2
     WHERE week = ?3 AND identifier = ?4`,
  ).bind(row.status, row.note, row.week, row.identifier).run();
  if (!result.success || Number(result.meta?.changes || 0) < 1) {
    throw new HttpError("appraisal request not found", 404);
  }
  return "updated";
}

async function appraisalStatuses(env, week) {
  if (!WEEK_RE.test(week)) {
    throw new HttpError("bad week");
  }
  const result = await env.DB.prepare(
    `SELECT ts, week, identifier, pmid, doi, status, note
     FROM appraisal_requests
     WHERE week = ?1
     ORDER BY ts`,
  ).bind(week).all();
  return result.results || [];
}

async function syncRows(env, table) {
  const result = await env.DB.prepare(
    `SELECT ts, week, pmid, doi, journal, title, ${table === "feedback" ? "verdict" : "status"}, note
     FROM ${table}
     ORDER BY ts`,
  ).all();
  return result.results || [];
}

function appraisalForm(request, payload) {
  const row = validateAppraisalRequest(payload);
  const identifier = row.pmid ? `PMID ${row.pmid}` : row.doi;
  const actionUrl = new URL(request.url);
  actionUrl.search = "";
  const body = `<h1>要求文獻評讀</h1>
<div class="meta">${escapeHtml(row.week)} · ${escapeHtml(identifier)}</div>
<p>${escapeHtml(row.title || "(no title)")}</p>
<form method="post" action="${escapeHtml(actionUrl.toString())}" target="_top">
<input type="hidden" name="action" value="request_appraisal">
<input type="hidden" name="week" value="${escapeHtml(row.week)}">
<input type="hidden" name="pmid" value="${escapeHtml(row.pmid)}">
<input type="hidden" name="doi" value="${escapeHtml(row.doi)}">
<input type="hidden" name="journal" value="${escapeHtml(row.journal)}">
<input type="hidden" name="title" value="${escapeHtml(row.title)}">
<p>此頁已由 Cloudflare Access 驗證。確認後，本機排程會下載全文並產生完整評讀。</p>
<button type="submit" autofocus>確認送出評讀請求</button>
</form>`;
  return htmlPage("要求文獻評讀", body);
}

function appraisalResultPage(row, status) {
  const identifier = row.pmid ? `PMID ${row.pmid}` : row.doi;
  return htmlPage(
    "已送出評讀請求",
    `<h1>已送出評讀請求</h1>
<div class="meta">${escapeHtml(row.week)} · ${escapeHtml(identifier)} · ${escapeHtml(status)}</div>
<p>${escapeHtml(row.title || "(no title)")}</p>
<p>本機完成下載與文獻評讀後，會用 Discord 傳送完整評讀連結。</p>`,
  );
}

async function handlePost(request, env) {
  const payload = await parsePayload(request);
  if (payload.action === "sync" || payload.action === "sync_appraisal_requests") {
    await requireSecret(payload.token, env.SYNC_TOKEN, "sync token");
    const table = payload.action === "sync" ? "feedback" : "appraisal_requests";
    return jsonResponse({ ok: true, rows: await syncRows(env, table) });
  }
  if (payload.action === "update_appraisal_request") {
    await requireSecret(payload.token, env.SYNC_TOKEN, "sync token");
    const row = validateAppraisalStatusUpdate(payload);
    const status = await updateAppraisalRequestStatus(env, row);
    return jsonResponse({ ok: true, status });
  }
  if (payload.action === "request_appraisal") {
    const row = validateAppraisalRequest(payload);
    await upsertAppraisalRequest(env, row);
    return appraisalResultPage(row, "評讀中");
  }
  const row = validateFeedback(payload);
  const status = await upsertFeedback(env, row);
  return jsonResponse({ ok: true, status });
}

function securityHeaders() {
  return {
    "Cache-Control": "no-store",
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
  };
}

function withHeaders(response, headers) {
  const merged = new Headers(response.headers);
  for (const [name, value] of Object.entries(headers)) {
    merged.set(name, value);
  }
  return new Response(response.body, { status: response.status, headers: merged });
}

async function handle(request, env) {
  const url = new URL(request.url);
  if (url.pathname !== "/api" && !url.pathname.startsWith("/api/")) {
    throw new HttpError("not found", 404);
  }
  await requireAccess(request, env);
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204 });
  }
  if (request.method === "POST") {
    return handlePost(request, env);
  }
  if (request.method === "GET") {
    if (url.searchParams.get("view") === "appraisal_statuses") {
      const week = text(url.searchParams.get("week"), 12);
      return jsonResponse({ ok: true, rows: await appraisalStatuses(env, week) });
    }
    if (url.searchParams.get("view") === "appraise" || url.searchParams.get("action") === "appraise") {
      return appraisalForm(request, Object.fromEntries(url.searchParams));
    }
    return jsonResponse({ ok: true, service: "journal-fetcher-relay" });
  }
  throw new HttpError("method not allowed", 405);
}

export default {
  async fetch(request, env) {
    let response;
    try {
      response = await handle(request, env);
    } catch (error) {
      const status = error instanceof HttpError ? error.status : 500;
      const message = error instanceof HttpError ? error.message : "internal error";
      const isAppraisalForm = request.method === "POST"
        && (request.headers.get("Content-Type") || "").includes("application/x-www-form-urlencoded");
      response = isAppraisalForm
        ? htmlPage(
          "評讀請求失敗",
          `<h1>評讀請求失敗</h1><p>${escapeHtml(message)}</p><p><a href="javascript:history.back()">返回重試</a></p>`,
          status,
        )
        : jsonResponse({ ok: false, error: message }, status);
    }
    return withHeaders(response, securityHeaders());
  },
};

export const _test = {
  escapeHtml,
  normalizeDoi,
  appraisalForm,
  parsePayload,
  requireAccess,
  secretsEqual,
  validateAppraisalRequest,
  validateAppraisalStatusUpdate,
  validateFeedback,
};
