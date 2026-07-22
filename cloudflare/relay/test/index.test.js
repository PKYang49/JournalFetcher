import assert from "node:assert/strict";
import test from "node:test";

import { _test } from "../src/index.js";

test("validates and normalizes feedback", () => {
  const row = _test.validateFeedback({
    week: "2026-W30",
    pmid: "42462272",
    doi: "https://doi.org/10.1000/ABC.",
    verdict: "up",
    title: "Example",
  });
  assert.equal(row.identifier, "pmid:42462272");
  assert.equal(row.doi, "10.1000/abc");
  assert.equal(row.verdict, "up");
});

test("uses normalized DOI when PMID is absent", () => {
  const row = _test.validateAppraisalRequest({
    week: "2026-W30",
    doi: "10.1000/ABC",
  });
  assert.equal(row.identifier, "doi:10.1000/abc");
});

test("validates appraisal lifecycle updates", () => {
  const row = _test.validateAppraisalStatusUpdate({
    week: "2026-W30",
    pmid: "42462272",
    status: "failed",
    note: "PDF 下載失敗",
  });
  assert.equal(row.identifier, "pmid:42462272");
  assert.equal(row.status, "failed");
  assert.equal(row.note, "PDF 下載失敗");
  assert.throws(
    () => _test.validateAppraisalStatusUpdate({
      week: "2026-W30", pmid: "42462272", status: "unknown",
    }),
    /bad appraisal status/,
  );
});

test("rejects malformed public payloads", () => {
  assert.throws(
    () => _test.validateFeedback({ week: "W30", pmid: "1", verdict: "maybe" }),
    /bad week/,
  );
  assert.throws(
    () => _test.validateFeedback({ week: "2026-W30", pmid: "42462272", verdict: "maybe" }),
    /bad verdict/,
  );
});

test("escapes form content", () => {
  assert.equal(_test.escapeHtml(`<script>"x"</script>`), "&lt;script&gt;&quot;x&quot;&lt;/script&gt;");
});

test("compares secrets without storing plaintext derivatives", async () => {
  assert.equal(await _test.secretsEqual("same", "same"), true);
  assert.equal(await _test.secretsEqual("wrong", "same"), false);
  assert.equal(await _test.secretsEqual("", "CHANGE_ME"), false);
});

test("parses JSON sent as text/plain by historical weekly pages", async () => {
  const request = new Request("https://relay.example", {
    method: "POST",
    headers: { "Content-Type": "text/plain;charset=utf-8" },
    body: JSON.stringify({ week: "2026-W30", verdict: "up" }),
  });
  assert.deepEqual(await _test.parsePayload(request), { week: "2026-W30", verdict: "up" });
});

test("Access-protected appraisal form has no passphrase", async () => {
  const request = new Request("https://relay.example/api?view=appraise");
  const response = _test.appraisalForm(request, {
    week: "2026-W30",
    pmid: "42462272",
    title: "Example",
  });
  const html = await response.text();
  assert.match(html, /確認送出評讀請求/);
  assert.doesNotMatch(html, /passphrase/i);
});

test("rejects API calls without an Access assertion before network verification", async () => {
  await assert.rejects(
    () => _test.requireAccess(new Request("https://relay.example/api"), {
      TEAM_DOMAIN: "example.cloudflareaccess.com",
      POLICY_AUD: "audience",
    }),
    /assertion missing/,
  );
});
