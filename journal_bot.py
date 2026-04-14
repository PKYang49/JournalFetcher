#!/usr/bin/env python3
"""
Discord Bot：接收 DOI 或 URL，呼叫 dlbydoi.download_one() 下載 PDF，
存到 Google Drive 同步資料夾，並把 PDF 回傳到 Discord channel。
"""

import asyncio
import logging
import os
import re
import traceback
from pathlib import Path

import discord
from dotenv import load_dotenv

from dlbydoi import download_one

load_dotenv()

BOT_TOKEN = os.environ["DISCORD_BOT_TOKEN"]
ALLOWED_IDS = {int(x) for x in os.environ["DISCORD_ALLOWED_USER_IDS"].split(",") if x.strip()}
CHANNEL_ID = int(os.environ["DISCORD_CHANNEL_ID"])
OUT_DIR = Path(os.environ["BOT_OUTPUT_DIR"]).expanduser()
SEND_PDF = os.environ.get("BOT_SEND_PDF", "true").lower() == "true"

# Discord 免費 server 附件上限 10 MB，保守一點用 9.5 MB
DISCORD_MAX_ATTACHMENT = int(9.5 * 1024 * 1024)

OUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = Path(__file__).parent / "output" / "journal_bot.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("journal_bot")

DOI_RE = re.compile(r"10\.\d{4,}/[^\s?#]+")


def extract_dois(text: str) -> list[str]:
    """Extract unique DOIs, stripping URL cruft and trailing punctuation."""
    seen, out = set(), []
    for m in DOI_RE.finditer(text):
        doi = m.group(0).rstrip(".,);]>'\"")
        if doi not in seen:
            seen.add(doi)
            out.append(doi)
    return out


def _blocking_download(doi: str) -> Path | None:
    return download_one(doi, OUT_DIR)


async def _download(doi: str) -> tuple[Path | None, str | None]:
    loop = asyncio.get_running_loop()
    try:
        path = await loop.run_in_executor(None, _blocking_download, doi)
        return path, None
    except Exception as e:
        log.error("download failed for %s: %s\n%s", doi, e, traceback.format_exc())
        return None, f"{type(e).__name__}: {e}"


intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


@client.event
async def on_ready():
    log.info(
        "Bot ready as %s (id=%s), watching channel=%s allowed=%s",
        client.user, client.user.id, CHANNEL_ID, ALLOWED_IDS,
    )


@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.id != CHANNEL_ID:
        return
    if message.author.id not in ALLOWED_IDS:
        log.warning("rejected user=%s text=%r", message.author.id, message.content)
        return

    text = message.content or ""
    dois = extract_dois(text)
    log.info("user=%s dois=%s", message.author.id, dois)

    if not dois:
        await message.reply(
            "❓ 沒認出 DOI。請傳純 DOI（`10.xxxx/...`）或 `https://doi.org/...` 連結。",
            mention_author=False,
        )
        return

    if len(dois) == 1:
        await message.reply(f"⏳ 下載中：`{dois[0]}`", mention_author=False)
    else:
        await message.reply(f"⏳ 批次下載 {len(dois)} 篇...", mention_author=False)

    success, failed = [], []
    for doi in dois:
        path, err = await _download(doi)
        if path is None:
            failed.append((doi, err or "所有下載方法皆失敗"))
            continue
        success.append(path)

        if len(dois) == 1:
            size = path.stat().st_size
            size_kb = size // 1024
            await message.channel.send(f"✅ `{path.name}` ({size_kb} KB)")
            if SEND_PDF and size < DISCORD_MAX_ATTACHMENT:
                await message.channel.send(file=discord.File(path))
            elif SEND_PDF:
                await message.channel.send(
                    f"📁 檔案超過 Discord 上傳上限，已存到 Google Drive `Papers/{path.name}`"
                )

    if len(dois) > 1:
        lines = [f"✅ {len(success)} 成功 / ❌ {len(failed)} 失敗"]
        if failed:
            lines.append("失敗：")
            lines += [f"• `{d}` — {e}" for d, e in failed]
        await message.channel.send("\n".join(lines))
    elif failed:
        d, e = failed[0]
        await message.channel.send(f"❌ `{d}`\n{e}")


def main():
    log.info("Bot 啟動，OUT_DIR=%s", OUT_DIR)
    client.run(BOT_TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
