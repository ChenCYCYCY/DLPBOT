import asyncio
import json
import os
import sys
from typing import Any, Dict, Optional

import aiohttp
import discord
import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row
from keep_alive import keep_alive

load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
GUILD_ID_RAW = os.getenv("DISCORD_GUILD_ID", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
POLL_INTERVAL = max(1, int(os.getenv("DISCORD_BOT_POLL_INTERVAL", "3") or "3"))
MAX_ATTEMPTS = max(1, int(os.getenv("DISCORD_BOT_JOB_MAX_ATTEMPTS", "5") or "5"))

# DLP website health monitor. This checks the WEBSITE process, not the Discord Bot itself.
DLP_WEBSITE_URL = os.getenv("DLP_WEBSITE_URL", "https://dlpweb.onrender.com").strip().rstrip("/")
DLP_WEBSITE_STATUS_URL = os.getenv(
    "DLP_WEBSITE_STATUS_URL",
    f"{DLP_WEBSITE_URL}/api/system/status",
).strip()
WEBSITE_CHECK_INTERVAL = max(15, int(os.getenv("DLP_WEBSITE_CHECK_INTERVAL", "60") or "60"))
WEBSITE_TIMEOUT = max(3, int(os.getenv("DLP_WEBSITE_TIMEOUT", "10") or "10"))

ROLE_BY_LEVEL = {
    1: os.getenv("DISCORD_ROLE_LONGTOU_ID", "").strip(),
    2: os.getenv("DISCORD_ROLE_ZHANGQI_ID", "").strip(),
    3: os.getenv("DISCORD_ROLE_TANGZHU_ID", "").strip(),
    4: os.getenv("DISCORD_ROLE_ZHANJIANG_ID", "").strip(),
    5: os.getenv("DISCORD_ROLE_MENSHENG_ID", "").strip(),
}

INTERVIEWEE_ROLE_ID = os.getenv("DISCORD_ROLE_INTERVIEWEE_ID", "").strip()
CITIZEN_ROLE_ID = os.getenv("DISCORD_ROLE_CITIZEN_ID", "").strip()


def require_env() -> int:
    missing = []
    if not TOKEN:
        missing.append("DISCORD_BOT_TOKEN")
    if not GUILD_ID_RAW.isdigit():
        missing.append("DISCORD_GUILD_ID")
    if not DATABASE_URL:
        missing.append("DATABASE_URL")
    for level, env_name in [
        (1, "DISCORD_ROLE_LONGTOU_ID"),
        (2, "DISCORD_ROLE_ZHANGQI_ID"),
        (3, "DISCORD_ROLE_TANGZHU_ID"),
        (4, "DISCORD_ROLE_ZHANJIANG_ID"),
        (5, "DISCORD_ROLE_MENSHENG_ID"),
    ]:
        if not ROLE_BY_LEVEL[level].isdigit():
            missing.append(env_name)
    if not INTERVIEWEE_ROLE_ID.isdigit():
        missing.append("DISCORD_ROLE_INTERVIEWEE_ID")
    if not CITIZEN_ROLE_ID.isdigit():
        missing.append("DISCORD_ROLE_CITIZEN_ID")
    if missing:
        print("[BOT] Missing/invalid environment variables: " + ", ".join(missing), flush=True)
        sys.exit(1)
    return int(GUILD_ID_RAW)


GUILD_ID = require_env()

intents = discord.Intents.none()
intents.guilds = True
intents.members = False
client = discord.Client(intents=intents)
_worker_task: Optional[asyncio.Task] = None
_website_status_task: Optional[asyncio.Task] = None
_last_website_presence: Optional[str] = None



def _safe_count(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        number = int(value)
        return number if number >= 0 else None
    except (TypeError, ValueError):
        return None


def _presence_text(kind: str, online: Optional[int] = None, offline: Optional[int] = None) -> str:
    if kind == "maintenance":
        return "🟡DLP專用系統維護中｜請耐心等候🟡"
    if kind == "error":
        return "🔴DLP專用系統異常｜處理中請稍後再嘗試🔴"
    if online is not None and offline is not None:
        return f"🟢DLP專用系統正常｜上線 {online} 人｜離線 {offline} 人🟢"
    return "🟢DLP專用系統正常🟢"


async def _set_website_presence(
    kind: str,
    *,
    online: Optional[int] = None,
    offline: Optional[int] = None,
    detail: str = "",
) -> None:
    global _last_website_presence

    text = _presence_text(kind, online, offline)
    if kind == "maintenance":
        discord_status = discord.Status.idle
    elif kind == "error":
        discord_status = discord.Status.dnd
    else:
        discord_status = discord.Status.online

    # Discord activity names have a practical length limit; keep the public text compact.
    text = text[:128]
    presence_key = f"{discord_status.value}|{text}"
    if presence_key == _last_website_presence:
        return

    await client.change_presence(
        status=discord_status,
        activity=discord.Game(name=text),
    )
    _last_website_presence = presence_key
    suffix = f" ({detail})" if detail else ""
    print(f"[WEB STATUS] {text}{suffix}", flush=True)


async def _read_status_json(response: aiohttp.ClientResponse) -> Dict[str, Any]:
    try:
        data = await response.json(content_type=None)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


async def check_website_status(session: aiohttp.ClientSession) -> None:
    """Check the DLP website endpoint and reflect WEBSITE health in Discord presence.

    Expected optional JSON response:
      {"status": "online", "online": 12, "offline": 8}
      {"status": "maintenance"}

    HTTP 429 and other failures are treated as website abnormal.
    """
    try:
        async with session.get(
            DLP_WEBSITE_STATUS_URL,
            headers={"User-Agent": "DLP-DiscordBot-WebsiteMonitor/1.0"},
            allow_redirects=True,
        ) as response:
            data = await _read_status_json(response)
            website_state = str(data.get("status") or "").strip().lower()

            # Maintenance wins even when the website intentionally uses 503.
            if website_state in {"maintenance", "maintaining", "maintenance_mode"}:
                await _set_website_presence(
                    "maintenance",
                    detail=f"HTTP {response.status}",
                )
                return

            if response.status == 429:
                await _set_website_presence("error", detail="HTTP 429")
                return

            if response.status >= 400:
                await _set_website_presence("error", detail=f"HTTP {response.status}")
                return

            if website_state in {"error", "offline", "down", "unhealthy"}:
                await _set_website_presence("error", detail=f"API status={website_state}")
                return

            online = _safe_count(data.get("online"))
            offline = _safe_count(data.get("offline"))
            await _set_website_presence(
                "online",
                online=online,
                offline=offline,
                detail=f"HTTP {response.status}",
            )

    except asyncio.TimeoutError:
        await _set_website_presence("error", detail="timeout")
    except aiohttp.ClientError as exc:
        await _set_website_presence("error", detail=f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        await _set_website_presence("error", detail=f"{type(exc).__name__}: {exc}")


async def website_status_loop() -> None:
    await client.wait_until_ready()
    timeout = aiohttp.ClientTimeout(total=WEBSITE_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        print(
            f"[WEB STATUS] Monitoring {DLP_WEBSITE_STATUS_URL} every {WEBSITE_CHECK_INTERVAL}s",
            flush=True,
        )
        while not client.is_closed():
            await check_website_status(session)
            await asyncio.sleep(WEBSITE_CHECK_INTERVAL)


def _db_connect():
    return psycopg.connect(DATABASE_URL, autocommit=False, row_factory=dict_row)


def ensure_queue_schema_sync() -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS discord_bot_jobs (
                  id BIGSERIAL PRIMARY KEY,
                  job_type VARCHAR(64) NOT NULL,
                  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                  status VARCHAR(20) NOT NULL DEFAULT 'pending',
                  attempts INTEGER NOT NULL DEFAULT 0,
                  last_error TEXT DEFAULT NULL,
                  dedupe_key VARCHAR(160) DEFAULT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  processed_at TIMESTAMPTZ DEFAULT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_discord_bot_jobs_dedupe_key
                  ON discord_bot_jobs(dedupe_key) WHERE dedupe_key IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_discord_bot_jobs_status_created
                  ON discord_bot_jobs(status, created_at);
                """
            )
            cur.execute(
                """UPDATE discord_bot_jobs
                   SET status='failed', last_error='Bot worker restarted before this job finished'
                   WHERE status='processing'"""
            )
            conn.commit()


def claim_job_sync() -> Optional[Dict[str, Any]]:
    """Atomically claim one pending/retryable job so only one worker handles it."""
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH next_job AS (
                  SELECT id
                  FROM discord_bot_jobs
                  WHERE status = 'pending'
                     OR (status = 'failed' AND attempts < %s)
                  ORDER BY created_at ASC, id ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE discord_bot_jobs j
                   SET status = 'processing', attempts = attempts + 1, last_error = NULL
                  FROM next_job
                 WHERE j.id = next_job.id
                RETURNING j.*
                """,
                (MAX_ATTEMPTS,),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


def finish_job_sync(job_id: int) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE discord_bot_jobs
                   SET status='done', processed_at=NOW(), last_error=NULL
                   WHERE id=%s""",
                (job_id,),
            )
            conn.commit()


def fail_job_sync(job_id: int, error: str) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE discord_bot_jobs
                   SET status=CASE WHEN attempts >= %s THEN 'dead' ELSE 'failed' END,
                       last_error=%s
                   WHERE id=%s""",
                (MAX_ATTEMPTS, error[:4000], job_id),
            )
            conn.commit()


async def get_member(user_id: int) -> discord.Member:
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        guild = await client.fetch_guild(GUILD_ID)
    if isinstance(guild, discord.Guild):
        member = guild.get_member(user_id)
        if member is not None:
            return member
        return await guild.fetch_member(user_id)
    raise RuntimeError("Guild is not available in cache")


def dlp_roles(guild: discord.Guild):
    ids = {int(v) for v in ROLE_BY_LEVEL.values() if v.isdigit()}
    return [r for r in guild.roles if r.id in ids]


async def _get_role(guild: discord.Guild, role_id_raw: str, env_name: str) -> discord.Role:
    if not role_id_raw.isdigit():
        raise RuntimeError(f"{env_name} is missing or invalid")
    role = guild.get_role(int(role_id_raw))
    if role is None:
        raise RuntimeError(f"Discord role from {env_name} ({role_id_raw}) not found")
    return role


async def _safe_dm(member: discord.Member, content: str) -> None:
    try:
        await member.send(content)
        print(f"[BOT] DM OK: {member.id}", flush=True)
    except discord.Forbidden:
        print(f"[BOT] DM skipped (user DMs closed): {member.id}", flush=True)
    except discord.HTTPException as exc:
        print(f"[BOT] DM failed for {member.id}: {exc}", flush=True)


async def _remove_roles_if_present(member: discord.Member, roles, reason: str) -> None:
    existing = [r for r in roles if r in member.roles]
    if existing:
        await member.remove_roles(*existing, reason=reason)


async def apply_job(job: Dict[str, Any]) -> None:
    payload = job.get("payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)

    job_type = str(job.get("job_type") or "")
    user_id_raw = str(payload.get("discord_user_id") or "").strip()
    if not user_id_raw.isdigit():
        raise ValueError("discord_user_id missing or invalid")

    member = await get_member(int(user_id_raw))
    guild = member.guild

    if job_type == "direct_dm":
        title = str(payload.get("title") or "DLP｜大聯社通知").strip()
        message = str(payload.get("message") or "").strip()
        event_type = str(payload.get("event_type") or "notification").strip()
        link = str(payload.get("link") or "").strip()
        embed = discord.Embed(
            title=f"🔔 {title}",
            description=message or "您有一則新的 DLP 系統通知。",
            color=0xB91C1C,
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="通知類型", value=event_type, inline=True)
        if link:
            embed.add_field(name="網站位置", value=link, inline=False)
        embed.set_footer(text="DLP｜大聯社 通知中心")
        try:
            await member.send(embed=embed)
        except discord.Forbidden:
            print(f"[BOT] DM blocked by user: {member.id}", flush=True)
        print(f"[BOT] direct_dm EMBED OK: {member.id}", flush=True)
        return

    if job_type == "application_received":
        interviewee_role = await _get_role(guild, INTERVIEWEE_ROLE_ID, "DISCORD_ROLE_INTERVIEWEE_ID")
        citizen_role = await _get_role(guild, CITIZEN_ROLE_ID, "DISCORD_ROLE_CITIZEN_ID")
        # Pending interview is its own state: temporarily remove citizen, then apply interviewee.
        await _remove_roles_if_present(member, [citizen_role], "DLP application submitted")
        if interviewee_role not in member.roles:
            await member.add_roles(interviewee_role, reason="DLP application submitted")
        case_id = str(payload.get("case_id") or "").strip()
        await _safe_dm(
            member,
            f"📨 **DLP｜大聯社 入幫申請已收到**\n\n您的入幫申請已成功送出{f'（案件編號：`{case_id}`）' if case_id else ''}。\n目前申請已進入審核流程，請耐心等候幹部審核與後續通知。\n\n請留意本 Bot 的私訊通知。"
        )
        print(f"[BOT] application_received OK: {member.id}", flush=True)
        return

    if job_type == "application_approved":
        interviewee_role = await _get_role(guild, INTERVIEWEE_ROLE_ID, "DISCORD_ROLE_INTERVIEWEE_ID")
        citizen_role = await _get_role(guild, CITIZEN_ROLE_ID, "DISCORD_ROLE_CITIZEN_ID")
        mensheng_role = await _get_role(guild, ROLE_BY_LEVEL[5], "DISCORD_ROLE_MENSHENG_ID")
        await _remove_roles_if_present(member, [interviewee_role, citizen_role], "DLP application approved")
        if mensheng_role not in member.roles:
            await member.add_roles(mensheng_role, reason="DLP application approved")
        original_name = str(payload.get("discord_name") or member.name or "成員").strip()
        base_name = original_name
        while base_name.lower().startswith("dlp."):
            base_name = base_name[4:].strip()
        nickname = f"DLP.{base_name or '成員'}"[:32]
        if member.nick != nickname:
            await member.edit(nick=nickname, reason="DLP application approved")
        case_id = str(payload.get("case_id") or "").strip()
        await _safe_dm(
            member,
            f"✅ **DLP｜大聯社 入幫審核通過**\n\n恭喜您，入幫申請{f'（案件編號：`{case_id}`）' if case_id else ''}已通過審核。\n您目前已加入【門生】身分組，Discord 暱稱也已同步為 `{nickname}`。\n\n請進入內部管理系統閱讀並同意組織規章後，再使用其他功能。"
        )
        print(f"[BOT] application_approved OK: {member.id}", flush=True)
        return

    if job_type == "application_rejected":
        interviewee_role = await _get_role(guild, INTERVIEWEE_ROLE_ID, "DISCORD_ROLE_INTERVIEWEE_ID")
        citizen_role = await _get_role(guild, CITIZEN_ROLE_ID, "DISCORD_ROLE_CITIZEN_ID")
        await _remove_roles_if_present(member, [interviewee_role], "DLP application rejected")
        if citizen_role not in member.roles:
            await member.add_roles(citizen_role, reason="DLP application rejected")
        case_id = str(payload.get("case_id") or "").strip()
        reason = str(payload.get("reason") or "未符合目前招募標準").strip()
        await _safe_dm(
            member,
            f"❌ **DLP｜大聯社 入幫審核結果**\n\n您的入幫申請{f'（案件編號：`{case_id}`）' if case_id else ''}本次未通過審核。\n審核原因：**{reason}**\n\n您的身分已恢復為市民；若後續符合重新申請條件，可再次提出申請。"
        )
        print(f"[BOT] application_rejected OK: {member.id}", flush=True)
        return

    if job_type == "member_kicked":
        citizen_role = await _get_role(guild, CITIZEN_ROLE_ID, "DISCORD_ROLE_CITIZEN_ID")
        interviewee_role = await _get_role(guild, INTERVIEWEE_ROLE_ID, "DISCORD_ROLE_INTERVIEWEE_ID")
        cleanup_roles = dlp_roles(guild) + [interviewee_role]
        await _remove_roles_if_present(member, cleanup_roles, "DLP member removed from gang")
        if citizen_role not in member.roles:
            await member.add_roles(citizen_role, reason="DLP member removed from gang")
        reason = str(payload.get("reason") or "幹部除名處分").strip()
        await _safe_dm(
            member,
            f"🚪 **DLP｜大聯社 離幫／除名通知**\n\n您目前已被移出 DLP｜大聯社。\n原因：**{reason}**\n\n系統已移除您的 DLP 階級身分組，並恢復為【市民】身分。"
        )
        print(f"[BOT] member_kicked OK: {member.id}", flush=True)
        return

    if job_type == "nickname_sync":
        nickname = str(payload.get("nickname") or "").strip()[:32]
        if not nickname:
            raise ValueError("nickname missing")
        if member.nick != nickname:
            await member.edit(nick=nickname, reason="DLP website approved nickname sync")
        print(f"[BOT] nickname_sync OK: {member.id} -> {nickname}", flush=True)
        return

    if job_type == "rank_sync":
        level = int(payload.get("rank_level") or 99)
        all_rank_roles = dlp_roles(guild)
        current_rank_roles = [r for r in member.roles if r in all_rank_roles]
        target_id = ROLE_BY_LEVEL.get(level, "")
        target_role = guild.get_role(int(target_id)) if target_id.isdigit() else None

        to_remove = [r for r in current_rank_roles if target_role is None or r.id != target_role.id]
        if to_remove:
            await member.remove_roles(*to_remove, reason="DLP website rank synchronization")
        if target_role is not None and target_role not in member.roles:
            await member.add_roles(target_role, reason="DLP website rank synchronization")
        print(f"[BOT] rank_sync OK: {member.id} -> level {level}", flush=True)
        return

    if job_type == "gang_roles_strip":
        roles = [r for r in member.roles if r in dlp_roles(guild)]
        if roles:
            await member.remove_roles(*roles, reason="DLP website gang role cleanup")
        print(f"[BOT] gang_roles_strip OK: {member.id}", flush=True)
        return

    if job_type in {"role_add", "role_remove"}:
        role_id_raw = str(payload.get("role_id") or "").strip()
        if not role_id_raw.isdigit():
            raise ValueError("role_id missing or invalid")
        role = guild.get_role(int(role_id_raw))
        if role is None:
            raise RuntimeError(f"Discord role {role_id_raw} not found")
        if job_type == "role_add" and role not in member.roles:
            await member.add_roles(role, reason="DLP website role sync")
        elif job_type == "role_remove" and role in member.roles:
            await member.remove_roles(role, reason="DLP website role sync")
        print(f"[BOT] {job_type} OK: {member.id} / {role.id}", flush=True)
        return

    raise ValueError(f"Unknown job_type: {job_type}")


async def worker_loop() -> None:
    await client.wait_until_ready()
    await asyncio.to_thread(ensure_queue_schema_sync)
    print(f"[BOT] Worker online. Poll interval={POLL_INTERVAL}s, guild={GUILD_ID}", flush=True)
    while not client.is_closed():
        try:
            job = await asyncio.to_thread(claim_job_sync)
            if not job:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            job_id = int(job["id"])
            try:
                await apply_job(job)
                await asyncio.to_thread(finish_job_sync, job_id)
            except Exception as exc:
                print(f"[BOT] Job #{job_id} failed: {type(exc).__name__}: {exc}", flush=True)
                await asyncio.to_thread(fail_job_sync, job_id, f"{type(exc).__name__}: {exc}")
        except Exception as exc:
            print(f"[BOT] Worker loop error: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(POLL_INTERVAL)


@client.event
async def on_ready():
    global _worker_task, _website_status_task
    print(f"[BOT] Logged in as {client.user} ({client.user.id if client.user else 'unknown'})", flush=True)
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(worker_loop())
    if _website_status_task is None or _website_status_task.done():
        _website_status_task = asyncio.create_task(website_status_loop())


if __name__ == "__main__":
    keep_alive()
    print("[BOT] Keep-alive HTTP server started.", flush=True)
    client.run(TOKEN, log_handler=None)
