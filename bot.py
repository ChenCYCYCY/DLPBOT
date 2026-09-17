import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, Optional

import aiohttp
import discord
from discord import app_commands
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
JOB_MIN_INTERVAL = max(0.1, float(os.getenv("DISCORD_BOT_JOB_MIN_INTERVAL", "0.35") or "0.35"))
MEMBER_CACHE_TTL = max(10, int(os.getenv("DISCORD_MEMBER_CACHE_TTL", "120") or "120"))
MEMBER_SNAPSHOT_INTERVAL = max(60, int(os.getenv("DISCORD_MEMBER_SNAPSHOT_INTERVAL", "300") or "300"))

# DLP website health monitor. This checks the WEBSITE process, not the Discord Bot itself.
DLP_WEBSITE_URL = os.getenv("DLP_WEBSITE_URL", "https://dlpweb.onrender.com").strip().rstrip("/")
DLP_WEBSITE_STATUS_URL = os.getenv(
    "DLP_WEBSITE_STATUS_URL",
    f"{DLP_WEBSITE_URL}/api/bot-health",
).strip()
WEBSITE_CHECK_INTERVAL = max(15, int(os.getenv("DLP_WEBSITE_CHECK_INTERVAL", "60") or "60"))
WEBSITE_TIMEOUT = max(3, int(os.getenv("DLP_WEBSITE_TIMEOUT", "10") or "10"))
# Manual maintenance switch. Set DLP_MAINTENANCE_MODE=true in Render to force maintenance presence.
DLP_MAINTENANCE_MODE_DEFAULT = os.getenv("DLP_MAINTENANCE_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}

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
intents.members = True
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
_commands_synced = False
_worker_task: Optional[asyncio.Task] = None
_website_status_task: Optional[asyncio.Task] = None
_member_snapshot_task: Optional[asyncio.Task] = None
_last_website_presence: Optional[str] = None
_rate_limit_until_monotonic: float = 0.0
_member_cache: Dict[int, tuple[float, discord.Member]] = {}



def ensure_runtime_settings_schema_sync() -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS discord_bot_runtime_settings (
                  setting_key VARCHAR(100) PRIMARY KEY,
                  setting_value TEXT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  updated_by VARCHAR(100) DEFAULT NULL
                );
                """
            )
            cur.execute(
                """
                INSERT INTO discord_bot_runtime_settings(setting_key, setting_value, updated_by)
                VALUES ('maintenance_mode', %s, 'env-default')
                ON CONFLICT (setting_key) DO NOTHING
                """,
                ("true" if DLP_MAINTENANCE_MODE_DEFAULT else "false",),
            )
            conn.commit()


def get_maintenance_mode_sync() -> bool:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT setting_value FROM discord_bot_runtime_settings WHERE setting_key='maintenance_mode'"
            )
            row = cur.fetchone()
            if not row:
                return DLP_MAINTENANCE_MODE_DEFAULT
            return str(row["setting_value"]).strip().lower() in {"1", "true", "yes", "on"}


def set_maintenance_mode_sync(enabled: bool, updated_by: str) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_bot_runtime_settings(setting_key, setting_value, updated_at, updated_by)
                VALUES ('maintenance_mode', %s, NOW(), %s)
                ON CONFLICT (setting_key) DO UPDATE SET
                  setting_value = EXCLUDED.setting_value,
                  updated_at = NOW(),
                  updated_by = EXCLUDED.updated_by
                """,
                ("true" if enabled else "false", updated_by[:100]),
            )
            conn.commit()


async def is_maintenance_mode() -> bool:
    try:
        return await asyncio.to_thread(get_maintenance_mode_sync)
    except Exception as exc:
        print(f"[MAINTENANCE] Failed to read DB setting: {type(exc).__name__}: {exc}", flush=True)
        return DLP_MAINTENANCE_MODE_DEFAULT


def _safe_count(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        number = int(value)
        return number if number >= 0 else None
    except (TypeError, ValueError):
        return None


def _presence_text(
    kind: str,
    online: Optional[int] = None,
    offline: Optional[int] = None,
    wait_minutes: Optional[int] = None,
) -> str:
    if kind == "maintenance":
        return "🟡DLP系統維護中🟡"
    if kind == "rate_limited":
        minutes = max(1, int(wait_minutes or 60))
        return f"🟠DLP系統限流受限｜等待{minutes}分"
    if kind == "error":
        return "🔴DLP系統異常🔴"
    if online is not None:
        return f"🟢DLP正常｜{online}人在線🟢"
    return "🟢DLP正常🟢"


async def _set_website_presence(
    kind: str,
    *,
    online: Optional[int] = None,
    offline: Optional[int] = None,
    wait_minutes: Optional[int] = None,
    detail: str = "",
) -> None:
    global _last_website_presence

    text = _presence_text(kind, online, offline, wait_minutes)
    if kind == "maintenance":
        discord_status = discord.Status.idle
    elif kind == "rate_limited":
        # OAuth/Discord 429 means the website is still alive, but login is temporarily limited.
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


def _oauth_is_rate_limited(data: Dict[str, Any]) -> bool:
    """Return True whenever the website reports a Discord/Cloudflare 429 state.

    Supported production payloads include:
      {"oauth_status": 429}
      {"oauth_rate_limited": true}
      {"website_paused": true, "pause_reason": "rate_limited"}
      {"oauth": "rate_limited"}
      {"oauth": {"status": "rate_limited", "http_status": 429}}
    """
    oauth = data.get("oauth")
    oauth_state = ""
    oauth_status: Any = data.get("oauth_status")

    # Explicit boolean from /api/bot-health is authoritative.
    if data.get("oauth_rate_limited") is True:
        return True

    # Full-site 429 lock payload.
    pause_reason = str(data.get("pause_reason") or "").strip().lower()
    if data.get("website_paused") is True and pause_reason in {
        "rate_limited", "rate-limit", "rate_limit", "429", "oauth_429"
    }:
        return True

    if isinstance(oauth, dict):
        oauth_state = str(oauth.get("status") or oauth.get("state") or "").strip().lower()
        if oauth_status is None:
            oauth_status = oauth.get("http_status") or oauth.get("status_code") or oauth.get("code")
        if oauth.get("rate_limited") is True:
            return True
    else:
        oauth_state = str(oauth or data.get("oauth_state") or "").strip().lower()

    try:
        oauth_status_int = int(oauth_status) if oauth_status is not None else None
    except (TypeError, ValueError):
        oauth_status_int = None

    return (
        oauth_status_int == 429
        or oauth_state in {
            "429",
            "rate_limited",
            "ratelimited",
            "rate-limit",
            "rate_limit",
            "limited",
            "circuit_open",
            "circuit-open",
        }
    )


def _reported_retry_minutes(data: Dict[str, Any]) -> int:
    """Prefer the website's remaining cooldown so Discord mirrors the website."""
    retry_minutes = data.get("retry_minutes")
    if retry_minutes is None and isinstance(data.get("oauth"), dict):
        retry_minutes = data["oauth"].get("retry_minutes")
    try:
        value = int(float(retry_minutes))
        if value > 0:
            return value
    except (TypeError, ValueError):
        pass

    retry_after = data.get("retry_after")
    if retry_after is None and isinstance(data.get("oauth"), dict):
        retry_after = data["oauth"].get("retry_after")
    try:
        seconds = max(0, int(float(retry_after)))
        if seconds > 0:
            return max(1, (seconds + 59) // 60)
    except (TypeError, ValueError):
        pass

    return 60


def _oauth_retry_detail(data: Dict[str, Any]) -> str:
    return (
        f"OAuth HTTP 429; retry_minutes={_reported_retry_minutes(data)}; "
        f"oauth_rate_limited={data.get('oauth_rate_limited')}; "
        f"website_paused={data.get('website_paused')}"
    )


def _start_rate_limit_countdown(minutes: int = 60) -> None:
    global _rate_limit_until_monotonic
    _rate_limit_until_monotonic = time.monotonic() + max(1, minutes) * 60


def _rate_limit_minutes_left() -> int:
    if _rate_limit_until_monotonic <= 0:
        return 0
    remaining = _rate_limit_until_monotonic - time.monotonic()
    if remaining <= 0:
        return 0
    return max(1, int((remaining + 59) // 60))


async def check_website_status(session: aiohttp.ClientSession) -> None:
    """Check DLP website health and reflect it in Discord presence.

    Priority:
      1. Manual maintenance -> maintenance
      2. Website unavailable/5xx/etc. -> system error
      3. Website alive but Discord OAuth is rate-limited -> login limited (429)
      4. Healthy -> normal

    A 429 never closes the Discord bot or stops the worker.
    """
    if await is_maintenance_mode():
        await _set_website_presence("maintenance", detail="manual maintenance mode")
        return

    try:
        async with session.get(
            DLP_WEBSITE_STATUS_URL,
            headers={"User-Agent": "DLP-DiscordBot-WebsiteMonitor/1.1"},
            allow_redirects=True,
        ) as response:
            data = await _read_status_json(response)
            website_state = str(data.get("status") or "").strip().lower()
            print(
                "[WEB STATUS CHECK] "
                f"HTTP={response.status} status={website_state or '-'} "
                f"oauth_status={data.get('oauth_status')} "
                f"oauth_rate_limited={data.get('oauth_rate_limited')} "
                f"website_paused={data.get('website_paused')} "
                f"retry_minutes={data.get('retry_minutes')}",
                flush=True,
            )

            # If the health endpoint itself returns 429, keep the bot online and
            # expose a dedicated 429 state instead of calling the whole system down.
            if response.status == 429:
                _start_rate_limit_countdown(60)
                await _set_website_presence(
                    "rate_limited",
                    wait_minutes=60,
                    detail="health endpoint HTTP 429; countdown started",
                )
                return

            if response.status >= 400:
                await _set_website_presence("error", detail=f"HTTP {response.status}")
                return

            if website_state in {"error", "offline", "down", "unhealthy"}:
                await _set_website_presence("error", detail=f"API status={website_state}")
                return

            # The website remains healthy during a Discord OAuth 429.  The website
            # should publish that condition in /api/bot-health rather than the bot
            # making additional requests to Discord itself.
            # IMPORTANT: evaluate 429 BEFORE treating status=online as healthy.
            # The website intentionally remains HTTP 200/"online" while OAuth is limited.
            if _oauth_is_rate_limited(data):
                retry_minutes = _reported_retry_minutes(data)
                _start_rate_limit_countdown(retry_minutes)
                await _set_website_presence(
                    "rate_limited",
                    wait_minutes=retry_minutes,
                    detail=f"{_oauth_retry_detail(data)}; countdown started",
                )
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
            # Manual maintenance always has the highest priority.
            if await is_maintenance_mode():
                await _set_website_presence("maintenance", detail="manual maintenance mode")
                await asyncio.sleep(60)
                continue

            # After any detected 429, freeze normal website checks for one hour.
            # Update the Discord presence once per minute with the remaining time.
            minutes_left = _rate_limit_minutes_left()
            if minutes_left > 0:
                await _set_website_presence(
                    "rate_limited",
                    wait_minutes=minutes_left,
                    detail=f"429 cooldown; {minutes_left} minute(s) remaining",
                )
                await asyncio.sleep(60)
                continue

            # Cooldown ended (or no 429 yet): perform one real website check.
            # If it is still 429, check_website_status starts a fresh 60-minute countdown.
            await check_website_status(session)
            await asyncio.sleep(WEBSITE_CHECK_INTERVAL)


def _db_connect():
    return psycopg.connect(DATABASE_URL, autocommit=False, row_factory=dict_row)


def ensure_queue_schema_sync() -> None:
    ensure_runtime_settings_schema_sync()
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
                  processed_at TIMESTAMPTZ DEFAULT NULL,
                  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                ALTER TABLE discord_bot_jobs ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
                CREATE UNIQUE INDEX IF NOT EXISTS uq_discord_bot_jobs_dedupe_key
                  ON discord_bot_jobs(dedupe_key) WHERE dedupe_key IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_discord_bot_jobs_status_created
                  ON discord_bot_jobs(status, created_at);
                CREATE INDEX IF NOT EXISTS idx_discord_bot_jobs_status_available
                  ON discord_bot_jobs(status, available_at, created_at);
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
                  WHERE (status = 'pending' OR (status = 'failed' AND attempts < %s))
                    AND COALESCE(available_at, NOW()) <= NOW()
                  ORDER BY available_at ASC, created_at ASC, id ASC
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
                       last_error=%s,
                       available_at = CASE
                         WHEN attempts >= %s THEN NOW()
                         ELSE NOW() + make_interval(secs => LEAST(60, (2 * POWER(2, GREATEST(attempts - 1, 0)))::int + FLOOR(random() * 3)::int))
                       END
                   WHERE id=%s""",
                (MAX_ATTEMPTS, error[:4000], MAX_ATTEMPTS, job_id),
            )
            conn.commit()


def ensure_member_snapshot_schema_sync() -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS discord_member_role_cache (
                  discord_user_id VARCHAR(64) PRIMARY KEY,
                  discord_name VARCHAR(100) DEFAULT NULL,
                  nickname VARCHAR(100) DEFAULT NULL,
                  roles JSONB NOT NULL DEFAULT '[]'::jsonb,
                  guild_id VARCHAR(64) NOT NULL,
                  synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_discord_member_role_cache_synced
                  ON discord_member_role_cache(synced_at DESC);
                """
            )
            conn.commit()


def upsert_member_snapshot_sync(member_id: int, discord_name: str, nickname: Optional[str], roles: list[str]) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_member_role_cache
                  (discord_user_id, discord_name, nickname, roles, guild_id, synced_at)
                VALUES (%s,%s,%s,%s::jsonb,%s,NOW())
                ON CONFLICT(discord_user_id) DO UPDATE SET
                  discord_name=EXCLUDED.discord_name,
                  nickname=EXCLUDED.nickname,
                  roles=EXCLUDED.roles,
                  guild_id=EXCLUDED.guild_id,
                  synced_at=NOW()
                """,
                (str(member_id), discord_name[:100], (nickname or '')[:100] or None, json.dumps(roles), str(GUILD_ID)),
            )
            conn.commit()


def replace_guild_member_snapshot_sync(rows: list[tuple[str, str, Optional[str], str, str]]) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            for user_id, name, nickname, roles_json, guild_id in rows:
                cur.execute(
                    """INSERT INTO discord_member_role_cache
                       (discord_user_id,discord_name,nickname,roles,guild_id,synced_at)
                       VALUES(%s,%s,%s,%s::jsonb,%s,NOW())
                       ON CONFLICT(discord_user_id) DO UPDATE SET
                         discord_name=EXCLUDED.discord_name,nickname=EXCLUDED.nickname,
                         roles=EXCLUDED.roles,guild_id=EXCLUDED.guild_id,synced_at=NOW()""",
                    (user_id, name, nickname, roles_json, guild_id),
                )
            conn.commit()


async def sync_member_snapshot_once() -> None:
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        raise RuntimeError('Guild is not available in cache')
    if not guild.chunked:
        await guild.chunk(cache=True)
    rows: list[tuple[str, str, Optional[str], str, str]] = []
    for member in guild.members:
        if member.bot:
            continue
        roles = [str(role.id) for role in member.roles if role.id != guild.id]
        rows.append((str(member.id), str(member.name)[:100], (member.nick or '')[:100] or None, json.dumps(roles), str(GUILD_ID)))
    if rows:
        await asyncio.to_thread(replace_guild_member_snapshot_sync, rows)
    print(f'[BOT] Discord member role snapshot synced: {len(rows)} members', flush=True)


async def member_snapshot_loop() -> None:
    await client.wait_until_ready()
    await asyncio.to_thread(ensure_member_snapshot_schema_sync)
    while not client.is_closed():
        try:
            await sync_member_snapshot_once()
        except Exception as exc:
            print(f'[BOT] Member snapshot sync failed: {type(exc).__name__}: {exc}', flush=True)
        await asyncio.sleep(MEMBER_SNAPSHOT_INTERVAL)


@client.event
async def on_member_update(before: discord.Member, after: discord.Member):
    try:
        roles = [str(role.id) for role in after.roles if role.id != after.guild.id]
        await asyncio.to_thread(upsert_member_snapshot_sync, after.id, after.name, after.nick, roles)
    except Exception as exc:
        print(f'[BOT] Member snapshot update failed for {after.id}: {type(exc).__name__}: {exc}', flush=True)


@client.event
async def on_member_join(member: discord.Member):
    try:
        roles = [str(role.id) for role in member.roles if role.id != member.guild.id]
        await asyncio.to_thread(upsert_member_snapshot_sync, member.id, member.name, member.nick, roles)
    except Exception as exc:
        print(f'[BOT] Member snapshot join update failed for {member.id}: {type(exc).__name__}: {exc}', flush=True)


async def get_member(user_id: int) -> discord.Member:
    now = time.monotonic()
    cached = _member_cache.get(user_id)
    if cached and now - cached[0] <= MEMBER_CACHE_TTL:
        return cached[1]

    guild = client.get_guild(GUILD_ID)
    if guild is None:
        guild = await client.fetch_guild(GUILD_ID)
    if isinstance(guild, discord.Guild):
        member = guild.get_member(user_id)
        if member is None:
            member = await guild.fetch_member(user_id)
        _member_cache[user_id] = (now, member)
        if len(_member_cache) > 1000:
            oldest = min(_member_cache, key=lambda k: _member_cache[k][0])
            _member_cache.pop(oldest, None)
        return member
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
        raise


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
                await asyncio.sleep(JOB_MIN_INTERVAL)
            except Exception as exc:
                print(f"[BOT] Job #{job_id} failed: {type(exc).__name__}: {exc}", flush=True)
                await asyncio.to_thread(fail_job_sync, job_id, f"{type(exc).__name__}: {exc}")
        except Exception as exc:
            print(f"[BOT] Worker loop error: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(POLL_INTERVAL)



async def _apply_manual_maintenance(enabled: bool, actor: str, actor_id: int) -> str:
    """Persist manual maintenance mode and immediately refresh Discord presence."""
    await asyncio.to_thread(set_maintenance_mode_sync, enabled, actor)
    if enabled:
        await _set_website_presence(
            "maintenance",
            detail=f"manual command by {actor_id}",
        )
        return "🟡 已開啟維護模式｜Bot 狀態：`DLP維護中`"

    timeout = aiohttp.ClientTimeout(total=WEBSITE_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await check_website_status(session)
    return "🟢 已關閉維護模式｜已恢復自動偵測網站狀態"


@client.event
async def on_message(message: discord.Message):
    """Traditional text command fallback.

    Supported:
      !維護 開
      !維護 關
      !維護 狀態
      !maintenance on/off/status
    """
    if message.author.bot or message.guild is None:
        return

    content = (message.content or "").strip()
    if not content:
        return

    parts = content.split()
    command = parts[0].lower()
    if command not in {"!維護", "!maintenance"}:
        return

    perms = getattr(message.author, "guild_permissions", None)
    if not perms or not perms.manage_guild:
        await message.reply("❌ 你沒有權限使用這個指令。", mention_author=False)
        return

    if len(parts) < 2:
        await message.reply(
            "用法：`!維護 開`、`!維護 關`、`!維護 狀態`",
            mention_author=False,
        )
        return

    action = parts[1].strip().lower()
    try:
        if action in {"狀態", "status", "查看"}:
            enabled = await is_maintenance_mode()
            text = "🟡 維護模式：已開啟" if enabled else "🟢 維護模式：已關閉"
            await message.reply(text, mention_author=False)
            return

        if action in {"開", "開啟", "on", "true"}:
            text = await _apply_manual_maintenance(
                True,
                f"{message.author} ({message.author.id})",
                message.author.id,
            )
            await message.reply(text, mention_author=False)
            return

        if action in {"關", "關閉", "off", "false"}:
            text = await _apply_manual_maintenance(
                False,
                f"{message.author} ({message.author.id})",
                message.author.id,
            )
            await message.reply(text, mention_author=False)
            return

        await message.reply(
            "❌ 不認得這個操作。請用：`!維護 開`、`!維護 關`、`!維護 狀態`",
            mention_author=False,
        )
    except Exception as exc:
        print(f"[MAINTENANCE TEXT CMD] {type(exc).__name__}: {exc}", flush=True)
        await message.reply("❌ 維護模式切換失敗，請查看 Bot 日誌。", mention_author=False)


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"[SLASH CMD ERROR] {type(error).__name__}: {error}", flush=True)
    text = "❌ 指令執行失敗，請查看 Bot 日誌。"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except Exception:
        pass

@tree.command(
    name="maintenance",
    description="切換 DLP 網站維護狀態",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(action="選擇要執行的操作")
@app_commands.choices(
    action=[
        app_commands.Choice(name="🟡 開啟維護模式", value="on"),
        app_commands.Choice(name="🟢 關閉維護模式", value="off"),
        app_commands.Choice(name="🔎 查看目前狀態", value="status"),
    ]
)
@app_commands.default_permissions(manage_guild=True)
async def maintenance_command(
    interaction: discord.Interaction,
    action: app_commands.Choice[str],
):
    perms = getattr(interaction.user, "guild_permissions", None)
    if not perms or not perms.manage_guild:
        await interaction.response.send_message(
            "❌ 你沒有權限使用這個指令。",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        if action.value == "status":
            enabled = await is_maintenance_mode()
            text = "🟡 維護模式：已開啟" if enabled else "🟢 維護模式：已關閉"
            await interaction.followup.send(text, ephemeral=True)
            return

        enabled = action.value == "on"
        updater = f"{interaction.user} ({interaction.user.id})"
        text = await _apply_manual_maintenance(enabled, updater, interaction.user.id)
        await interaction.followup.send(text, ephemeral=True)
    except Exception as exc:
        print(f"[MAINTENANCE CMD] {type(exc).__name__}: {exc}", flush=True)
        await interaction.followup.send(
            "❌ 維護模式切換失敗，請查看 Bot 日誌。",
            ephemeral=True,
        )


@client.event
async def on_ready():
    global _worker_task, _website_status_task, _member_snapshot_task, _commands_synced
    print(f"[BOT] Logged in as {client.user} ({client.user.id if client.user else 'unknown'})", flush=True)
    try:
        await asyncio.to_thread(ensure_runtime_settings_schema_sync)
    except Exception as exc:
        print(f"[BOT] Runtime settings schema init failed: {type(exc).__name__}: {exc}", flush=True)
    if not _commands_synced:
        try:
            await tree.sync(guild=discord.Object(id=GUILD_ID))
            _commands_synced = True
            print("[BOT] Slash commands synced: /maintenance", flush=True)
        except Exception as exc:
            print(f"[BOT] Slash command sync failed: {type(exc).__name__}: {exc}", flush=True)
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(worker_loop())
    if _website_status_task is None or _website_status_task.done():
        _website_status_task = asyncio.create_task(website_status_loop())
    if _member_snapshot_task is None or _member_snapshot_task.done():
        _member_snapshot_task = asyncio.create_task(member_snapshot_loop())


if __name__ == "__main__":
    keep_alive()
    print("[BOT] Keep-alive HTTP server started.", flush=True)
    client.run(TOKEN, log_handler=None)
