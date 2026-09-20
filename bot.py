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
MEMBER_SNAPSHOT_INTERVAL = max(60, int(os.getenv("DISCORD_MEMBER_SNAPSHOT_INTERVAL", "60") or "60"))

# DLP website health monitor with automatic primary/backup failover.
# Primary is always preferred. Backup is used only when the primary website itself is unreachable/unhealthy.
DLP_WEBSITE_PRIMARY_URL = os.getenv(
    "DLP_WEBSITE_PRIMARY_URL",
    os.getenv("DLP_WEBSITE_URL", "https://web-production-021c2.up.railway.app"),
).strip().rstrip("/")
DLP_WEBSITE_BACKUP_URL = os.getenv(
    "DLP_WEBSITE_BACKUP_URL",
    "https://daybreak-stove-subpanel.ngrok-free.dev",
).strip().rstrip("/")
DLP_WEBSITE_PRIMARY_STATUS_URL = os.getenv(
    "DLP_WEBSITE_PRIMARY_STATUS_URL",
    os.getenv("DLP_WEBSITE_STATUS_URL", f"{DLP_WEBSITE_PRIMARY_URL}/api/bot-health"),
).strip()
DLP_WEBSITE_BACKUP_STATUS_URL = os.getenv(
    "DLP_WEBSITE_BACKUP_STATUS_URL",
    f"{DLP_WEBSITE_BACKUP_URL}/api/bot-health",
).strip()
WEBSITE_CHECK_INTERVAL = max(15, int(os.getenv("DLP_WEBSITE_CHECK_INTERVAL", "60") or "60"))
WEBSITE_TIMEOUT = max(3, int(os.getenv("DLP_WEBSITE_TIMEOUT", "10") or "10"))
# Manual maintenance switch. Set DLP_MAINTENANCE_MODE=true in Render to force maintenance presence.
DLP_MAINTENANCE_MODE_DEFAULT = os.getenv("DLP_MAINTENANCE_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}
# 手動野戰狀態：僅作為初次建立 DB 設定時的預設值。平常請用 Discord 指令切換。
DLP_FIELD_BATTLE_MODE_DEFAULT = os.getenv("DLP_FIELD_BATTLE_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}

ROLE_BY_LEVEL = {
    1: os.getenv("DISCORD_ROLE_LONGTOU_ID", "").strip(),
    2: os.getenv("DISCORD_ROLE_ZHANGQI_ID", "").strip(),
    3: os.getenv("DISCORD_ROLE_TANGZHU_ID", "").strip(),
    4: os.getenv("DISCORD_ROLE_ZHANJIANG_ID", "").strip(),
    5: os.getenv("DISCORD_ROLE_MENSHENG_ID", "").strip(),
}

INTERVIEWEE_ROLE_ID = os.getenv("DISCORD_ROLE_INTERVIEWEE_ID", "").strip()
CITIZEN_ROLE_ID = os.getenv("DISCORD_ROLE_CITIZEN_ID", "").strip()
MAINTAINER_ROLE_ID = os.getenv("DISCORD_MAINTAINER_ROLE_ID", "").strip()


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
    if not MAINTAINER_ROLE_ID.isdigit():
        missing.append("DISCORD_MAINTAINER_ROLE_ID")
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
_last_presence_state: Dict[str, Any] = {"kind": "online", "online": None, "offline": None, "wait_minutes": None, "detail": ""}
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
            cur.execute(
                """
                INSERT INTO discord_bot_runtime_settings(setting_key, setting_value, updated_by)
                VALUES ('field_battle_mode', %s, 'env-default')
                ON CONFLICT (setting_key) DO NOTHING
                """,
                ("true" if DLP_FIELD_BATTLE_MODE_DEFAULT else "false",),
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


def get_field_battle_mode_sync() -> bool:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT setting_value FROM discord_bot_runtime_settings WHERE setting_key='field_battle_mode'"
            )
            row = cur.fetchone()
            if not row:
                return DLP_FIELD_BATTLE_MODE_DEFAULT
            return str(row["setting_value"]).strip().lower() in {"1", "true", "yes", "on"}


def set_field_battle_mode_sync(enabled: bool, updated_by: str) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_bot_runtime_settings(setting_key, setting_value, updated_at, updated_by)
                VALUES ('field_battle_mode', %s, NOW(), %s)
                ON CONFLICT (setting_key) DO UPDATE SET
                  setting_value = EXCLUDED.setting_value,
                  updated_at = NOW(),
                  updated_by = EXCLUDED.updated_by
                """,
                ("true" if enabled else "false", updated_by[:100]),
            )
            conn.commit()


async def is_field_battle_mode() -> bool:
    try:
        return await asyncio.to_thread(get_field_battle_mode_sync)
    except Exception as exc:
        print(f"[FIELD BATTLE] Failed to read DB setting: {type(exc).__name__}: {exc}", flush=True)
        return DLP_FIELD_BATTLE_MODE_DEFAULT


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
        return "🔴DLP系統異常中🔴"
    return "🟢DLP系統正常中🟢"


async def _set_website_presence(
    kind: str,
    *,
    online: Optional[int] = None,
    offline: Optional[int] = None,
    wait_minutes: Optional[int] = None,
    detail: str = "",
    line: str = "primary",
) -> None:
    global _last_website_presence, _last_presence_state

    _last_presence_state = {
        "kind": kind,
        "online": online,
        "offline": offline,
        "wait_minutes": wait_minutes,
        "detail": detail,
        "line": line,
    }
    field_battle_enabled = await is_field_battle_mode()

    # 野戰開啟時使用警示圖示；關閉時使用雙劍圖示。
    if field_battle_enabled:
        battle_text = "🚨野戰狀態:開啟🚨"
    else:
        battle_text = "⚔️野戰狀態:關閉⚔️"

    if line == "backup" and kind == "online":
        system_text = "🔵DLP使用備用線🔵"
    else:
        system_text = _presence_text(kind, online, offline, wait_minutes)

    text = f"{system_text} ｜ {battle_text}"
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


async def _fetch_website_health(
    session: aiohttp.ClientSession,
    status_url: str,
    *,
    line: str,
) -> tuple[bool, Dict[str, Any], str]:
    """Return (usable, payload, detail) for one website line.

    A line is considered unusable only when the website cannot be reached, returns
    HTTP >= 400, or explicitly reports an unhealthy/offline state. OAuth 429 does
    NOT trigger failover because the website itself is still online.
    """
    try:
        async with session.get(
            status_url,
            headers={"User-Agent": "DLP-DiscordBot-WebsiteMonitor/1.2"},
            allow_redirects=True,
        ) as response:
            data = await _read_status_json(response)
            website_state = str(data.get("status") or "").strip().lower()
            print(
                f"[WEB STATUS CHECK][{line.upper()}] URL={status_url} "
                f"HTTP={response.status} status={website_state or '-'} "
                f"oauth_status={data.get('oauth_status')} "
                f"oauth_rate_limited={data.get('oauth_rate_limited')}",
                flush=True,
            )
            if response.status >= 400:
                return False, data, f"HTTP {response.status}"
            if website_state in {"error", "offline", "down", "unhealthy"}:
                return False, data, f"API status={website_state}"
            return True, data, f"HTTP {response.status}"
    except asyncio.TimeoutError:
        return False, {}, "timeout"
    except aiohttp.ClientError as exc:
        return False, {}, f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        return False, {}, f"{type(exc).__name__}: {exc}"


async def check_website_status(session: aiohttp.ClientSession) -> None:
    """Prefer Railway; automatically use ngrok only while Railway is unavailable."""
    if await is_maintenance_mode():
        await _set_website_presence("maintenance", detail="manual maintenance mode")
        return

    primary_ok, primary_data, primary_detail = await _fetch_website_health(
        session, DLP_WEBSITE_PRIMARY_STATUS_URL, line="primary"
    )

    if primary_ok:
        # Primary recovered: fail back immediately and automatically.
        if _oauth_is_rate_limited(primary_data):
            retry_minutes = _reported_retry_minutes(primary_data)
            _start_rate_limit_countdown(retry_minutes)
            await _set_website_presence(
                "rate_limited",
                wait_minutes=retry_minutes,
                detail=f"primary; {_oauth_retry_detail(primary_data)}",
                line="primary",
            )
            return
        await _set_website_presence(
            "online",
            online=_safe_count(primary_data.get("online")),
            offline=_safe_count(primary_data.get("offline")),
            detail=f"primary {primary_detail}",
            line="primary",
        )
        return

    print(f"[WEB FAILOVER] Primary unavailable ({primary_detail}); checking backup...", flush=True)
    backup_ok, backup_data, backup_detail = await _fetch_website_health(
        session, DLP_WEBSITE_BACKUP_STATUS_URL, line="backup"
    )

    if backup_ok:
        # Backup is active: always show the requested blue backup-line status.
        # Field-battle on/off still comes from the existing shared DB setting.
        await _set_website_presence(
            "online",
            online=_safe_count(backup_data.get("online")),
            offline=_safe_count(backup_data.get("offline")),
            detail=f"backup active; primary={primary_detail}; backup={backup_detail}",
            line="backup",
        )
        return

    await _set_website_presence(
        "error",
        detail=f"primary={primary_detail}; backup={backup_detail}",
    )


async def website_status_loop() -> None:
    await client.wait_until_ready()
    timeout = aiohttp.ClientTimeout(total=WEBSITE_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        print(
            f"[WEB STATUS] Primary={DLP_WEBSITE_PRIMARY_STATUS_URL} | Backup={DLP_WEBSITE_BACKUP_STATUS_URL} | every {WEBSITE_CHECK_INTERVAL}s",
            flush=True,
        )
        while not client.is_closed():
            # Manual maintenance always has the highest priority.
            if await is_maintenance_mode():
                await _set_website_presence("maintenance", detail="manual maintenance mode")
                await asyncio.sleep(60)
                continue

            # Always keep checking website availability so failover still works even
            # while Discord OAuth on the primary line is temporarily rate-limited.
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
            guild_id = str(GUILD_ID)
            current_ids: list[str] = []
            for user_id, name, nickname, roles_json, row_guild_id in rows:
                current_ids.append(str(user_id))
                cur.execute(
                    """INSERT INTO discord_member_role_cache
                       (discord_user_id,discord_name,nickname,roles,guild_id,synced_at)
                       VALUES(%s,%s,%s,%s::jsonb,%s,NOW())
                       ON CONFLICT(discord_user_id) DO UPDATE SET
                         discord_name=EXCLUDED.discord_name,nickname=EXCLUDED.nickname,
                         roles=EXCLUDED.roles,guild_id=EXCLUDED.guild_id,synced_at=NOW()""",
                    (user_id, name, nickname, roles_json, row_guild_id),
                )

            # Remove people who are no longer in the Discord guild. Without this cleanup,
            # a departed user could remain in the website cache until it expired.
            if current_ids:
                cur.execute(
                    "DELETE FROM discord_member_role_cache WHERE guild_id=%s AND NOT (discord_user_id = ANY(%s::text[]))",
                    (guild_id, current_ids),
                )
            else:
                cur.execute("DELETE FROM discord_member_role_cache WHERE guild_id=%s", (guild_id,))
            conn.commit()


def delete_member_snapshot_sync(member_id: int) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM discord_member_role_cache WHERE discord_user_id=%s AND guild_id=%s",
                (str(member_id), str(GUILD_ID)),
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


@client.event
async def on_member_remove(member: discord.Member):
    try:
        await asyncio.to_thread(delete_member_snapshot_sync, member.id)
        _member_cache.pop(member.id, None)
        print(f'[BOT] Discord member left guild; snapshot removed: {member.id}', flush=True)
    except Exception as exc:
        print(f'[BOT] Member snapshot remove failed for {member.id}: {type(exc).__name__}: {exc}', flush=True)


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




DM_EVENT_TYPE_LABELS = {
    "notification": "系統通知",
    "new_application": "新入幫申請",
    "rank_change": "階級異動",
    "violation": "違規懲處",
    "report_submitted": "新回報案件",
    "report_result": "回報處理結果",
    "announcement": "幫派公告",
    "blacklist_appeal": "黑名單申訴結果",
    "leave_request": "請假申請",
    "name_change": "改名申請",
    "promotion": "晉升通知",
    "demotion": "降階通知",
    "application": "入幫申請",
    "important": "重要公告",
    "urgent": "緊急公告",
    "normal": "一般公告",
    "一般": "一般公告",
    "重要": "重要公告",
    "緊急": "緊急公告",
}

def _dm_event_type_label(value: str) -> str:
    key = str(value or "notification").strip()
    if key in DM_EVENT_TYPE_LABELS:
        return DM_EVENT_TYPE_LABELS[key]
    lowered = key.lower()
    if lowered in DM_EVENT_TYPE_LABELS:
        return DM_EVENT_TYPE_LABELS[lowered]
    # 未知內部代碼不直接顯示英文/底線給使用者。
    if any(ch.isascii() and ch.isalpha() for ch in key) or "_" in key:
        return "系統通知"
    return key or "系統通知"

async def apply_job(job: Dict[str, Any]) -> None:
    payload = job.get("payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)

    job_type = str(job.get("job_type") or "")

    if job_type == "maintainer_support_alert":
        role_id_raw = MAINTAINER_ROLE_ID
        if not role_id_raw.isdigit():
            raise ValueError("DISCORD_MAINTAINER_ROLE_ID missing or invalid")
        guild = client.get_guild(int(GUILD_ID)) if str(GUILD_ID).isdigit() else None
        if guild is None:
            raise RuntimeError("Discord guild not found")
        role = guild.get_role(int(role_id_raw))
        if role is None:
            raise RuntimeError(f"Maintainer role {role_id_raw} not found")
        case_no = str(payload.get("case_no") or "支援案件")
        user_name = str(payload.get("user_name") or "未知使用者")
        client_version = str(payload.get("client_version") or "未知")
        latest_version = str(payload.get("latest_version") or "未知")
        embed = discord.Embed(title="🚨 DLP｜系統支援通知", description="有人因版本異常無法進入系統。", color=0xDC2626, timestamp=discord.utils.utcnow())
        embed.add_field(name="使用者", value=user_name, inline=True)
        embed.add_field(name="案件編號", value=case_no, inline=True)
        embed.add_field(name="目前版本", value=client_version, inline=True)
        embed.add_field(name="最新版本", value=latest_version, inline=True)
        embed.add_field(name="處理位置", value="DLP 管理員後台控制中心 → 系統支援中心", inline=False)
        embed.set_footer(text="DLP｜大聯社 系統支援")
        sent = 0
        for target in list(role.members):
            if target.bot:
                continue
            try:
                await target.send(embed=embed)
                sent += 1
            except discord.Forbidden:
                print(f"[BOT] Maintainer support DM blocked: {target.id}", flush=True)
            except Exception as exc:
                print(f"[BOT] Maintainer support DM failed {target.id}: {exc}", flush=True)
        print(f"[BOT] maintainer_support_alert OK: {sent} maintainer DM(s)", flush=True)
        return

    user_id_raw = str(payload.get("discord_user_id") or "").strip()
    if not user_id_raw.isdigit():
        raise ValueError("discord_user_id missing or invalid")

    member = await get_member(int(user_id_raw))
    guild = member.guild

    if job_type == "direct_dm":
        title = str(payload.get("title") or "DLP｜大聯社通知").strip()
        message = str(payload.get("message") or "").strip()
        event_type = _dm_event_type_label(str(payload.get("event_type") or "notification").strip())
        embed = discord.Embed(
            title=f"🔔 {title}",
            description=message or "您有一則新的 DLP 系統通知。",
            color=0xB91C1C,
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="通知類型", value=event_type, inline=True)
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


async def _refresh_presence_after_field_battle_change() -> None:
    state = dict(_last_presence_state)
    await _set_website_presence(
        str(state.get("kind") or "online"),
        online=state.get("online"),
        offline=state.get("offline"),
        wait_minutes=state.get("wait_minutes"),
        detail="manual field battle status update",
        line=str(state.get("line") or "primary"),
    )


async def _apply_manual_field_battle(enabled: bool, actor: str) -> str:
    await asyncio.to_thread(set_field_battle_mode_sync, enabled, actor)
    await _refresh_presence_after_field_battle_change()
    return f"⚔️ 目前野戰狀態：{'開啟' if enabled else '關閉'}"


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
    if command not in {"!維護", "!maintenance", "!野戰", "!fieldbattle", "!battle"}:
        return

    perms = getattr(message.author, "guild_permissions", None)
    if not perms or not perms.manage_guild:
        await message.reply("❌ 你沒有權限使用這個指令。", mention_author=False)
        return

    if len(parts) < 2:
        usage = (
            "用法：`!野戰 開`、`!野戰 關`、`!野戰 狀態`"
            if command in {"!野戰", "!fieldbattle", "!battle"}
            else "用法：`!維護 開`、`!維護 關`、`!維護 狀態`"
        )
        await message.reply(usage, mention_author=False)
        return

    action = parts[1].strip().lower()
    try:
        if command in {"!野戰", "!fieldbattle", "!battle"}:
            if action in {"狀態", "status", "查看"}:
                enabled = await is_field_battle_mode()
                await message.reply(
                    f"⚔️ 目前野戰狀態：{'開啟' if enabled else '關閉'}",
                    mention_author=False,
                )
                return
            if action in {"開", "開啟", "on", "true"}:
                text = await _apply_manual_field_battle(True, f"{message.author} ({message.author.id})")
                await message.reply(text, mention_author=False)
                return
            if action in {"關", "關閉", "off", "false"}:
                text = await _apply_manual_field_battle(False, f"{message.author} ({message.author.id})")
                await message.reply(text, mention_author=False)
                return
            await message.reply(
                "❌ 不認得這個操作。請用：`!野戰 開`、`!野戰 關`、`!野戰 狀態`",
                mention_author=False,
            )
            return

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


@tree.command(
    name="fieldbattle",
    description="手動切換 DLP 目前野戰狀態",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(action="選擇目前野戰狀態")
@app_commands.choices(
    action=[
        app_commands.Choice(name="⚔️ 開啟野戰狀態", value="on"),
        app_commands.Choice(name="🛡️ 關閉野戰狀態", value="off"),
        app_commands.Choice(name="🔎 查看目前野戰狀態", value="status"),
    ]
)
@app_commands.default_permissions(manage_guild=True)
async def fieldbattle_command(
    interaction: discord.Interaction,
    action: app_commands.Choice[str],
):
    perms = getattr(interaction.user, "guild_permissions", None)
    if not perms or not perms.manage_guild:
        await interaction.response.send_message("❌ 你沒有權限使用這個指令。", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        if action.value == "status":
            enabled = await is_field_battle_mode()
            await interaction.followup.send(
                f"⚔️ 目前野戰狀態：{'開啟' if enabled else '關閉'}",
                ephemeral=True,
            )
            return
        enabled = action.value == "on"
        text = await _apply_manual_field_battle(enabled, f"{interaction.user} ({interaction.user.id})")
        await interaction.followup.send(text, ephemeral=True)
    except Exception as exc:
        print(f"[FIELD BATTLE CMD] {type(exc).__name__}: {exc}", flush=True)
        await interaction.followup.send("❌ 野戰狀態切換失敗，請查看 Bot 日誌。", ephemeral=True)


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
            print("[BOT] Slash commands synced: /maintenance, /fieldbattle", flush=True)
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
