import asyncio
import json
import os
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone
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
# Always respect the guild configured in .env.
# Do NOT silently replace a test/legacy guild ID with another server ID:
# a bot that is not a member of that replacement guild will receive Discord 403 Missing Access.
GUILD_ID_RAW = os.getenv("DISCORD_GUILD_ID", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
POLL_INTERVAL = max(1, int(os.getenv("DISCORD_BOT_POLL_INTERVAL", "3") or "3"))
MAX_ATTEMPTS = max(1, int(os.getenv("DISCORD_BOT_JOB_MAX_ATTEMPTS", "5") or "5"))
JOB_MIN_INTERVAL = max(0.1, float(os.getenv("DISCORD_BOT_JOB_MIN_INTERVAL", "0.35") or "0.35"))
MEMBER_CACHE_TTL = max(10, int(os.getenv("DISCORD_MEMBER_CACHE_TTL", "120") or "120"))
MEMBER_SNAPSHOT_INTERVAL = max(60, int(os.getenv("DISCORD_MEMBER_SNAPSHOT_INTERVAL", "60") or "60"))

# DLP website health monitor with automatic three-line failover.
# Priority: Railway primary -> Abasthan backup1 -> ngrok backup2.
DLP_WEBSITE_PRIMARY_URL = os.getenv(
    "DLP_WEBSITE_PRIMARY_URL",
    os.getenv("DLP_WEBSITE_URL", "https://web-production-021c2.up.railway.app"),
).strip().rstrip("/")
DLP_WEBSITE_BACKUP1_URL = os.getenv(
    "DLP_WEBSITE_BACKUP1_URL",
    "https://behest-burly-dolphin.abasthan.app",
).strip().rstrip("/")
DLP_WEBSITE_BACKUP2_URL = os.getenv(
    "DLP_WEBSITE_BACKUP2_URL",
    os.getenv("DLP_WEBSITE_BACKUP_URL", "https://daybreak-stove-subpanel.ngrok-free.dev"),
).strip().rstrip("/")
DLP_WEBSITE_PRIMARY_STATUS_URL = os.getenv(
    "DLP_WEBSITE_PRIMARY_STATUS_URL",
    os.getenv("DLP_WEBSITE_STATUS_URL", f"{DLP_WEBSITE_PRIMARY_URL}/api/bot-health"),
).strip()
DLP_WEBSITE_BACKUP1_STATUS_URL = os.getenv(
    "DLP_WEBSITE_BACKUP1_STATUS_URL",
    f"{DLP_WEBSITE_BACKUP1_URL}/api/bot-health",
).strip()
DLP_WEBSITE_BACKUP2_STATUS_URL = os.getenv(
    "DLP_WEBSITE_BACKUP2_STATUS_URL",
    os.getenv("DLP_WEBSITE_BACKUP_STATUS_URL", f"{DLP_WEBSITE_BACKUP2_URL}/api/bot-health"),
).strip()
WEBSITE_CHECK_INTERVAL = max(15, int(os.getenv("DLP_WEBSITE_CHECK_INTERVAL", "60") or "60"))
WEBSITE_TIMEOUT = max(3, int(os.getenv("DLP_WEBSITE_TIMEOUT", "10") or "10"))
# Manual maintenance switch. Set DLP_MAINTENANCE_MODE=true in Render to force maintenance presence.
DLP_MAINTENANCE_MODE_DEFAULT = os.getenv("DLP_MAINTENANCE_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}
# 手動野戰狀態：僅作為初次建立 DB 設定時的預設值。平常請用 Discord 指令切換。
DLP_FIELD_BATTLE_MODE_DEFAULT = os.getenv("DLP_FIELD_BATTLE_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}

# DLP 成員 Discord 名稱自動稽核。
# 每 30 分鐘掃描現役成員；首次不符立即口頭警告+50萬，
# 之後每滿 24 小時仍未整改，再追加 1 隻警告，罰款每階段 +50萬。
NAME_COMPLIANCE_SCAN_INTERVAL = max(300, int(os.getenv("DLP_NAME_COMPLIANCE_SCAN_INTERVAL", "1800") or "1800"))
NAME_COMPLIANCE_GRACE_SECONDS = max(3600, int(os.getenv("DLP_NAME_COMPLIANCE_GRACE_SECONDS", "86400") or "86400"))
VIOLATION_RECORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_VIOLATION_RECORD", "").strip()
VIOLATION_PLAYER_WEBHOOK = os.getenv("DISCORD_WEBHOOK_VIOLATION_PLAYER", "").strip()
VIOLATION_RECORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_VIOLATION_RECORD", "").strip()
VIOLATION_PLAYER_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_VIOLATION_PLAYER", "").strip()
# Bot 自動名稱稽核的正式懲處公告頻道。
# 僅影響 Bot 自動/手動 namecheck 產生的名稱不符懲處；網站原本手動懲處 Webhook 不動。
NAME_PENALTY_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_NAME_PENALTY", "").strip()

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
WELCOME_CHANNEL_ID = os.getenv("DISCORD_WELCOME_CHANNEL_ID", "").strip()


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
_field_battle_watch_task: Optional[asyncio.Task] = None
_name_compliance_task: Optional[asyncio.Task] = None
_last_effective_field_battle: Optional[bool] = None
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
    """Return the Bot's manual battlefield switch.

    This remains independent from website war records so /fieldbattle can still
    manually force the Discord presence to battlefield ON.
    """
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT setting_value FROM discord_bot_runtime_settings WHERE setting_key='field_battle_mode'"
            )
            row = cur.fetchone()
            if not row:
                return DLP_FIELD_BATTLE_MODE_DEFAULT
            return str(row["setting_value"]).strip().lower() in {"1", "true", "yes", "on"}


def get_website_field_battle_mode_sync() -> bool:
    """Detect whether the website currently has an active war report.

    The internal website treats a war record with no ended_at value as
    "currently in battle". Reading the same PostgreSQL table lets the Bot
    mirror that state without changing the website's existing health/status
    logic and without adding another public API.
    """
    with _db_connect() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1
                        FROM war_reports
                       WHERE report_kind = 'war'
                         AND ended_at IS NULL
                       LIMIT 1
                    ) AS active
                    """
                )
                row = cur.fetchone()
                return bool(row and row.get("active"))
            except Exception as exc:
                # Older databases may not have war_reports yet. Roll back the
                # failed transaction and simply treat website battle as OFF.
                conn.rollback()
                print(f"[FIELD BATTLE] Website war detection unavailable: {type(exc).__name__}: {exc}", flush=True)
                return False


def get_effective_field_battle_mode_sync() -> bool:
    # Website active war OR Bot manual switch = battlefield ON.
    return get_website_field_battle_mode_sync() or get_field_battle_mode_sync()


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
        return await asyncio.to_thread(get_effective_field_battle_mode_sync)
    except Exception as exc:
        print(f"[FIELD BATTLE] Failed to read battlefield state: {type(exc).__name__}: {exc}", flush=True)
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

    if line in {"backup1", "backup2"} and kind == "online":
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
    """Priority: Railway primary -> Abasthan backup1 -> ngrok backup2."""
    if await is_maintenance_mode():
        await _set_website_presence("maintenance", detail="manual maintenance mode")
        return

    primary_ok, primary_data, primary_detail = await _fetch_website_health(
        session, DLP_WEBSITE_PRIMARY_STATUS_URL, line="primary"
    )

    if primary_ok:
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

    print(f"[WEB FAILOVER] Primary unavailable ({primary_detail}); checking backup1...", flush=True)
    backup1_ok, backup1_data, backup1_detail = await _fetch_website_health(
        session, DLP_WEBSITE_BACKUP1_STATUS_URL, line="backup1"
    )

    if backup1_ok:
        await _set_website_presence(
            "online",
            online=_safe_count(backup1_data.get("online")),
            offline=_safe_count(backup1_data.get("offline")),
            detail=f"backup1 active; primary={primary_detail}; backup1={backup1_detail}",
            line="backup1",
        )
        return

    print(f"[WEB FAILOVER] Backup1 unavailable ({backup1_detail}); checking backup2...", flush=True)
    backup2_ok, backup2_data, backup2_detail = await _fetch_website_health(
        session, DLP_WEBSITE_BACKUP2_STATUS_URL, line="backup2"
    )

    if backup2_ok:
        await _set_website_presence(
            "online",
            online=_safe_count(backup2_data.get("online")),
            offline=_safe_count(backup2_data.get("offline")),
            detail=f"backup2 active; primary={primary_detail}; backup1={backup1_detail}; backup2={backup2_detail}",
            line="backup2",
        )
        return

    await _set_website_presence(
        "error",
        detail=f"primary={primary_detail}; backup1={backup1_detail}; backup2={backup2_detail}",
    )


async def website_status_loop() -> None:
    await client.wait_until_ready()
    timeout = aiohttp.ClientTimeout(total=WEBSITE_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        print(
            f"[WEB STATUS] Primary={DLP_WEBSITE_PRIMARY_STATUS_URL} | Backup1={DLP_WEBSITE_BACKUP1_STATUS_URL} | Backup2={DLP_WEBSITE_BACKUP2_STATUS_URL} | every {WEBSITE_CHECK_INTERVAL}s",
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



def ensure_name_compliance_schema_sync() -> None:
    """Create the persistent state used to prevent a 30-minute scan from re-penalizing the same mismatch."""
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS discord_name_compliance_cases (
                  discord_user_id VARCHAR(64) PRIMARY KEY,
                  member_id BIGINT DEFAULT NULL,
                  expected_name VARCHAR(100) NOT NULL,
                  discord_name VARCHAR(100) NOT NULL,
                  first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  last_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  last_penalty_at TIMESTAMPTZ DEFAULT NULL,
                  penalty_stage INTEGER NOT NULL DEFAULT 0,
                  active BOOLEAN NOT NULL DEFAULT TRUE,
                  resolved_at TIMESTAMPTZ DEFAULT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_discord_name_compliance_active
                  ON discord_name_compliance_cases(active, last_penalty_at);

                ALTER TABLE violations ADD COLUMN IF NOT EXISTS punishment_level VARCHAR(8);
                ALTER TABLE violations ADD COLUMN IF NOT EXISTS warning_points INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE violations ADD COLUMN IF NOT EXISTS oral_warning BOOLEAN NOT NULL DEFAULT FALSE;
                ALTER TABLE violations ADD COLUMN IF NOT EXISTS fine_wan INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE violations ADD COLUMN IF NOT EXISTS suspension_days INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE violations ADD COLUMN IF NOT EXISTS rank_action VARCHAR(80);
                ALTER TABLE violations ADD COLUMN IF NOT EXISTS dismissal_action VARCHAR(80);

                CREATE TABLE IF NOT EXISTS clearance_reviews (
                  id BIGSERIAL PRIMARY KEY,
                  member_id INTEGER NOT NULL,
                  discord_user_id VARCHAR(64),
                  member_name VARCHAR(120) NOT NULL,
                  rank_title VARCHAR(60),
                  warning_count INTEGER NOT NULL DEFAULT 3,
                  trigger_violation_id INTEGER,
                  status VARCHAR(32) NOT NULL DEFAULT 'pending_officer',
                  officer_decision VARCHAR(20),
                  officer_note TEXT,
                  officer_reviewer_discord_id VARCHAR(64),
                  officer_reviewer_name VARCHAR(120),
                  officer_reviewed_at TIMESTAMPTZ,
                  final_decision VARCHAR(20),
                  final_note TEXT,
                  final_reviewer_discord_id VARCHAR(64),
                  final_reviewer_name VARCHAR(120),
                  final_reviewed_at TIMESTAMPTZ,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_clearance_reviews_one_active
                  ON clearance_reviews(member_id)
                  WHERE status IN ('pending_officer','pending_final');
                """
            )
            conn.commit()


def _normalize_member_name(value: str) -> str:
    # Unicode normalize + trim. We intentionally keep punctuation/case semantics simple:
    # Discord nicknames and DLP system names should visibly match.
    return unicodedata.normalize("NFKC", str(value or "")).strip().casefold()


def _punishment_warning_points(punishment: str) -> int:
    """Mirror the website's current warning-point interpretation."""
    p = str(punishment or "").strip()
    if "黑名單" in p and "踢出" in p:
        return 99
    if "警告三隻" in p or "3" in p or "三" in p:
        return 3
    if "警告兩隻" in p or "2" in p or "兩" in p or "二" in p:
        return 2
    if "警告一隻" in p or "1" in p or "一" in p:
        return 1
    # 口頭警告屬初犯告誡，不計入有效警告次數。
    # 自動名稱稽核第 1 階段會寫成「口頭警告＋罰款 50 萬」，因此用包含判斷。
    if "口頭警告" in p:
        return 0
    # 其他無法辨識的有效處分保守視為 1 隻警告。
    return 1


def _recalculate_warning_count_sync(cur, expected_name: str, stored_discord_name: str) -> int:
    cur.execute(
        """
        SELECT punishment, warning_points, oral_warning
          FROM violations
         WHERE COALESCE(status, 'active') = 'active'
           AND (
                LOWER(COALESCE(target_name,'')) = LOWER(%s)
             OR LOWER(COALESCE(game_name,'')) = LOWER(%s)
             OR (%s <> '' AND LOWER(COALESCE(discord_name,'')) = LOWER(%s))
           )
        """,
        (expected_name, expected_name, stored_discord_name, stored_discord_name),
    )
    total = 0
    for row in cur.fetchall() or []:
        if bool(row.get("oral_warning")):
            pts = 0
        elif row.get("warning_points") is not None:
            pts = max(0, int(row.get("warning_points") or 0))
        else:
            pts = _punishment_warning_points(str(row.get("punishment") or ""))
        if pts >= 99:
            return 99
        total += pts
    return total


def _register_name_penalty_sync(member_row: Dict[str, Any], discord_display_name: str, stage: int) -> Dict[str, Any]:
    """Insert a violation exactly into the website's violations table and synchronize member status."""
    expected_name = str(member_row.get("game_name") or "").strip()
    stored_discord_name = str(member_row.get("discord_name") or "").strip()
    discord_user_id = str(member_row.get("discord_user_id") or "").strip()
    member_id = member_row.get("id")
    fine_wan = 50 * max(1, stage)
    punishment = (
        f"口頭警告＋罰款 {fine_wan} 萬"
        if stage == 1
        else f"警告一隻＋罰款 {fine_wan} 萬"
    )
    title = (
        "Discord 名稱與系統登記名稱不符"
        if stage == 1
        else f"Discord 名稱逾期 24 小時仍未整改（第 {stage} 階段）"
    )
    details = (
        f"系統名稱：{expected_name}｜Discord 目前名稱：{discord_display_name}。"
        f"本次為第 {stage} 階段自動懲處；請於 24 小時內完成整改，否則下一階段將再次追加警告與罰款。"
    )
    operator = "DLP 名稱稽核系統"

    with _db_connect() as conn:
        with conn.cursor() as cur:
            # Lock both member and case so multiple Bot instances cannot duplicate a stage.
            cur.execute("SELECT * FROM gang_members WHERE id=%s FOR UPDATE", (member_id,))
            locked = cur.fetchone()
            if not locked:
                raise RuntimeError(f"gang member disappeared: {member_id}")
            cur.execute(
                "SELECT * FROM discord_name_compliance_cases WHERE discord_user_id=%s FOR UPDATE",
                (discord_user_id,),
            )
            case = cur.fetchone()
            current_stage = int((case or {}).get("penalty_stage") or 0)
            if current_stage >= stage and bool((case or {}).get("active")):
                conn.rollback()
                return {"skipped": True, "stage": current_stage, "reason": "stage already penalized"}

            cur.execute(
                """
                INSERT INTO violations(
                  target_name, game_name, discord_name, violation_type, violation_title,
                  severity, details, punishment, punishment_level, warning_points, oral_warning,
                  fine_wan, suspension_days, rank_action, dismissal_action,
                  issued_by, handled_by, date, created_at, is_traitor, status, evidence
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                          TO_CHAR((NOW() AT TIME ZONE 'Asia/Taipei')::date, 'YYYY-MM-DD'),NOW(),FALSE,'active','[]'::jsonb)
                RETURNING *
                """,
                (
                    expected_name, expected_name, discord_display_name or stored_discord_name,
                    "discord_name_mismatch", title, "low" if stage == 1 else "medium", details, punishment,
                    "P1" if stage == 1 else "P2",
                    0 if stage == 1 else 1,
                    stage == 1,
                    fine_wan, 0, "none", "none",
                    operator, operator,
                ),
            )
            violation = dict(cur.fetchone())

            warning_count = _recalculate_warning_count_sync(cur, expected_name, stored_discord_name)
            clearance_triggered = warning_count >= 3
            note = f"【違規處分】{title}（處分：{punishment}）"
            cur.execute(
                """
                UPDATE gang_members
                   SET merits = CASE
                     WHEN COALESCE(NULLIF(merits,''),'') = '' THEN %s
                     ELSE merits || '；' || %s
                   END
                 WHERE id=%s
                """,
                (note, note, member_id),
            )

            cur.execute(
                "UPDATE gang_members SET warning_count=%s WHERE id=%s",
                (warning_count, member_id),
            )

            if clearance_triggered:
                cur.execute(
                    """
                    INSERT INTO clearance_reviews(
                      member_id,discord_user_id,member_name,rank_title,warning_count,
                      trigger_violation_id,status,updated_at
                    ) VALUES(%s,%s,%s,%s,%s,%s,'pending_officer',NOW())
                    ON CONFLICT DO NOTHING
                    """,
                    (member_id, discord_user_id, expected_name, str(locked.get("rank_title") or ""), warning_count, violation["id"]),
                )
                cur.execute(
                    """
                    INSERT INTO management_notifications(discord_user_id,event_type,title,message,link)
                    VALUES(%s,'clearance_review','已進入清退評估',%s,'/internal?tab=review_center&review=clearance')
                    """,
                    (discord_user_id, f"您目前累積 {warning_count} 隻有效正式警告，已進入清退評估；這不代表已被清退。"),
                )

            # Same website notification center behavior as a manual punishment.
            cur.execute(
                """
                INSERT INTO management_notifications(discord_user_id,event_type,title,message,link)
                VALUES(%s,'violation','懲處通知',%s,'/internal?tab=violations')
                """,
                (discord_user_id, f"您收到一筆懲處：{title}｜處分：{punishment}"),
            )

            cur.execute(
                """
                INSERT INTO discord_name_compliance_cases(
                  discord_user_id,member_id,expected_name,discord_name,first_detected_at,last_detected_at,
                  last_penalty_at,penalty_stage,active,resolved_at,updated_at
                ) VALUES(%s,%s,%s,%s,NOW(),NOW(),NOW(),%s,TRUE,NULL,NOW())
                ON CONFLICT(discord_user_id) DO UPDATE SET
                  member_id=EXCLUDED.member_id,
                  expected_name=EXCLUDED.expected_name,
                  discord_name=EXCLUDED.discord_name,
                  last_detected_at=NOW(),
                  last_penalty_at=NOW(),
                  penalty_stage=EXCLUDED.penalty_stage,
                  active=TRUE,
                  resolved_at=NULL,
                  updated_at=NOW()
                """,
                (discord_user_id, member_id, expected_name, discord_display_name, stage),
            )
            conn.commit()
            return {
                "skipped": False,
                "violation": violation,
                "warning_count": warning_count,
                "is_kicked": False,
                "clearance_triggered": clearance_triggered,
                "punishment": punishment,
                "title": title,
                "details": details,
                "stage": stage,
                "expected_name": expected_name,
                "discord_name": discord_display_name,
                "discord_user_id": discord_user_id,
            }


def _load_name_compliance_members_sync() -> list[Dict[str, Any]]:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, discord_user_id, discord_name, game_name, rank_level, rank_title,
                       status, is_blacklisted, warning_count
                  FROM gang_members
                 WHERE status='active'
                   AND COALESCE(is_blacklisted,FALSE)=FALSE
                   AND rank_level BETWEEN 1 AND 5
                   AND rank_title <> '市民'
                   AND rank_title <> '後台維護人員'
                   AND discord_user_id IS NOT NULL
                   AND LENGTH(TRIM(discord_user_id)) > 10
                   AND game_name IS NOT NULL
                   AND LENGTH(TRIM(game_name)) > 0
                 ORDER BY rank_level ASC, id ASC
                """
            )
            return [dict(r) for r in (cur.fetchall() or [])]


def _load_name_compliance_case_sync(discord_user_id: str) -> Optional[Dict[str, Any]]:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM discord_name_compliance_cases WHERE discord_user_id=%s",
                (discord_user_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def _touch_name_compliance_mismatch_sync(member_row: Dict[str, Any], discord_name: str) -> Dict[str, Any]:
    uid = str(member_row.get("discord_user_id") or "").strip()
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_name_compliance_cases(
                  discord_user_id,member_id,expected_name,discord_name,first_detected_at,last_detected_at,
                  penalty_stage,active,resolved_at,updated_at
                ) VALUES(%s,%s,%s,%s,NOW(),NOW(),0,TRUE,NULL,NOW())
                ON CONFLICT(discord_user_id) DO UPDATE SET
                  member_id=EXCLUDED.member_id,
                  expected_name=EXCLUDED.expected_name,
                  discord_name=EXCLUDED.discord_name,
                  first_detected_at=CASE
                    WHEN discord_name_compliance_cases.active THEN discord_name_compliance_cases.first_detected_at
                    ELSE NOW()
                  END,
                  last_detected_at=NOW(),
                  last_penalty_at=CASE
                    WHEN discord_name_compliance_cases.active THEN discord_name_compliance_cases.last_penalty_at
                    ELSE NULL
                  END,
                  penalty_stage=CASE
                    WHEN discord_name_compliance_cases.active THEN discord_name_compliance_cases.penalty_stage
                    ELSE 0
                  END,
                  active=TRUE,
                  resolved_at=NULL,
                  updated_at=NOW()
                RETURNING *
                """,
                (uid, member_row.get("id"), str(member_row.get("game_name") or "").strip(), discord_name),
            )
            row = dict(cur.fetchone())
            conn.commit()
            return row


def _resolve_name_compliance_case_sync(discord_user_id: str, current_name: str) -> bool:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE discord_name_compliance_cases
                   SET active=FALSE,resolved_at=NOW(),discord_name=%s,last_detected_at=NOW(),updated_at=NOW()
                 WHERE discord_user_id=%s AND active=TRUE
                RETURNING discord_user_id
                """,
                (current_name, discord_user_id),
            )
            changed = cur.fetchone() is not None
            conn.commit()
            return changed


async def _post_webhook_embed(url: str, embed: discord.Embed) -> bool:
    if not url:
        return False
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json={"embeds": [embed.to_dict()]}) as resp:
            if 200 <= resp.status < 300:
                return True
            body = await resp.text()
            raise RuntimeError(f"Webhook HTTP {resp.status}: {body[:300]}")


async def _send_embed_to_channel(channel_id_raw: str, embed: discord.Embed) -> bool:
    if not str(channel_id_raw or "").isdigit():
        return False
    channel = client.get_channel(int(channel_id_raw))
    if channel is None:
        try:
            channel = await client.fetch_channel(int(channel_id_raw))
        except Exception:
            return False
    if not hasattr(channel, "send"):
        return False
    await channel.send(embed=embed)
    return True


async def send_name_penalty_notifications(result: Dict[str, Any], member: discord.Member) -> None:
    if result.get("skipped"):
        return
    target = result["expected_name"]
    current = result["discord_name"]
    title = result["title"]
    punishment = result["punishment"]
    warning_count = int(result.get("warning_count") or 0)
    is_kicked = bool(result.get("is_kicked"))
    clearance_triggered = bool(result.get("clearance_triggered"))
    stage = int(result.get("stage") or 1)
    now = discord.utils.utcnow()

    # 自動名稱稽核的 Discord 公告統一採用網站手動懲處的「玩家版」樣式。
    # 公開公告不額外塞入 Discord 名稱/整改欄位，避免和網站原本格式產生落差。
    player_embed = discord.Embed(
        title="⚠️【大聯社 幫會紀律處分通告】",
        description=f"成員 **{target}** 因違反幫規紀律，已依幫規處以懲戒！",
        color=0xDC2626,
        timestamp=now,
    )
    player_embed.add_field(name="受處分成員", value=f"**{target}**", inline=True)
    player_embed.add_field(name="違規事由", value=title, inline=True)
    player_embed.add_field(name="懲戒處分", value=f"**{punishment}**", inline=False)
    player_embed.add_field(
        name="有效警告記點",
        value=(
            f"累計 **{warning_count} 隻** "
            + ("（⚠️ 已達 3 隻，進入清退評估；不代表已被清退）" if clearance_triggered else "")
        ),
        inline=False,
    )
    player_embed.set_footer(text="DLP 幫規懲戒警示 (Bot 自動名稱稽核)")

    try:
        sent = await _send_embed_to_channel(NAME_PENALTY_CHANNEL_ID, player_embed)
        if not sent:
            print(
                "[NAME-CHECK] 尚未設定 DISCORD_CHANNEL_NAME_PENALTY，正式懲處公告未送出。",
                flush=True,
            )
        else:
            print(
                f"[NAME-CHECK] 懲處公告已送至頻道 {NAME_PENALTY_CHANNEL_ID}: {target} | {punishment}",
                flush=True,
            )
    except Exception as exc:
        print(f"[NAME-CHECK] 懲處公告發送失敗: {type(exc).__name__}: {exc}", flush=True)

    # 當事人仍另外收到 24 小時整改 DM；公開頻道只維持圖二的簡潔通告樣式。
    if not is_kicked:
        dm = discord.Embed(
            title="🚨 DLP｜Discord 名稱整改通知",
            description=(
                f"系統偵測到您的 Discord 伺服器名稱與 DLP 系統登記名稱不一致。\n\n"
                f"系統登記：**{target}**\nDiscord 目前：**{current}**\n\n"
                f"本次處分：**{punishment}**\n"
                "請於 **24 小時內**完成名稱整改。若逾期仍未修正，系統將自動追加下一階段懲處。"
                + ("\n\n⚠️ 您目前已達 **3 隻以上有效正式警告**，系統已建立清退評估；須經管理層審核，不會自動踢除。" if clearance_triggered else "")
            ),
            color=0xDC2626,
            timestamp=now,
        )
        dm.set_footer(text="DLP｜大聯社 名稱自動稽核")
        try:
            await member.send(embed=dm)
        except Exception as exc:
            print(f"[NAME-CHECK] 無法 DM {member} ({member.id}): {type(exc).__name__}: {exc}", flush=True)


async def run_name_compliance_scan() -> None:
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        raise RuntimeError("Discord guild not found for name compliance scan")

    rows = await asyncio.to_thread(_load_name_compliance_members_sync)
    total = len(rows)
    normal = 0
    mismatch = 0
    penalized = 0
    missing = 0
    print(f"[NAME-CHECK] 開始掃描 {total} 名現役成員", flush=True)

    for row in rows:
        uid_raw = str(row.get("discord_user_id") or "").strip()
        if not uid_raw.isdigit():
            missing += 1
            continue
        discord_member = guild.get_member(int(uid_raw))
        if discord_member is None:
            try:
                discord_member = await guild.fetch_member(int(uid_raw))
            except discord.NotFound:
                missing += 1
                print(f"[NAME-CHECK] ⚠ Discord 成員不存在: {row.get('game_name')} ({uid_raw})", flush=True)
                continue
            except discord.HTTPException as exc:
                # API failure is not a name violation. Never penalize on uncertain data.
                missing += 1
                print(f"[NAME-CHECK] Discord 查詢失敗，跳過 {uid_raw}: {exc}", flush=True)
                continue

        expected = str(row.get("game_name") or "").strip()
        actual = str(discord_member.nick or discord_member.display_name or discord_member.name or "").strip()
        if _normalize_member_name(expected) == _normalize_member_name(actual):
            normal += 1
            resolved = await asyncio.to_thread(_resolve_name_compliance_case_sync, uid_raw, actual)
            if resolved:
                print(f"[NAME-CHECK] ✅ 已整改結案: {expected} = {actual}", flush=True)
            continue

        mismatch += 1
        case = await asyncio.to_thread(_load_name_compliance_case_sync, uid_raw)
        if not case or not bool(case.get("active")):
            case = await asyncio.to_thread(_touch_name_compliance_mismatch_sync, row, actual)

        stage = int(case.get("penalty_stage") or 0)
        last_penalty = case.get("last_penalty_at")
        should_penalize = stage == 0
        if not should_penalize and last_penalty:
            if getattr(last_penalty, "tzinfo", None) is None:
                last_penalty = last_penalty.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - last_penalty.astimezone(timezone.utc)).total_seconds()
            should_penalize = elapsed >= NAME_COMPLIANCE_GRACE_SECONDS

        if should_penalize:
            next_stage = stage + 1
            result = await asyncio.to_thread(_register_name_penalty_sync, row, actual, next_stage)
            if not result.get("skipped"):
                penalized += 1
                await send_name_penalty_notifications(result, discord_member)
                print(
                    f"[NAME-CHECK] 🚨 {expected} != {actual} | 階段 {next_stage} | {result.get('punishment')}",
                    flush=True,
                )
        else:
            await asyncio.to_thread(_touch_name_compliance_mismatch_sync, row, actual)
            print(f"[NAME-CHECK] ⏳ 尚在 24 小時整改期: {expected} != {actual} (stage={stage})", flush=True)

        await asyncio.sleep(0.15)

    print(
        f"[NAME-CHECK] 完成：{total} 人 / 正常 {normal} / 名稱不符 {mismatch} / 本輪懲處 {penalized} / Discord不存在或查詢失敗 {missing}",
        flush=True,
    )


async def name_compliance_loop() -> None:
    await client.wait_until_ready()
    try:
        await asyncio.to_thread(ensure_name_compliance_schema_sync)
    except Exception as exc:
        print(f"[NAME-CHECK] Schema init failed: {type(exc).__name__}: {exc}", flush=True)
        return

    # Give Discord member cache a few seconds after login, then perform the first scan.
    await asyncio.sleep(15)
    while not client.is_closed():
        try:
            await run_name_compliance_scan()
        except Exception as exc:
            print(f"[NAME-CHECK] Scan failed: {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(NAME_COMPLIANCE_SCAN_INTERVAL)


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
                  avatar_url TEXT DEFAULT NULL,
                  synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                ALTER TABLE discord_member_role_cache
                  ADD COLUMN IF NOT EXISTS avatar_url TEXT DEFAULT NULL;
                CREATE INDEX IF NOT EXISTS idx_discord_member_role_cache_synced
                  ON discord_member_role_cache(synced_at DESC);
                """
            )
            conn.commit()


def upsert_member_snapshot_sync(
    member_id: int,
    discord_name: str,
    nickname: Optional[str],
    roles: list[str],
    avatar_url: Optional[str] = None,
) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_member_role_cache
                  (discord_user_id, discord_name, nickname, roles, guild_id, avatar_url, synced_at)
                VALUES (%s,%s,%s,%s::jsonb,%s,%s,NOW())
                ON CONFLICT(discord_user_id) DO UPDATE SET
                  discord_name=EXCLUDED.discord_name,
                  nickname=EXCLUDED.nickname,
                  roles=EXCLUDED.roles,
                  guild_id=EXCLUDED.guild_id,
                  avatar_url=EXCLUDED.avatar_url,
                  synced_at=NOW()
                """,
                (
                    str(member_id),
                    discord_name[:100],
                    (nickname or '')[:100] or None,
                    json.dumps(roles),
                    str(GUILD_ID),
                    avatar_url or None,
                ),
            )
            if avatar_url:
                cur.execute(
                    "UPDATE gang_members SET discord_avatar_url=%s WHERE discord_user_id=%s",
                    (avatar_url, str(member_id)),
                )
            conn.commit()


def replace_guild_member_snapshot_sync(
    rows: list[tuple[str, str, Optional[str], str, str, Optional[str]]]
) -> None:
    with _db_connect() as conn:
        with conn.cursor() as cur:
            guild_id = str(GUILD_ID)
            current_ids: list[str] = []
            for user_id, name, nickname, roles_json, row_guild_id, avatar_url in rows:
                current_ids.append(str(user_id))
                cur.execute(
                    """INSERT INTO discord_member_role_cache
                       (discord_user_id,discord_name,nickname,roles,guild_id,avatar_url,synced_at)
                       VALUES(%s,%s,%s,%s::jsonb,%s,%s,NOW())
                       ON CONFLICT(discord_user_id) DO UPDATE SET
                         discord_name=EXCLUDED.discord_name,
                         nickname=EXCLUDED.nickname,
                         roles=EXCLUDED.roles,
                         guild_id=EXCLUDED.guild_id,
                         avatar_url=EXCLUDED.avatar_url,
                         synced_at=NOW()""",
                    (user_id, name, nickname, roles_json, row_guild_id, avatar_url),
                )
                if avatar_url:
                    cur.execute(
                        "UPDATE gang_members SET discord_avatar_url=%s WHERE discord_user_id=%s",
                        (avatar_url, user_id),
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
    rows: list[tuple[str, str, Optional[str], str, str, Optional[str]]] = []
    for member in guild.members:
        if member.bot:
            continue
        roles = [str(role.id) for role in member.roles if role.id != guild.id]
        avatar_url = None
        try:
            avatar_url = str(member.display_avatar.url) if member.display_avatar else None
        except Exception:
            avatar_url = None
        rows.append((str(member.id), str(member.name)[:100], (member.nick or '')[:100] or None, json.dumps(roles), str(GUILD_ID), avatar_url))
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
        await asyncio.to_thread(upsert_member_snapshot_sync, after.id, after.name, after.nick, roles, str(after.display_avatar.url) if after.display_avatar else None)
    except Exception as exc:
        print(f'[BOT] Member snapshot update failed for {after.id}: {type(exc).__name__}: {exc}', flush=True)


@client.event
async def on_member_join(member: discord.Member):
    try:
        roles = [str(role.id) for role in member.roles if role.id != member.guild.id]
        await asyncio.to_thread(upsert_member_snapshot_sync, member.id, member.name, member.nick, roles, str(member.display_avatar.url) if member.display_avatar else None)
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
    "broadcast": "內部廣播",
    "event_reminder": "活動提醒",
    "daily_summary": "每日管理摘要",
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

    if job_type == "maintainer_contact_alert":
        role_id_raw = MAINTAINER_ROLE_ID
        if not role_id_raw.isdigit():
            raise ValueError("DISCORD_MAINTAINER_ROLE_ID missing or invalid")
        guild = client.get_guild(int(GUILD_ID)) if str(GUILD_ID).isdigit() else None
        if guild is None:
            raise RuntimeError("Discord guild not found")
        role = guild.get_role(int(role_id_raw))
        if role is None:
            raise RuntimeError(f"Maintainer role {role_id_raw} not found")
        embed = discord.Embed(title="🛠️ DLP｜系統問題聯絡通知", description="有人透過網站送出系統問題，請主動與對方聯絡。", color=0xDC2626, timestamp=discord.utils.utcnow())
        embed.add_field(name="案件編號", value=str(payload.get("case_no") or "未知"), inline=True)
        embed.add_field(name="緊急程度", value=str(payload.get("urgency") or "一般"), inline=True)
        embed.add_field(name="聯絡人", value=str(payload.get("contact_name") or "未知"), inline=True)
        dc = str(payload.get("discord_name") or "未提供")
        did = str(payload.get("discord_id") or "").strip()
        embed.add_field(name="Discord", value=f"{dc}{f' ({did})' if did else ''}", inline=False)
        embed.add_field(name="主旨", value=str(payload.get("subject") or "未提供")[:1024], inline=False)
        embed.add_field(name="問題內容", value=str(payload.get("details") or "未提供")[:1024], inline=False)
        evidence = str(payload.get("evidence_url") or "").strip()
        if evidence:
            embed.add_field(name="附件／證據", value=evidence[:1024], inline=False)
        embed.set_footer(text="DLP｜大聯社 系統問題聯絡窗口")
        sent = 0
        for target in list(role.members):
            if target.bot:
                continue
            try:
                await target.send(embed=embed)
                sent += 1
            except discord.Forbidden:
                print(f"[BOT] Maintainer contact DM blocked: {target.id}", flush=True)
            except Exception as exc:
                print(f"[BOT] Maintainer contact DM failed {target.id}: {exc}", flush=True)
        print(f"[BOT] maintainer_contact_alert OK: {sent} maintainer DM(s)", flush=True)
        return

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
        if WELCOME_CHANNEL_ID.isdigit():
            try:
                welcome_channel = guild.get_channel(int(WELCOME_CHANNEL_ID))
                if welcome_channel is not None:
                    embed = discord.Embed(title="🏴 歡迎加入 DLP｜大聯社", description=f"歡迎 {member.mention} 正式加入 DLP。", color=0xB91C1C, timestamp=discord.utils.utcnow())
                    embed.add_field(name="成員名稱", value=nickname, inline=True)
                    embed.add_field(name="起始階級", value="門生", inline=True)
                    embed.add_field(name="下一步", value="請登入 DLP 內部系統完成規章確認。", inline=False)
                    embed.set_footer(text="DLP｜大聯社")
                    await welcome_channel.send(embed=embed)
            except Exception as exc:
                print(f"[BOT] welcome channel message failed: {exc}", flush=True)
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


async def field_battle_watch_loop() -> None:
    """Watch website war records and refresh only the battlefield suffix.

    The existing website health presence remains untouched; this reuses the
    last known website health state and changes presence only when the effective
    battlefield state actually changes.
    """
    global _last_effective_field_battle
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            enabled = await is_field_battle_mode()
            if _last_effective_field_battle is None:
                _last_effective_field_battle = enabled
            elif enabled != _last_effective_field_battle:
                _last_effective_field_battle = enabled
                state = dict(_last_presence_state)
                await _set_website_presence(
                    str(state.get("kind") or "online"),
                    online=state.get("online"),
                    offline=state.get("offline"),
                    wait_minutes=state.get("wait_minutes"),
                    detail="website/manual field battle changed",
                    line=str(state.get("line") or "primary"),
                )
                print(
                    f"[FIELD BATTLE] Effective state changed -> {'ON' if enabled else 'OFF'}",
                    flush=True,
                )
        except Exception as exc:
            print(f"[FIELD BATTLE] Watch loop error: {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(10)


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
    if command not in {"!維護", "!maintenance", "!野戰", "!fieldbattle", "!battle", "!namecheck", "!名稱偵測"}:
        return

    if command in {"!namecheck", "!名稱偵測"}:
        if not await _can_use_namecheck(message.author):
            await message.reply("❌ 你沒有權限使用這個指令。", mention_author=False)
            return
        try:
            data = await _manual_namecheck_rows()
            enforcement = await _apply_namecheck_system_penalties(data)
            lines = [
                "🔍 **DLP｜名稱手動偵測＋系統懲處**",
                f"掃描人數：{data['total']}｜正常：{data['normal']}｜名稱不符：{data['mismatch']}｜找不到：{data['missing']}",
                f"🚨 已上系統懲處：{enforcement['penalized']}｜⏳ 24小時內不重複：{enforcement['waiting']}",
            ]
            bad = [r for r in data['results'] if r['status'] != 'normal']
            for r in bad[:20]:
                icon = "⚠️" if r['status'] == 'mismatch' else "❓"
                extra = ""
                if r.get("enforcement_status") == "penalized":
                    pr = r.get("penalty_result") or {}
                    vio = pr.get("violation") or {}
                    extra = f"｜🚨 已上系統：{pr.get('punishment','—')} (ID {vio.get('id','—')})"
                elif r.get("enforcement_status") == "waiting":
                    extra = "｜⏳ 24小時內不重複處罰"
                lines.append(f"{icon} 系統：`{r['expected'] or '—'}`｜Discord：`{r['actual'] or '—'}`｜ID：`{r['discord_user_id'] or '—'}`{extra}")
            if len(bad) > 20:
                lines.append(f"…其餘 {len(bad)-20} 筆未展開")
            await message.reply("\n".join(lines), mention_author=False)
        except Exception as exc:
            print(f"[NAME-CHECK TEXT CMD] {type(exc).__name__}: {exc}", flush=True)
            await message.reply(f"❌ 名稱手動偵測失敗：`{type(exc).__name__}`，請查看 Bot 日誌。", mention_author=False)
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


def _can_use_namecheck_sync(discord_user_id: str) -> bool:
    """Allow DLP 龍頭 / 掌旗 / 後台維護人員 to run the inspection command.

    This deliberately uses the website PostgreSQL identity instead of Discord's
    Manage Server permission, because DLP internal roles do not necessarily have
    Discord guild-management permissions.
    """
    uid = str(discord_user_id or "").strip()
    if not uid:
        return False
    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rank_level, rank_title, status, COALESCE(is_blacklisted,FALSE) AS is_blacklisted
                  FROM gang_members
                 WHERE discord_user_id=%s
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (uid,),
            )
            row = cur.fetchone()
            if not row:
                return False
            data = dict(row)
            if str(data.get("status") or "") != "active" or bool(data.get("is_blacklisted")):
                return False
            title = str(data.get("rank_title") or "").strip()
            level = data.get("rank_level")
            return title in {"龍頭", "掌旗", "後台維護人員"} or level in {1, 2}


async def _can_use_namecheck(user: discord.abc.User) -> bool:
    # Discord administrators/manage-guild users remain accepted as an emergency fallback.
    perms = getattr(user, "guild_permissions", None)
    if perms and (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
        return True
    try:
        return await asyncio.to_thread(_can_use_namecheck_sync, str(user.id))
    except Exception as exc:
        print(f"[NAME-CHECK AUTH] {type(exc).__name__}: {exc}", flush=True)
        return False


async def _manual_namecheck_rows(target_member: Optional[discord.Member] = None) -> Dict[str, Any]:
    """Read DLP website member records and compare them with Discord server display names.

    The comparison itself is read-only. The slash/text command may subsequently pass
    mismatched rows into the same system-penalty routine used by the automatic 30-minute scan.
    """
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        raise RuntimeError("Discord guild not found")

    rows = await asyncio.to_thread(_load_name_compliance_members_sync)
    if target_member is not None:
        target_id = str(target_member.id)
        rows = [r for r in rows if str(r.get("discord_user_id") or "").strip() == target_id]

    results = []
    normal = mismatch = missing = 0
    for row in rows:
        uid_raw = str(row.get("discord_user_id") or "").strip()
        expected = str(row.get("game_name") or "").strip()
        if not uid_raw.isdigit():
            missing += 1
            results.append({"status": "missing", "expected": expected, "actual": "無有效 Discord ID", "discord_user_id": uid_raw})
            continue

        discord_member = guild.get_member(int(uid_raw))
        if discord_member is None:
            try:
                discord_member = await guild.fetch_member(int(uid_raw))
            except discord.NotFound:
                missing += 1
                results.append({"status": "missing", "expected": expected, "actual": "Discord 群組內找不到成員", "discord_user_id": uid_raw})
                continue
            except discord.HTTPException as exc:
                missing += 1
                results.append({"status": "missing", "expected": expected, "actual": f"Discord 查詢失敗：{exc}", "discord_user_id": uid_raw})
                continue

        actual = str(discord_member.nick or discord_member.display_name or discord_member.name or "").strip()
        if _normalize_member_name(expected) == _normalize_member_name(actual):
            normal += 1
            status = "normal"
        else:
            mismatch += 1
            status = "mismatch"

        case = None
        try:
            case = await asyncio.to_thread(_load_name_compliance_case_sync, uid_raw)
        except Exception:
            # A fresh/test database may not have the compliance table yet. The command remains read-only.
            case = None

        results.append({
            "status": status,
            "expected": expected,
            "actual": actual,
            "discord_user_id": uid_raw,
            "member": discord_member,
            "case": case,
            "source_row": row,
        })
        await asyncio.sleep(0.05)

    return {
        "total": len(rows),
        "normal": normal,
        "mismatch": mismatch,
        "missing": missing,
        "results": results,
    }



async def _apply_namecheck_system_penalties(data: Dict[str, Any]) -> Dict[str, int]:
    """Apply system penalties for manual name-check mismatches.

    Rules are intentionally identical to the automatic 30-minute scan:
    - no active case / stage 0 -> stage 1 immediately (oral warning + 50萬)
    - active case still inside 24h -> no duplicate punishment
    - every completed 24h while still mismatched -> next stage (+1 warning, fine +50萬)

    This writes directly to the same PostgreSQL tables used by the website so the
    punishment appears in the internal system immediately.
    """
    await asyncio.to_thread(ensure_name_compliance_schema_sync)
    penalized = 0
    waiting = 0
    skipped = 0

    for item in data.get("results", []):
        if item.get("status") != "mismatch":
            continue

        row = item.get("source_row") or {}
        actual = str(item.get("actual") or "").strip()
        uid_raw = str(item.get("discord_user_id") or "").strip()
        discord_member = item.get("member")
        if not row or not uid_raw or not discord_member:
            skipped += 1
            item["enforcement_status"] = "skipped"
            continue

        case = await asyncio.to_thread(_load_name_compliance_case_sync, uid_raw)
        if not case or not bool(case.get("active")):
            case = await asyncio.to_thread(_touch_name_compliance_mismatch_sync, row, actual)

        stage = int((case or {}).get("penalty_stage") or 0)
        last_penalty = (case or {}).get("last_penalty_at")
        should_penalize = stage == 0

        if not should_penalize and last_penalty:
            if getattr(last_penalty, "tzinfo", None) is None:
                last_penalty = last_penalty.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - last_penalty.astimezone(timezone.utc)).total_seconds()
            should_penalize = elapsed >= NAME_COMPLIANCE_GRACE_SECONDS

        if should_penalize:
            next_stage = stage + 1
            result = await asyncio.to_thread(_register_name_penalty_sync, row, actual, next_stage)
            if result.get("skipped"):
                skipped += 1
                item["enforcement_status"] = "skipped"
                item["penalty_result"] = result
            else:
                penalized += 1
                item["enforcement_status"] = "penalized"
                item["penalty_result"] = result
                await send_name_penalty_notifications(result, discord_member)
                print(
                    f"[NAME-CHECK MANUAL] 🚨 已上系統懲處: {result.get('expected_name')} != {actual} | "
                    f"階段 {next_stage} | {result.get('punishment')} | violation_id={result.get('violation',{}).get('id')}",
                    flush=True,
                )
        else:
            await asyncio.to_thread(_touch_name_compliance_mismatch_sync, row, actual)
            waiting += 1
            item["enforcement_status"] = "waiting"
            item["case"] = await asyncio.to_thread(_load_name_compliance_case_sync, uid_raw)

    return {"penalized": penalized, "waiting": waiting, "skipped": skipped}

def _format_namecheck_case(case: Optional[Dict[str, Any]]) -> str:
    if not case or not bool(case.get("active")):
        return "尚無進行中的名稱異常案件"
    stage = int(case.get("penalty_stage") or 0)
    last_penalty = case.get("last_penalty_at")
    if stage <= 0 or not last_penalty:
        return "異常案件已建立，尚未執行第 1 階段處分"
    try:
        if getattr(last_penalty, "tzinfo", None) is None:
            last_penalty = last_penalty.replace(tzinfo=timezone.utc)
        due = last_penalty.astimezone(timezone.utc) + timedelta(seconds=NAME_COMPLIANCE_GRACE_SECONDS)
        now = datetime.now(timezone.utc)
        if due <= now:
            remain = "下一階段已到期"
        else:
            seconds = int((due - now).total_seconds())
            hours, rem = divmod(seconds, 3600)
            minutes = rem // 60
            remain = f"距下一階段約 {hours} 小時 {minutes} 分"
    except Exception:
        remain = "下一階段時間無法計算"
    return f"目前階段：{stage}｜{remain}"


@tree.command(
    name="namecheck",
    description="比對 DLP 系統名稱與 Discord 名稱，發現不符即依規則上系統懲處",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(member="選填：只檢查指定 Discord 成員；不選則掃描全部現役成員")
async def namecheck_command(
    interaction: discord.Interaction,
    member: Optional[discord.Member] = None,
):
    if not await _can_use_namecheck(interaction.user):
        await interaction.response.send_message(
            "❌ 你沒有權限使用這個指令。僅限龍頭、掌旗、後台維護人員或具 Discord 管理伺服器權限者。",
            ephemeral=True,
        )
        return

    # Deliberately NOT ephemeral: the test result is returned to the same channel where /namecheck is used.
    await interaction.response.defer(thinking=True)
    try:
        data = await _manual_namecheck_rows(member)
        enforcement = await _apply_namecheck_system_penalties(data)
        if member is not None and data["total"] == 0:
            await interaction.followup.send(
                f"❓ 找不到 {member.mention} 對應的 DLP 現役成員資料。\n"
                "此指令是用網站 PostgreSQL 的 `gang_members.discord_user_id` 對照 Discord 成員。",
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return

        embed = discord.Embed(
            title="🔍 DLP｜名稱手動偵測",
            description=(
                "比對來源：網站 PostgreSQL `gang_members.game_name` ↔ Discord 伺服器暱稱。\n"
                "**發現名稱不符會直接依名稱稽核規則寫入網站系統懲處。**"
            ),
            color=0xDC2626 if data["mismatch"] else 0x16A34A,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="掃描人數", value=str(data["total"]), inline=True)
        embed.add_field(name="✅ 正常", value=str(data["normal"]), inline=True)
        embed.add_field(name="⚠️ 名稱不符", value=str(data["mismatch"]), inline=True)
        embed.add_field(name="❓ Discord 找不到 / 查詢失敗", value=str(data["missing"]), inline=True)
        embed.add_field(name="🚨 本次已上系統懲處", value=str(enforcement["penalized"]), inline=True)
        embed.add_field(name="⏳ 24小時內不重複處罰", value=str(enforcement["waiting"]), inline=True)
        if member is not None:
            embed.add_field(name="指定成員", value=f"{member.mention} (`{member.id}`)", inline=False)
        embed.set_footer(text=f"執行者：{interaction.user}｜結果回傳至目前指令頻道")
        await interaction.followup.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

        # Show mismatches/missing first; for a single member also show normal result.
        detail_rows = [r for r in data["results"] if r["status"] != "normal"]
        if member is not None:
            detail_rows = data["results"]

        if not detail_rows:
            await interaction.followup.send("✅ 本次掃描沒有發現名稱不一致。")
            return

        # Discord embeds allow max 25 fields. Split into pages of 20 for comfortable margins.
        for page_start in range(0, len(detail_rows), 20):
            page = detail_rows[page_start:page_start + 20]
            detail_embed = discord.Embed(
                title=("⚠️ 名稱偵測明細" if data["mismatch"] else "名稱偵測明細"),
                color=0xF59E0B if any(r["status"] == "mismatch" for r in page) else 0x6B7280,
            )
            for r in page:
                if r["status"] == "normal":
                    icon = "✅"
                elif r["status"] == "mismatch":
                    icon = "⚠️"
                else:
                    icon = "❓"
                case_text = _format_namecheck_case(r.get("case")) if r["status"] == "mismatch" else ""
                value = (
                    f"系統：`{r['expected'] or '—'}`\n"
                    f"Discord：`{r['actual'] or '—'}`\n"
                    f"Discord ID：`{r['discord_user_id'] or '—'}`"
                )
                if case_text:
                    value += f"\n{case_text}"
                enforcement_status = r.get("enforcement_status")
                result = r.get("penalty_result") or {}
                if enforcement_status == "penalized":
                    vio = result.get("violation") or {}
                    value += (
                        f"\n🚨 **已上系統懲處**：{result.get('punishment','—')}"
                        f"\n系統懲處 ID：`{vio.get('id','—')}`｜目前警告：`{result.get('warning_count',0)}`"
                    )
                elif enforcement_status == "waiting":
                    value += "\n⏳ 已有進行中案件，尚未滿下一個 24 小時，不重複處罰。"
                detail_embed.add_field(
                    name=f"{icon} {r['expected'] or '未命名成員'}",
                    value=value[:1024],
                    inline=False,
                )
            await interaction.followup.send(embed=detail_embed, allowed_mentions=discord.AllowedMentions.none())
    except Exception as exc:
        print(f"[NAME-CHECK CMD] {type(exc).__name__}: {exc}", flush=True)
        await interaction.followup.send(
            f"❌ 名稱手動偵測失敗：`{type(exc).__name__}`。請查看 Bot 日誌。"
        )


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
    global _worker_task, _website_status_task, _member_snapshot_task, _field_battle_watch_task, _name_compliance_task, _commands_synced
    print(f"[BOT] Logged in as {client.user} ({client.user.id if client.user else 'unknown'})", flush=True)
    visible_guilds = ", ".join(f"{g.name}({g.id})" for g in client.guilds) or "(none)"
    print(f"[BOT] Configured guild={GUILD_ID}; bot-visible guilds={visible_guilds}", flush=True)
    if client.get_guild(GUILD_ID) is None:
        print(
            f"[BOT] ERROR: configured DISCORD_GUILD_ID={GUILD_ID} is not accessible to this bot. "
            "Set DISCORD_GUILD_ID to a guild listed above, or invite this bot account into that guild.",
            flush=True,
        )
    try:
        await asyncio.to_thread(ensure_runtime_settings_schema_sync)
    except Exception as exc:
        print(f"[BOT] Runtime settings schema init failed: {type(exc).__name__}: {exc}", flush=True)
    if not _commands_synced:
        if client.get_guild(GUILD_ID) is None:
            print(
                f"[BOT] Slash command sync skipped: bot cannot access configured guild {GUILD_ID}.",
                flush=True,
            )
        else:
            try:
                synced = await tree.sync(guild=discord.Object(id=GUILD_ID))
                _commands_synced = True
                synced_names = ", ".join(f"/{cmd.name}" for cmd in synced) or "(none)"
                print(f"[BOT] Slash commands synced: {synced_names}", flush=True)
            except Exception as exc:
                print(f"[BOT] Slash command sync failed: {type(exc).__name__}: {exc}", flush=True)
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(worker_loop())
    if _website_status_task is None or _website_status_task.done():
        _website_status_task = asyncio.create_task(website_status_loop())
    if _member_snapshot_task is None or _member_snapshot_task.done():
        _member_snapshot_task = asyncio.create_task(member_snapshot_loop())
    if _field_battle_watch_task is None or _field_battle_watch_task.done():
        _field_battle_watch_task = asyncio.create_task(field_battle_watch_loop())
    if _name_compliance_task is None or _name_compliance_task.done():
        _name_compliance_task = asyncio.create_task(name_compliance_loop())


if __name__ == "__main__":
    keep_alive()
    print("[BOT] Keep-alive HTTP server started.", flush=True)
    client.run(TOKEN, log_handler=None)
