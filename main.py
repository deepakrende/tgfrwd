import asyncio
import logging
import os
import signal
from contextlib import suppress
from datetime import datetime, timezone

import asyncpg
from telethon import TelegramClient, errors
from telethon.sessions import StringSession


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("telegram-forwarder")


# ============================================================
# ENVIRONMENT
# ============================================================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

SOURCE_CHANNEL = os.environ["SOURCE_CHANNEL"]
DESTINATION_CHANNEL = os.environ["DESTINATION_CHANNEL"]

DATABASE_URL = os.environ["DATABASE_URL"]

SESSION_STRINGS = [
    os.environ.get("SESSION_1", "").strip(),
    os.environ.get("SESSION_2", "").strip(),
    os.environ.get("SESSION_3", "").strip(),
    os.environ.get("SESSION_4", "").strip(),
    os.environ.get("SESSION_5", "").strip(),
]

WORKERS_PER_ACCOUNT = int(os.getenv("WORKERS_PER_ACCOUNT", "1"))

QUEUE_BATCH_SIZE = int(os.getenv("QUEUE_BATCH_SIZE", "500"))
SCAN_BATCH_SIZE = int(os.getenv("SCAN_BATCH_SIZE", "500"))

PROCESSING_LEASE_SECONDS = int(
    os.getenv("PROCESSING_LEASE_SECONDS", "1800")
)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "10"))

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
FORWARD_DELAY_SECONDS = float(
    os.getenv("FORWARD_DELAY_SECONDS", "0.2")
)

SCAN_LOG_EVERY = int(os.getenv("SCAN_LOG_EVERY", "500"))

WORKER_RESTART_DELAY = int(
    os.getenv("WORKER_RESTART_DELAY", "5")
)

TELEGRAM_RECONNECT_DELAY = int(
    os.getenv("TELEGRAM_RECONNECT_DELAY", "5")
)


# ============================================================
# GLOBALS
# ============================================================

pool = None
clients = {}
source_entities = {}
destination_entities = {}

shutdown_event = asyncio.Event()


# ============================================================
# CONFIG VALIDATION
# ============================================================

def validate_config():
    if not SOURCE_CHANNEL:
        raise RuntimeError("SOURCE_CHANNEL is empty")

    if not DESTINATION_CHANNEL:
        raise RuntimeError("DESTINATION_CHANNEL is empty")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty")

    if len(SESSION_STRINGS) != 5:
        raise RuntimeError("Exactly 5 sessions are required")

    for i, session in enumerate(SESSION_STRINGS, start=1):
        if not session:
            raise RuntimeError(f"SESSION_{i} is empty")


# ============================================================
# ENTITY RESOLUTION
# ============================================================

async def resolve_entity(client, value, label):
    """
    Resolve channel username/link/numeric ID into a Telethon entity.
    """

    value = str(value).strip()

    # Remove common Telegram URL prefixes
    normalized = value

    for prefix in (
        "https://t.me/",
        "http://t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
        "t.me/",
        "telegram.me/",
    ):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
            break

    normalized = normalized.rstrip("/")

    # Try direct resolution first
    try:
        entity = await client.get_entity(normalized)

        log.info(
            "%s resolved for account %s: %s",
            label,
            getattr(client, "_forwarder_account_no", "?"),
            getattr(entity, "title", getattr(entity, "username", entity)),
        )

        return entity

    except Exception:
        pass

    # Numeric Telegram ID
    try:
        target_id = int(normalized)

        async for dialog in client.iter_dialogs():
            entity = dialog.entity

            if getattr(entity, "id", None) == abs(target_id):
                log.info(
                    "%s resolved from dialogs for account %s",
                    label,
                    getattr(client, "_forwarder_account_no", "?"),
                )
                return entity

    except (ValueError, TypeError):
        pass

    # Final attempt
    try:
        target_id = int(normalized)
        entity = await client.get_entity(target_id)

        log.info(
            "%s resolved by numeric ID for account %s",
            label,
            getattr(client, "_forwarder_account_no", "?"),
        )

        return entity

    except Exception as e:
        raise RuntimeError(
            f"Account {getattr(client, '_forwarder_account_no', '?')} "
            f"cannot resolve {label}: {value!r}. "
            f"Make sure this account has access to the channel. "
            f"Error: {e}"
        )


# ============================================================
# DATABASE
# ============================================================

async def init_db():
    global pool

    async with pool.acquire() as conn:

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id BIGSERIAL PRIMARY KEY,
                source_message_id BIGINT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                processing_by TEXT,
                processing_started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_status
            ON messages(status)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_source_id
            ON messages(source_message_id)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_processing_started
            ON messages(processing_started_at)
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_state (
                account_no INTEGER PRIMARY KEY,
                cooldown_until TIMESTAMPTZ,
                last_error TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        for account_no in range(1, 6):
            await conn.execute(
                """
                INSERT INTO account_state(account_no)
                VALUES($1)
                ON CONFLICT(account_no) DO NOTHING
                """,
                account_no,
            )

    log.info("Database initialized")


# ============================================================
# DATABASE CONNECTION HEALTH
# ============================================================

async def db_health_check():
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")


# ============================================================
# STALE JOB RECOVERY
# ============================================================

async def reset_stale_jobs():
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE messages
            SET
                status = 'pending',
                processing_by = NULL,
                processing_started_at = NULL,
                updated_at = NOW()
            WHERE status = 'processing'
              AND processing_started_at <
                  NOW() - ($1::INTEGER * INTERVAL '1 second')
            """,
            PROCESSING_LEASE_SECONDS,
        )

    log.info("Stale job recovery: %s", result)


async def stale_job_recovery_loop():
    while not shutdown_event.is_set():
        try:
            await reset_stale_jobs()

        except Exception as e:
            log.error(
                "STALE RECOVERY error: %s",
                e,
            )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=60,
            )
        except asyncio.TimeoutError:
            pass


# ============================================================
# QUEUE
# ============================================================

async def insert_message_batch(message_ids):
    if not message_ids:
        return 0

    async with pool.acquire() as conn:
        result = await conn.fetchval(
            """
            INSERT INTO messages(source_message_id)
            SELECT UNNEST($1::BIGINT[])
            ON CONFLICT(source_message_id) DO NOTHING
            RETURNING 1
            """,
            message_ids,
        )

        # The above RETURNING only gives one row through fetchval.
        # Use command below for accurate count.
        await conn.execute(
            """
            INSERT INTO messages(source_message_id)
            SELECT UNNEST($1::BIGINT[])
            ON CONFLICT(source_message_id) DO NOTHING
            """,
            [],
        )

    return 0


async def insert_message_ids(message_ids):
    """
    Correct bulk insertion with accurate inserted count.
    """

    if not message_ids:
        return 0

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            INSERT INTO messages(source_message_id)
            SELECT UNNEST($1::BIGINT[])
            ON CONFLICT(source_message_id) DO NOTHING
            RETURNING source_message_id
            """,
            message_ids,
        )

    return len(rows)


async def get_latest_stored_message_id():
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT COALESCE(MAX(source_message_id), 0)
            FROM messages
            """
        )

    return int(value or 0)


async def get_scan_start_id():
    return await get_latest_stored_message_id()


# ============================================================
# HISTORY SCANNER
# ============================================================

async def enqueue_history(account_no, client, source_entity):
    log.info(
        "A%d history scanner started",
        account_no,
    )

    seen = 0
    inserted = 0

    try:
        async for message in client.iter_messages(
            source_entity,
            reverse=True,
        ):
            if shutdown_event.is_set():
                break

            seen += 1

            try:
                added = await insert_message_ids(
                    [message.id]
                )
                inserted += added

            except Exception as e:
                log.error(
                    "History DB insert error at %s: %s",
                    message.id,
                    e,
                )
                await asyncio.sleep(2)

            if seen % SCAN_LOG_EVERY == 0:
                log.info(
                    "HISTORY A%d | seen=%s inserted=%s",
                    account_no,
                    seen,
                    inserted,
                )

    except errors.FloodWaitError as e:
        log.warning(
            "HISTORY A%d FloodWait: %s seconds",
            account_no,
            e.seconds,
        )

        await asyncio.sleep(e.seconds)

    except Exception as e:
        log.error(
            "HISTORY A%d error: %s",
            account_no,
            e,
        )

    log.info(
        "HISTORY A%d finished | seen=%s inserted=%s",
        account_no,
        seen,
        inserted,
    )


# ============================================================
# CLAIM JOB
# ============================================================

async def claim_job(worker_name):
    async with pool.acquire() as conn:

        async with conn.transaction():

            row = await conn.fetchrow(
                """
                SELECT
                    id,
                    source_message_id,
                    attempts
                FROM messages
                WHERE status = 'pending'
                  AND attempts < $1
                ORDER BY source_message_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                MAX_RETRIES,
            )

            if not row:
                return None

            await conn.execute(
                """
                UPDATE messages
                SET
                    status = 'processing',
                    processing_by = $1,
                    processing_started_at = NOW(),
                    attempts = attempts + 1,
                    updated_at = NOW()
                WHERE id = $2
                """,
                worker_name,
                row["id"],
            )

            return {
                "db_id": row["id"],
                "source_message_id": row["source_message_id"],
                "attempts": row["attempts"] + 1,
            }


# ============================================================
# JOB STATUS
# ============================================================

async def mark_completed(db_id):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE messages
            SET
                status = 'completed',
                processing_by = NULL,
                processing_started_at = NULL,
                completed_at = NOW(),
                updated_at = NOW(),
                last_error = NULL
            WHERE id = $1
            """,
            db_id,
        )


async def mark_retry(db_id, error_text, attempts):
    new_status = (
        "failed"
        if attempts >= MAX_RETRIES
        else "pending"
    )

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE messages
            SET
                status = $2,
                processing_by = NULL,
                processing_started_at = NULL,
                last_error = $3,
                updated_at = NOW()
            WHERE id = $1
            """,
            db_id,
            new_status,
            error_text[:4000],
        )


async def mark_floodwait_pending(
    db_id,
    error_text,
):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE messages
            SET
                status = 'pending',
                attempts = GREATEST(attempts - 1, 0),
                processing_by = NULL,
                processing_started_at = NULL,
                last_error = $2,
                updated_at = NOW()
            WHERE id = $1
            """,
            db_id,
            error_text[:4000],
        )


# ============================================================
# ACCOUNT COOLDOWN
# ============================================================

async def set_cooldown(account_no, seconds, error_text):
    cooldown_until = datetime.now(timezone.utc)

    from datetime import timedelta

    cooldown_until += timedelta(seconds=seconds)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE account_state
            SET
                cooldown_until = $2,
                last_error = $3,
                updated_at = NOW()
            WHERE account_no = $1
            """,
            account_no,
            cooldown_until,
            error_text[:4000],
        )

    log.warning(
        "A%d cooldown set for %s seconds",
        account_no,
        seconds,
    )


async def clear_cooldown(account_no):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE account_state
            SET
                cooldown_until = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE account_no = $1
            """,
            account_no,
        )


async def get_cooldown(account_no):
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT cooldown_until
            FROM account_state
            WHERE account_no = $1
            """,
            account_no,
        )

    return value


async def wait_for_account_cooldown(account_no):
    while not shutdown_event.is_set():

        cooldown = await get_cooldown(account_no)

        if cooldown is None:
            return

        now = datetime.now(timezone.utc)

        remaining = (
            cooldown - now
        ).total_seconds()

        if remaining <= 0:
            await clear_cooldown(account_no)
            return

        log.info(
            "A%d waiting for FloodWait cooldown: %.0f seconds",
            account_no,
            remaining,
        )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=min(remaining, 30),
            )
        except asyncio.TimeoutError:
            pass


# ============================================================
# FORWARD
# ============================================================

async def forward_message(
    client,
    source_entity,
    destination_entity,
    message_id,
):
    return await client.forward_messages(
        entity=destination_entity,
        messages=message_id,
        from_peer=source_entity,
    )


# ============================================================
# TELEGRAM CONNECTION
# ============================================================

async def ensure_client_connected(account_no, client):
    """
    Make absolutely sure the Telegram client is connected.
    """

    if client.is_connected():
        return True

    log.warning(
        "A%d Telegram client is disconnected. Reconnecting...",
        account_no,
    )

    try:
        await client.connect()

        if not await client.is_user_authorized():
            raise RuntimeError(
                f"A{account_no} session is no longer authorized"
            )

        log.info(
            "A%d Telegram reconnect successful",
            account_no,
        )

        return True

    except Exception as e:
        log.error(
            "A%d Telegram reconnect failed: %s",
            account_no,
            e,
        )

        return False


async def reconnect_client(account_no, client):
    """
    Force a clean reconnect.
    """

    try:
        await client.disconnect()
    except Exception:
        pass

    await asyncio.sleep(TELEGRAM_RECONNECT_DELAY)

    try:
        await client.connect()

        if not await client.is_user_authorized():
            raise RuntimeError(
                f"A{account_no} session is unauthorized"
            )

        log.info(
            "A%d reconnected successfully",
            account_no,
        )

        # Re-resolve entities after reconnect.
        source_entities[account_no] = await resolve_entity(
            client,
            SOURCE_CHANNEL,
            "SOURCE_CHANNEL",
        )

        destination_entities[account_no] = await resolve_entity(
            client,
            DESTINATION_CHANNEL,
            "DESTINATION_CHANNEL",
        )

        return True

    except Exception as e:
        log.error(
            "A%d reconnect failed: %s",
            account_no,
            e,
        )
        return False


# ============================================================
# WORKER
# ============================================================

async def worker_once(
    account_no,
    worker_index,
):
    worker_name = f"worker-{account_no}-{worker_index}"

    client = clients[account_no]

    successful = 0
    errors_count = 0

    while not shutdown_event.is_set():

        # ----------------------------------------------------
        # Ensure Telegram connection
        # ----------------------------------------------------

        if not await ensure_client_connected(
            account_no,
            client,
        ):
            await asyncio.sleep(
                TELEGRAM_RECONNECT_DELAY
            )
            continue

        # ----------------------------------------------------
        # Respect only this account's cooldown
        # ----------------------------------------------------

        await wait_for_account_cooldown(account_no)

        if shutdown_event.is_set():
            break

        # ----------------------------------------------------
        # Claim job
        # ----------------------------------------------------

        try:
            job = await claim_job(worker_name)

        except (
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            ConnectionError,
        ) as e:

            log.error(
                "A%d %s PostgreSQL connection problem: %s",
                account_no,
                worker_name,
                e,
            )

            await asyncio.sleep(3)
            continue

        except Exception as e:

            log.error(
                "A%d %s claim error: %s",
                account_no,
                worker_name,
                e,
            )

            await asyncio.sleep(2)
            continue

        if not job:
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=POLL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

            continue

        message_id = job["source_message_id"]

        log.info(
            "A%d-W%d forwarding source message %s",
            account_no,
            worker_index,
            message_id,
        )

        # ----------------------------------------------------
        # Forward
        # ----------------------------------------------------

        try:

            source_entity = source_entities[account_no]
            destination_entity = destination_entities[
                account_no
            ]

            await forward_message(
                client,
                source_entity,
                destination_entity,
                message_id,
            )

            await mark_completed(job["db_id"])

            successful += 1

            log.info(
                "A%d-W%d completed source message %s | "
                "success=%s errors=%s",
                account_no,
                worker_index,
                message_id,
                successful,
                errors_count,
            )

            await clear_cooldown(account_no)

            if FORWARD_DELAY_SECONDS > 0:
                await asyncio.sleep(
                    FORWARD_DELAY_SECONDS
                )

        # ----------------------------------------------------
        # FloodWait
        # ----------------------------------------------------

        except errors.FloodWaitError as e:

            errors_count += 1

            log.warning(
                "A%d-W%d FloodWait %s seconds on message %s",
                account_no,
                worker_index,
                e.seconds,
                message_id,
            )

            # FloodWait MUST NOT consume a retry.
            try:
                await mark_floodwait_pending(
                    job["db_id"],
                    f"FloodWait: {e.seconds} seconds",
                )
            except Exception as db_error:
                log.error(
                    "A%d failed to return FloodWait job "
                    "to queue: %s",
                    account_no,
                    db_error,
                )

            await set_cooldown(
                account_no,
                e.seconds,
                f"FloodWait: {e.seconds}s",
            )

            continue

        # ----------------------------------------------------
        # Connection closed
        # ----------------------------------------------------

        except (
            ConnectionError,
            asyncio.TimeoutError,
            OSError,
        ) as e:

            errors_count += 1

            error_text = str(e)

            log.error(
                "A%d-W%d connection error on message %s: %s",
                account_no,
                worker_index,
                message_id,
                error_text,
            )

            # Return job to pending so another worker
            # can process it if necessary.
            try:
                await mark_retry(
                    job["db_id"],
                    error_text,
                    job["attempts"],
                )
            except Exception as db_error:
                log.error(
                    "A%d job recovery DB error: %s",
                    account_no,
                    db_error,
                )

            # Reconnect this account only.
            await reconnect_client(
                account_no,
                client,
            )

            await asyncio.sleep(2)

        # ----------------------------------------------------
        # Telegram RPC errors
        # ----------------------------------------------------

        except errors.RPCError as e:

            errors_count += 1

            error_text = (
                f"{type(e).__name__}: {e}"
            )

            log.error(
                "A%d-W%d Telegram RPC error on %s: %s",
                account_no,
                worker_index,
                message_id,
                error_text,
            )

            try:
                await mark_retry(
                    job["db_id"],
                    error_text,
                    job["attempts"],
                )
            except Exception as db_error:
                log.error(
                    "A%d retry DB error: %s",
                    account_no,
                    db_error,
                )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

        # ----------------------------------------------------
        # Any other exception
        # ----------------------------------------------------

        except Exception as e:

            errors_count += 1

            error_text = (
                f"{type(e).__name__}: {e}"
            )

            log.exception(
                "A%d-W%d unexpected error on %s",
                account_no,
                worker_index,
                message_id,
            )

            try:
                await mark_retry(
                    job["db_id"],
                    error_text,
                    job["attempts"],
                )
            except Exception as db_error:
                log.error(
                    "A%d retry DB error: %s",
                    account_no,
                    db_error,
                )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )


# ============================================================
# PERMANENT WORKER SUPERVISOR
# ============================================================

async def worker_supervisor(
    account_no,
    worker_index,
):
    """
    VERY IMPORTANT:

    This supervisor never permanently dies.

    If worker_once() crashes because of a connection
    problem or an unexpected exception, it starts it again.
    """

    worker_name = (
        f"worker-{account_no}-{worker_index}"
    )

    restart_count = 0

    while not shutdown_event.is_set():

        try:

            log.info(
                "STARTING %s",
                worker_name,
            )

            await worker_once(
                account_no,
                worker_index,
            )

            if shutdown_event.is_set():
                break

            restart_count += 1

            log.warning(
                "WORKER %s exited. "
                "Automatic restart #%s in %s seconds",
                worker_name,
                restart_count,
                WORKER_RESTART_DELAY,
            )

        except asyncio.CancelledError:
            raise

        except Exception as e:

            restart_count += 1

            log.exception(
                "WORKER %s crashed: %s",
                worker_name,
                e,
            )

            log.warning(
                "WORKER %s automatic restart #%s",
                worker_name,
                restart_count,
            )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=WORKER_RESTART_DELAY,
            )
        except asyncio.TimeoutError:
            pass

    log.info(
        "WORKER SUPERVISOR %s stopped",
        worker_name,
    )


# ============================================================
# STATUS
# ============================================================

async def status_loop():
    while not shutdown_event.is_set():

        try:
            async with pool.acquire() as conn:

                counts = await conn.fetch(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM messages
                    GROUP BY status
                    """
                )

                total = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM messages
                    """
                )

                oldest_pending = await conn.fetchval(
                    """
                    SELECT MIN(source_message_id)
                    FROM messages
                    WHERE status = 'pending'
                    """
                )

                newest = await conn.fetchval(
                    """
                    SELECT MAX(source_message_id)
                    FROM messages
                    """
                )

            count_map = {
                row["status"]: row["count"]
                for row in counts
            }

            completed = count_map.get(
                "completed",
                0,
            )

            failed = count_map.get(
                "failed",
                0,
            )

            pending = count_map.get(
                "pending",
                0,
            )

            processing = count_map.get(
                "processing",
                0,
            )

            cooldowns = []

            for account_no in range(1, 6):

                cooldown = await get_cooldown(
                    account_no
                )

                if cooldown:
                    remaining = (
                        cooldown
                        - datetime.now(timezone.utc)
                    ).total_seconds()

                    if remaining > 0:
                        cooldowns.append(
                            f"A{account_no}={remaining:.0f}s"
                        )
                    else:
                        cooldowns.append(
                            f"A{account_no}=ready"
                        )
                else:
                    cooldowns.append(
                        f"A{account_no}=ready"
                    )

            log.info(
                "STATUS total=%s | completed=%s "
                "failed=%s pending=%s processing=%s | "
                "oldest_pending=%s newest=%s | %s",
                total,
                completed,
                failed,
                pending,
                processing,
                oldest_pending,
                newest,
                " ".join(cooldowns),
            )

        except Exception as e:
            log.error(
                "STATUS error: %s",
                e,
            )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=30,
            )
        except asyncio.TimeoutError:
            pass


# ============================================================
# NEW MESSAGE MONITOR
# ============================================================

async def monitor_new_messages(
    account_no,
):
    client = clients[account_no]

    log.info(
        "A%d new-message monitor started",
        account_no,
    )

    while not shutdown_event.is_set():

        try:

            if not await ensure_client_connected(
                account_no,
                client,
            ):
                await asyncio.sleep(5)
                continue

            source_entity = source_entities[
                account_no
            ]

            latest_id = (
                await get_latest_stored_message_id()
            )

            messages = await client.get_messages(
                source_entity,
                limit=100,
                min_id=latest_id,
            )

            if messages:

                ids = [
                    message.id
                    for message in messages
                    if message.id > latest_id
                ]

                if ids:
                    added = await insert_message_ids(
                        ids
                    )

                    log.info(
                        "A%d NEW messages detected=%s "
                        "inserted=%s",
                        account_no,
                        len(ids),
                        added,
                    )

            await asyncio.sleep(10)

        except errors.FloodWaitError as e:

            log.warning(
                "A%d NEW monitor FloodWait=%s seconds",
                account_no,
                e.seconds,
            )

            await asyncio.sleep(e.seconds)

        except (
            ConnectionError,
            asyncio.TimeoutError,
            OSError,
        ) as e:

            log.error(
                "A%d NEW monitor connection error: %s",
                account_no,
                e,
            )

            await reconnect_client(
                account_no,
                client,
            )

            await asyncio.sleep(5)

        except Exception as e:

            log.error(
                "A%d NEW monitor error: %s",
                account_no,
                e,
            )

            await asyncio.sleep(10)


# ============================================================
# CONNECT ACCOUNTS
# ============================================================

async def connect_accounts():

    for account_no, session_string in enumerate(
        SESSION_STRINGS,
        start=1,
    ):

        client = TelegramClient(
            StringSession(session_string),
            API_ID,
            API_HASH,
            connection_retries=20,
            retry_delay=5,
            auto_reconnect=True,
            request_retries=5,
        )

        client._forwarder_account_no = account_no

        clients[account_no] = client

    # Connect sequentially so all sessions don't
    # hammer Telegram at exactly the same moment.

    for account_no in range(1, 6):

        client = clients[account_no]

        log.info(
            "Connecting Telegram account %d...",
            account_no,
        )

        await client.connect()

        if not await client.is_user_authorized():
            raise RuntimeError(
                f"SESSION_{account_no} is not authorized"
            )

        me = await client.get_me()

        log.info(
            "A%d connected | Telegram ID=%s | username=%s",
            account_no,
            getattr(me, "id", "?"),
            getattr(me, "username", None),
        )

        await asyncio.sleep(1)


# ============================================================
# RESOLVE ALL ENTITIES
# ============================================================

async def resolve_all_entities():

    for account_no in range(1, 6):

        client = clients[account_no]

        source_entities[account_no] = (
            await resolve_entity(
                client,
                SOURCE_CHANNEL,
                "SOURCE_CHANNEL",
            )
        )

        destination_entities[account_no] = (
            await resolve_entity(
                client,
                DESTINATION_CHANNEL,
                "DESTINATION_CHANNEL",
            )
        )

        log.info(
            "A%d source/destination ready",
            account_no,
        )


# ============================================================
# ACCOUNT HEALTH MONITOR
# ============================================================

async def account_health_loop():

    while not shutdown_event.is_set():

        for account_no in range(1, 6):

            if shutdown_event.is_set():
                break

            client = clients.get(account_no)

            if not client:
                continue

            try:

                if not client.is_connected():

                    log.warning(
                        "HEALTH A%d is disconnected. "
                        "Reconnecting...",
                        account_no,
                    )

                    await reconnect_client(
                        account_no,
                        client,
                    )

            except Exception as e:

                log.error(
                    "HEALTH A%d error: %s",
                    account_no,
                    e,
                )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=20,
            )
        except asyncio.TimeoutError:
            pass


# ============================================================
# MAIN
# ============================================================

async def main():

    global pool

    validate_config()

    log.info(
        "Starting 5-account Telegram forwarder"
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=20,
        command_timeout=60,
        max_inactive_connection_lifetime=60,
    )

    await init_db()

    await reset_stale_jobs()

    # --------------------------------------------------------
    # Telegram
    # --------------------------------------------------------

    await connect_accounts()

    await resolve_all_entities()

    # --------------------------------------------------------
    # Background tasks
    # --------------------------------------------------------

    tasks = []

    # Five forwarding supervisors
    for account_no in range(1, 6):

        for worker_index in range(
            1,
            WORKERS_PER_ACCOUNT + 1,
        ):

            tasks.append(
                asyncio.create_task(
                    worker_supervisor(
                        account_no,
                        worker_index,
                    ),
                    name=(
                        f"worker-supervisor-"
                        f"{account_no}-{worker_index}"
                    ),
                )
            )

    # Status
    tasks.append(
        asyncio.create_task(
            status_loop(),
            name="status",
        )
    )

    # Stale job recovery
    tasks.append(
        asyncio.create_task(
            stale_job_recovery_loop(),
            name="stale-recovery",
        )
    )

    # Account health
    tasks.append(
        asyncio.create_task(
            account_health_loop(),
            name="account-health",
        )
    )

    # New message monitor
    # Only one monitor is necessary.
    tasks.append(
        asyncio.create_task(
            monitor_new_messages(1),
            name="new-message-monitor",
        )
    )

    # --------------------------------------------------------
    # History scan
    # --------------------------------------------------------

    # Use A1 for the historical scan.
    # The other four accounts remain available for forwarding.

    history_task = asyncio.create_task(
        enqueue_history(
            1,
            clients[1],
            source_entities[1],
        ),
        name="history-scanner",
    )

    tasks.append(history_task)

    log.info(
        "ALL SERVICES STARTED"
    )

    log.info(
        "Forwarding workers: %d",
        5 * WORKERS_PER_ACCOUNT,
    )

    # --------------------------------------------------------
    # Wait forever until shutdown
    # --------------------------------------------------------

    try:

        await shutdown_event.wait()

    finally:

        log.info(
            "Shutdown requested"
        )

        for task in tasks:

            if not task.done():
                task.cancel()

        with suppress(Exception):

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        # Disconnect Telegram
        for account_no, client in clients.items():

            try:
                await client.disconnect()

                log.info(
                    "A%d disconnected",
                    account_no,
                )

            except Exception:
                pass

        # Close DB
        if pool:

            await pool.close()

        log.info(
            "Shutdown complete"
        )


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def request_shutdown():

    if not shutdown_event.is_set():

        log.info(
            "Shutdown signal received"
        )

        shutdown_event.set()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        with suppress(NotImplementedError):
            loop.add_signal_handler(
                sig,
                request_shutdown,
            )

    try:

        loop.run_until_complete(
            main()
        )

    except KeyboardInterrupt:

        request_shutdown()

    finally:

        with suppress(Exception):
            loop.run_until_complete(
                asyncio.sleep(0)
            )

        loop.close()
