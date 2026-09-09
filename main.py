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
# CONFIG
# ============================================================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

SOURCE = os.environ["SOURCE_CHANNEL"].strip()
DESTINATION = os.environ["DESTINATION_CHANNEL"].strip()

DATABASE_URL = os.environ["DATABASE_URL"]

# ============================================================
# EXACTLY 5 TELEGRAM ACCOUNTS
# ============================================================

SESSION_STRINGS = [
    os.getenv("SESSION_1", "").strip(),
    os.getenv("SESSION_2", "").strip(),
    os.getenv("SESSION_3", "").strip(),
    os.getenv("SESSION_4", "").strip(),
    os.getenv("SESSION_5", "").strip(),
]


# ============================================================
# WORKER SETTINGS
# ============================================================

# 1 worker per account = 5 total workers
WORKERS_PER_ACCOUNT = int(
    os.getenv("WORKERS_PER_ACCOUNT", "1")
)

QUEUE_BATCH_SIZE = int(
    os.getenv("QUEUE_BATCH_SIZE", "500")
)

SCAN_BATCH_SIZE = int(
    os.getenv("SCAN_BATCH_SIZE", "500")
)

# Processing lease.
# If Render crashes while a message is processing,
# another worker can recover it after this period.
PROCESSING_LEASE_SECONDS = int(
    os.getenv("PROCESSING_LEASE_SECONDS", "1800")
)

# Real forwarding/API failures before marking failed.
# FloodWait does NOT consume retries.
MAX_RETRIES = int(
    os.getenv("MAX_RETRIES", "5")
)

RETRY_DELAY_SECONDS = int(
    os.getenv("RETRY_DELAY_SECONDS", "10")
)

POLL_SECONDS = int(
    os.getenv("POLL_SECONDS", "15")
)

# Delay between successful forwards by one worker.
FORWARD_DELAY_SECONDS = float(
    os.getenv("FORWARD_DELAY_SECONDS", "0.2")
)

SCAN_LOG_EVERY = int(
    os.getenv("SCAN_LOG_EVERY", "500")
)

WORKER_LOG_EVERY = int(
    os.getenv("WORKER_LOG_EVERY", "25")
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("telegram-forwarder")


# ============================================================
# SHUTDOWN
# ============================================================

shutdown_event = asyncio.Event()


def request_shutdown():
    if not shutdown_event.is_set():
        logger.info("Shutdown requested...")
        shutdown_event.set()


# ============================================================
# HELPERS
# ============================================================

def utcnow():
    return datetime.now(timezone.utc)


def validate_config():

    if not SOURCE:
        raise RuntimeError(
            "SOURCE_CHANNEL is empty."
        )

    if not DESTINATION:
        raise RuntimeError(
            "DESTINATION_CHANNEL is empty."
        )

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is empty."
        )

    if len(SESSION_STRINGS) != 5:
        raise RuntimeError(
            "Exactly 5 sessions are required."
        )

    for account_no, session in enumerate(
        SESSION_STRINGS,
        start=1,
    ):

        if not session:
            raise RuntimeError(
                f"SESSION_{account_no} is empty."
            )

        try:
            StringSession(session)

        except Exception as exc:

            raise RuntimeError(
                f"SESSION_{account_no} is NOT "
                f"a valid Telethon StringSession."
            ) from exc


# ============================================================
# TELEGRAM ENTITY RESOLUTION
# ============================================================

async def resolve_entity(
    client,
    value,
    label,
):
    """
    Resolve Telegram entity.

    Supports:

        @username
        username
        https://t.me/username
        numeric Telegram channel ID

    For numeric IDs, dialogs are searched as a fallback.
    """

    value = str(value).strip()

    normalized = value

    prefixes = [
        "https://t.me/",
        "http://t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
    ]

    for prefix in prefixes:

        if normalized.startswith(prefix):

            normalized = normalized[
                len(prefix):
            ]

            break

    normalized = normalized.rstrip("/")

    # --------------------------------------------------------
    # Direct resolution
    # --------------------------------------------------------

    try:

        entity = await client.get_entity(
            normalized
        )

        logger.info(
            "%s resolved directly: %s",
            label,
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or getattr(entity, "id", None),
        )

        return entity

    except Exception as exc:

        logger.warning(
            "%s direct resolution failed: %s",
            label,
            exc,
        )

    # --------------------------------------------------------
    # Numeric Telegram ID
    # --------------------------------------------------------

    try:

        target_id = int(normalized)

    except (ValueError, TypeError):

        raise RuntimeError(
            f"Could not resolve {label} '{value}'. "
            f"Check the username/link and make sure "
            f"this account has access."
        )

    positive_target_id = abs(target_id)

    logger.info(
        "%s searching account dialogs for ID %s...",
        label,
        target_id,
    )

    try:

        async for dialog in client.iter_dialogs():

            entity = dialog.entity

            entity_id = getattr(
                entity,
                "id",
                None,
            )

            if entity_id is None:
                continue

            if (
                entity_id == target_id
                or entity_id == positive_target_id
            ):

                logger.info(
                    "%s resolved from dialogs: %s",
                    label,
                    getattr(entity, "title", None)
                    or getattr(entity, "username", None)
                    or entity_id,
                )

                return entity

    except Exception:

        logger.exception(
            "Error searching dialogs for %s",
            label,
        )

    # --------------------------------------------------------
    # Final resolution attempt
    # --------------------------------------------------------

    try:

        entity = await client.get_entity(
            target_id
        )

        logger.info(
            "%s resolved after dialog search.",
            label,
        )

        return entity

    except Exception:
        pass

    raise RuntimeError(
        f"Could not resolve {label} '{value}'. "
        f"This Telegram account must have access to "
        f"the channel/group."
    )


# ============================================================
# DATABASE
# ============================================================

async def init_db(pool):

    schema = """
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
    );

    CREATE INDEX IF NOT EXISTS idx_messages_status
        ON messages(status);

    CREATE INDEX IF NOT EXISTS idx_messages_source_message_id
        ON messages(source_message_id);

    CREATE INDEX IF NOT EXISTS idx_messages_processing_started
        ON messages(processing_started_at);


    CREATE TABLE IF NOT EXISTS account_state (
        account_no INTEGER PRIMARY KEY,

        cooldown_until TIMESTAMPTZ,

        last_error TEXT,

        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    async with pool.acquire() as conn:

        await conn.execute(schema)

        # ====================================================
        # FIVE ACCOUNTS
        # ====================================================

        for account_no in range(1, 6):

            await conn.execute(
                """
                INSERT INTO account_state (
                    account_no,
                    cooldown_until,
                    last_error
                )
                VALUES (
                    $1,
                    NULL,
                    NULL
                )
                ON CONFLICT (account_no)
                DO NOTHING
                """,
                account_no,
            )

    logger.info(
        "Database initialized."
    )


# ============================================================
# STALE JOB RECOVERY
# ============================================================

async def reset_stale_jobs(pool):

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
                  NOW() - (
                      $1 * INTERVAL '1 second'
                  )
            """,
            PROCESSING_LEASE_SECONDS,
        )

    logger.info(
        "Startup stale-job recovery: %s",
        result,
    )


async def stale_job_recovery_loop(pool):

    logger.info(
        "Stale-job recovery loop started."
    )

    while not shutdown_event.is_set():

        try:

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
                          NOW() - (
                              $1 * INTERVAL '1 second'
                          )
                    """,
                    PROCESSING_LEASE_SECONDS,
                )

            if result != "UPDATE 0":

                logger.warning(
                    "Runtime stale-job recovery: %s",
                    result,
                )

        except Exception:

            logger.exception(
                "Stale-job recovery error."
            )

        try:

            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=60,
            )

        except asyncio.TimeoutError:
            pass

    logger.info(
        "Stale-job recovery loop stopped."
    )


# ============================================================
# DATABASE POSITION
# ============================================================

async def get_scan_start_id(pool):

    async with pool.acquire() as conn:

        row = await conn.fetchrow(
            """
            SELECT COALESCE(
                MAX(source_message_id),
                0
            ) AS max_id

            FROM messages
            """
        )

    return int(
        row["max_id"]
    )


# ============================================================
# INSERT MESSAGE BATCH
# ============================================================

async def insert_message_batch(
    pool,
    message_ids,
):

    if not message_ids:
        return 0

    clean_ids = list(
        dict.fromkeys(
            int(x)
            for x in message_ids
        )
    )

    async with pool.acquire() as conn:

        result = await conn.execute(
            """
            INSERT INTO messages (
                source_message_id,
                status,
                attempts,
                created_at,
                updated_at
            )
            SELECT
                x,
                'pending',
                0,
                NOW(),
                NOW()

            FROM UNNEST(
                $1::BIGINT[]
            ) AS x

            ON CONFLICT (
                source_message_id
            )
            DO NOTHING
            """,
            clean_ids,
        )

    try:

        inserted_count = int(
            result.split()[-1]
        )

    except Exception:

        inserted_count = 0

    return inserted_count


# ============================================================
# HISTORY SCANNER
# ============================================================

async def enqueue_history(
    pool,
    client,
    source_entity,
):

    logger.info(
        "Starting/resuming source history scan..."
    )

    total_seen = 0
    total_inserted = 0

    max_existing_id = (
        await get_scan_start_id(
            pool
        )
    )

    logger.info(
        "Current database maximum "
        "source message ID: %s",
        max_existing_id,
    )

    pending_batch = []

    try:

        logger.info(
            "Beginning Telegram history iteration..."
        )

        async for message in client.iter_messages(
            source_entity,
            reverse=True,
        ):

            if shutdown_event.is_set():
                break

            total_seen += 1

            pending_batch.append(
                int(message.id)
            )

            if len(
                pending_batch
            ) >= SCAN_BATCH_SIZE:

                inserted = (
                    await insert_message_batch(
                        pool,
                        pending_batch,
                    )
                )

                total_inserted += inserted

                pending_batch.clear()

                if (
                    total_seen
                    % SCAN_LOG_EVERY
                    == 0
                ):

                    logger.info(
                        "History scan: "
                        "seen=%s inserted=%s",
                        total_seen,
                        total_inserted,
                    )

        # ----------------------------------------------------
        # Flush remaining IDs
        # ----------------------------------------------------

        if (
            pending_batch
            and not shutdown_event.is_set()
        ):

            inserted = (
                await insert_message_batch(
                    pool,
                    pending_batch,
                )
            )

            total_inserted += inserted

            pending_batch.clear()

    except errors.FloodWaitError as exc:

        logger.warning(
            "History scanner FloodWait: "
            "%s seconds.",
            exc.seconds,
        )

        try:

            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=exc.seconds,
            )

        except asyncio.TimeoutError:
            pass

    except Exception:

        logger.exception(
            "History scanner error."
        )

    logger.info(
        "History scan pass finished: "
        "seen=%s inserted=%s",
        total_seen,
        total_inserted,
    )


# ============================================================
# CLAIM JOB
# ============================================================

async def claim_job(
    pool,
    account_no,
    worker_id,
):

    worker_name = (
        f"account-{account_no}-worker-{worker_id}"
    )

    async with pool.acquire() as conn:

        async with conn.transaction():

            row = await conn.fetchrow(
                """
                SELECT source_message_id

                FROM messages

                WHERE status = 'pending'

                  AND attempts < $1

                ORDER BY source_message_id ASC

                FOR UPDATE SKIP LOCKED

                LIMIT 1
                """,
                MAX_RETRIES,
            )

            if not row:
                return None

            message_id = int(
                row[
                    "source_message_id"
                ]
            )

            await conn.execute(
                """
                UPDATE messages

                SET
                    status = 'processing',

                    attempts = attempts + 1,

                    processing_by = $1,

                    processing_started_at = NOW(),

                    updated_at = NOW()

                WHERE source_message_id = $2
                """,
                worker_name,
                message_id,
            )

            return message_id


# ============================================================
# MARK COMPLETED
# ============================================================

async def mark_completed(
    pool,
    message_id,
):

    async with pool.acquire() as conn:

        await conn.execute(
            """
            UPDATE messages

            SET
                status = 'completed',

                completed_at = NOW(),

                processing_by = NULL,

                processing_started_at = NULL,

                last_error = NULL,

                updated_at = NOW()

            WHERE source_message_id = $1
            """,
            message_id,
        )


# ============================================================
# NORMAL RETRY
# ============================================================

async def mark_retry(
    pool,
    message_id,
    error_message,
):

    """
    Used for REAL forwarding errors.

    attempts has already been incremented.

    When attempts reaches MAX_RETRIES,
    the job becomes failed.
    """

    async with pool.acquire() as conn:

        await conn.execute(
            """
            UPDATE messages

            SET
                status = CASE

                    WHEN attempts >= $2
                    THEN 'failed'

                    ELSE 'pending'

                END,

                processing_by = NULL,

                processing_started_at = NULL,

                last_error = $3,

                updated_at = NOW()

            WHERE source_message_id = $1
            """,
            message_id,
            MAX_RETRIES,
            error_message,
        )


# ============================================================
# FLOODWAIT RETURN TO QUEUE
# ============================================================

async def mark_floodwait_pending(
    pool,
    message_id,
    seconds,
):

    """
    FloodWait is NOT a real forwarding failure.

    Therefore:

        attempts = attempts - 1

    This returns the job to its previous retry state.
    """

    async with pool.acquire() as conn:

        await conn.execute(
            """
            UPDATE messages

            SET
                status = 'pending',

                attempts = GREATEST(
                    attempts - 1,
                    0
                ),

                processing_by = NULL,

                processing_started_at = NULL,

                last_error = $2,

                updated_at = NOW()

            WHERE source_message_id = $1
            """,
            message_id,
            f"FloodWait {seconds}s",
        )


# ============================================================
# ACCOUNT COOLDOWN
# ============================================================

async def set_cooldown(
    pool,
    account_no,
    seconds,
    error_message,
):

    async with pool.acquire() as conn:

        await conn.execute(
            """
            UPDATE account_state

            SET
                cooldown_until =
                    NOW()
                    + (
                        $2 * INTERVAL '1 second'
                    ),

                last_error = $3,

                updated_at = NOW()

            WHERE account_no = $1
            """,
            account_no,
            seconds,
            error_message,
        )


async def clear_cooldown(
    pool,
    account_no,
):

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


async def get_cooldown(
    pool,
    account_no,
):

    async with pool.acquire() as conn:

        row = await conn.fetchrow(
            """
            SELECT cooldown_until

            FROM account_state

            WHERE account_no = $1
            """,
            account_no,
        )

    if (
        not row
        or row["cooldown_until"] is None
    ):

        return 0

    remaining = (
        row["cooldown_until"]
        - utcnow()
    ).total_seconds()

    return max(
        0,
        int(remaining),
    )


async def wait_for_account_cooldown(
    pool,
    account_no,
):

    remaining = await get_cooldown(
        pool,
        account_no,
    )

    if remaining <= 0:
        return True

    logger.warning(
        "Account %s cooldown active: "
        "%s seconds remaining.",
        account_no,
        remaining,
    )

    while remaining > 0:

        if shutdown_event.is_set():
            return False

        await asyncio.sleep(
            min(
                remaining,
                10,
            )
        )

        remaining = await get_cooldown(
            pool,
            account_no,
        )

    await clear_cooldown(
        pool,
        account_no,
    )

    logger.info(
        "Account %s cooldown finished. "
        "Worker resuming.",
        account_no,
    )

    return True


# ============================================================
# DIRECT FORWARD
# ============================================================

async def forward_message(
    client,
    message_id,
    source_entity,
    destination_entity,
):

    """
    Direct Telegram forwarding.

    IMPORTANT:
    This produces Telegram's normal
    'Forwarded from' attribution.
    """

    return await client.forward_messages(
        entity=destination_entity,

        messages=message_id,

        from_peer=source_entity,
    )


# ============================================================
# WORKER
# ============================================================

async def worker(
    pool,
    client,
    account_no,
    worker_id,
    source_entity,
    destination_entity,
):

    worker_name = (
        f"A{account_no}-W{worker_id}"
    )

    logger.info(
        "%s started.",
        worker_name,
    )

    successful = 0
    errors_count = 0

    while not shutdown_event.is_set():

        # ====================================================
        # ACCOUNT-SPECIFIC FLOODWAIT
        # ====================================================

        ready = await wait_for_account_cooldown(
            pool,
            account_no,
        )

        if not ready:
            break

        # ====================================================
        # CLAIM JOB
        # ====================================================

        message_id = await claim_job(
            pool,
            account_no,
            worker_id,
        )

        if message_id is None:

            await asyncio.sleep(
                min(
                    POLL_SECONDS,
                    5,
                )
            )

            continue

        logger.info(
            "%s forwarding source message %s",
            worker_name,
            message_id,
        )

        # ====================================================
        # FORWARD
        # ====================================================

        try:

            await forward_message(
                client,
                message_id,
                source_entity,
                destination_entity,
            )

            await mark_completed(
                pool,
                message_id,
            )

            successful += 1

            logger.info(
                "%s completed source message %s "
                "| success=%s errors=%s",
                worker_name,
                message_id,
                successful,
                errors_count,
            )

            await clear_cooldown(
                pool,
                account_no,
            )

            if (
                FORWARD_DELAY_SECONDS
                > 0
            ):

                await asyncio.sleep(
                    FORWARD_DELAY_SECONDS
                )

        # ====================================================
        # FLOODWAIT
        # ====================================================

        except errors.FloodWaitError as exc:

            seconds = int(
                exc.seconds
            )

            logger.warning(
                "%s FloodWait: %s seconds "
                "(%.1f minutes).",
                worker_name,
                seconds,
                seconds / 60,
            )

            # -----------------------------------------------
            # DO NOT consume retry.
            # -----------------------------------------------

            await mark_floodwait_pending(
                pool,
                message_id,
                seconds,
            )

            # -----------------------------------------------
            # ONLY THIS ACCOUNT COOLS DOWN.
            # -----------------------------------------------

            await set_cooldown(
                pool,
                account_no,
                seconds,
                f"FloodWait {seconds}s",
            )

            logger.warning(
                "%s paused because of FloodWait. "
                "Other accounts continue.",
                worker_name,
            )

            continue

        # ====================================================
        # RPC ERROR
        # ====================================================

        except errors.RPCError as exc:

            errors_count += 1

            error_message = (
                f"{type(exc).__name__}: {exc}"
            )

            logger.warning(
                "%s RPC error for message %s: %s",
                worker_name,
                message_id,
                error_message,
            )

            await mark_retry(
                pool,
                message_id,
                error_message,
            )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

        # ====================================================
        # TIMEOUT
        # ====================================================

        except asyncio.TimeoutError:

            errors_count += 1

            error_message = (
                "Telegram request timeout"
            )

            logger.warning(
                "%s timeout for message %s",
                worker_name,
                message_id,
            )

            await mark_retry(
                pool,
                message_id,
                error_message,
            )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

        # ====================================================
        # OTHER ERROR
        # ====================================================

        except Exception as exc:

            errors_count += 1

            error_message = (
                f"{type(exc).__name__}: {exc}"
            )

            logger.exception(
                "%s unexpected error for message %s",
                worker_name,
                message_id,
            )

            await mark_retry(
                pool,
                message_id,
                error_message,
            )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

        # ====================================================
        # PERIODIC WORKER LOG
        # ====================================================

        if (
            successful > 0
            and successful % WORKER_LOG_EVERY == 0
        ):

            logger.info(
                "%s heartbeat | "
                "successful=%s | errors=%s",
                worker_name,
                successful,
                errors_count,
            )

    logger.info(
        "%s stopped | successful=%s | errors=%s",
        worker_name,
        successful,
        errors_count,
    )


# ============================================================
# STATUS LOOP
# ============================================================

async def status_loop(pool):

    logger.info(
        "Status loop started."
    )

    while not shutdown_event.is_set():

        try:

            async with pool.acquire() as conn:

                counts = await conn.fetch(
                    """
                    SELECT
                        status,
                        COUNT(*) AS count

                    FROM messages

                    GROUP BY status

                    ORDER BY status
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
                    SELECT source_message_id

                    FROM messages

                    WHERE status = 'pending'

                    ORDER BY source_message_id ASC

                    LIMIT 1
                    """
                )

                newest = await conn.fetchval(
                    """
                    SELECT MAX(
                        source_message_id
                    )

                    FROM messages
                    """
                )

                processing = await conn.fetchval(
                    """
                    SELECT COUNT(*)

                    FROM messages

                    WHERE status = 'processing'
                    """
                )

                failed = await conn.fetchval(
                    """
                    SELECT COUNT(*)

                    FROM messages

                    WHERE status = 'failed'
                    """
                )

            parts = [
                f"{row['status']}={row['count']}"
                for row in counts
            ]

            logger.info(
                "STATUS total=%s | %s "
                "| processing=%s "
                "| failed=%s "
                "| oldest_pending=%s "
                "| newest=%s",
                total,
                " ".join(parts),
                processing,
                failed,
                oldest_pending,
                newest,
            )

            # ------------------------------------------------
            # SHOW ALL FIVE ACCOUNT COOLDOWNS
            # ------------------------------------------------

            for account_no in range(1, 6):

                cooldown = await get_cooldown(
                    pool,
                    account_no,
                )

                if cooldown > 0:

                    logger.info(
                        "ACCOUNT %s cooldown=%ss",
                        account_no,
                        cooldown,
                    )

        except Exception:

            logger.exception(
                "Status loop error."
            )

        try:

            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=60,
            )

        except asyncio.TimeoutError:
            pass

    logger.info(
        "Status loop stopped."
    )


# ============================================================
# NEW MESSAGE MONITOR
# ============================================================

async def get_latest_stored_message_id(
    pool,
):

    async with pool.acquire() as conn:

        value = await conn.fetchval(
            """
            SELECT COALESCE(
                MAX(source_message_id),
                0
            )

            FROM messages
            """
        )

    return int(
        value or 0
    )


async def monitor_new_messages(
    pool,
    client,
    source_entity,
):

    logger.info(
        "New-message monitor started."
    )

    while not shutdown_event.is_set():

        try:

            latest_id = (
                await get_latest_stored_message_id(
                    pool
                )
            )

            messages = await client.get_messages(
                source_entity,
                limit=100,
                min_id=latest_id,
            )

            if messages:

                ids = [
                    int(message.id)

                    for message in messages

                    if message.id > latest_id
                ]

                if ids:

                    ids = list(
                        dict.fromkeys(ids)
                    )

                    inserted = (
                        await insert_message_batch(
                            pool,
                            ids,
                        )
                    )

                    logger.info(
                        "New-message monitor: "
                        "detected=%s queued=%s",
                        len(ids),
                        inserted,
                    )

            await asyncio.sleep(
                POLL_SECONDS
            )

        except errors.FloodWaitError as exc:

            logger.warning(
                "New-message monitor FloodWait: "
                "%s seconds.",
                exc.seconds,
            )

            try:

                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=exc.seconds,
                )

            except asyncio.TimeoutError:
                pass

        except Exception:

            logger.exception(
                "New-message monitor error."
            )

            try:

                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=POLL_SECONDS,
                )

            except asyncio.TimeoutError:
                pass

    logger.info(
        "New-message monitor stopped."
    )


# ============================================================
# CONNECT ALL FIVE ACCOUNTS
# ============================================================

async def connect_accounts():

    clients = []

    for account_no, session_string in enumerate(
        SESSION_STRINGS,
        start=1,
    ):

        logger.info(
            "Connecting account %s...",
            account_no,
        )

        client = TelegramClient(
            StringSession(session_string),

            API_ID,

            API_HASH,

            connection_retries=10,

            retry_delay=5,

            auto_reconnect=True,
        )

        await client.start()

        me = await client.get_me()

        logger.info(
            "Account %s connected: %s",
            account_no,

            getattr(
                me,
                "first_name",
                None,
            )
            or getattr(
                me,
                "username",
                None,
            )
            or me.id,
        )

        clients.append(
            client
        )

    return clients


# ============================================================
# DISCONNECT
# ============================================================

async def disconnect_clients(
    clients,
):

    for account_no, client in enumerate(
        clients,
        start=1,
    ):

        try:

            logger.info(
                "Disconnecting account %s...",
                account_no,
            )

            await client.disconnect()

        except Exception:

            logger.exception(
                "Error disconnecting account %s",
                account_no,
            )


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def install_signal_handlers():

    try:

        loop = asyncio.get_running_loop()

        for sig in (
            signal.SIGTERM,
            signal.SIGINT,
        ):

            with suppress(
                NotImplementedError
            ):

                loop.add_signal_handler(
                    sig,
                    request_shutdown,
                )

    except Exception:

        pass


# ============================================================
# MAIN
# ============================================================

async def main():

    install_signal_handlers()

    validate_config()

    logger.info(
        "================================================"
    )

    logger.info(
        "Starting Telegram 5-account forwarder..."
    )

    logger.info(
        "Accounts: 5"
    )

    logger.info(
        "Workers per account: %s",
        WORKERS_PER_ACCOUNT,
    )

    logger.info(
        "Total workers: %s",
        5 * WORKERS_PER_ACCOUNT,
    )

    logger.info(
        "MAX_RETRIES: %s",
        MAX_RETRIES,
    )

    logger.info(
        "PROCESSING_LEASE_SECONDS: %s",
        PROCESSING_LEASE_SECONDS,
    )

    logger.info(
        "================================================"
    )

    pool = await asyncpg.create_pool(
        DATABASE_URL,

        min_size=1,

        max_size=15,

        command_timeout=60,
    )

    clients = []

    tasks = []

    try:

        # ====================================================
        # DATABASE
        # ====================================================

        await init_db(
            pool
        )

        await reset_stale_jobs(
            pool
        )

        # ====================================================
        # CONNECT FIVE ACCOUNTS
        # ====================================================

        clients = await connect_accounts()

        if len(clients) != 5:

            raise RuntimeError(
                f"Expected 5 clients, "
                f"got {len(clients)}"
            )

        logger.info(
            "All 5 Telegram accounts connected."
        )

        # ====================================================
        # RESOLVE ACCOUNT 1
        # ====================================================

        logger.info(
            "Resolving Telegram source/destination..."
        )

        source_entity = await resolve_entity(
            clients[0],
            SOURCE,
            "SOURCE_CHANNEL",
        )

        destination_entity = await resolve_entity(
            clients[0],
            DESTINATION,
            "DESTINATION_CHANNEL",
        )

        logger.info(
            "SOURCE resolved successfully: %s",
            getattr(
                source_entity,
                "title",
                None,
            )
            or getattr(
                source_entity,
                "username",
                None,
            )
            or getattr(
                source_entity,
                "id",
                None,
            ),
        )

        logger.info(
            "DESTINATION resolved successfully: %s",
            getattr(
                destination_entity,
                "title",
                None,
            )
            or getattr(
                destination_entity,
                "username",
                None,
            )
            or getattr(
                destination_entity,
                "id",
                None,
            ),
        )

        # ====================================================
        # RESOLVE SOURCE + DESTINATION FOR ALL 5
        # ====================================================

        account_entities = []

        for account_index, client in enumerate(
            clients
        ):

            account_no = (
                account_index + 1
            )

            if account_no == 1:

                account_entities.append(
                    (
                        source_entity,
                        destination_entity,
                    )
                )

                continue

            logger.info(
                "Resolving source/destination "
                "for account %s...",
                account_no,
            )

            account_source = await resolve_entity(
                client,
                SOURCE,
                f"ACCOUNT_{account_no}_SOURCE",
            )

            account_destination = await resolve_entity(
                client,
                DESTINATION,
                f"ACCOUNT_{account_no}_DESTINATION",
            )

            account_entities.append(
                (
                    account_source,
                    account_destination,
                )
            )

        logger.info(
            "Source/destination entities resolved "
            "for all 5 accounts."
        )

        # ====================================================
        # START FIVE ACCOUNT WORKERS
        # ====================================================

        worker_count = 0

        for account_index, client in enumerate(
            clients
        ):

            account_no = (
                account_index + 1
            )

            (
                account_source,
                account_destination,
            ) = account_entities[
                account_index
            ]

            for worker_id in range(
                1,
                WORKERS_PER_ACCOUNT + 1,
            ):

                worker_count += 1

                task = asyncio.create_task(
                    worker(
                        pool,

                        client,

                        account_no,

                        worker_id,

                        account_source,

                        account_destination,
                    ),
                    name=(
                        f"worker-{account_no}-{worker_id}"
                    ),
                )

                tasks.append(
                    task
                )

        logger.info(
            "Started %s forwarding workers.",
            worker_count,
        )

        # ====================================================
        # STATUS LOOP
        # ====================================================

        tasks.append(
            asyncio.create_task(
                status_loop(pool),
                name="status-loop",
            )
        )

        # ====================================================
        # STALE JOB RECOVERY
        # ====================================================

        tasks.append(
            asyncio.create_task(
                stale_job_recovery_loop(pool),
                name="stale-job-recovery",
            )
        )

        # ====================================================
        # NEW MESSAGE MONITOR
        # ====================================================

        tasks.append(
            asyncio.create_task(
                monitor_new_messages(
                    pool,
                    clients[0],
                    source_entity,
                ),
                name="new-message-monitor",
            )
        )

        # ====================================================
        # HISTORY SCANNER
        # ====================================================

        history_task = asyncio.create_task(
            enqueue_history(
                pool,
                clients[0],
                source_entity,
            ),
            name="history-scanner",
        )

        tasks.append(
            history_task
        )

        # ====================================================
        # WAIT FOR HISTORY SCANNER
        # ====================================================

        await history_task

        logger.info(
            "Historical scan task completed."
        )

        # ====================================================
        # MAIN KEEP-ALIVE LOOP
        # ====================================================

        while not shutdown_event.is_set():

            # ------------------------------------------------
            # Detect critical task failures.
            # ------------------------------------------------

            for task in list(tasks):

                if not task.done():
                    continue

                task_name = task.get_name()

                # --------------------------------------------
                # Ignore completed history scanner.
                # --------------------------------------------

                if task_name == "history-scanner":
                    continue

                # --------------------------------------------
                # Worker died unexpectedly.
                # --------------------------------------------

                if task_name.startswith(
                    "worker-"
                ):

                    try:

                        exception = task.exception()

                    except (
                        asyncio.CancelledError
                    ):

                        exception = None

                    if exception:

                        logger.error(
                            "WORKER %s stopped "
                            "unexpectedly: %s",
                            task_name,
                            exception,
                        )

                    continue

                # --------------------------------------------
                # Critical service died.
                # --------------------------------------------

                if task_name in {
                    "status-loop",
                    "stale-job-recovery",
                    "new-message-monitor",
                }:

                    try:

                        exception = task.exception()

                    except (
                        asyncio.CancelledError
                    ):

                        exception = None

                    if exception:

                        logger.error(
                            "CRITICAL TASK %s stopped "
                            "with exception: %s",
                            task_name,
                            exception,
                        )

                        # ------------------------------------
                        # Restart status loop
                        # ------------------------------------

                        if task_name == "status-loop":

                            new_task = (
                                asyncio.create_task(
                                    status_loop(pool),
                                    name="status-loop",
                                )
                            )

                            tasks.append(
                                new_task
                            )

                        # ------------------------------------
                        # Restart recovery
                        # ------------------------------------

                        elif (
                            task_name
                            == "stale-job-recovery"
                        ):

                            new_task = (
                                asyncio.create_task(
                                    stale_job_recovery_loop(
                                        pool
                                    ),
                                    name=(
                                        "stale-job-recovery"
                                    ),
                                )
                            )

                            tasks.append(
                                new_task
                            )

                        # ------------------------------------
                        # Restart new message monitor
                        # ------------------------------------

                        elif (
                            task_name
                            == "new-message-monitor"
                        ):

                            new_task = (
                                asyncio.create_task(
                                    monitor_new_messages(
                                        pool,
                                        clients[0],
                                        source_entity,
                                    ),
                                    name=(
                                        "new-message-monitor"
                                    ),
                                )
                            )

                            tasks.append(
                                new_task
                            )

            await asyncio.sleep(
                1
            )

    except asyncio.CancelledError:

        logger.info(
            "Main task cancelled."
        )

    except Exception:

        logger.exception(
            "Fatal application error."
        )

    finally:

        logger.info(
            "Stopping application..."
        )

        shutdown_event.set()

        # ====================================================
        # CANCEL TASKS
        # ====================================================

        for task in tasks:

            if not task.done():

                task.cancel()

        if tasks:

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        # ====================================================
        # DISCONNECT ACCOUNTS
        # ====================================================

        if clients:

            await disconnect_clients(
                clients
            )

        # ====================================================
        # CLOSE DATABASE
        # ====================================================

        await pool.close()

        logger.info(
            "Application stopped."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "Stopped by keyboard interrupt."
        )
