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

SOURCE = os.environ["SOURCE_CHANNEL"]
DESTINATION = os.environ["DESTINATION_CHANNEL"]

DATABASE_URL = os.environ["DATABASE_URL"]

# EXACTLY 3 TELEGRAM ACCOUNTS
SESSION_STRINGS = [
    os.getenv("SESSION_1", "").strip(),
    os.getenv("SESSION_2", "").strip(),
    os.getenv("SESSION_3", "").strip(),
]

WORKERS_PER_ACCOUNT = int(os.getenv("WORKERS_PER_ACCOUNT", "1"))

QUEUE_BATCH_SIZE = int(os.getenv("QUEUE_BATCH_SIZE", "500"))
SCAN_BATCH_SIZE = int(os.getenv("SCAN_BATCH_SIZE", "500"))

PROCESSING_LEASE_SECONDS = int(
    os.getenv("PROCESSING_LEASE_SECONDS", "1800")
)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "10"))

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))

# Small delay between successful forwards.
# Helps avoid hammering Telegram.
FORWARD_DELAY_SECONDS = float(
    os.getenv("FORWARD_DELAY_SECONDS", "0.2")
)

# Number of source messages requested at once while scanning.
# Telethon's iterator handles the actual API pagination.
SCAN_LOG_EVERY = int(
    os.getenv("SCAN_LOG_EVERY", "500")
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
# GLOBAL SHUTDOWN
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
        raise RuntimeError("SOURCE_CHANNEL is empty")

    if not DESTINATION:
        raise RuntimeError("DESTINATION_CHANNEL is empty")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty")

    if len(SESSION_STRINGS) != 3:
        raise RuntimeError("Exactly 3 sessions are required")

    for i, session in enumerate(SESSION_STRINGS, start=1):
        if not session:
            raise RuntimeError(f"SESSION_{i} is empty")

        try:
            StringSession(session)
        except Exception as exc:
            raise RuntimeError(
                f"SESSION_{i} is NOT a valid Telethon StringSession"
            ) from exc


# ============================================================
# DATABASE INITIALIZATION
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

        # EXACTLY 3 ACCOUNTS
        for account_no in range(1, 4):
            await conn.execute(
                """
                INSERT INTO account_state (
                    account_no,
                    cooldown_until,
                    last_error
                )
                VALUES ($1, NULL, NULL)
                ON CONFLICT (account_no)
                DO NOTHING
                """,
                account_no,
            )

    logger.info("Database initialized")


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
                  NOW() - ($1 * INTERVAL '1 second')
            """,
            PROCESSING_LEASE_SECONDS,
        )

    logger.info("Stale jobs reset: %s", result)


# ============================================================
# HISTORY SCANNER
# ============================================================

async def get_scan_start_id(pool):
    """
    Returns the highest source message ID already present.

    We scan backwards/forwards based on the database state so
    restarts don't unnecessarily insert duplicate jobs.
    """

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COALESCE(MAX(source_message_id), 0) AS max_id
            FROM messages
            """
        )

    return int(row["max_id"])


async def enqueue_history(pool, client):
    """
    Scan the source channel and continuously insert message IDs.

    IMPORTANT:
    Workers run at the same time as this function.
    """

    logger.info("Starting/resuming source history scan...")

    total_seen = 0
    total_inserted = 0

    # Existing maximum ID.
    max_existing_id = await get_scan_start_id(pool)

    logger.info(
        "Current database maximum source message ID: %s",
        max_existing_id,
    )

    try:
        # ----------------------------------------------------
        # IMPORTANT:
        # We scan the entire history in reverse=True.
        #
        # ON CONFLICT DO NOTHING makes this safe to resume.
        # ----------------------------------------------------

        pending_batch = []

        async for message in client.iter_messages(
            SOURCE,
            reverse=True,
        ):
            if shutdown_event.is_set():
                break

            total_seen += 1

            pending_batch.append(message.id)

            if len(pending_batch) >= SCAN_BATCH_SIZE:
                inserted = await insert_message_batch(
                    pool,
                    pending_batch,
                )

                total_inserted += inserted

                pending_batch.clear()

                if total_seen % SCAN_LOG_EVERY == 0:
                    logger.info(
                        "History scan: seen=%s inserted=%s",
                        total_seen,
                        total_inserted,
                    )

        # Final partial batch
        if pending_batch and not shutdown_event.is_set():
            inserted = await insert_message_batch(
                pool,
                pending_batch,
            )

            total_inserted += inserted

    except errors.FloodWaitError as exc:
        logger.error(
            "History scanner FloodWait: %s seconds",
            exc.seconds,
        )

        await asyncio.sleep(exc.seconds)

    except Exception:
        logger.exception("History scanner error")

    logger.info(
        "History scan pass finished: seen=%s inserted=%s",
        total_seen,
        total_inserted,
    )


async def insert_message_batch(pool, message_ids):
    if not message_ids:
        return 0

    rows = [(int(message_id),) for message_id in message_ids]

    async with pool.acquire() as conn:
        result = await conn.executemany(
            """
            INSERT INTO messages (
                source_message_id,
                status,
                attempts,
                created_at,
                updated_at
            )
            VALUES ($1, 'pending', 0, NOW(), NOW())
            ON CONFLICT (source_message_id)
            DO NOTHING
            """,
            rows,
        )

    # asyncpg executemany does not conveniently return affected
    # row count, so use a count query for this batch.
    async with pool.acquire() as conn:
        inserted_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM messages
            WHERE source_message_id = ANY($1::BIGINT[])
            """,
            message_ids,
        )

    # This represents records existing after insertion, not
    # strictly newly inserted records. It is only used for logging.
    return int(inserted_count or 0)


# ============================================================
# JOB CLAIMING
# ============================================================

async def claim_job(pool, account_no, worker_id):
    worker_name = f"account-{account_no}-worker-{worker_id}"

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

            message_id = int(row["source_message_id"])

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
# JOB STATUS
# ============================================================

async def mark_completed(pool, message_id):
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


async def mark_pending(pool, message_id, error_message=None):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE messages
            SET
                status = 'pending',
                processing_by = NULL,
                processing_started_at = NULL,
                last_error = $2,
                updated_at = NOW()
            WHERE source_message_id = $1
              AND attempts < $3
            """,
            message_id,
            error_message,
            MAX_RETRIES,
        )


async def mark_failed(pool, message_id, error_message):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE messages
            SET
                status = 'failed',
                processing_by = NULL,
                processing_started_at = NULL,
                last_error = $2,
                updated_at = NOW()
            WHERE source_message_id = $1
            """,
            message_id,
            error_message,
        )


# ============================================================
# ACCOUNT COOLDOWN
# ============================================================

async def set_cooldown(pool, account_no, seconds, error_message):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE account_state
            SET
                cooldown_until =
                    NOW() + ($2 * INTERVAL '1 second'),
                last_error = $3,
                updated_at = NOW()
            WHERE account_no = $1
            """,
            account_no,
            seconds,
            error_message,
        )


async def clear_cooldown(pool, account_no):
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


async def get_cooldown(pool, account_no):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT cooldown_until
            FROM account_state
            WHERE account_no = $1
            """,
            account_no,
        )

    if not row or row["cooldown_until"] is None:
        return 0

    remaining = (
        row["cooldown_until"] - utcnow()
    ).total_seconds()

    return max(0, int(remaining))


async def wait_for_account_cooldown(pool, account_no):
    remaining = await get_cooldown(
        pool,
        account_no,
    )

    if remaining > 0:
        logger.info(
            "Account %s cooldown: %s seconds remaining",
            account_no,
            remaining,
        )

        # Don't sleep one giant block so shutdown remains responsive.
        while remaining > 0:
            if shutdown_event.is_set():
                return False

            await asyncio.sleep(
                min(remaining, 10)
            )

            remaining = await get_cooldown(
                pool,
                account_no,
            )

        await clear_cooldown(pool, account_no)

    return True


# ============================================================
# FORWARD ONE MESSAGE
# ============================================================

async def forward_message(client, message_id):
    """
    Direct Telegram forwarding.

    No download.
    No upload.
    Telegram performs the forwarding.
    """

    sent = await client.forward_messages(
        entity=DESTINATION,
        messages=message_id,
        from_peer=SOURCE,
    )

    return sent


# ============================================================
# WORKER
# ============================================================

async def worker(pool, client, account_no, worker_id):
    worker_name = f"A{account_no}-W{worker_id}"

    logger.info(
        "%s started",
        worker_name,
    )

    while not shutdown_event.is_set():

        # --------------------------------------------
        # Account-specific FloodWait cooldown
        # --------------------------------------------

        ready = await wait_for_account_cooldown(
            pool,
            account_no,
        )

        if not ready:
            break

        # --------------------------------------------
        # Claim next database job
        # --------------------------------------------

        message_id = await claim_job(
            pool,
            account_no,
            worker_id,
        )

        if message_id is None:
            # Nothing available right now.
            await asyncio.sleep(
                min(POLL_SECONDS, 5)
            )
            continue

        logger.info(
            "%s forwarding source message %s",
            worker_name,
            message_id,
        )

        # --------------------------------------------
        # Forward
        # --------------------------------------------

        try:
            await forward_message(
                client,
                message_id,
            )

            await mark_completed(
                pool,
                message_id,
            )

            logger.info(
                "%s completed source message %s",
                worker_name,
                message_id,
            )

            await clear_cooldown(
                pool,
                account_no,
            )

            if FORWARD_DELAY_SECONDS > 0:
                await asyncio.sleep(
                    FORWARD_DELAY_SECONDS
                )

        # --------------------------------------------
        # FloodWait
        # --------------------------------------------

        except errors.FloodWaitError as exc:
            seconds = int(exc.seconds)

            logger.warning(
                "%s FloodWait: %s seconds",
                worker_name,
                seconds,
            )

            # Put this job back into pending so another
            # account can process it.
            await mark_pending(
                pool,
                message_id,
                f"FloodWait {seconds}s",
            )

            # Cool down ONLY this account.
            await set_cooldown(
                pool,
                account_no,
                seconds,
                f"FloodWait {seconds}s",
            )

            # Continue loop; other accounts keep working.

        # --------------------------------------------
        # RPC errors
        # --------------------------------------------

        except errors.RPCError as exc:
            error_message = (
                f"{type(exc).__name__}: {exc}"
            )

            logger.warning(
                "%s RPC error for message %s: %s",
                worker_name,
                message_id,
                error_message,
            )

            # Usually retryable unless max attempts reached.
            await mark_pending(
                pool,
                message_id,
                error_message,
            )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

        # --------------------------------------------
        # Timeout
        # --------------------------------------------

        except asyncio.TimeoutError:
            error_message = "Telegram request timeout"

            logger.warning(
                "%s timeout for message %s",
                worker_name,
                message_id,
            )

            await mark_pending(
                pool,
                message_id,
                error_message,
            )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

        # --------------------------------------------
        # Any unexpected error
        # --------------------------------------------

        except Exception as exc:
            error_message = (
                f"{type(exc).__name__}: {exc}"
            )

            logger.exception(
                "%s unexpected error for message %s",
                worker_name,
                message_id,
            )

            await mark_pending(
                pool,
                message_id,
                error_message,
            )

            await asyncio.sleep(
                RETRY_DELAY_SECONDS
            )

    logger.info(
        "%s stopped",
        worker_name,
    )


# ============================================================
# STATUS LOOP
# ============================================================

async def status_loop(pool):
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
                    SELECT MAX(source_message_id)
                    FROM messages
                    """
                )

            parts = [
                f"{row['status']}={row['count']}"
                for row in counts
            ]

            logger.info(
                "STATUS total=%s | %s | oldest_pending=%s | newest=%s",
                total,
                " ".join(parts),
                oldest_pending,
                newest,
            )

        except Exception:
            logger.exception(
                "Status loop error"
            )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=60,
            )
        except asyncio.TimeoutError:
            pass


# ============================================================
# NEW MESSAGE MONITOR
# ============================================================

async def get_latest_stored_message_id(pool):
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

    return int(value or 0)


async def monitor_new_messages(pool, client):
    """
    Continuously watches the source channel for new messages.

    This runs independently from the historical scanner.
    """

    logger.info(
        "New-message monitor started"
    )

    while not shutdown_event.is_set():

        try:
            latest_id = await get_latest_stored_message_id(
                pool
            )

            # Fetch messages newer than the newest
            # message currently stored in PostgreSQL.
            messages = await client.get_messages(
                SOURCE,
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
                    ids.reverse()

                    inserted = await insert_message_batch(
                        pool,
                        ids,
                    )

                    logger.info(
                        "New-message monitor: detected %s new messages",
                        len(ids),
                    )

            await asyncio.sleep(
                POLL_SECONDS
            )

        except errors.FloodWaitError as exc:
            logger.warning(
                "New-message monitor FloodWait: %s seconds",
                exc.seconds,
            )

            await asyncio.sleep(
                exc.seconds
            )

        except Exception:
            logger.exception(
                "New-message monitor error"
            )

            await asyncio.sleep(
                POLL_SECONDS
            )

    logger.info(
        "New-message monitor stopped"
    )


# ============================================================
# ACCOUNT CONNECTION
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
            getattr(me, "first_name", None)
            or getattr(me, "username", None)
            or me.id,
        )

        clients.append(client)

    return clients


# ============================================================
# DISCONNECT
# ============================================================

async def disconnect_clients(clients):
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
# MAIN
# ============================================================

async def main():
    validate_config()

    logger.info(
        "Starting Telegram 3-account forwarder..."
    )

    # --------------------------------------------
    # PostgreSQL
    # --------------------------------------------

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=60,
    )

    clients = []

    tasks = []

    try:
        # ----------------------------------------
        # Database
        # ----------------------------------------

        await init_db(pool)

        await reset_stale_jobs(pool)

        # ----------------------------------------
        # Telegram accounts
        # ----------------------------------------

        clients = await connect_accounts()

        if len(clients) != 3:
            raise RuntimeError(
                f"Expected 3 clients, got {len(clients)}"
            )

        logger.info(
            "All 3 Telegram accounts connected."
        )

        # ----------------------------------------
        # START WORKERS FIRST
        #
        # This is the important change.
        #
        # Workers begin consuming the DB queue
        # immediately while history is scanned.
        # ----------------------------------------

        for account_no, client in enumerate(
            clients,
            start=1,
        ):
            for worker_id in range(
                1,
                WORKERS_PER_ACCOUNT + 1,
            ):
                tasks.append(
                    asyncio.create_task(
                        worker(
                            pool,
                            client,
                            account_no,
                            worker_id,
                        ),
                        name=(
                            f"worker-{account_no}-{worker_id}"
                        ),
                    )
                )

        logger.info(
            "Started %s forwarding workers",
            len(tasks),
        )

        # ----------------------------------------
        # Status monitor
        # ----------------------------------------

        tasks.append(
            asyncio.create_task(
                status_loop(pool),
                name="status-loop",
            )
        )

        # ----------------------------------------
        # New message monitor
        #
        # Use account 1 for monitoring.
        # ----------------------------------------

        tasks.append(
            asyncio.create_task(
                monitor_new_messages(
                    pool,
                    clients[0],
                ),
                name="new-message-monitor",
            )
        )

        # ----------------------------------------
        # Historical scan
        #
        # Runs concurrently with workers.
        # ----------------------------------------

        history_task = asyncio.create_task(
            enqueue_history(
                pool,
                clients[0],
            ),
            name="history-scanner",
        )

        tasks.append(history_task)

        # ----------------------------------------
        # Wait until history scan completes.
        #
        # Workers and monitor continue running.
        # ----------------------------------------

        await history_task

        logger.info(
            "Historical scan task completed."
        )

        # ----------------------------------------
        # Keep application alive.
        #
        # Workers will continue processing pending
        # jobs and the monitor will watch new messages.
        # ----------------------------------------

        while not shutdown_event.is_set():
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        logger.info(
            "Main task cancelled"
        )

    except Exception:
        logger.exception(
            "Fatal application error"
        )

    finally:
        logger.info(
            "Stopping application..."
        )

        shutdown_event.set()

        # ----------------------------------------
        # Give tasks a moment to exit cleanly
        # ----------------------------------------

        if tasks:
            for task in tasks:
                if not task.done():
                    task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        # ----------------------------------------
        # Telegram
        # ----------------------------------------

        if clients:
            await disconnect_clients(
                clients
            )

        # ----------------------------------------
        # PostgreSQL
        # ----------------------------------------

        await pool.close()

        logger.info(
            "Application stopped."
        )


# ============================================================
# SIGNAL HANDLING
# ============================================================

def install_signal_handlers():
    try:
        loop = asyncio.get_running_loop()

        for sig in (
            signal.SIGTERM,
            signal.SIGINT,
        ):
            with suppress(NotImplementedError):
                loop.add_signal_handler(
                    sig,
                    request_shutdown,
                )

    except Exception:
        pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    install_signal_handlers()

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info(
            "Stopped by keyboard interrupt."
        )
