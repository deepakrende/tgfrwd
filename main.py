import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

import asyncpg
from dotenv import load_dotenv
from telethon import TelegramClient, errors
from telethon.sessions import StringSession


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("forwarder")


API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

SOURCE = int(os.environ["SOURCE_CHANNEL"])
DESTINATION = int(os.environ["DESTINATION_CHANNEL"])

DATABASE_URL = os.environ["DATABASE_URL"]


WORKERS_PER_ACCOUNT = int(
    os.getenv("WORKERS_PER_ACCOUNT", "1")
)

QUEUE_BATCH_SIZE = int(
    os.getenv("QUEUE_BATCH_SIZE", "500")
)

SCAN_BATCH_SIZE = int(
    os.getenv("SCAN_BATCH_SIZE", "500")
)

LEASE_SECONDS = int(
    os.getenv("PROCESSING_LEASE_SECONDS", "1800")
)

MAX_RETRIES = int(
    os.getenv("MAX_RETRIES", "5")
)

RETRY_DELAY = int(
    os.getenv("RETRY_DELAY_SECONDS", "10")
)

POLL_SECONDS = int(
    os.getenv("POLL_SECONDS", "15")
)


SESSION_STRINGS = [
    os.getenv(f"SESSION_{i}", "")
    for i in range(1, 4)
]


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

async def init_db(pool):

    with open("schema.sql", "r", encoding="utf-8") as file:
        schema = file.read()

    async with pool.acquire() as con:

        await con.execute(schema)

        for account_no in range(1, 4):

            await con.execute(
                """
                INSERT INTO account_state(account_no)
                VALUES($1)
                ON CONFLICT(account_no) DO NOTHING
                """,
                account_no,
            )


# ============================================================
# RESET STALE JOBS
# ============================================================

async def reset_stale_jobs(pool):

    async with pool.acquire() as con:

        result = await con.execute(
            """
            UPDATE messages
            SET
                status = 'pending',
                locked_at = NULL,
                updated_at = NOW()
            WHERE
                status = 'processing'
                AND locked_at < NOW() -
                    ($1 * INTERVAL '1 second')
            """,
            LEASE_SECONDS,
        )

        log.info(
            "Stale jobs reset: %s",
            result,
        )


# ============================================================
# QUEUE HISTORICAL MESSAGES
# ============================================================

async def enqueue_history(pool, client):

    log.info(
        "Starting/resuming source history scan..."
    )

    inserted = 0

    async for message in client.iter_messages(
        SOURCE,
        reverse=True,
    ):

        if message is None:
            continue

        result = await pool.execute(
            """
            INSERT INTO messages(source_message_id)
            VALUES($1)
            ON CONFLICT(source_message_id)
            DO NOTHING
            """,
            message.id,
        )

        if result.endswith("1"):
            inserted += 1

        if (
            inserted
            and inserted % QUEUE_BATCH_SIZE == 0
        ):
            log.info(
                "Queued %s new source messages",
                inserted,
            )

    log.info(
        "History scan complete. Newly queued: %s",
        inserted,
    )


# ============================================================
# CLAIM NEXT JOB
# ============================================================

async def claim_job(
    pool,
    account_no,
):

    async with pool.acquire() as con:

        row = await con.fetchrow(
            """
            WITH candidate AS
            (
                SELECT source_message_id
                FROM messages
                WHERE
                    status = 'pending'
                    AND attempts < $2
                ORDER BY source_message_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )

            UPDATE messages AS m

            SET
                status = 'processing',
                account_no = $1,
                attempts = attempts + 1,
                locked_at = NOW(),
                updated_at = NOW()

            FROM candidate AS c

            WHERE
                m.source_message_id =
                c.source_message_id

            RETURNING
                m.source_message_id,
                m.attempts
            """,
            account_no,
            MAX_RETRIES,
        )

        return row


# ============================================================
# MARK COMPLETED
# ============================================================

async def mark_completed(
    pool,
    source_id,
    destination_id,
    account_no,
):

    async with pool.acquire() as con:

        await con.execute(
            """
            UPDATE messages

            SET
                status = 'completed',
                destination_message_id = $2,
                account_no = $3,
                locked_at = NULL,
                completed_at = NOW(),
                updated_at = NOW()

            WHERE
                source_message_id = $1
            """,
            source_id,
            destination_id,
            account_no,
        )


# ============================================================
# RETURN JOB TO PENDING
# ============================================================

async def mark_pending(
    pool,
    source_id,
    error_text,
):

    async with pool.acquire() as con:

        await con.execute(
            """
            UPDATE messages

            SET
                status = 'pending',
                locked_at = NULL,
                last_error = $2,
                updated_at = NOW()

            WHERE
                source_message_id = $1
            """,
            source_id,
            error_text[:4000],
        )


# ============================================================
# MARK JOB FAILED
# ============================================================

async def mark_failed(
    pool,
    source_id,
    error_text,
):

    async with pool.acquire() as con:

        await con.execute(
            """
            UPDATE messages

            SET
                status = 'failed',
                locked_at = NULL,
                last_error = $2,
                updated_at = NOW()

            WHERE
                source_message_id = $1
            """,
            source_id,
            error_text[:4000],
        )


# ============================================================
# ACCOUNT FLOOD-WAIT COOLDOWN
# ============================================================

async def set_cooldown(
    pool,
    account_no,
    seconds,
    error_text,
):

    until = (
        datetime.now(timezone.utc)
        + timedelta(seconds=seconds)
    )

    async with pool.acquire() as con:

        await con.execute(
            """
            UPDATE account_state

            SET
                cooldown_until = $2,
                last_error = $3,
                updated_at = NOW()

            WHERE
                account_no = $1
            """,
            account_no,
            until,
            error_text[:4000],
        )

    return until


# ============================================================
# CHECK ACCOUNT COOLDOWN
# ============================================================

async def is_on_cooldown(
    pool,
    account_no,
):

    async with pool.acquire() as con:

        row = await con.fetchrow(
            """
            SELECT cooldown_until
            FROM account_state
            WHERE account_no = $1
            """,
            account_no,
        )

    if row is None:
        return False

    cooldown_until = row["cooldown_until"]

    if cooldown_until is None:
        return False

    return (
        cooldown_until
        > datetime.now(timezone.utc)
    )


# ============================================================
# FORWARDING WORKER
# ============================================================

async def worker(
    pool,
    client,
    account_no,
    worker_id,
):

    worker_name = (
        f"account-{account_no}/"
        f"worker-{worker_id}"
    )

    log.info(
        "%s started",
        worker_name,
    )

    while True:

        # ----------------------------------------------------
        # Check account FloodWait cooldown
        # ----------------------------------------------------

        if await is_on_cooldown(
            pool,
            account_no,
        ):

            await asyncio.sleep(2)

            continue

        # ----------------------------------------------------
        # Get next pending message
        # ----------------------------------------------------

        job = await claim_job(
            pool,
            account_no,
        )

        if not job:

            await asyncio.sleep(
                POLL_SECONDS
            )

            continue

        source_id = job[
            "source_message_id"
        ]

        try:

            # ------------------------------------------------
            # Direct Telegram forwarding
            # ------------------------------------------------

            sent = await client.forward_messages(
                DESTINATION,
                source_id,
                from_peer=SOURCE,
            )

            # ------------------------------------------------
            # Get destination message ID
            # ------------------------------------------------

            if isinstance(sent, list):

                if sent:
                    destination_id = sent[0].id
                else:
                    destination_id = None

            else:

                destination_id = sent.id

            # ------------------------------------------------
            # Mark successful
            # ------------------------------------------------

            await mark_completed(
                pool,
                source_id,
                destination_id,
                account_no,
            )

            log.info(
                "%s completed source=%s",
                worker_name,
                source_id,
            )

        # ----------------------------------------------------
        # TELEGRAM FLOOD WAIT
        # ----------------------------------------------------

        except errors.FloodWaitError as error:

            await mark_pending(
                pool,
                source_id,
                f"FloodWait: {error.seconds}s",
            )

            until = await set_cooldown(
                pool,
                account_no,
                error.seconds,
                f"FloodWait {error.seconds}s",
            )

            log.warning(
                "%s is in FloodWait for %s seconds",
                worker_name,
                error.seconds,
            )

            log.warning(
                "%s unavailable until %s",
                worker_name,
                until.isoformat(),
            )

            await asyncio.sleep(1)

        # ----------------------------------------------------
        # TELEGRAM / NETWORK ERRORS
        # ----------------------------------------------------

        except (
            errors.RPCError,
            asyncio.TimeoutError,
        ) as error:

            error_text = (
                f"{type(error).__name__}: "
                f"{error}"
            )

            if job["attempts"] >= MAX_RETRIES:

                await mark_failed(
                    pool,
                    source_id,
                    error_text,
                )

                log.error(
                    "%s permanently failed "
                    "source=%s: %s",
                    worker_name,
                    source_id,
                    error_text,
                )

            else:

                await mark_pending(
                    pool,
                    source_id,
                    error_text,
                )

                await asyncio.sleep(
                    RETRY_DELAY
                )

        # ----------------------------------------------------
        # OTHER ERRORS
        # ----------------------------------------------------

        except Exception as error:

            error_text = (
                f"{type(error).__name__}: "
                f"{error}"
            )

            if job["attempts"] >= MAX_RETRIES:

                await mark_failed(
                    pool,
                    source_id,
                    error_text,
                )

            else:

                await mark_pending(
                    pool,
                    source_id,
                    error_text,
                )

                await asyncio.sleep(
                    RETRY_DELAY
                )

            log.exception(
                "%s error on source=%s",
                worker_name,
                source_id,
            )


# ============================================================
# STATUS LOGGER
# ============================================================

async def status_loop(pool):

    while True:

        try:

            async with pool.acquire() as con:

                row = await con.fetchrow(
                    """
                    SELECT

                        COUNT(*)
                        FILTER (
                            WHERE status = 'completed'
                        ) AS completed,

                        COUNT(*)
                        FILTER (
                            WHERE status = 'pending'
                        ) AS pending,

                        COUNT(*)
                        FILTER (
                            WHERE status = 'processing'
                        ) AS processing,

                        COUNT(*)
                        FILTER (
                            WHERE status = 'failed'
                        ) AS failed,

                        COUNT(*) AS total

                    FROM messages
                    """
                )

            log.info(
                "STATUS | total=%s | "
                "completed=%s | "
                "pending=%s | "
                "processing=%s | "
                "failed=%s",
                row["total"],
                row["completed"],
                row["pending"],
                row["processing"],
                row["failed"],
            )

        except Exception:

            log.exception(
                "Status loop error"
            )

        await asyncio.sleep(60)


# ============================================================
# MONITOR NEW MESSAGES
# ============================================================

async def monitor_new_messages(
    pool,
    client,
):

    async with pool.acquire() as con:

        row = await con.fetchrow(
            """
            SELECT
                COALESCE(
                    MAX(source_message_id),
                    0
                ) AS max_id
            FROM messages
            """
        )

        last_seen = row["max_id"]

    log.info(
        "New-message monitor started "
        "from source ID %s",
        last_seen,
    )

    while True:

        try:

            found = 0

            async for message in client.iter_messages(
                SOURCE,
                min_id=last_seen,
                reverse=True,
            ):

                if message.id <= last_seen:
                    continue

                await pool.execute(
                    """
                    INSERT INTO messages(
                        source_message_id
                    )
                    VALUES($1)

                    ON CONFLICT(
                        source_message_id
                    )
                    DO NOTHING
                    """,
                    message.id,
                )

                last_seen = max(
                    last_seen,
                    message.id,
                )

                found += 1

            if found:

                log.info(
                    "Queued %s new message(s), "
                    "latest=%s",
                    found,
                    last_seen,
                )

        except Exception:

            log.exception(
                "New-message monitor error"
            )

        await asyncio.sleep(
            POLL_SECONDS
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    log.info(
        "Starting Telegram 5-account forwarder..."
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=20,
    )

    await init_db(pool)

    await reset_stale_jobs(pool)

    # --------------------------------------------------------
    # Connect all 5 Telegram accounts
    # --------------------------------------------------------

    clients = []

    for i, session in enumerate(
        SESSION_STRINGS,
        start=1,
    ):

        if not session.strip():

            raise RuntimeError(
                f"SESSION_{i} is empty"
            )

        # ----------------------------------------------------
        # Validate StringSession
        # ----------------------------------------------------

        try:

            session_obj = StringSession(
                session.strip()
            )

        except ValueError:

            raise RuntimeError(
                f"SESSION_{i} is NOT a valid "
                f"Telethon StringSession"
            )

        # ----------------------------------------------------
        # Create Telegram client
        # ----------------------------------------------------

        client = TelegramClient(
            session_obj,
            API_ID,
            API_HASH,
            sequential_updates=True,
        )

        # ----------------------------------------------------
        # Connect
        # ----------------------------------------------------

        await client.start()

        me = await client.get_me()

        log.info(
            "Account %s connected: "
            "%s (%s)",
            i,
            me.first_name,
            me.id,
        )

        clients.append(client)

    # --------------------------------------------------------
    # Historical scan
    # --------------------------------------------------------

    await enqueue_history(
        pool,
        clients[0],
    )

    # --------------------------------------------------------
    # Start workers
    # --------------------------------------------------------

    tasks = []

    for i, client in enumerate(
        clients,
        start=1,
    ):

        for worker_number in range(
            1,
            WORKERS_PER_ACCOUNT + 1,
        ):

            tasks.append(
                asyncio.create_task(
                    worker(
                        pool,
                        client,
                        i,
                        worker_number,
                    )
                )
            )

    # --------------------------------------------------------
    # Status task
    # --------------------------------------------------------

    tasks.append(
        asyncio.create_task(
            status_loop(pool)
        )
    )

    # --------------------------------------------------------
    # New-message monitor
    # --------------------------------------------------------

    tasks.append(
        asyncio.create_task(
            monitor_new_messages(
                pool,
                clients[0],
            )
        )
    )

    # --------------------------------------------------------
    # Run everything
    # --------------------------------------------------------

    try:

        await asyncio.gather(
            *tasks
        )

    finally:

        log.info(
            "Shutting down..."
        )

        for client in clients:

            await client.disconnect()

        await pool.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    asyncio.run(main())
