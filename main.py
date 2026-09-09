import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

import asyncpg
from dotenv import load_dotenv
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

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

WORKERS_PER_ACCOUNT = int(os.getenv("WORKERS_PER_ACCOUNT", "1"))
QUEUE_BATCH_SIZE = int(os.getenv("QUEUE_BATCH_SIZE", "500"))
SCAN_BATCH_SIZE = int(os.getenv("SCAN_BATCH_SIZE", "500"))
LEASE_SECONDS = int(os.getenv("PROCESSING_LEASE_SECONDS", "1800"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY_SECONDS", "10"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))

SESSION_STRINGS = [os.getenv(f"SESSION_{i}", "") for i in range(1, 6)]


async def init_db(pool):
    schema = open("schema.sql", "r", encoding="utf-8").read()
    async with pool.acquire() as con:
        await con.execute(schema)
        for n in range(1, 6):
            await con.execute(
                """INSERT INTO account_state(account_no)
                   VALUES($1) ON CONFLICT(account_no) DO NOTHING""", n
            )


async def reset_stale_jobs(pool):
    async with pool.acquire() as con:
        count = await con.execute(
            """UPDATE messages
               SET status='pending', locked_at=NULL, updated_at=NOW()
               WHERE status='processing'
                 AND locked_at < NOW() - ($1 * INTERVAL '1 second')""",
            LEASE_SECONDS,
        )
        log.info("Stale jobs reset: %s", count)


async def enqueue_history(pool, clients):
    # Use the first available account for scanning. This does not forward;
    # it only records source message IDs in PostgreSQL.
    client = clients[0]
    log.info("Starting/resuming source history scan...")

    # Telegram returns history newest -> oldest. Store every message ID.
    # Existing IDs are ignored by the primary key.
    inserted = 0
    async for msg in client.iter_messages(SOURCE, reverse=True):
        if msg is None:
            continue
        result = await pool.execute(
            """INSERT INTO messages(source_message_id)
               VALUES($1) ON CONFLICT(source_message_id) DO NOTHING""",
            msg.id,
        )
        if result.endswith("1"):
            inserted += 1

        if inserted and inserted % QUEUE_BATCH_SIZE == 0:
            log.info("Queued %s new source messages", inserted)

    log.info("History scan complete. Newly queued: %s", inserted)


async def claim_job(pool, account_no):
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """WITH candidate AS (
                 SELECT source_message_id
                 FROM messages
                 WHERE status='pending'
                   AND attempts < $2
                 ORDER BY source_message_id
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
               )
               UPDATE messages m
               SET status='processing',
                   account_no=$1,
                   attempts=attempts+1,
                   locked_at=NOW(),
                   updated_at=NOW()
               FROM candidate c
               WHERE m.source_message_id=c.source_message_id
               RETURNING m.source_message_id, m.attempts""",
            account_no,
            MAX_RETRIES,
        )
        return row


async def mark_completed(pool, source_id, destination_id, account_no):
    async with pool.acquire() as con:
        await con.execute(
            """UPDATE messages
               SET status='completed',
                   destination_message_id=$2,
                   account_no=$3,
                   locked_at=NULL,
                   completed_at=NOW(),
                   updated_at=NOW()
               WHERE source_message_id=$1""",
            source_id, destination_id, account_no,
        )


async def mark_pending(pool, source_id, error_text):
    async with pool.acquire() as con:
        await con.execute(
            """UPDATE messages
               SET status='pending',
                   locked_at=NULL,
                   last_error=$2,
                   updated_at=NOW()
               WHERE source_message_id=$1""",
            source_id, error_text[:4000],
        )


async def mark_failed(pool, source_id, error_text):
    async with pool.acquire() as con:
        await con.execute(
            """UPDATE messages
               SET status='failed',
                   locked_at=NULL,
                   last_error=$2,
                   updated_at=NOW()
               WHERE source_message_id=$1""",
            source_id, error_text[:4000],
        )


async def set_cooldown(pool, account_no, seconds, error_text):
    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    async with pool.acquire() as con:
        await con.execute(
            """UPDATE account_state
               SET cooldown_until=$2, last_error=$3, updated_at=NOW()
               WHERE account_no=$1""",
            account_no, until, error_text[:4000],
        )
    return until


async def clear_cooldown(pool, account_no):
    async with pool.acquire() as con:
        await con.execute(
            """UPDATE account_state
               SET cooldown_until=NULL, last_error=NULL, updated_at=NOW()
               WHERE account_no=$1""",
            account_no,
        )


async def is_on_cooldown(pool, account_no):
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT cooldown_until FROM account_state WHERE account_no=$1",
            account_no,
        )
    return row["cooldown_until"] and row["cooldown_until"] > datetime.now(timezone.utc)


async def worker(pool, client, account_no, worker_id):
    name = f"account-{account_no}/worker-{worker_id}"
    while True:
        if await is_on_cooldown(pool, account_no):
            await asyncio.sleep(2)
            continue

        job = await claim_job(pool, account_no)
        if not job:
            await asyncio.sleep(POLL_SECONDS)
            continue

        source_id = job["source_message_id"]
        try:
            # Direct forwarding: no download/re-upload.
            sent = await client.forward_messages(
                DESTINATION,
                source_id,
                from_peer=SOURCE,
            )
            if isinstance(sent, list):
                destination_id = sent[0].id if sent else None
            else:
                destination_id = sent.id

            await mark_completed(pool, source_id, destination_id, account_no)
            log.info("%s completed source=%s", name, source_id)

        except errors.FloodWaitError as e:
            await mark_pending(pool, source_id, f"FloodWait: {e.seconds}s")
            until = await set_cooldown(
                pool, account_no, e.seconds, f"FloodWait {e.seconds}s"
            )
            log.warning(
                "%s flood wait for %ss; unavailable until %s",
                name, e.seconds, until.isoformat()
            )
            await asyncio.sleep(1)

        except (errors.RPCError, asyncio.TimeoutError) as e:
            text = f"{type(e).__name__}: {e}"
            if job["attempts"] >= MAX_RETRIES:
                await mark_failed(pool, source_id, text)
                log.error("%s permanently failed source=%s: %s",
                          name, source_id, text)
            else:
                await mark_pending(pool, source_id, text)
                await asyncio.sleep(RETRY_DELAY)

        except Exception as e:
            text = f"{type(e).__name__}: {e}"
            if job["attempts"] >= MAX_RETRIES:
                await mark_failed(pool, source_id, text)
            else:
                await mark_pending(pool, source_id, text)
                await asyncio.sleep(RETRY_DELAY)
            log.exception("%s error on source=%s", name, source_id)


async def status_loop(pool):
    while True:
        try:
            async with pool.acquire() as con:
                row = await con.fetchrow(
                    """SELECT
                       COUNT(*) FILTER (WHERE status='completed') AS completed,
                       COUNT(*) FILTER (WHERE status='pending') AS pending,
                       COUNT(*) FILTER (WHERE status='processing') AS processing,
                       COUNT(*) FILTER (WHERE status='failed') AS failed,
                       COUNT(*) AS total
                       FROM messages"""
                )
            log.info(
                "STATUS total=%s completed=%s pending=%s processing=%s failed=%s",
                row["total"], row["completed"], row["pending"],
                row["processing"], row["failed"]
            )
        except Exception:
            log.exception("Status loop error")
        await asyncio.sleep(60)


async def monitor_new_messages(pool, client):
    # After the historical scan, keep adding new messages.
    last_seen = 0
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT COALESCE(MAX(source_message_id),0) AS max_id FROM messages"
        )
        last_seen = row["max_id"]

    while True:
        try:
            found = 0
            async for msg in client.iter_messages(
                SOURCE, min_id=last_seen, reverse=True
            ):
                if msg.id <= last_seen:
                    continue
                await pool.execute(
                    """INSERT INTO messages(source_message_id)
                       VALUES($1) ON CONFLICT DO NOTHING""",
                    msg.id,
                )
                last_seen = max(last_seen, msg.id)
                found += 1

            if found:
                log.info("Queued %s new message(s), latest=%s", found, last_seen)
        except Exception:
            log.exception("New-message monitor error")

        await asyncio.sleep(POLL_SECONDS)


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=20)
    await init_db(pool)
    await reset_stale_jobs(pool)

    clients = []
    for i, session in enumerate(SESSION_STRINGS, start=1):
        if not session.strip():
            raise RuntimeError(f"SESSION_{i} is empty")

        try:
            session_obj = StringSession(session.strip())
        except ValueError:
            raise RuntimeError(
                f"SESSION_{i} is NOT a valid Telethon StringSession"
            )

    client = TelegramClient(
        session_obj,
        api_id=API_ID,
        api_hash=API_HASH,
        sequential_updates=True,
    )
    await client.start()
    me = await client.get_me()
    log.info("Account %s connected: %s (%s)", i, me.first_name, me.id)
    clients.append(client)


await enqueue_history(pool, clients)

    tasks = []
    for i, client in enumerate(clients, start=1):
        for w in range(1, WORKERS_PER_ACCOUNT + 1):
            tasks.append(asyncio.create_task(worker(pool, client, i, w)))

    tasks.append(asyncio.create_task(status_loop(pool)))
    tasks.append(asyncio.create_task(monitor_new_messages(pool, clients[0])))

    try:
        await asyncio.gather(*tasks)
    finally:
        for client in clients:
            await client.disconnect()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
