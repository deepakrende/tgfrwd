# Telegram 5-Account Forwarder

Telethon userbot for migrating/forwarding authorized channel messages to another channel.

## Features

- 5 Telethon StringSession accounts
- PostgreSQL persistent queue/state
- Resumes after restart/redeploy
- Per-account FloodWait cooldown
- If all accounts are waiting, pending jobs remain safely queued until an account is available
- Duplicate protection using source message IDs
- Historical backfill
- Continuous monitoring for new messages
- Docker + Render worker configuration

## GitHub

Upload all files in this repository. Do **not** upload `.env` or session files.

## Render

1. Create a PostgreSQL database and obtain its connection string.
2. Create a Render **Background Worker** from this GitHub repository.
3. Set the environment variables listed in `render.yaml`.
4. Deploy.

You can also use the included `render.yaml` as a Blueprint configuration.

## Environment variables

Required:

- `API_ID`
- `API_HASH`
- `SOURCE_CHANNEL`
- `DESTINATION_CHANNEL`
- `DATABASE_URL`
- `SESSION_1`
- `SESSION_2`
- `SESSION_3`
- `SESSION_4`
- `SESSION_5`

Optional defaults are already included in `render.yaml`.

## Session strings

`SESSION_1` through `SESSION_5` must be Telethon StringSession values. Keep them private. Never commit them to GitHub.

## Important

Use this only with channels/content you are authorized to copy. Telegram limits are respected: a FloodWait pauses the affected account for the duration returned by Telegram rather than attempting to bypass the restriction.
