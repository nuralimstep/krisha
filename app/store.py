import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite

from app.krisha import Listing, has_rented_notice


def now() -> str:
    return datetime.now(UTC).isoformat()


class Store:
    def __init__(self, path: Path):
        self.path = path
        self.db: aiosqlite.Connection | None = None
        self.write_lock = asyncio.Lock()

    async def open(self, legacy_chat_id: int | None = None):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS listings (
                id TEXT PRIMARY KEY, payload TEXT NOT NULL, matched INTEGER NOT NULL,
                first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                listing_id TEXT PRIMARY KEY REFERENCES listings(id),
                sent_at TEXT
            );
            CREATE TABLE IF NOT EXISTS listing_checks (
                listing_id TEXT PRIMARY KEY REFERENCES listings(id),
                checked_at TEXT NOT NULL, inactive_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS messages (
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                listing_id TEXT NOT NULL REFERENCES listings(id),
                sent_at TEXT NOT NULL, cleaned_at TEXT,
                PRIMARY KEY (chat_id, message_id)
            );
            CREATE INDEX IF NOT EXISTS messages_listing_id ON messages(listing_id);
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY CHECK (chat_id > 0),
                paused INTEGER NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                chat_id INTEGER NOT NULL REFERENCES subscribers(chat_id),
                listing_id TEXT NOT NULL REFERENCES listings(id), sent_at TEXT,
                PRIMARY KEY (chat_id, listing_id)
            );
            CREATE TABLE IF NOT EXISTS hidden_listings (
                chat_id INTEGER NOT NULL REFERENCES subscribers(chat_id),
                listing_id TEXT NOT NULL REFERENCES listings(id), hidden_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, listing_id)
            );
        """)
        await self._migrate_subscriptions(legacy_chat_id)

    async def _migrate_subscriptions(self, legacy_chat_id: int | None):
        if await self.get_meta("subscriptions_migrated"):
            return
        saved = await self.get_meta("chat_id")
        owner = legacy_chat_id or (int(saved) if saved else None)
        async with self.write_lock:
            try:
                if owner:
                    paused = await self.get_meta("paused") == "1"
                    await self.db.execute(
                        "INSERT OR IGNORE INTO subscribers VALUES (?, ?, 0, ?)", (owner, paused, now())
                    )
                    await self.db.execute(
                        "INSERT OR IGNORE INTO deliveries SELECT ?, listing_id, sent_at FROM outbox", (owner,)
                    )
                    await self.db.execute(
                        """INSERT OR IGNORE INTO hidden_listings
                        SELECT ?, listing_id, checked_at FROM listing_checks WHERE inactive_reason='manual'""",
                        (owner,),
                    )
                # Reports made by the old single owner must not hide apartments for everyone.
                await self.db.execute(
                    "UPDATE listing_checks SET inactive_reason=NULL WHERE inactive_reason='manual'"
                )
                await self.db.execute("INSERT INTO meta VALUES ('subscriptions_migrated', '1')")
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise

    async def subscribe(self, chat_id: int):
        if type(chat_id) is not int or chat_id <= 0:
            raise ValueError("A private Telegram chat ID is required")
        async with self.write_lock:
            try:
                cursor = await self.db.execute(
                    "INSERT OR IGNORE INTO subscribers VALUES (?, 0, 0, ?)", (chat_id, now())
                )
                if cursor.rowcount:
                    # A new user receives up to five current offers, even if another
                    # subscriber has already received every one of them.
                    await self.db.execute(
                        """INSERT INTO deliveries (chat_id, listing_id)
                        SELECT ?, id FROM listings WHERE matched=1
                        ORDER BY last_seen DESC, first_seen DESC LIMIT 5""",
                        (chat_id,),
                    )
                else:
                    await self.db.execute(
                        "UPDATE subscribers SET paused=0, blocked=0 WHERE chat_id=?", (chat_id,)
                    )
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise

    async def subscriber(self, chat_id: int) -> dict | None:
        async with self.db.execute("SELECT * FROM subscribers WHERE chat_id=?", (chat_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def subscribers(self) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM subscribers WHERE blocked=0 ORDER BY created_at, chat_id"
        ) as cur:
            return [dict(row) for row in await cur.fetchall()]

    async def pause_subscriber(self, chat_id: int, *, blocked: bool = False):
        async with self.write_lock:
            await self.db.execute(
                "UPDATE subscribers SET paused=1, blocked=? WHERE chat_id=?", (blocked, chat_id)
            )
            await self.db.commit()

    async def subscription_counts(self) -> dict:
        async with self.db.execute(
            """SELECT COUNT(*) AS total,
            COALESCE(SUM(paused=0 AND blocked=0), 0) AS active FROM subscribers"""
        ) as cur:
            return dict(await cur.fetchone())

    async def close(self):
        if self.db:
            await self.db.close()

    async def get_meta(self, key: str, default: str = "") -> str:
        async with self.db.execute("SELECT value FROM meta WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
        return row[0] if row else default

    async def set_meta(self, key: str, value: str):
        async with self.write_lock:
            await self.db.execute(
                "INSERT INTO meta VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            await self.db.commit()

    async def record_scan(self, listings: list[Listing], matches: set[str]) -> int:
        async with self.write_lock:
            try:
                return await self._record_scan(listings, matches)
            except BaseException:
                await self.db.rollback()
                raise

    async def _record_scan(self, listings: list[Listing], matches: set[str]) -> int:
        initial = not await self.get_meta("initialized")
        queued = 0
        timestamp = now()
        async with self.db.execute(
            "SELECT listing_id FROM listing_checks WHERE inactive_reason IS NOT NULL"
        ) as cur:
            inactive_ids = {row[0] for row in await cur.fetchall()}
        # /latest and the delivery queue reflect the most recent successful scan.
        await self.db.execute("UPDATE listings SET matched=0 WHERE matched=1")
        # Only five initial alerts; later every newly matching listing is queued.
        for listing in listings:
            rented = has_rented_notice(listing.description)
            matched = listing.id in matches and listing.id not in inactive_ids and not rented
            await self.db.execute(
                """
                INSERT INTO listings VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,
                    matched=excluded.matched, last_seen=excluded.last_seen
            """,
                (
                    listing.id,
                    json.dumps(listing.to_dict(), ensure_ascii=False),
                    matched,
                    timestamp,
                    timestamp,
                ),
            )
            if rented:
                await self._set_availability(listing.id, "rented")
            if matched:
                # The remaining initial matches are recorded as seen, without flooding the chat.
                sent_at = timestamp if initial and queued >= 5 else None
                cursor = await self.db.execute(
                    "INSERT OR IGNORE INTO outbox VALUES (?, ?)",
                    (listing.id, sent_at),
                )
                if cursor.rowcount and sent_at is None:
                    queued += 1
                    await self.db.execute(
                        """INSERT OR IGNORE INTO deliveries (chat_id, listing_id)
                        SELECT s.chat_id, ? FROM subscribers s WHERE NOT EXISTS (
                            SELECT 1 FROM hidden_listings h WHERE h.chat_id=s.chat_id AND h.listing_id=?
                        )""",
                        (listing.id, listing.id),
                    )
        await self.db.execute(
            "INSERT OR REPLACE INTO meta VALUES ('initialized', '1')",
        )
        await self.db.execute(
            "INSERT OR REPLACE INTO meta VALUES ('last_scan_at', ?)",
            (timestamp,),
        )
        await self.db.commit()
        return queued

    async def latest(self, limit: int = 5, *, chat_id: int | None = None) -> list[dict]:
        async with self.db.execute(
            """SELECT l.payload FROM listings l WHERE l.matched=1 AND NOT EXISTS (
                SELECT 1 FROM hidden_listings h WHERE h.listing_id=l.id AND h.chat_id=?
            ) ORDER BY l.last_seen DESC, l.first_seen DESC LIMIT ?""",
            (chat_id, limit),
        ) as cur:
            return [json.loads(row[0]) for row in await cur.fetchall()]

    async def pending(self, chat_id: int, limit: int = 30) -> list[dict]:
        async with self.db.execute(
            """SELECT l.payload FROM deliveries d JOIN listings l ON l.id=d.listing_id
            JOIN subscribers s ON s.chat_id=d.chat_id
            WHERE d.sent_at IS NULL AND l.matched=1 AND d.chat_id=? AND s.paused=0 AND s.blocked=0
              AND NOT EXISTS (SELECT 1 FROM hidden_listings h WHERE h.chat_id=d.chat_id AND h.listing_id=l.id)
            ORDER BY l.first_seen LIMIT ?""",
            (chat_id, limit),
        ) as cur:
            return [json.loads(row[0]) for row in await cur.fetchall()]

    async def mark_sent(self, listing_id: str, chat_id: int, message: dict | None = None):
        async with self.write_lock:
            if message is not None:
                await self._remember_message(listing_id, chat_id, message)
            await self.db.execute(
                "UPDATE deliveries SET sent_at=? WHERE listing_id=? AND chat_id=?",
                (now(), listing_id, chat_id),
            )
            await self.db.commit()

    async def hide_listing(self, chat_id: int, listing_id: str):
        async with self.write_lock:
            await self.db.execute(
                "INSERT OR IGNORE INTO hidden_listings VALUES (?, ?, ?)", (chat_id, listing_id, now())
            )
            await self.db.commit()

    async def _remember_message(self, listing_id: str, chat_id: int, message: dict):
        sent_at = datetime.fromtimestamp(message["date"], UTC).isoformat()
        await self.db.execute(
            "INSERT OR IGNORE INTO messages VALUES (?, ?, ?, ?, NULL)",
            (chat_id, message["message_id"], listing_id, sent_at),
        )

    async def remember_message(self, listing_id: str, chat_id: int, message: dict):
        async with self.write_lock:
            await self._remember_message(listing_id, chat_id, message)
            await self.db.commit()

    async def has_message(self, listing_id: str, chat_id: int, message_id: int) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM messages WHERE listing_id=? AND chat_id=? AND message_id=?",
            (listing_id, chat_id, message_id),
        ) as cur:
            return await cur.fetchone() is not None

    async def availability_candidates(self, interval_seconds: int, limit: int) -> list[str]:
        cutoff = (datetime.now(UTC) - timedelta(seconds=interval_seconds)).isoformat()
        async with self.db.execute(
            """
            SELECT l.id FROM listings l LEFT JOIN listing_checks c ON c.listing_id=l.id
            WHERE c.inactive_reason IS NULL AND (c.checked_at IS NULL OR c.checked_at<=?)
              AND (l.matched=1 OR EXISTS (
                  SELECT 1 FROM messages m WHERE m.listing_id=l.id AND m.cleaned_at IS NULL
              ))
            ORDER BY COALESCE(c.checked_at, l.first_seen), l.id LIMIT ?
            """,
            (cutoff, limit),
        ) as cur:
            return [row[0] for row in await cur.fetchall()]

    async def _set_availability(self, listing_id: str, state: str):
        reason = None if state in {"active", "unknown"} else state
        await self.db.execute(
            """
            INSERT INTO listing_checks VALUES (?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET checked_at=excluded.checked_at,
                inactive_reason=COALESCE(listing_checks.inactive_reason, excluded.inactive_reason)
            """,
            (listing_id, now(), reason),
        )
        if reason:
            await self.db.execute("UPDATE listings SET matched=0 WHERE id=?", (listing_id,))

    async def set_availability(self, listing_id: str, state: str):
        if state not in {"active", "unknown", "archived", "removed", "rented"}:
            raise ValueError("Unknown availability state")
        async with self.write_lock:
            await self._set_availability(listing_id, state)
            await self.db.commit()

    async def messages_to_clean(self, chat_id: int) -> list[dict]:
        async with self.db.execute(
            """
            SELECT m.chat_id, m.message_id, m.sent_at, l.payload FROM messages m
            LEFT JOIN listing_checks c ON c.listing_id=m.listing_id
            LEFT JOIN hidden_listings h ON h.listing_id=m.listing_id AND h.chat_id=m.chat_id
            JOIN listings l ON l.id=m.listing_id
            WHERE m.cleaned_at IS NULL AND m.chat_id=?
              AND (c.inactive_reason IS NOT NULL OR h.listing_id IS NOT NULL)
            ORDER BY m.sent_at LIMIT 30
            """,
            (chat_id,),
        ) as cur:
            return [dict(row) for row in await cur.fetchall()]

    async def mark_message_cleaned(self, chat_id: int, message_id: int):
        async with self.write_lock:
            await self.db.execute(
                "UPDATE messages SET cleaned_at=? WHERE chat_id=? AND message_id=?",
                (now(), chat_id, message_id),
            )
            await self.db.commit()

    async def counts(self, chat_id: int | None = None) -> dict:
        async with self.db.execute(
            """SELECT COUNT(*) AS seen, COALESCE(SUM(matched=1 AND NOT EXISTS (
                SELECT 1 FROM hidden_listings h WHERE h.listing_id=l.id AND h.chat_id=?
            )), 0) AS matched FROM listings l""",
            (chat_id,),
        ) as cur:
            row = await cur.fetchone()
        counts = dict(row)
        async with self.db.execute(
            """SELECT COUNT(*) FROM listings l WHERE EXISTS (
                SELECT 1 FROM listing_checks c WHERE c.listing_id=l.id AND c.inactive_reason IS NOT NULL
            ) OR EXISTS (
                SELECT 1 FROM hidden_listings h WHERE h.listing_id=l.id AND h.chat_id=?
            )""",
            (chat_id,),
        ) as cur:
            counts["inactive"] = (await cur.fetchone())[0]
        return counts
