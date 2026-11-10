import asyncio
import json
import logging
import secrets
from datetime import UTC, datetime, timedelta

import httpx

from app.config import Settings
from app.krisha import SourceError, check_availability, fetch_listings, is_match
from app.store import Store, now
from app.telegram import Telegram, TelegramError, listing_message

logger = logging.getLogger(__name__)


class Tracker:
    def __init__(self, settings: Settings, store: Store, client: httpx.AsyncClient):
        self.settings = settings
        self.store = store
        token = settings.telegram_bot_token.get_secret_value()
        self.telegram = Telegram(client, token) if token else None
        self.client = client
        self.scan_lock = asyncio.Lock()
        self.last_error: str | None = None
        self.telegram_error: str | None = None
        self.delivery_error: str | None = None
        self.availability_error: str | None = None
        self.availability_retry_at = 0.0
        self.last_success_at: str | None = None
        self.last_count = 0
        self.delivery_retry_at = 0.0

    async def chat_id(self) -> int | None:
        saved = await self.store.get_meta("chat_id")
        return self.settings.telegram_chat_id or (int(saved) if saved else None)

    async def scan(self):
        async with self.scan_lock:
            try:
                listings = await fetch_listings(self.client, self.settings)
                matches = {item.id for item in listings if is_match(item, self.settings)}
                queued = await self.store.record_scan(listings, matches)
                self.last_count = len(matches)
                self.last_success_at = now()
                self.last_error = None
                logger.info(
                    "Scan completed: %s cards, %s matches, %s queued", len(listings), len(matches), queued
                )
                return {"scanned": len(listings), "matched": len(matches), "queued": queued}
            except SourceError as exc:
                self.last_error = str(exc)
                logger.warning("%s", self.last_error)
                raise

    async def deliver(self):
        chat_id = await self.chat_id()
        if not self.telegram or not chat_id:
            return
        loop = asyncio.get_running_loop()
        if loop.time() < self.delivery_retry_at:
            return
        # Cleaning already-sent cards continues even while new alerts are paused.
        for message in await self.store.messages_to_clean(chat_id):
            try:
                old = datetime.now(UTC) - datetime.fromisoformat(message["sent_at"]) >= timedelta(hours=48)
                await self.telegram.remove_listing_message(
                    chat_id, message["message_id"], json.loads(message["payload"]), old=old
                )
            except TelegramError as exc:
                self.delivery_error = str(exc)
                self.delivery_retry_at = loop.time() + max(exc.retry_after, 30)
                logger.warning("Message cleanup deferred: %s", exc)
                return
            await self.store.mark_message_cleaned(chat_id, message["message_id"])
            self.delivery_error = None
            await asyncio.sleep(1.1)
        if await self.store.get_meta("paused") == "1":
            return
        for listing in await self.store.pending():
            try:
                message = await self.telegram.send(
                    chat_id, listing_message(listing), listing_id=listing["id"]
                )
            except TelegramError as exc:
                self.delivery_error = str(exc)
                self.delivery_retry_at = loop.time() + max(exc.retry_after, 30)
                logger.warning("Delivery deferred: %s", exc)
                break
            # Mark only after Telegram acknowledges receipt. A crash between these
            # steps can repeat a message; Telegram has no idempotency key.
            await self.store.mark_sent(listing["id"], chat_id, message)
            self.delivery_error = None
            await asyncio.sleep(1.1)

    async def watch(self):
        failures = 0
        while True:
            try:
                await self.scan()
                failures = 0
            except SourceError:
                failures += 1
            delay = min(self.settings.poll_interval_seconds * 2 ** min(failures, 4), 900)
            await asyncio.sleep(delay)

    async def delivery_loop(self):
        while True:
            await self.deliver()
            await asyncio.sleep(2)

    async def recheck_availability(self):
        loop = asyncio.get_running_loop()
        if loop.time() < self.availability_retry_at:
            return
        candidates = await self.store.availability_candidates(
            self.settings.availability_interval_seconds, self.settings.availability_batch_size
        )
        for index, listing_id in enumerate(candidates):
            try:
                state = await check_availability(self.client, listing_id)
            except SourceError as exc:
                # Rotate past an unrecognizable advert after the backoff; it must
                # not permanently prevent checking every other apartment.
                await self.store.set_availability(listing_id, "unknown")
                self.availability_error = str(exc)
                self.availability_retry_at = loop.time() + self.settings.availability_interval_seconds
                logger.warning("%s", exc)
                # One blocked request is enough: stop this batch, keep all cards.
                return
            await self.store.set_availability(listing_id, state)
            if index + 1 < len(candidates):
                await asyncio.sleep(1)
        self.availability_error = None

    async def availability_loop(self):
        while True:
            await asyncio.sleep(self.settings.poll_interval_seconds)
            await self.recheck_availability()

    async def status(self):
        return {
            "search": {
                "city": "Алматы",
                "rooms": 1,
                "min_price": self.settings.min_price,
                "max_price": self.settings.max_price,
                "min_area": self.settings.min_area,
                "require_photo": self.settings.require_photo,
                "owners_only": self.settings.owners_only,
            },
            "poll_interval_seconds": self.settings.poll_interval_seconds,
            "availability_interval_seconds": self.settings.availability_interval_seconds,
            "telegram_configured": self.telegram is not None,
            "chat_connected": await self.chat_id() is not None,
            "paused": await self.store.get_meta("paused") == "1",
            "last_success_at": self.last_success_at or await self.store.get_meta("last_scan_at") or None,
            "source_error": self.last_error,
            "availability_error": self.availability_error,
            "telegram_error": self.telegram_error or self.delivery_error,
            "counts": await self.store.counts(),
        }

    async def handle_update(self, update: dict):
        if "callback_query" in update:
            await self.handle_callback(update["callback_query"])
            return
        message = update.get("message", {})
        chat = message.get("chat", {})
        # Pair only a private chat and never answer unrelated users.
        if chat.get("type") != "private" or not isinstance(chat.get("id"), int):
            return
        incoming_id = chat["id"]
        text = message.get("text", "").strip()
        parts = text.split(maxsplit=1)
        command = parts[0].split("@")[0] if parts else ""
        connected = await self.chat_id()
        if connected is None:
            code = parts[1] if len(parts) > 1 else ""
            expected = self.settings.bot_access_code.get_secret_value()
            if (
                command != "/start"
                or not expected
                or not secrets.compare_digest(
                    code.encode(),
                    expected.encode(),
                )
            ):
                return
            await self.store.set_meta("chat_id", str(incoming_id))
        elif connected != incoming_id:
            return

        if command == "/start":
            await self.store.set_meta("paused", "0")
            await self.telegram.send(
                incoming_id,
                "Отслеживаю однокомнатные квартиры в Алматы за "
                f"{self.settings.min_price:,}–{self.settings.max_price:,} ₸/месяц.\n"
                f"Проверка каждые {self.settings.poll_interval_seconds} сек.\n"
                "/latest — последние варианты\n/status — состояние\n/stop — пауза\n"
                "Кнопка «Уже сдали» убирает неактуальный вариант.",
            )
        elif command == "/stop":
            await self.store.set_meta("paused", "1")
            await self.telegram.send(incoming_id, "Уведомления приостановлены. /start — продолжить.")
        elif command == "/latest":
            listings = await self.store.latest()
            if not listings:
                await self.telegram.send(
                    incoming_id, "Подходящих вариантов пока нет. /status — состояние поиска."
                )
            for listing in listings:
                message = await self.telegram.send(
                    incoming_id, listing_message(listing), listing_id=listing["id"]
                )
                await self.store.remember_message(listing["id"], incoming_id, message)
                await asyncio.sleep(1.1)
        elif command == "/status":
            status = await self.status()
            state = "Ошибка загрузки Krisha" if self.last_error else "Поиск работает"
            if not status["last_success_at"]:
                state = "Ожидаю первую успешную проверку"
            await self.telegram.send(
                incoming_id,
                f"{state}\nПоследняя проверка: {status['last_success_at'] or 'ещё не было'}\n"
                f"Подходящих в последней проверке: {status['counts']['matched']}\n"
                f"Убрано неактуальных: {status['counts']['inactive']}\n"
                f"Уведомления: {'пауза' if status['paused'] else 'включены'}"
                + (
                    "\nПроверка доступности объявлений временно недоступна."
                    if self.availability_error
                    else ""
                ),
            )

    async def handle_callback(self, query: dict):
        chat_id = await self.chat_id()
        message = query.get("message", {})
        chat = message.get("chat", {})
        if (
            not chat_id
            or query.get("from", {}).get("id") != chat_id
            or chat.get("type") != "private"
            or chat.get("id") != chat_id
        ):
            return
        action, _, listing_id = query.get("data", "").partition(":")
        if action != "inactive" or not listing_id.isascii() or not listing_id.isdigit():
            return
        if not await self.store.has_message(listing_id, chat_id, message.get("message_id")):
            return
        await self.store.set_availability(listing_id, "manual")
        await self.telegram.call(
            "answerCallbackQuery", callback_query_id=query["id"], text="Убираю этот вариант."
        )

    async def poll_telegram(self):
        offset = int(await self.store.get_meta("telegram_offset", "0"))
        while True:
            try:
                updates = await self.telegram.call(
                    "getUpdates", offset=offset, timeout=25, allowed_updates=["message", "callback_query"]
                )
                for update in updates:
                    await self.handle_update(update)
                    offset = update["update_id"] + 1
                    await self.store.set_meta("telegram_offset", str(offset))
                self.telegram_error = None
            except TelegramError as exc:
                self.telegram_error = str(exc)
                logger.warning("Telegram polling failed: %s", exc)
                await asyncio.sleep(max(exc.retry_after, 10))
