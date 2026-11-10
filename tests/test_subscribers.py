import asyncio
import json
import sqlite3
from datetime import UTC, datetime

import httpx
import pytest

from app.config import Settings
from app.krisha import Listing
from app.store import Store
from app.tracker import Tracker


def flat(listing_id="123"):
    return Listing(
        id=listing_id,
        title="1-комнатная квартира · 32 м²",
        price=125000,
        area=32,
        address="Алматы, Абая 1",
        description="Отдельная квартира",
        url=f"https://krisha.kz/a/show/{listing_id}",
        has_photo=True,
        owner=True,
        monthly=True,
        city="Алматы",
    )


def update(chat_id, text):
    return {"message": {"chat": {"id": chat_id, "type": "private"}, "text": text}}


def sent(message_id=9):
    return {"message_id": message_id, "date": int(datetime.now(UTC).timestamp())}


@pytest.fixture
async def store(tmp_path):
    instance = Store(tmp_path / "subscribers.sqlite3")
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
def config(tmp_path):
    # An existing TELEGRAM_CHAT_ID no longer restricts other users.
    return Settings(
        _env_file=None,
        database_path=tmp_path / "subscribers.sqlite3",
        telegram_bot_token="test-token",
        telegram_chat_id=42,
    )


@pytest.fixture
def no_wait(monkeypatch):
    async def skip_sleep(seconds):
        pass

    monkeypatch.setattr("app.tracker.asyncio.sleep", skip_sleep)


async def test_two_users_get_welcome_and_the_same_existing_offer(store, config, no_wait):
    await store.record_scan([flat()], {"123"})
    requests = []

    def respond(request):
        payload = json.loads(request.content)
        requests.append(payload)
        return httpx.Response(200, json={"ok": True, "result": sent(len(requests))})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.handle_update(update(42, "/start"))
        await tracker.handle_update(update(7, "/start"))
        assert len(await store.pending(42)) == len(await store.pending(7)) == 1
        await tracker.deliver()
        await tracker.deliver()
    welcome_chats = [item["chat_id"] for item in requests if "Отслеживаю" in item["text"]]
    offers = [item["chat_id"] for item in requests if "reply_markup" in item]
    assert welcome_chats == [42, 7]
    assert sorted(offers) == [7, 42]
    assert await store.pending(42) == await store.pending(7) == []


async def test_unsubscribed_user_gets_help_without_receiving_alerts(store, config):
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": sent()})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await Tracker(config, store, client).handle_update(update(7, "/latest"))
    assert requests[0]["chat_id"] == 7
    assert "/start" in requests[0]["text"]
    assert await store.subscriber(7) is None


async def test_acknowledgement_is_per_subscriber(store):
    await store.subscribe(42)
    await store.subscribe(7)
    await store.record_scan([flat()], {"123"})
    await store.mark_sent("123", 42, sent())
    assert await store.pending(42) == []
    assert [item["id"] for item in await store.pending(7)] == ["123"]
    assert await store.record_scan([flat(), flat("124")], {"123", "124"}) == 1
    assert [item["id"] for item in await store.pending(42)] == ["124"]
    assert {item["id"] for item in await store.pending(7)} == {"123", "124"}


async def test_stop_and_resume_affect_only_sender(store, config, no_wait):
    await store.subscribe(42)
    await store.subscribe(7)
    await store.record_scan([flat()], {"123"})
    offers = []

    def respond(request):
        payload = json.loads(request.content)
        if "reply_markup" in payload:
            offers.append(payload["chat_id"])
        return httpx.Response(200, json={"ok": True, "result": sent()})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.handle_update(update(42, "/stop"))
        await tracker.deliver()
        assert offers == [7]
        await tracker.handle_update(update(42, "/start"))
        await tracker.deliver()
    assert offers == [7, 42]
    assert (await store.subscription_counts()) == {"total": 2, "active": 2}


async def test_repeated_start_does_not_repeat_old_alerts_after_restart(store, config, no_wait):
    await store.record_scan([flat()], {"123"})
    requests = []

    def respond(request):
        payload = json.loads(request.content)
        requests.append(payload)
        return httpx.Response(200, json={"ok": True, "result": sent(len(requests))})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.handle_update(update(7, "/start"))
        await tracker.deliver()
        restarted = Tracker(config, store, client)
        await restarted.handle_update(update(7, "/start"))
        await restarted.deliver()
    assert len([item for item in requests if "reply_markup" in item]) == 1


async def test_forbidden_recipient_does_not_block_other_users(store, config, no_wait):
    await store.subscribe(42)
    await store.subscribe(7)
    await store.record_scan([flat()], {"123"})
    offers = []

    def respond(request):
        payload = json.loads(request.content)
        if payload["chat_id"] == 42:
            return httpx.Response(403, json={"ok": False, "error_code": 403})
        offers.append(payload["chat_id"])
        return httpx.Response(200, json={"ok": True, "result": sent()})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.deliver()
        await tracker.deliver()
    assert offers == [7]
    assert (await store.subscriber(42))["blocked"] == 1
    assert (await store.subscriber(7))["paused"] == 0
    assert await store.pending(7) == []
    await store.subscribe(42)
    assert len(await store.pending(42)) == 1


async def test_transient_failure_is_isolated_between_recipients(store, config, no_wait):
    await store.subscribe(42)
    await store.subscribe(7)
    await store.record_scan([flat()], {"123"})
    offers = []

    def respond(request):
        payload = json.loads(request.content)
        if payload["chat_id"] == 42:
            return httpx.Response(500, json={"ok": False, "error_code": 500})
        offers.append(payload["chat_id"])
        return httpx.Response(200, json={"ok": True, "result": sent()})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await Tracker(config, store, client).deliver()
    assert offers == [7]
    assert len(await store.pending(42)) == 1
    assert (await store.subscriber(42))["blocked"] == 0
    assert await store.pending(7) == []


async def test_button_hides_and_deletes_only_own_cards(store, config, no_wait):
    await store.subscribe(42)
    await store.subscribe(7)
    await store.record_scan([flat()], {"123"})
    # Message IDs can coincide in different chats.
    await store.mark_sent("123", 42, sent(9))
    await store.mark_sent("123", 7, sent(9))
    removed_from = []

    def respond(request):
        payload = json.loads(request.content)
        if request.url.path.endswith("/deleteMessage"):
            removed_from.append(payload["chat_id"])
        return httpx.Response(200, json={"ok": True, "result": True})

    query = {
        "id": "click",
        "from": {"id": 42},
        "data": "inactive:123",
        "message": {"message_id": 9, "chat": {"id": 42, "type": "private"}},
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.handle_update({"callback_query": query})
        await store.record_scan([flat()], {"123"})
        assert await store.latest(chat_id=42) == []
        assert len(await store.latest(chat_id=7)) == 1
        assert len(await store.latest()) == 1
        await tracker.deliver()
        assert removed_from == [42]
        assert (await tracker.status(42))["counts"]["inactive"] == 1
        assert (await tracker.status(7))["counts"]["inactive"] == 0
        # A confirmed closure from Krisha still applies to everyone.
        await store.set_availability("123", "removed")
        await tracker.deliver()
    assert removed_from == [42, 7]
    assert await store.latest(chat_id=7) == []


async def test_other_subscriber_cannot_use_someone_elses_message(store, config):
    await store.subscribe(42)
    await store.subscribe(7)
    await store.record_scan([flat()], {"123"})
    await store.mark_sent("123", 42, sent(9))

    def unexpected_request(request):
        pytest.fail("A forged callback must not trigger any Telegram API call")

    query = {
        "id": "forged",
        "from": {"id": 7},
        "data": "inactive:123",
        "message": {"message_id": 9, "chat": {"id": 7, "type": "private"}},
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected_request)) as client:
        await Tracker(config, store, client).handle_update({"callback_query": query})
    assert len(await store.latest(chat_id=42)) == len(await store.latest(chat_id=7)) == 1


async def test_legacy_queue_pause_and_hidden_items_migrate_once(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    # Construct the actual pre-subscriptions schema and history.
    timestamp = datetime.now(UTC).isoformat()
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE listings (id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                matched INTEGER NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL);
            CREATE TABLE outbox (listing_id TEXT PRIMARY KEY REFERENCES listings(id), sent_at TEXT);
            CREATE TABLE listing_checks (listing_id TEXT PRIMARY KEY REFERENCES listings(id),
                checked_at TEXT NOT NULL, inactive_reason TEXT);
        """)
        db.executemany(
            "INSERT INTO meta VALUES (?, ?)", [("chat_id", "42"), ("paused", "1"), ("initialized", "1")]
        )
        for listing_id in ("123", "124"):
            db.execute(
                "INSERT INTO listings VALUES (?, ?, 1, ?, ?)",
                (listing_id, json.dumps(flat(listing_id).to_dict()), timestamp, timestamp),
            )
        db.execute("INSERT INTO outbox VALUES ('123', ?)", (timestamp,))
        db.execute("INSERT INTO outbox VALUES ('124', NULL)")
        db.execute("INSERT INTO listing_checks VALUES ('123', ?, 'manual')", (timestamp,))
    migrated = Store(path)
    await migrated.open()
    try:
        assert (await migrated.subscriber(42))["paused"] == 1
        await migrated.subscribe(7)
        await migrated.record_scan([flat(), flat("124")], {"123", "124"})
        assert [item["id"] for item in await migrated.latest(chat_id=42)] == ["124"]
        assert {item["id"] for item in await migrated.latest(chat_id=7)} == {"123", "124"}
        await migrated.subscribe(42)
        assert [item["id"] for item in await migrated.pending(42)] == ["124"]
        await migrated.pause_subscriber(42)
    finally:
        await migrated.close()
    restarted = Store(path)
    await restarted.open(42)
    try:
        assert (await restarted.subscriber(42))["paused"] == 1
        assert await restarted.subscription_counts() == {"total": 2, "active": 1}
    finally:
        await restarted.close()


async def test_simultaneous_subscriptions_and_scan_do_not_lose_alerts(store):
    await asyncio.gather(store.subscribe(42), store.subscribe(7), store.record_scan([flat()], {"123"}))
    assert len(await store.pending(42)) == len(await store.pending(7)) == 1


async def test_blocked_command_reply_does_not_swallow_next_users_start(store, config, no_wait):
    replies = []
    batches = 0

    def respond(request):
        nonlocal batches
        if request.url.path.endswith("/getUpdates"):
            batches += 1
            if batches > 1:
                raise asyncio.CancelledError
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": [
                        {"update_id": 1, **update(42, "/start")},
                        {"update_id": 2, **update(7, "/start")},
                    ],
                },
            )
        payload = json.loads(request.content)
        if payload["chat_id"] == 42:
            return httpx.Response(403, json={"ok": False, "error_code": 403})
        replies.append(payload["chat_id"])
        return httpx.Response(200, json={"ok": True, "result": sent()})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        with pytest.raises(asyncio.CancelledError):
            await tracker.poll_telegram()
    assert replies == [7]
    assert await store.get_meta("telegram_offset") == "3"
    assert (await store.subscriber(42))["blocked"] == 1
    assert (await store.subscriber(7))["paused"] == 0


def test_bot_token_no_longer_requires_private_access_code(tmp_path):
    config = Settings(_env_file=None, telegram_bot_token="test-token")
    assert config.telegram_chat_id is None
