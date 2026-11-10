import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.config import Settings
from app.krisha import Listing, SourceError, check_availability, has_rented_notice, parse_availability
from app.store import Store
from app.telegram import Telegram, TelegramError
from app.tracker import Tracker


def flat(description="Отдельная квартира с мебелью"):
    return Listing(
        id="123",
        title="1-комнатная квартира · 32 м²",
        price=125000,
        area=32,
        address="Алматы, Абая 1",
        description=description,
        url="https://krisha.kz/a/show/123",
        has_photo=True,
        owner=True,
        monthly=True,
        city="Алматы",
    )


def card(body="", description="Квартира свободна"):
    return (
        '<link rel="canonical" href="https://krisha.kz/a/show/123">'
        "<h1>1-комнатная квартира · 32 м²</h1>"
        f'<div class="offer__description">{description}</div>{body}'
    )


def sent(message_id=9, age_hours=0):
    return {
        "message_id": message_id,
        "date": int((datetime.now(UTC) - timedelta(hours=age_hours)).timestamp()),
    }


@pytest.fixture
async def store(tmp_path):
    instance = Store(tmp_path / "test.sqlite3")
    await instance.open()
    await instance.record_scan([flat()], {"123"})
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
def config(tmp_path):
    return Settings(
        _env_file=None,
        database_path=tmp_path / "test.sqlite3",
        telegram_bot_token="test-token",
        telegram_chat_id=42,
    )


@pytest.fixture
def no_wait(monkeypatch):
    async def skip_sleep(seconds):
        pass

    monkeypatch.setattr("app.tracker.asyncio.sleep", skip_sleep)


@pytest.mark.parametrize(
    "text",
    [
        "Квартира уже сдана!",
        "Сдана. Спасибо!",
        "Уже заселились.",
        "Жильцы найдены",
        "Объявление не актуально",
        "Квартира заселена",
        "Описание. Квартира сдана, не звонить.",
    ],
)
def test_explicit_rented_notices(text):
    assert has_rented_notice(text)


@pytest.mark.parametrize(
    "text",
    [
        "Квартира не сдана",
        "Дом сдан в эксплуатацию",
        "Сдана в эксплуатацию в 2024 году",
        "Квартира сдана? Позвоните уточнить.",
        "Если квартира сдана, посмотрите другие варианты",
        "Можно заселиться сразу",
        "Без подселения",
        "Квартира будет сдана семье",
    ],
)
def test_mentions_questions_and_negations_do_not_close_listing(text):
    assert not has_rented_notice(text)


def test_detail_parser_distinguishes_active_archived_and_rented():
    assert parse_availability(card(), "123") == "active"
    archived = "<div><span>В архиве</span><p>Объявление может быть неактуальным.</p></div>"
    assert parse_availability(card(body=archived), "123") == "archived"
    assert parse_availability(card(description="Квартира уже сдана"), "123") == "rented"
    # A neighbouring archived advert is not a status of the requested apartment.
    assert parse_availability(card(body=f'<div class="a-card">{archived}</div>'), "123") == "active"


@pytest.mark.parametrize(
    "html",
    [
        "<h1>Please complete CAPTCHA</h1>",
        card().replace("/show/123", "/show/999"),
        card().replace("1-комнатная квартира · 32 м²", ""),
    ],
)
def test_unrecognized_page_is_unknown_and_not_removed(html):
    with pytest.raises(SourceError):
        parse_availability(html, "123")


@pytest.mark.parametrize("status_code", [404, 410])
async def test_removed_http_status(status_code):
    transport = httpx.MockTransport(lambda request: httpx.Response(status_code))
    async with httpx.AsyncClient(transport=transport) as client:
        assert await check_availability(client, "123") == "removed"


@pytest.mark.parametrize("status_code", [403, 429, 500, 503])
async def test_temporary_failures_never_mean_removed(status_code):
    transport = httpx.MockTransport(lambda request: httpx.Response(status_code))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(SourceError):
            await check_availability(client, "123")


async def test_redirect_to_search_is_not_removal():
    def respond(request):
        if request.url.path.startswith("/a/show/"):
            return httpx.Response(302, headers={"Location": "https://krisha.kz/arenda/kvartiry/"})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True) as client:
        with pytest.raises(SourceError, match="redirected"):
            await check_availability(client, "123")


async def test_migrates_original_database_without_losing_queue(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE listings (id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                matched INTEGER NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL);
            CREATE TABLE outbox (listing_id TEXT PRIMARY KEY REFERENCES listings(id), sent_at TEXT);
        """)
        timestamp = datetime.now(UTC).isoformat()
        db.execute(
            "INSERT INTO listings VALUES (?, ?, 1, ?, ?)",
            ("123", json.dumps(flat().to_dict()), timestamp, timestamp),
        )
        db.execute("INSERT INTO outbox VALUES ('123', NULL)")
    migrated = Store(path)
    await migrated.open()
    try:
        assert [item["id"] for item in await migrated.pending()] == ["123"]
        assert await migrated.messages_to_clean(42) == []
        await migrated.set_availability("123", "removed")
        assert await migrated.pending() == []
    finally:
        await migrated.close()


async def test_manual_inactive_survives_scans_and_restart(tmp_path):
    path = tmp_path / "persist.sqlite3"
    first = Store(path)
    await first.open()
    await first.record_scan([flat()], {"123"})
    await first.mark_sent("123", 42, sent())
    await first.set_availability("123", "manual")
    await first.close()
    second = Store(path)
    await second.open()
    try:
        assert await second.record_scan([flat()], {"123"}) == 0
        assert await second.latest() == []
        assert await second.pending() == []
        assert len(await second.messages_to_clean(42)) == 1
        assert await second.availability_candidates(300, 20) == []
        # A stale in-flight request cannot undo an explicit removal.
        await second.set_availability("123", "active")
        assert await second.latest() == []
        assert (await second.counts())["inactive"] == 1
    finally:
        await second.close()


async def test_preview_rented_notice_queues_cleanup(store):
    await store.mark_sent("123", 42, sent())
    await store.record_scan([flat(description="Квартира уже сдана")], set())
    assert await store.latest() == []
    assert len(await store.messages_to_clean(42)) == 1


async def test_falling_off_first_pages_does_not_delete_sent_message(store, config, no_wait):
    await store.mark_sent("123", 42, sent())
    await store.record_scan([], set())
    assert await store.availability_candidates(300, 20) == ["123"]
    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=card()))
    async with httpx.AsyncClient(transport=transport) as client:
        await Tracker(config, store, client).recheck_availability()
    assert await store.messages_to_clean(42) == []
    assert (await store.counts())["inactive"] == 0
    assert await store.availability_candidates(300, 20) == []


async def test_blocked_recheck_keeps_cards_and_backs_off(store, config, no_wait):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(403)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.recheck_availability()
        await tracker.recheck_availability()
        assert tracker.availability_error
    assert len(calls) == 1
    assert len(await store.latest()) == 1
    assert await store.messages_to_clean(42) == []


async def test_unrecognized_ad_does_not_permanently_block_other_apartments(store, config, no_wait):
    await store.record_scan([flat(), replace(flat(), id="124")], {"123", "124"})
    checked = []

    def respond(request):
        checked.append(request.url.path)
        if request.url.path.endswith("/123"):
            return httpx.Response(200, text="unrecognized page")
        return httpx.Response(200, text=card().replace("/show/123", "/show/124"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.recheck_availability()
        assert tracker.availability_error
        tracker.availability_retry_at = 0
        await tracker.recheck_availability()
    assert checked == ["/a/show/123", "/a/show/124"]
    assert len(await store.latest()) == 2
    assert (await store.counts())["inactive"] == 0


async def test_deliver_and_latest_remember_all_message_ids(store, config, no_wait):
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": sent(message_id=len(requests))})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.deliver()
        await tracker.handle_update({"message": {"chat": {"id": 42, "type": "private"}, "text": "/latest"}})
    assert requests[0]["reply_markup"]["inline_keyboard"][0][0]["callback_data"] == "inactive:123"
    await store.set_availability("123", "removed")
    assert {item["message_id"] for item in await store.messages_to_clean(42)} == {1, 2}


async def test_auto_removed_ad_cleans_all_copies_even_when_paused(store, config, no_wait):
    await store.mark_sent("123", 42, sent(9))
    await store.remember_message("123", 42, sent(10))
    await store.set_meta("paused", "1")
    deleted = []

    def respond(request):
        if request.url.host == "krisha.kz":
            return httpx.Response(410)
        assert request.url.path.endswith("/deleteMessage")
        deleted.append(json.loads(request.content)["message_id"])
        return httpx.Response(200, json={"ok": True, "result": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.recheck_availability()
        assert await store.latest() == []
        await tracker.deliver()
        await tracker.deliver()
    assert set(deleted) == {9, 10}
    assert await store.messages_to_clean(42) == []


async def test_cleanup_failure_is_retried_without_losing_message(store, config, no_wait):
    await store.mark_sent("123", 42, sent())
    await store.set_availability("123", "removed")
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 120}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        await tracker.deliver()
        await tracker.deliver()
    assert len(calls) == 1
    assert len(await store.messages_to_clean(42)) == 1


async def test_old_messages_are_marked_and_removed_from_cleanup_queue(store, config, no_wait):
    await store.mark_sent("123", 42, sent(age_hours=49))
    await store.set_availability("123", "archived")
    requests = []

    def respond(request):
        assert request.url.path.endswith("/editMessageText")
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await Tracker(config, store, client).deliver()
    assert "больше неактуально" in requests[0]["text"]
    assert requests[0]["reply_markup"] == {"inline_keyboard": []}
    assert await store.messages_to_clean(42) == []


@pytest.mark.parametrize(
    "description,expected",
    [
        ("Bad Request: message can't be deleted for everyone", ["deleteMessage", "editMessageText"]),
        ("Bad Request: message to delete not found", ["deleteMessage"]),
    ],
)
async def test_delete_fallback_and_already_deleted_are_idempotent(description, expected):
    methods = []

    def respond(request):
        method = request.url.path.rsplit("/", 1)[1]
        methods.append(method)
        if method == "deleteMessage":
            return httpx.Response(400, json={"ok": False, "error_code": 400, "description": description})
        return httpx.Response(200, json={"ok": True, "result": {}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await Telegram(client, "test-token").remove_listing_message(42, 9, flat().to_dict(), old=False)
    assert methods == expected


async def test_transient_delete_failure_does_not_attempt_edit():
    calls = []

    def respond(request):
        calls.append(request.url.path)
        return httpx.Response(500, json={"ok": False, "error_code": 500})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(TelegramError):
            await Telegram(client, "test-token").remove_listing_message(42, 9, flat().to_dict(), old=False)
    assert len(calls) == 1


async def test_button_requires_owner_and_saved_message(store, config):
    await store.mark_sent("123", 42, sent())
    answered = []

    def respond(request):
        assert request.url.path.endswith("/answerCallbackQuery")
        answered.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": True})

    def query(user_id=42, chat_id=42, message_id=9, listing_id="123", kind="private"):
        return {
            "id": "callback-id",
            "from": {"id": user_id},
            "data": f"inactive:{listing_id}",
            "message": {"message_id": message_id, "chat": {"id": chat_id, "type": kind}},
        }

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tracker = Tracker(config, store, client)
        for invalid in [
            query(user_id=7),
            query(chat_id=7),
            query(kind="group"),
            query(message_id=99),
            query(listing_id="999"),
        ]:
            await tracker.handle_update({"callback_query": invalid})
            assert len(await store.latest()) == 1
        assert answered == []
        await tracker.handle_update({"callback_query": query()})
        assert await store.latest() == []
        assert len(await store.messages_to_clean(42)) == 1
        assert len(answered) == 1
