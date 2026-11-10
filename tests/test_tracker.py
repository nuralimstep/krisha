from dataclasses import replace

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.krisha import Listing, SourceError, fetch_listings, is_match, parse_listings
from app.main import create_app
from app.store import Store
from app.telegram import Telegram, TelegramError, listing_message
from app.tracker import Tracker


def settings(tmp_path, **kwargs):
    return Settings(_env_file=None, database_path=tmp_path / "test.sqlite3", **kwargs)


def flat(**kwargs):
    defaults = dict(
        id="123",
        title="1-комнатная квартира · 32 м² · 3/5 этаж",
        price=125000,
        area=32,
        address="Алмалинский р-н, Абая 1",
        description="Мебель, без подселения",
        url="https://krisha.kz/a/show/123",
        has_photo=True,
        owner=True,
        monthly=True,
        city="Алматы",
    )
    defaults.update(kwargs)
    return Listing(**defaults)


@pytest.mark.parametrize("price, expected", [(99999, False), (100000, True), (150000, True), (150001, False)])
def test_price_boundaries(tmp_path, price, expected):
    assert is_match(flat(price=price), settings(tmp_path)) is expected


@pytest.mark.parametrize(
    "changes",
    [
        {"title": "2-комнатная квартира · 32 м²"},
        {"city": "Алматы, Наурызбайский район (пригород)"},
        {"city": "Астана"},
        {"monthly": False},
        {"has_photo": False},
        {"area": 4.4},
        {"area": None},
        {"description": "Ищем на подселение девушку"},
        {"description": "Сдаётся комната в квартире"},
        {"description": "Сдам койко-место"},
        {"description": "Посуточная аренда"},
        {"description": "Сдаю на два дня"},
    ],
)
def test_unsuitable_offers_excluded(tmp_path, changes):
    assert not is_match(flat(**changes), settings(tmp_path))


def test_owner_filter_and_no_sharing_negation(tmp_path):
    assert is_match(flat(description="Квартира без подселения"), settings(tmp_path))
    assert not is_match(flat(owner=False), settings(tmp_path, owners_only=True))


def test_parser_extracts_real_card_shape():
    html = """<div class="a-card" data-id="123">
      <a class="a-card__title" href="/a/show/123">1-комнатная квартира · 32,5 м² · 3/5 этаж</a>
      <div class="a-card__price">125&nbsp;000&nbsp;<span>₸</span><span>за месяц</span></div>
      <div class="a-card__subtitle">Абая 1</div>
      <div class="a-card__text-preview">Хорошая квартира</div>
      <img class="a-image__img" src="//photos.krisha.kz/photo.jpg">
      <div class="user-owner"></div><div class="a-card__stats-item">Алматы</div>
    </div><a class="paginator__btn--next"></a>"""
    listings, has_next = parse_listings(html)
    assert has_next and len(listings) == 1
    item = listings[0]
    assert (item.price, item.area, item.city) == (125000, 32.5, "Алматы")
    assert item.owner and item.monthly and item.has_photo
    assert item.url == "https://krisha.kz/a/show/123"
    assert not parse_listings(html.replace("photo.jpg", "nophoto/flat.png"))[0][0].has_photo


def test_challenge_does_not_look_like_empty_results():
    with pytest.raises(SourceError):
        parse_listings("<html>Please complete CAPTCHA</html>")
    assert parse_listings('<div class="a-list-empty"></div>') == ([], False)


async def test_blocked_source_reports_failure_without_exposing_urls(tmp_path):
    transport = httpx.MockTransport(lambda req: httpx.Response(429, text="rate limited"))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(SourceError, match="HTTPStatusError"):
            await fetch_listings(client, settings(tmp_path))


async def test_history_survives_restart_and_new_items_queue_once(tmp_path):
    store = Store(tmp_path / "db.sqlite3")
    await store.open()
    try:
        assert await store.record_scan([flat()], {"123"}) == 1
        await store.mark_sent("123")
    finally:
        await store.close()
    store = Store(tmp_path / "db.sqlite3")
    await store.open()
    try:
        assert await store.record_scan([flat(), flat(id="124")], {"123", "124"}) == 1
        assert [item["id"] for item in await store.pending()] == ["124"]
        assert await store.record_scan([flat(), flat(id="124")], {"123", "124"}) == 0
        await store.record_scan([], set())
        assert await store.latest() == []
        assert await store.pending() == []
    finally:
        await store.close()


async def test_initial_scan_limits_alerts_without_dropping_future_new_offers(tmp_path):
    store = Store(tmp_path / "db.sqlite3")
    await store.open()
    try:
        initial = [flat(id=str(i)) for i in range(10)]
        assert await store.record_scan(initial, {item.id for item in initial}) == 5
        assert len(await store.pending()) == 5
        assert await store.record_scan(initial, {item.id for item in initial}) == 0
        assert await store.record_scan([flat(id="999")], {"999"}) == 1
    finally:
        await store.close()


async def test_telegram_failure_retains_pending_notification(tmp_path):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 120}})

    config = settings(tmp_path, telegram_bot_token="private-token", telegram_chat_id=42)
    store = Store(config.database_path)
    await store.open()
    try:
        await store.record_scan([flat()], {"123"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            tracker = Tracker(config, store, client)
            await tracker.deliver()
            assert len(await store.pending()) == 1
            await tracker.deliver()
            assert len(calls) == 1  # retry_after respected
    finally:
        await store.close()


async def test_pairing_rejects_strangers_and_groups(tmp_path):
    config = settings(tmp_path, telegram_bot_token="private-token", bot_access_code="secret-access-code")
    store = Store(config.database_path)
    await store.open()
    try:
        transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True, "result": {}}))
        async with httpx.AsyncClient(transport=transport) as client:
            tracker = Tracker(config, store, client)

            def update(chat_id, text, kind="private"):
                return {"message": {"chat": {"id": chat_id, "type": kind}, "text": text}}

            await tracker.handle_update(update(7, "/start wrong"))
            await tracker.handle_update(update(7, "/start неверный-код"))
            await tracker.handle_update(update(7, "/start secret-access-code", "group"))
            assert await tracker.chat_id() is None
            await tracker.handle_update(update(42, "/start secret-access-code"))
            assert await tracker.chat_id() == 42
            await tracker.handle_update(update(7, "/start secret-access-code"))
            await tracker.handle_update(update(7, "/stop"))
            assert await tracker.chat_id() == 42
            assert await store.get_meta("paused") == "0"
            await tracker.handle_update(update(42, "/stop"))
            assert await store.get_meta("paused") == "1"
    finally:
        await store.close()


async def test_token_not_in_telegram_errors():
    transport = httpx.MockTransport(lambda req: httpx.Response(500, text="bad gateway"))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(TelegramError) as exc:
            await Telegram(client, "secret-token").send(42, "hi")
        assert "secret-token" not in str(exc.value)


def test_message_escapes_untrusted_html():
    message = listing_message(replace(flat(), description='<a href="evil">&danger</a>').to_dict())
    assert "&lt;a" in message and "&amp;danger" in message
    assert '<a href="evil"' not in message


def test_api_authorization_and_worker_lifecycle(tmp_path, monkeypatch):
    async def fake_fetch(client, config):
        return [flat()]

    monkeypatch.setattr("app.tracker.fetch_listings", fake_fetch)
    app = create_app(settings(tmp_path, admin_token="admin-test-secret"))
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/status").status_code == 401
        assert client.post("/check").status_code == 401
        headers = {"Authorization": "Bearer admin-test-secret"}
        result = client.get("/status", headers=headers)
        assert result.status_code == 200
        assert result.json()["search"]["city"] == "Алматы"
        assert "admin-test-secret" not in result.text
        assert client.get("/listings?limit=1000", headers=headers).status_code == 422
        assert client.get("/docs").status_code == 404
    assert all(task.done() for task in app.state.tasks)
