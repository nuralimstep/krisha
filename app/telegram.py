from html import escape

import httpx


class TelegramError(Exception):
    def __init__(self, code: int, retry_after: int = 0, description: str = ""):
        self.code = code
        self.retry_after = retry_after
        # Only inspect known error phrases; never include the remote text in logs.
        self.description = description.casefold()
        super().__init__(f"Telegram API error {code}")


class Telegram:
    def __init__(self, client: httpx.AsyncClient, token: str):
        self.client = client
        self.base_url = f"https://api.telegram.org/bot{token}/"

    async def call(self, method: str, **payload):
        # Never log an HTTPX exception: its URL contains the bot token.
        try:
            response = await self.client.post(self.base_url + method, json=payload, timeout=40)
            body = response.json()
        except (httpx.HTTPError, ValueError):
            raise TelegramError(502) from None
        if not body.get("ok"):
            raise TelegramError(
                body.get("error_code", response.status_code),
                body.get("parameters", {}).get("retry_after", 0),
                body.get("description", ""),
            )
        return body["result"]

    async def send(self, chat_id: int, text: str, listing_id: str | None = None) -> dict:
        payload = {}
        if listing_id is not None:
            payload["reply_markup"] = {
                "inline_keyboard": [[{"text": "Уже сдали", "callback_data": f"inactive:{listing_id}"}]]
            }
        return await self.call(
            "sendMessage",
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            link_preview_options={"is_disabled": True},
            **payload,
        )

    async def remove_listing_message(self, chat_id: int, message_id: int, listing: dict, old: bool):
        if not old:
            try:
                await self.call("deleteMessage", chat_id=chat_id, message_id=message_id)
                return
            except TelegramError as exc:
                if exc.code == 400 and "message to delete not found" in exc.description:
                    return
                if exc.code != 400 or not any(
                    phrase in exc.description for phrase in ("can't be deleted", "too old")
                ):
                    raise
        # Telegram permits deleting a message only within 48 hours. Our own text
        # messages can still be edited, so replace old cards with a clear notice.
        try:
            await self.call(
                "editMessageText",
                chat_id=chat_id,
                message_id=message_id,
                text="❌ <b>Объявление больше неактуально</b>\n\n" + listing_message(listing),
                parse_mode="HTML",
                reply_markup={"inline_keyboard": []},
                link_preview_options={"is_disabled": True},
            )
        except TelegramError as exc:
            if exc.code == 400 and any(
                phrase in exc.description
                for phrase in ("message is not modified", "message to edit not found")
            ):
                return
            raise


def listing_message(listing: dict) -> str:
    price = f"{listing['price']:,}".replace(",", " ")
    owner = "Собственник" if listing["owner"] else "Автор: агентство или не указан"
    return (
        f"🏠 <b>{escape(listing['title'])}</b>\n"
        f"<b>{price} ₸ / месяц</b>\n"
        f"📍 {escape(listing['address'])}\n"
        f"{owner}\n\n{escape(listing['description'][:600])}\n\n"
        f'<a href="{escape(listing["url"], quote=True)}">Открыть на Krisha.kz</a>'
    )
