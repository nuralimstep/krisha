from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: int | None = None
    bot_access_code: SecretStr = SecretStr("")
    admin_token: SecretStr = SecretStr("")
    poll_interval_seconds: int = Field(default=60, ge=30, le=3600)
    search_pages: int = Field(default=3, ge=1, le=20)
    availability_interval_seconds: int = Field(default=300, ge=60, le=86400)
    availability_batch_size: int = Field(default=20, ge=1, le=100)
    min_price: int = Field(default=100_000, ge=1)
    max_price: int = Field(default=150_000, ge=1)
    min_area: float = Field(default=20, ge=0)
    require_photo: bool = True
    owners_only: bool = False
    database_path: Path = Path("data/krisha.sqlite3")

    @model_validator(mode="before")
    @classmethod
    def empty_chat_id(cls, values):
        if values.get("telegram_chat_id") == "":
            values["telegram_chat_id"] = None
        return values

    @model_validator(mode="after")
    def validate_configuration(self):
        if self.min_price > self.max_price:
            raise ValueError("MIN_PRICE must not exceed MAX_PRICE")
        if self.telegram_chat_id is not None and self.telegram_chat_id <= 0:
            raise ValueError("TELEGRAM_CHAT_ID must be a private chat ID")
        if self.telegram_bot_token.get_secret_value():
            if not self.telegram_chat_id and len(self.bot_access_code.get_secret_value()) < 16:
                raise ValueError("Set TELEGRAM_CHAT_ID or BOT_ACCESS_CODE (16+ characters)")
        return self
