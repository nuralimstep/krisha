"""Create local secrets once; never overwrite existing credentials."""

import os
import secrets
from pathlib import Path


def main():
    target = Path(".env")
    template = Path(".env.example").read_text()
    template = template.replace("replace-with-random-code", secrets.token_urlsafe(24))
    template = template.replace("replace-with-long-random-secret", secrets.token_urlsafe(32))
    try:
        with open(target, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as file:
            file.write(template)
    except FileExistsError:
        print(".env уже существует, сохранён без изменений.")
    else:
        print(".env создан. Добавьте TELEGRAM_BOT_TOKEN из @BotFather.")


if __name__ == "__main__":
    main()
