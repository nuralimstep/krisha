import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import Settings
from app.krisha import SourceError
from app.store import Store
from app.tracker import Tracker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
# Request URLs contain the Telegram token.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
bearer = HTTPBearer(auto_error=False)


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = settings or Settings()
        store = Store(config.database_path)
        await store.open()
        tasks = []
        try:
            async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
                tracker = Tracker(config, store, client)
                app.state.tracker = tracker
                tasks = [
                    asyncio.create_task(tracker.watch(), name="krisha-watcher"),
                    asyncio.create_task(tracker.delivery_loop(), name="telegram-delivery"),
                    asyncio.create_task(tracker.availability_loop(), name="availability-checker"),
                ]
                if tracker.telegram:
                    tasks.append(asyncio.create_task(tracker.poll_telegram(), name="telegram-poll"))
                app.state.tasks = tasks
                try:
                    yield
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await store.close()

    app = FastAPI(
        title="Krisha Telegram Tracker",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    async def authorize(
        request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]
    ):
        token = request.app.state.tracker.settings.admin_token.get_secret_value()
        if not token:
            raise HTTPException(503, "ADMIN_TOKEN is not configured")
        if not credentials or not secrets.compare_digest(credentials.credentials.encode(), token.encode()):
            raise HTTPException(401, "Invalid bearer token", headers={"WWW-Authenticate": "Bearer"})

    @app.get("/")
    async def root():
        return {"service": "Krisha Telegram Tracker", "health": "/health"}

    @app.get("/health")
    async def health(request: Request):
        if any(task.done() for task in request.app.state.tasks):
            raise HTTPException(503, "Background worker stopped")
        return {"status": "ok"}

    @app.get("/status", dependencies=[Depends(authorize)])
    async def status(request: Request):
        return await request.app.state.tracker.status()

    @app.get("/listings", dependencies=[Depends(authorize)])
    async def listings(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20):
        return await request.app.state.tracker.store.latest(limit)

    @app.post("/check", dependencies=[Depends(authorize)])
    async def check(request: Request):
        tracker = request.app.state.tracker
        if tracker.scan_lock.locked():
            raise HTTPException(409, "Scan is already running")
        try:
            return await tracker.scan()
        except SourceError as exc:
            raise HTTPException(502, str(exc)) from None

    return app


app = create_app()
