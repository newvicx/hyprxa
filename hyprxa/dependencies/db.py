from collections.abc import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession
from pymongo.errors import PyMongoError

from hyprxa.settings import MONGO_SETTINGS
from hyprxa.util.mongo import DatabaseUnavailable, SessionManager



_session = SessionManager(MONGO_SETTINGS.get_async_client)


async def get_mongo_client() -> AsyncGenerator[AsyncIOMotorClient, None]:
    """Get an `AsyncIOMotorClient` for the scope of a query.
    
    This uses `SessionManager` so two coroutines can share the same client. This
    should only be used for reads or atomic writes. For an exclusive client,
    use `get_exclusive_mongo_client`.
    """
    async with _session.get_client() as client:
        yield client


async def get_exclusive_mongo_client() -> AsyncGenerator[AsyncIOMotorClient, None]:
    """Get an exclusive `AsyncIOMotorClient` for the scope of a query.
    
    The client instance is only tied to a single coroutine so multiple writes
    can be done without the risk of an error in another coroutine interrupting
    this one.
    """
    client = MONGO_SETTINGS.get_async_client()
    try:
        pong = await client.admin.command("ping")
        if not pong.get("ok"):
            raise DatabaseUnavailable("Unable to ping server.")
        yield client
    finally:
        client.close()


async def get_mongo_session() -> AsyncGenerator[AsyncIOMotorClientSession, None]:
    """Get an `AsyncIOMotorClientSession` for transaction operation.
    
    This always creates a new client and ends the session on exit. If the
    the transaction was not committed it is aborted.
    """
    client = MONGO_SETTINGS.get_async_client()
    try:
        pong = await client.admin.command("ping")
        if not pong.get("ok"):
            raise DatabaseUnavailable("Unable to ping server.")
    except PyMongoError:
        client.close()
        raise
    session = AsyncIOMotorClientSession(client)
    try:
        yield session
    finally:
        session.end_session()
        client.close()