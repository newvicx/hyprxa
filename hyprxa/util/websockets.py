import logging
from collections.abc import AsyncIterable, Awaitable
from enum import IntEnum
from typing import Any, Callable

import anyio
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState



async def ws_handler(
    websocket: WebSocket,
    logger: logging.Logger,
    receive: Callable[[WebSocket], Awaitable[None]] | None = None,
    send: AsyncIterable[Any] | None = None
) -> None:
    """Manages a websocket connection."""
    async def wrap_send() -> None:
        async for data in send:
            match data:
                case str():
                    await websocket.send_text(data)
                case bytes():
                    await websocket.send_bytes(data)
                case _:
                    await websocket.send_json(data)

    receive = receive or _null_receive

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(receive(websocket))
            tg.start_soon(wrap_send(send))

    except WebSocketDisconnect as e:
        logger.debug(
            "Websocket connection closed by user",
            extra={
                "code": e.code,
                "reason": e.reason
            }
        )
    
    except Exception:
        logger.error("Connection closed abnormally", exc_info=True)
        try:
            await websocket.close(1006)
        except Exception:
            pass

    finally:
        if websocket.state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(1006)
            except:
                pass


async def _null_receive(websocket: WebSocket) -> None:
    """Receives messages from a websocket but does nothing with them."""
    while True:
        msg = await websocket.receive()
        if msg["type"] == "websocket.disconnect":
            code = msg["code"]
            if isinstance(code, IntEnum): # wsproto
                raise WebSocketDisconnect(code=code.value, reason=code.name)
            # websockets
            raise WebSocketDisconnect(code=code.code, reason=code.reason)