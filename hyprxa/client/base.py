import asyncio
import functools
import logging
import sys
import time
from collections.abc import Awaitable
from typing import Callable, Set, Tuple, Type

from fastapi import status
from httpx import (
    AsyncClient,
    Client,
    LocalProtocolError,
    PoolTimeout,
    ReadError,
    ReadTimeout,
    RemoteProtocolError,
    Response,
    WriteError,
)



_LOGGER = logging.getLogger("hyprxa.client")


class HyprxaHttpxClient(Client):
    """A wrapper for the httpx client with support for retry-after
    headers for:
        - 503 Service unavailable
    
    Additionally, this client will always call `raise_for_status` on responses.
    """
    RETRY_MAX = 5

    def _send_with_retry(
        self,
        request: Callable[[], Awaitable[Response]],
        retry_codes: Set[int] = set(),
        retry_exceptions: Tuple[Type[Exception], ...] = tuple(),
    ) -> Response:
        """Send a request and retry it if it fails.

        Sends the provided request and retries it up to self.RETRY_MAX times if
        the request either raises an exception listed in `retry_exceptions` or receives
        a response with a status code listed in `retry_codes`.
        
        Retries will be delayed based on either the retry header (preferred) or
        exponential backoff if a retry header is not provided.
        """
        try_count = 0
        response = None

        while try_count <= self.RETRY_MAX:
            try_count += 1
            retry_seconds = None
            exc_info = None

            try:
                response = request()
            except retry_exceptions:
                if try_count > self.RETRY_MAX:
                    raise
                # Otherwise, we will ignore this error but capture the info for logging
                exc_info = sys.exc_info()
            else:
                # We got a response; return immediately if it is not retryable
                if response.status_code not in retry_codes:
                    return response

                if "Retry-After" in response.headers:
                    retry_seconds = float(response.headers["Retry-After"])

            # Use an exponential back-off if not set in a header
            if retry_seconds is None:
                retry_seconds = 2**try_count

            _LOGGER.debug(
                (
                    "Encountered retryable exception during request. "
                    if exc_info
                    else "Received response with retryable status code. "
                )
                + (
                    f"Another attempt will be made in {retry_seconds}s. "
                    f"This is attempt {try_count}/{self.RETRY_MAX + 1}."
                ),
                exc_info=exc_info,
            )
            time.sleep(retry_seconds)

        assert (
            response is not None
        ), "Retry handling ended without response or exception"

        # We ran out of retries, return the failed response
        return response

    def send(self, *args, **kwargs) -> Response:
        api_request = functools.partial(super().send, *args, **kwargs)

        response = self._send_with_retry(
            request=api_request,
            retry_codes={
                status.HTTP_503_SERVICE_UNAVAILABLE,
            },
            retry_exceptions=(
                ReadTimeout,
                PoolTimeout,
                # `ConnectionResetError` when reading socket raises as a `ReadError`
                ReadError,
                # Sockets can be closed during writes resulting in a `WriteError`
                WriteError,
                # Uvicorn bug, see https://github.com/PrefectHQ/prefect/issues/7512
                RemoteProtocolError,
                # HTTP2 bug, see https://github.com/PrefectHQ/prefect/issues/7442
                LocalProtocolError,
            ),
        )

        # Always raise bad responses
        response.raise_for_status()

        return response


class HyprxaHttpxAsyncClient(AsyncClient):
    """A wrapper for the async httpx client with support for retry-after
    headers for:
        - 503 Service unavailable
    
    Additionally, this client will always call `raise_for_status` on responses.
    """
    RETRY_MAX = 5

    async def _send_with_retry(
        self,
        request: Callable[[], Awaitable[Response]],
        retry_codes: Set[int] = set(),
        retry_exceptions: Tuple[Type[Exception], ...] = tuple(),
    ) -> Response:
        """Send a request and retry it if it fails.

        Sends the provided request and retries it up to self.RETRY_MAX times if
        the request either raises an exception listed in `retry_exceptions` or receives
        a response with a status code listed in `retry_codes`.
        
        Retries will be delayed based on either the retry header (preferred) or
        exponential backoff if a retry header is not provided.
        """
        try_count = 0
        response = None

        while try_count <= self.RETRY_MAX:
            try_count += 1
            retry_seconds = None
            exc_info = None

            try:
                response = await request()
            except retry_exceptions:
                if try_count > self.RETRY_MAX:
                    raise
                # Otherwise, we will ignore this error but capture the info for logging
                exc_info = sys.exc_info()
            else:
                # We got a response; return immediately if it is not retryable
                if response.status_code not in retry_codes:
                    return response

                if "Retry-After" in response.headers:
                    retry_seconds = float(response.headers["Retry-After"])

            # Use an exponential back-off if not set in a header
            if retry_seconds is None:
                retry_seconds = 2**try_count

            _LOGGER.debug(
                (
                    "Encountered retryable exception during request. "
                    if exc_info
                    else "Received response with retryable status code. "
                )
                + (
                    f"Another attempt will be made in {retry_seconds}s. "
                    f"This is attempt {try_count}/{self.RETRY_MAX + 1}."
                ),
                exc_info=exc_info,
            )
            await asyncio.sleep(retry_seconds)

        assert (
            response is not None
        ), "Retry handling ended without response or exception"

        # We ran out of retries, return the failed response
        return response

    async def send(self, *args, **kwargs) -> Response:
        api_request = functools.partial(super().send, *args, **kwargs)

        response = await self._send_with_retry(
            request=api_request,
            retry_codes={
                status.HTTP_503_SERVICE_UNAVAILABLE,
            },
            retry_exceptions=(
                ReadTimeout,
                PoolTimeout,
                # `ConnectionResetError` when reading socket raises as a `ReadError`
                ReadError,
                # Sockets can be closed during writes resulting in a `WriteError`
                WriteError,
                # Uvicorn bug, see https://github.com/PrefectHQ/prefect/issues/7512
                RemoteProtocolError,
                # HTTP2 bug, see https://github.com/PrefectHQ/prefect/issues/7442
                LocalProtocolError,
            ),
        )

        # Always raise bad responses
        response.raise_for_status()

        return response