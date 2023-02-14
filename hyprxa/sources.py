from collections.abc import Iterable, MutableMapping, Sequence
from enum import Enum
from typing import Any, Dict, Tuple, Type

from fastapi import Request, WebSocket
from pydantic import create_model, validator

from hyprxa.auth import requires
from hyprxa.base import BaseSubscriber, BaseSubscription
from hyprxa.timeseries import (
    BaseClient,
    AnySourceSubscription,
    AnySourceSubscriptionRequest,
    BaseSourceSubscriptionRequest,
    UnitOp
)



class Source:
    """Represents the configuration for client to a single source."""
    def __init__(
        self,
        source: str,
        client: Type[BaseClient],
        subscriber: Type[BaseSubscriber],
        scopes: Sequence[str] | None,
        any_: bool,
        raise_on_no_scopes: bool,
        client_args: Sequence[Any],
        client_kwargs: Dict[str, Any]
    ) -> None:
        if not issubclass(client, BaseClient):
            raise TypeError("'client' must be a subclass of `BaseClient`.")
        if not issubclass(subscriber, BaseSubscriber):
            raise TypeError("'subscriber' must be a subclass of `BaseSubscriber`.")
        self.source = source
        self.client = client
        self.subscriber = subscriber
        self.scopes = list(scopes) or []
        self.any = any_
        self.raise_on_no_scopes = raise_on_no_scopes
        self.client_args = client_args
        self.client_kwargs = client_kwargs

    def __call__(self) -> Tuple[BaseClient, Type[BaseSubscriber]]:
        """Create a new client instance."""
        return self.client(*self.client_args, **self.client_kwargs), self.subscriber
    
    async def is_authorized(self, connection: Request | WebSocket) -> None:
        await requires(
            scopes=self.scopes,
            any_=self.any,
            raise_on_no_scopes=self.raise_on_no_scopes
        )(connection=connection)
    

class SourceMapping(MutableMapping):
    """Collection of available sources. Not thread safe."""
    def __init__(self) -> None:
        self._sources: Dict[str, Source] = {}

    def register(self, source: Source) -> None:
        """Register a source for use with the timeseries manager."""
        if not isinstance(source, Source):
            raise TypeError(f"Expected 'Source' got {type(source)}")
        if source.source in self._sources:
            raise ValueError(f"'{source.source}' is already registered.")
        self._sources[source.source] = source

    def compile_sources(self) -> Enum:
        """Generate an Enum of sources. Used for validation in API requests."""
        return Enum(
            "Sources",
            {k.replace(" ", "_").upper(): k.replace(" ", "_").lower() for k in self._sources.keys()}
        )

    def __getitem__(self, __key: Any) -> Source:
        return self._sources[__key]

    def __setitem__(self, _: Any, source: Source) -> None:
        self.register(source)

    def __delitem__(self, __key: Any) -> None:
        self._sources.__delitem__(__key)

    def __iter__(self) -> Iterable[Source]:
        for source in self._sources.values():
            yield source

    def __len__(self) -> int:
        return len(self._sources)
    

def add_source(
    source: str,
    client: Type[BaseClient],
    subscriber: Type[BaseSubscriber],
    scopes: Sequence[str] | None = None,
    any_: bool = False,
    raise_on_no_scopes: bool = False,
    *client_args: Any,
    **client_kwargs: Any
) -> None:
    """Add a source to the application.

    For more information on scopes, see `requires`.
    
    Args:
        source: The name of the source.
        client: A subclass of `BaseClient`.
        subscriber: A subclass of `BaseSubscriber`.
        scopes: The required scopes to access the source.
        any_: If `True` and the user has any of the scopes, authorization will
            succeed. Otherwise, the user will need all scopes.
        raise_on_no_scopes: If `True` a `NotConfigured` error will be raised
            when a route with no required scopes is hit.
        client_args: Arguments to the client constructor.
        client_kwargs: Keyword arguments to the client constructor.
    """
    s = Source(
        source=source,
        client=client,
        subscriber=subscriber,
        scopes=scopes,
        any_=any_,
        raise_on_no_scopes=raise_on_no_scopes,
        client_args=client_args,
        client_kwargs=client_kwargs
    )
    SOURCES.register(s)


SOURCES = SourceMapping()


def source_to_str(cls, v: Enum) -> str:
    """Convert source enum to string."""
    return v.value


ValidatedBaseSourceSubscription = lambda: create_model(
    "BaseSourceSubscription",
    source=(SOURCES.compile_sources(), ...),
    __base__=BaseSubscription,
    __validators__={"_source_converter": validator("source", allow_reuse=True)(source_to_str)}
)
ValidatedAnySourceSubscription = lambda: create_model(
    "AnySourceSubscription",
    source=(SOURCES.compile_sources(), ...),
    __base__=AnySourceSubscription,
    __validators__={"_source_converter": validator("source", allow_reuse=True)(source_to_str)}
)
ValidatedBaseSourceSubscriptionRequest = lambda model: create_model(
    "BaseSourceSubscriptionRequest",
    subscriptions=(Sequence[model], ...),
    __base__=BaseSourceSubscriptionRequest
)
ValidatedAnySourceSubscriptionRequest = lambda model: create_model(
    "AnySourceSubscriptionRequest",
    subscriptions=(Sequence[model], ...),
    __base__=AnySourceSubscriptionRequest
)
ValidatedUnitOp = lambda model: create_model(
    "UnitOp",
    data_mapping=(Dict[str, model], ...),
    __base__=UnitOp
)
