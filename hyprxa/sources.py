import threading
from collections.abc import Sequence
from enum import Enum
from typing import Any, Dict, Tuple, Type

from fastapi import Request
from pydantic import create_model

from hyprxa.auth import requires
from hyprxa.base import BaseSubscriber, BaseSubscription
from hyprxa.timeseries import BaseClient
from hyprxa.timeseries import AnySourceSubscription as BaseAny
from hyprxa.timeseries import AnySourceSubscriptionRequest as BaseAnyRequest
from hyprxa.timeseries import BaseSourceSubscriptionRequest as BaseRequest 



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
    
    def is_authorized(self, request: Request) -> None:
        requires(
            scopes=self.scopes,
            any_=self.any,
            raise_on_no_scopes=self.raise_on_no_scopes
        )(request=request)
    

class SourceCollection:
    """Collection of available sources."""
    def __init__(self) -> None:
        self._sources: Dict[str, Source] = {}
        self._source_lock: threading.Lock = threading.Lock()

    def register(self, source: Source) -> None:
        if not isinstance(source, Source):
            raise TypeError(f"Expected 'Source' got {type(source)}")
        with self._source_lock:
            if source.source in self._sources:
                raise ValueError(f"'{source.source}' is already registered.")
            self._sources[source.source] = source

    def compile_sources(self) -> Enum:
        return Enum("Sources", {k.upper(): k.lower() for k in self._sources.keys()})
    

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
    source_collection.register(s)


source_collection = SourceCollection()


BaseSourceSubscription = lambda: create_model(
    "BaseSourceSubscription",
    source=(source_collection.compile_sources(), ...),
    __base__=BaseSubscription
)
AnySourceSubscription = lambda: create_model(
    "AnySourceSubscription",
    source=(source_collection.compile_sources(), ...),
    __base__=BaseAny
)
BaseSourceSubscriptionRequest = lambda model: create_model(
    "BaseSourceSubscriptionRequest",
    subscriptions=(Sequence[model], ...),
    __base__=BaseRequest
)
AnySourceSubscriptionRequest = lambda model: create_model(
    "AnySourceSubscriptionRequest",
    subscriptions=(Sequence[model], ...),
    __base__=BaseAnyRequest
)