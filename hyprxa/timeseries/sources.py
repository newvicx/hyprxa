from collections.abc import Iterable, MutableMapping, Sequence
from enum import Enum
from typing import Any, Callable, Dict, List, Type

from pydantic import BaseModel
from starlette.requests import HTTPConnection

from hyprxa.auth.scopes import requires
from hyprxa.timeseries.base import BaseIntegration
from hyprxa.timeseries.models import BaseSourceSubscription



class Source:
    """Represents the configuration for client to a single source."""
    def __init__(
        self,
        source: str,
        integration: Callable[..., BaseIntegration],
        subscription_model: Type[BaseSourceSubscription],
        scopes: Sequence[str] | None,
        any_: bool,
        raise_on_no_scopes: bool,
        client_args: Sequence[Any],
        client_kwargs: Dict[str, Any]
    ) -> None:
        self.source = source
        self.integration = integration
        self.subscription_model = subscription_model
        self.scopes = list(scopes) or []
        self.any = any_
        self.raise_on_no_scopes = raise_on_no_scopes
        self.client_args = client_args
        self.client_kwargs = client_kwargs

    def __call__(self) -> BaseIntegration:
        """Create a new integration instance."""
        integration = self.integration(*self.client_args, **self.client_kwargs)
        if not isinstance(integration, BaseIntegration):
            raise TypeError("'integration' must be an instance of `Baseintegration`.")
        return integration
    
    async def is_authorized(self, connection: HTTPConnection) -> None:
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
        """Generate an enum of sources. Used for validation in API requests."""
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
    

class AvailableSources(BaseModel):
    """The available sources for the application."""
    sources: List[str]


_SOURCES = SourceMapping()
    

def add_source(
    source: str,
    integration: Callable[..., BaseIntegration],
    subscription_model: Type[BaseSourceSubscription],
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
        integration: A callable that returns a subclass of `BaseIntegration`.
        subscription_model: The subscription model to validate subscription
            requests against.
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
        integration=integration,
        subscription_model=subscription_model,
        scopes=scopes,
        any_=any_,
        raise_on_no_scopes=raise_on_no_scopes,
        client_args=client_args,
        client_kwargs=client_kwargs
    )
    _SOURCES.register(s)