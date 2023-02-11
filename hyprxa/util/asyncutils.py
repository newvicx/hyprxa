"""Utilities for interoperability with async functions and workers from various
contexts.

This is a handpicked set of utility functions directly from
[Prefect](https://github.com/PrefectHQ/prefect/blob/main/src/prefect/utilities/asyncutils.py)
"""
import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    ParamSpec,
    TypeGuard,
    TypeVar
)
from uuid import UUID, uuid4

import anyio
import anyio.abc



P = ParamSpec("P")
R = TypeVar("R")
# Global references to prevent garbage collection for `add_event_loop_shutdown_callback`
EVENT_LOOP_GC_REFS = {}


def is_async_fn(
    func: Callable[P, R] | Callable[P, Awaitable[R]]
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    """Returns `True` if a function returns a coroutine.

    See https://github.com/microsoft/pyright/issues/2142 for an example use
    """
    while hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    return inspect.iscoroutinefunction(func)


def is_async_gen_fn(func):
    """Returns `True` if a function is an async generator."""
    while hasattr(func, "__wrapped__"):
        func = func.__wrapped__

    return inspect.isasyncgenfunction(func)


async def add_event_loop_shutdown_callback(coroutine_fn: Callable[[], Awaitable]):
    """Adds a callback to the given callable on event loop closure.
    
    The callable must be a coroutine function. It will be awaited when the
    current event loop is shutting down.

    Requires use of `asyncio.run()` which waits for async generator shutdown by
    default or explicit call of `asyncio.shutdown_asyncgens()`. If the application
    is entered with `asyncio.run_until_complete()` and the user calls
    `asyncio.close()` without the generator shutdown call, this will not trigger
    callbacks.

    asyncio does not provided any other way to clean up a resource when the event
    loop is about to close.
    """

    async def on_shutdown(key):
        try:
            yield
        except GeneratorExit:
            await coroutine_fn()
            # Remove self from the garbage collection set
            EVENT_LOOP_GC_REFS.pop(key)

    # Create the iterator and store it in a global variable so it is not garbage
    # collected. If the iterator is garbage collected before the event loop closes, the
    # callback will not run. Since this function does not know the scope of the event
    # loop that is calling it, a reference with global scope is necessary to ensure
    # garbage collection does not occur until after event loop closure.
    key = id(on_shutdown)
    EVENT_LOOP_GC_REFS[key] = on_shutdown(key)

    # Begin iterating so it will be cleaned up as an incomplete generator
    await EVENT_LOOP_GC_REFS[key].__anext__()


class GatherIncomplete(RuntimeError):
    """Used to indicate retrieving gather results before completion"""


class GatherTaskGroup(anyio.abc.TaskGroup):
    """A task group that gathers results.
    
    AnyIO does not include support `gather`. This class extends the `TaskGroup`
    interface to allow simple gathering.
    
    See https://github.com/agronholm/anyio/issues/100
    
    This class should be instantiated with `create_gather_task_group`.
    """

    def __init__(self, task_group: anyio.abc.TaskGroup):
        self._results: Dict[UUID, Any] = {}
        # The concrete task group implementation to use
        self._task_group: anyio.abc.TaskGroup = task_group

    async def _run_and_store(self, key, fn, args):
        self._results[key] = await fn(*args)

    def start_soon(self, fn, *args) -> UUID:
        key = uuid4()
        # Put a placeholder in-case the result is retrieved earlier
        self._results[key] = GatherIncomplete
        self._task_group.start_soon(self._run_and_store, key, fn, args)
        return key

    async def start(self, fn, *args):
        """Since `start` returns the result of `task_status.started()` but here
        we must return the key instead, we just won't support this method for now.
        """
        raise RuntimeError("`GatherTaskGroup` does not support `start`.")

    def get_result(self, key: UUID) -> Any:
        result = self._results[key]
        if result is GatherIncomplete:
            raise GatherIncomplete(
                "Task is not complete. "
                "Results should not be retrieved until the task group exits."
            )
        return result

    async def __aenter__(self):
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, *tb):
        try:
            retval = await self._task_group.__aexit__(*tb)
            return retval
        finally:
            del self._task_group


def create_gather_task_group() -> GatherTaskGroup:
    """Create a new task group that gathers results"""
    # This function matches the AnyIO API which uses callables since the concrete
    # task group class depends on the async library being used and cannot be
    # determined until runtime
    return GatherTaskGroup(anyio.create_task_group())


async def gather(*calls: Callable[[], Coroutine[Any, Any, Any]]) -> List[Any]:
    """Run calls concurrently and gather their results.
    
    Unlike `asyncio.gather` this expects to receieve callables not coroutines.
    This matches `anyio` semantics.
    """
    keys = []
    async with create_gather_task_group() as tg:
        for call in calls:
            keys.append(tg.start_soon(call))
    return [tg.get_result(key) for key in keys]