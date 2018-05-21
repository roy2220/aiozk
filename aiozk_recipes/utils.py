import asyncio
import inspect
import typing


_T = typing.TypeVar("_T")


async def delay_cancellation(coro_or_future: typing.Awaitable[_T], *
                             , loop: typing.Optional[asyncio.AbstractEventLoop]=None) -> _T:
    future = asyncio.ensure_future(coro_or_future, loop=loop)
    was_cancelled = False

    while True:
        try:
            result = await asyncio.shield(future, loop=loop)
        except asyncio.CancelledError:
            if future.cancelled():
                raise

            was_cancelled = True
        else:
            break

    if was_cancelled:
        asyncio.Task.current_task(loop=loop).cancel()

    return result


async def atomize_cancellation(coro: typing.Awaitable[_T], *
                               , loop: typing.Optional[asyncio.AbstractEventLoop]=None) -> _T:
    assert inspect.iscoroutine(coro)
    task = loop.create_task(coro)
    was_cancelled = False

    while True:
        try:
            result = await asyncio.shield(task, loop=loop)
        except asyncio.CancelledError:
            if task.cancelled():
                raise

            if inspect.getcoroutinestate(coro) == inspect.CORO_CREATED:
                task.cancel()
                raise

            was_cancelled = True
        else:
            break

    if was_cancelled:
        asyncio.Task.current_task(loop=loop).cancel()

    return result
