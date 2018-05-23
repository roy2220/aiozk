import asyncio
import typing


_C = typing.TypeVar("_C", bound=typing.Callable)


slots = (
    "_method_lock",
)


def init(instance, loop: asyncio.AbstractEventLoop) -> None:
    instance._method_lock = asyncio.Lock(loop=loop)


def locked_method(method: _C) -> _C:
    async def wrapped_method(instance, *args, **kwargs):
        await instance._method_lock.acquire()

        try:
            return await method(instance, *args, **kwargs)
        finally:
            instance._method_lock.release()

    return wrapped_method  # type: ignore
