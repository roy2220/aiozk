import asyncio
import typing


_T = typing.TypeVar("_T")


slots = (
    "_method_lock",
)


def init(instance, loop: asyncio.AbstractEventLoop) -> None:
    instance._method_lock = asyncio.Lock(loop=loop)


def locked_method(method: _T) -> _T:
    async def wrapped_method(instance, *args, **kwargs):
        await instance._method_lock.acquire()

        try:
            return await method(instance, *args, **kwargs)
        finally:
            instance._method_lock.release()

    return wrapped_method  # type: ignore
