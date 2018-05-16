import asyncio
import collections
import typing

from .semaphore import Semaphore


_T = typing.TypeVar("_T")


class Deque(typing.Generic[_T]):
    def __init__(self, max_length: int, loop: asyncio.AbstractEventLoop) -> None:
        self._semaphore = Semaphore(0, max_length, 0, loop)
        self._items: typing.Deque[_T] = collections.deque()

    async def insert_tail(self, item: _T) -> None:
        await self._semaphore.up()
        self._items.append(item)

    def try_insert_tail(self, item: _T) -> bool:
        if not self._semaphore.try_up():
            return False

        self._items.append(item)
        return True

    async def remove_tail(self, commit_item_removal: bool=True) -> _T:
        await self._semaphore.down(not commit_item_removal)
        return self._items.pop()

    def try_remove_tail(self, commit_item_removal: bool=True) -> typing.Optional[_T]:
        if not self._semaphore.try_down(not commit_item_removal):
            return None

        return self._items.pop()

    async def insert_head(self, item: _T) -> None:
        await self._semaphore.up()
        self._items.appendleft(item)

    def try_insert_head(self, item: _T) -> bool:
        if not self._semaphore.try_up():
            return False

        self._items.appendleft(item)
        return True

    async def remove_head(self, commit_item_removal: bool=True) -> _T:
        await self._semaphore.down(not commit_item_removal)
        return self._items.popleft()

    def try_remove_head(self, commit_item_removal: bool=True) -> typing.Optional[_T]:
        if not self._semaphore.try_down(not commit_item_removal):
            return None

        return self._items.popleft()

    def try_remove_item(self, item: _T, commit_item_removal: bool=True) -> bool:
        try:
            self._items.remove(item)
        except ValueError:
            return False

        self._semaphore.try_down(not commit_item_removal)
        return True

    def commit_item_removals(self, number_of_item_removals: int) -> None:
        self._semaphore.increase_max_value(number_of_item_removals)

    def close(self, error_class: typing.Optional[typing.Type[Exception]]=None) -> None:
        self._semaphore.close(error_class)
        self._items.clear()

    def is_closed(self) -> bool:
        return self._semaphore.is_closed()
