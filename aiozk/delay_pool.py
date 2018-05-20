import asyncio
import random
import time
import typing


_T = typing.TypeVar("_T")


class DelayPool(typing.Generic[_T]):
    def __init__(self, items: typing.Iterable[_T], total_max_delay_duration: float) -> None:
        self._items = list(set(items))
        assert len(self._items) >= 1
        self.reset(total_max_delay_duration)

    def reset(self, total_max_delay_duration: float) -> None:
        assert total_max_delay_duration > 0, repr(total_max_delay_duration)
        random.shuffle(self._items)
        self._next_item_index = 0
        self._max_delay_duration = total_max_delay_duration / len(self._items)

    async def allocate_item(self, loop: asyncio.AbstractEventLoop) -> typing.Optional[_T]:
        if self._next_item_index == len(self._items):
            return None

        now = time.monotonic()

        if self._next_item_index == 0:
            self._next_item_allocable_time = now
        else:
            delay_duration = self._next_item_allocable_time - now

            if delay_duration >= 0.001:
                await asyncio.sleep(delay_duration, loop=loop)

        item = self._items[self._next_item_index]
        self._next_item_index += 1
        self._next_item_allocable_time += self._max_delay_duration
        return item

    def when_next_item_allocable(self) -> float:
        return self._next_item_allocable_time
