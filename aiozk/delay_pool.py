import asyncio
import logging
import math
import random
import time
import typing


_T = typing.TypeVar("_T")


class DelayPool(typing.Generic[_T]):
    def __init__(self, items: typing.Iterable[_T], item_reuse_factor: float
                 , total_max_delay_duration: float, loop: asyncio.AbstractEventLoop
                 , logger: logging.Logger) -> None:
        self._items = list(set(items))
        assert len(self._items) >= 1
        self.reset(item_reuse_factor, total_max_delay_duration)
        self._loop = loop
        self._logger = logger

    def reset(self, item_reuse_factor: float, total_max_delay_duration: float) -> None:
        assert item_reuse_factor >= 1.0, repr(item_reuse_factor)
        assert total_max_delay_duration > 0, repr(total_max_delay_duration)
        last_item_index = getattr(self, "_next_item_index", 0) - 1

        if last_item_index < 0:
            random.shuffle(self._items)
        else:
            last_item = self._items.pop(last_item_index)
            random.shuffle(self._items)
            self._items.append(last_item)

        self._used_item_count = 0
        self._number_of_items = math.ceil(item_reuse_factor * len(self._items))
        self._max_delay_duration = total_max_delay_duration / self._number_of_items

    async def allocate_item(self) -> typing.Optional[_T]:
        if self._used_item_count == self._number_of_items:
            return None

        now = time.monotonic()

        if self._used_item_count == 0:
            self._next_item_allocable_time = now
        else:
            delay_duration = self._next_item_allocable_time - now

            if delay_duration >= 0.001:
                self._logger.info("delay pool item allocation: delay_duration={!r}"
                                  .format(delay_duration))
                await asyncio.sleep(delay_duration, loop=self._loop)

        item = self._items[self._used_item_count % len(self._items)]
        self._used_item_count += 1
        self._next_item_allocable_time += self._max_delay_duration
        return item

    def when_next_item_allocable(self) -> float:
        return self._next_item_allocable_time
