import asyncio
import collections
import typing


class Semaphore:
    def __init__(self, min_value: int, max_value: int, value: int
                 , loop: asyncio.AbstractEventLoop) -> None:
        assert value >= min_value and value <= max_value
        self._min_value = min_value
        self._max_value = max_value
        self._value = value
        self._next_down_waiter_id = 0
        self._down_waiters: collections.OrderedDict[int, asyncio.Future[None]] = collections\
            .OrderedDict()
        self._next_up_waiter_id = 0
        self._up_waiters: collections.OrderedDict[int, asyncio.Future[None]] = collections\
            .OrderedDict()
        self._is_closed = False
        self._loop = loop

    async def down(self, decrease_max_value: bool=False) -> None:
        assert not self._is_closed

        if self._value == self._min_value:
            waiter_id = self._next_down_waiter_id
            self._next_down_waiter_id = waiter_id + 1

            while True:
                waiter: asyncio.Future[None] = asyncio.Future(loop=self._loop)
                self._down_waiters[waiter_id] = waiter

                try:
                    await waiter
                except Exception:
                    if not self._is_closed:
                        waiter_is_first = next(iter(self._down_waiters)) == waiter_id
                        del self._down_waiters[waiter_id]

                        if waiter_is_first and self._value > self._min_value:
                            self._notify_down_waiter()

                    raise

                if self._is_closed:
                    raise asyncio.CancelledError()

                if self._value > self._min_value:
                    del self._down_waiters[waiter_id]
                    break

            self._value -= 1

            if self._value > self._min_value:
                self._notify_down_waiter()
        else:
            self._value -= 1

        if decrease_max_value:
            self._max_value -= 1

        if self._value == self._max_value - 1:
            self._notify_up_waiter()

    def try_down(self, decrease_max_value: bool=False) -> bool:
        assert not self._is_closed

        if self._value == self._min_value:
            return False

        self._value -= 1

        if decrease_max_value:
            self._max_value -= 1

        if self._value == self._max_value - 1:
            self._notify_up_waiter()

        return True

    async def up(self, increase_min_value: bool=False) -> None:
        assert not self._is_closed

        if self._value == self._max_value:
            waiter_id = self._next_up_waiter_id
            self._next_up_waiter_id = waiter_id + 1

            while True:
                waiter: asyncio.Future[None] = asyncio.Future(loop=self._loop)
                self._up_waiters[waiter_id] = waiter

                try:
                    await waiter
                except Exception:
                    if not self._is_closed:
                        waiter_is_first = next(iter(self._up_waiters)) == waiter_id
                        del self._up_waiters[waiter_id]

                        if waiter_is_first and self._value < self._max_value:
                            self._notify_up_waiter()

                    raise

                if self._is_closed:
                    raise asyncio.CancelledError()

                if self._value < self._max_value:
                    del self._up_waiters[waiter_id]
                    break

            self._value += 1

            if self._value < self._max_value:
                self._notify_up_waiter()
        else:
            self._value += 1

        if increase_min_value:
            self._min_value += 1

        if self._value == self._min_value + 1:
            self._notify_down_waiter()

    def try_up(self, increase_min_value: bool=False) -> bool:
        assert not self._is_closed

        if self._value == self._max_value:
            return False

        self._value += 1

        if increase_min_value:
            self._min_value += 1

        if self._value == self._min_value + 1:
            self._notify_down_waiter()

        return True

    def decrease_min_value(self, min_value_decrement: int) -> None:
        assert min_value_decrement >= 0
        assert not self._is_closed

        if min_value_decrement == 0:
            return

        if self._value == self._min_value:
            self._notify_down_waiter()

        self._min_value -= min_value_decrement

    def increase_max_value(self, max_value_increment: int) -> None:
        assert max_value_increment >= 0
        assert not self._is_closed

        if max_value_increment == 0:
            return

        if self._value == self._max_value:
            self._notify_up_waiter()

        self._max_value += max_value_increment

    def close(self, error_class: typing.Type[Exception]=asyncio.CancelledError) -> None:
        assert not self._is_closed
        self._is_closed = True

        for waiter in self._down_waiters.values():
            if waiter.cancelled():
                continue

            waiter.set_exception(error_class())

        self._down_waiters.clear()

        for waiter in self._up_waiters.values():
            if waiter.cancelled():
                continue

            waiter.set_exception(error_class())

        self._up_waiters.clear()

    def is_closed(self) -> bool:
        return self._is_closed

    def _notify_down_waiter(self) -> None:
        for waiter in self._down_waiters.values():
            if not waiter.done():
                waiter.set_result(None)

            return

    def _notify_up_waiter(self) -> None:
        for waiter in self._up_waiters.values():
            if not waiter.done():
                waiter.set_result(None)

            return
