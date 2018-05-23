import asyncio
import typing
import uuid

from asyncio_toolkit.typing import BytesLike, Coroutine

from . import method_lock
from .locks import Lock
import aiozk


class _QueueBase:
    def __init__(self, client: aiozk.Client, path: str, lock: Lock) -> None:
        method_lock.init(self, client.get_loop())
        self._client = client
        self._path = client.normalize_path(path)
        self._lock = lock

    @method_lock.locked_method
    async def dequeue(self, max_number_of_items=1) -> typing.List[bytes]:
        await self._lock.acquire()
        watcher = None

        try:
            while True:
                (item_names,), watcher = await self._client.get_children_w(self._path
                                                                           , auto_retry=True)

                if len(item_names) >= 1:
                    if not watcher.is_removed():
                        watcher.remove()

                    break

                await watcher.wait_for_event()

            item_names2 = sorted(item_names, key=lambda item_name: item_name.rsplit("-", 1)[1])
            if max_number_of_items >= 0:
                item_names2 = item_names2[:max_number_of_items]

            async def get_item_data(item_name: str) -> bytes:
                return (await self._client.get_data(self._path + "/" + item_name
                                                    , auto_retry=True)).data

            getting_item_data = asyncio.gather(*(get_item_data(item_name) for item_name \
                in item_names2), loop=self._client.get_loop())

            try:
                item_data = await getting_item_data
            except Exception:
                getting_item_data.cancel()
                raise

            ops = (self._client.delete_op(self._path + "/" + item_name)
                   for item_name in item_names2)

            await self._client.multi(ops, auto_retry=True)
        except Exception:
            if self._client.is_running():
                if watcher is not None and not watcher.is_removed():
                    watcher.remove()

                await self._lock.release()

            raise

        await self._lock.release()
        return item_data

    async def _enqueue(self, item_name_prefix_end: str
                       , item_data: typing.Sequence[BytesLike]) -> None:
        assert "-" not in item_name_prefix_end, repr(item_name_prefix_end)
        item_name_prefix = uuid.uuid4().hex + "-" + item_name_prefix_end

        while True:
            ops = (self._client.create_op(self._path + "/" + item_name_prefix, bytes(item_data2)
                                          , sequential=True) for item_data2 in item_data)

            try:
                await self._client.multi(ops)
            except aiozk.ConnectionLossError:
                pass
            else:
                return

            children, = await self._client.get_children(self._path, auto_retry=True)

            if any(child.startswith(item_name_prefix) for child in children):
                return


class Queue(_QueueBase):
    def enqueue(self, item_data: typing.Sequence[BytesLike]) -> Coroutine[None]:
        return self._enqueue("", item_data)


_MAX_PRIORITY_QUEUE_ITEM_PRIORITY = 999


class PriorityQueue(_QueueBase):
    MAX_ITEM_PRIORITY: typing.ClassVar[int] = _MAX_PRIORITY_QUEUE_ITEM_PRIORITY

    def enqueue(self, item_priority: int, item_data: typing.Sequence[BytesLike]) -> Coroutine[None]:
        assert item_priority in range(_MAX_PRIORITY_QUEUE_ITEM_PRIORITY + 1), repr(item_priority)
        return self._enqueue("%.3d." % (_MAX_PRIORITY_QUEUE_ITEM_PRIORITY - item_priority)
                             , item_data)
