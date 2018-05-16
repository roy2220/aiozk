import asyncio
import typing
import uuid

from . import method_lock
from .locks import Lock
import aiozk


class _QueueBase:
    def __init__(self, client: aiozk.Client, path: str, lock: Lock) -> None:
        method_lock.init(self, client.get_loop())
        self._client = client
        self._path = path
        self._lock = lock

    @method_lock.locked_method
    async def dequeue(self, max_number_of_items=1) -> typing.List[bytes]:
        await self._lock.acquire()

        try:
            while True:
                (item_names,), watcher = await self._client.get_children(self._path, True
                                                                         , auto_retry=True)

                if len(item_names) >= 1:
                    item_names2 = sorted(item_names, key=lambda item_name: item_name.rsplit("-"
                                                                                            , 1)[1])
                    if max_number_of_items >= 0:
                        item_names2 = item_names2[:max_number_of_items]

                    async def get_item_data(item_name: str) -> bytes:
                        return (await self._client.get_data(self._path + "/" + item_name
                                                            , auto_retry=True))[0].data

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
                    return item_data

                await watcher.wait_for_event()
        finally:
            await self._lock.release()

    async def _enqueue(self, item_name_prefix_end: str
                       , item_data: typing.Union[bytes, bytearray]) -> None:
        assert "-" not in item_name_prefix_end
        item_name_prefix = uuid.uuid4().hex + "-" + item_name_prefix_end

        while True:
            try:
                await self._client.create(self._path + "/" + item_name_prefix, bytes(item_data)
                                          , sequential=True)
            except aiozk.ConnectionLossError:
                pass
            else:
                return

            (children,), _ = await self._client.get_children(self._path, auto_retry=True)

            if next((True for child in children if child.startswith(item_name_prefix)), False):
                return


class Queue(_QueueBase):
    def enqueue(self, item_data: typing.Union[bytes, bytearray]) -> typing.Awaitable[None]:
        return self._enqueue("", item_data)


_MAX_PRIORITY_QUEUE_ITEM_PRIORITY = 999


class PriorityQueue(_QueueBase):
    MAX_ITEM_PRIORITY = _MAX_PRIORITY_QUEUE_ITEM_PRIORITY

    def enqueue(self, item_priority: int, item_data: typing.Union[bytes, bytearray]) \
        -> typing.Awaitable[None]:
        assert item_priority in range(_MAX_PRIORITY_QUEUE_ITEM_PRIORITY + 1)
        return self._enqueue("%.3d." % (_MAX_PRIORITY_QUEUE_ITEM_PRIORITY - item_priority)
                             , item_data)
