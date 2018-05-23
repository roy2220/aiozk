import asyncio
import uuid

from asyncio_toolkit import utils

from . import method_lock
import aiozk


class Barrier:
    def __init__(self, client: aiozk.Client, path: str) -> None:
        self._client = client
        self._path = client.normalize_path(path)

    async def set(self) -> None:
        await self._client.set_data(self._path, b"\0", auto_retry=True)

    async def clear(self) -> None:
        await self._client.set_data(self._path, b"", auto_retry=True)

    async def wait_for(self) -> None:
        watcher = None

        try:
            while True:
                (data, _), watcher = await self._client.get_data_w(self._path, auto_retry=True)

                if len(data) >= 1:
                    if not watcher.is_removed():
                        watcher.remove()

                    return

                await watcher.wait_for_event()
        except Exception:
            if self._client.is_running():
                if watcher is not None and not watcher.is_removed():
                    watcher.remove()

            raise


class DoubleBarrier:
    def __init__(self, client: aiozk.Client, path: str, size: int) -> None:
        method_lock.init(self, client.get_loop())
        self._client = client
        self._path = client.normalize_path(path)
        self._size = size
        self._ready_signal_path = self._path + "/ready"
        self._my_waiter_path = ""

    @method_lock.locked_method
    async def enter(self) -> None:
        assert self._my_waiter_path == "", repr(self._my_waiter_path)
        my_waiter_path = None
        watcher = None

        try:
            while True:
                result, watcher = await self._client.exists_w(self._ready_signal_path
                                                              , auto_retry=True)

                if my_waiter_path is None:
                    my_waiter_path = self._path + "/" + uuid.uuid4().hex

                    try:
                        await self._client.create(my_waiter_path, ephemeral=True, auto_retry=True)
                    except aiozk.NodeExistsError:
                        pass

                if result is not None:
                    if not watcher.is_removed():
                        watcher.remove()

                    break

                children, = await self._client.get_children(self._path, auto_retry=True)

                if len(children) >= self._size:
                    try:
                        await self._client.create(self._ready_signal_path, auto_retry=True)
                    except aiozk.NodeExistsError:
                        pass

                    if not watcher.is_removed():
                        watcher.remove()

                    break

                await watcher.wait_for_event()
        except Exception:
            if self._client.is_running():
                async def block() -> None:
                    if my_waiter_path is not None:
                        try:
                            await self._client.delete(my_waiter_path, auto_retry=True)
                        except aiozk.NoNodeError:
                            pass

                    if watcher is not None and not watcher.is_removed():
                        watcher.remove()

                await utils.delay_cancellation(block(), loop=self._client.get_loop())

            raise

        self._my_waiter_path = my_waiter_path

    @method_lock.locked_method
    async def leave(self) -> None:
        assert self._my_waiter_path != ""
        my_waiter_path = self._my_waiter_path
        self._my_waiter_path = ""
        watcher = None

        try:
            ready_signal_name = self._ready_signal_path.rsplit("/", 1)[1]
            my_waiter_name = my_waiter_path.rsplit("/", 1)[1]
            my_waiter_index = 0

            while True:
                children, = await self._client.get_children(self._path, auto_retry=True)
                waiter_names = sorted(child for child in children if child != ready_signal_name)
                is_left = len(waiter_names) == len(children)

                if my_waiter_index < 0:
                    if is_left or len(waiter_names) == 0:
                        break
                else:
                    if is_left or len(waiter_names) == 1:
                        assert is_left or my_waiter_name == waiter_names[0], repr(my_waiter_name)

                        try:
                            await self._client.delete(my_waiter_path, auto_retry=True)
                        except aiozk.NoNodeError:
                            pass

                        break

                    my_waiter_index = waiter_names.index(my_waiter_name)

                    if my_waiter_index == 0:
                        result, watcher = await self._client.exists_w(self._path + "/" \
                            + waiter_names[-1], auto_retry=True)
                    else:
                        try:
                            await self._client.delete(my_waiter_path, auto_retry=True)
                        except aiozk.NoNodeError:
                            pass

                        my_waiter_index = -1

                if my_waiter_index < 0:
                    result, watcher = await self._client.exists_w(self._path + "/" + waiter_names[0]
                                                                  , auto_retry=True)

                if result is None:
                    watcher.remove()  # type: ignore
                else:
                    await watcher.wait_for_event()  # type: ignore

            if not is_left:
                try:
                    await self._client.delete(self._ready_signal_path, auto_retry=True)
                except aiozk.NoNodeError:
                    pass
        except Exception:
            if self._client.is_running():
                async def block() -> None:
                    try:
                        await self._client.delete(my_waiter_path, auto_retry=True)
                    except aiozk.NoNodeError:
                        pass

                    if watcher is not None and not watcher.is_removed():
                        watcher.remove()

                await utils.delay_cancellation(block(), loop=self._client.get_loop())

            raise
