import uuid

from . import method_lock
import aiozk


class Barrier:
    def __init__(self, client: aiozk.Client, path: str) -> None:
        self._client = client
        self._path = path

    async def set(self) -> None:
        await self._client.set_data(self._path, b"\0", auto_retry=True)

    async def clear(self) -> None:
        await self._client.set_data(self._path, b"", auto_retry=True)

    async def wait_for(self) -> None:
        while True:
            (data, _), watcher = await self._client.get_data(self._path, True, auto_retry=True)

            if len(data) >= 1:
                return

            await watcher.wait_for_event()


class DoubleBarrier:
    def __init__(self, client: aiozk.Client, path: str, length: int) -> None:
        method_lock.init(self, client.get_loop())
        self._client = client
        self._path = path
        self._length = length
        self._ready_signal_path = self._path + "/ready"
        self._my_waiter_path = ""

    @method_lock.locked_method
    async def enter(self) -> None:
        assert self._my_waiter_path == ""
        my_waiter_path = self._path + "/" + uuid.uuid4().hex

        while True:
            result, watcher = await self._client.exists(self._ready_signal_path, True
                                                        , auto_retry=True)

            try:
                await self._client.create(my_waiter_path, ephemeral=True, auto_retry=True)
            except aiozk.NodeExistsError:
                pass

            if result is not None:
                break

            (children,), _ = await self._client.get_children(self._path, auto_retry=True)

            if len(children) >= self._length:
                try:
                    await self._client.create(self._ready_signal_path, auto_retry=True)
                except aiozk.NodeExistsError:
                    pass

                break

            await watcher.wait_for_event()

        self._my_waiter_path = my_waiter_path

    @method_lock.locked_method
    async def leave(self) -> None:
        assert self._my_waiter_path != ""
        ready_signal_name = self._ready_signal_path.rsplit("/", 1)[1]
        my_waiter_name = self._my_waiter_path.rsplit("/", 1)[1]
        my_waiter_index = 0

        while True:
            (children,), _ = await self._client.get_children(self._path, auto_retry=True)
            waiter_names = sorted(child for child in children if child != ready_signal_name)
            is_left = len(waiter_names) == len(children)

            if my_waiter_index < 0:
                if is_left or len(waiter_names) == 0:
                    break
            else:
                if is_left or len(waiter_names) == 1:
                    assert is_left or my_waiter_name == waiter_names[0]

                    try:
                        await self._client.delete(self._my_waiter_path, auto_retry=True)
                    except aiozk.NoNodeError:
                        pass

                    break

                my_waiter_index = waiter_names.index(my_waiter_name)

                if my_waiter_index == 0:
                    result, watcher = await self._client.exists(self._path + "/" + waiter_names[-1]
                                                                , True, auto_retry=True)

                    if result is not None:
                        await watcher.wait_for_event()
                else:
                    try:
                        await self._client.delete(self._my_waiter_path, auto_retry=True)
                    except aiozk.NoNodeError:
                        pass

                    my_waiter_index = -1

            if my_waiter_index < 0:
                result, watcher = await self._client.exists(self._path + "/" + waiter_names[0]
                                                            , True, auto_retry=True)

                if result is not None:
                    await watcher.wait_for_event()

        if not is_left:
            try:
                await self._client.delete(self._ready_signal_path, auto_retry=True)
            except aiozk.NoNodeError:
                pass

        self._my_waiter_path = ""
