import uuid

from . import method_lock
import aiozk


class Lock:
    def __init__(self, client: aiozk.Client, path: str) -> None:
        method_lock.init(self, client.get_loop())
        self._client = client
        self._path = path
        self._my_locker_path = ""

    @method_lock.locked_method
    async def acquire(self) -> None:
        assert not self._is_locked()
        my_locker_name_prefix = uuid.uuid4().hex + "-"
        my_locker_path = None

        while True:
            try:
                my_locker_path, = await self._client.create(self._path + "/" + my_locker_name_prefix
                                                            , ephemeral=True, sequential=True)
            except aiozk.ConnectionLossError:
                pass

            (locker_names,), _ = await self._client.get_children(self._path, auto_retry=True)

            if my_locker_path is not None:
                break

            for locker_name in locker_names:
                if locker_name.startswith(my_locker_name_prefix):
                    my_locker_path = self._path + "/" + locker_name
                    break

            if my_locker_path is not None:
                break

        my_locker_name = my_locker_path.rsplit("/", 1)[1]

        while True:
            locker_names2 = sorted(locker_names, key=lambda locker_name: locker_name.rsplit("-"
                                                                                            , 1)[1])
            my_locker_index = locker_names2.index(my_locker_name)

            if my_locker_index == 0:
                break

            result, watcher = await self._client.exists(self._path + "/" + locker_names2\
                [my_locker_index - 1], True, auto_retry=True)

            if result is not None:
                await watcher.wait_for_event()

            (locker_names,), _ = await self._client.get_children(self._path, auto_retry=True)

        self._my_locker_path = my_locker_path

    @method_lock.locked_method
    async def release(self) -> None:
        assert self._is_locked()
        await self._client.delete(self._my_locker_path, auto_retry=True)
        self._my_locker_path = ""

    @method_lock.locked_method
    async def is_locked(self) -> bool:
        return self._is_locked()

    def _is_locked(self) -> bool:
        return self._my_locker_path != ""


class SharedLock(Lock):
    @method_lock.locked_method
    async def acquire_shared(self) -> None:
        assert not self._is_locked()
        my_locker_name_prefix = _SHARED_LOCKER_NAME_PREFIX + uuid.uuid4().hex + "-"
        my_locker_path = None

        while True:
            try:
                my_locker_path, = await self._client.create(self._path + "/" + my_locker_name_prefix
                                                            , ephemeral=True, sequential=True)
            except aiozk.ConnectionLossError:
                pass

            (locker_names,), _ = await self._client.get_children(self._path, auto_retry=True)

            if my_locker_path is not None:
                break

            for locker_name in locker_names:
                if locker_name.startswith(my_locker_name_prefix):
                    my_locker_path = self._path + "/" + locker_name
                    break

            if my_locker_path is not None:
                break

        my_locker_name = my_locker_path.rsplit("/", 1)[1]

        while True:
            locker_names2 = (locker_name for locker_name in locker_names
                                         if not locker_name.startswith(_SHARED_LOCKER_NAME_PREFIX) \
                                            or locker_name == my_locker_name)
            locker_names3 = sorted(locker_names2
                                   , key=lambda locker_name: locker_name.rsplit("-", 1)[1])
            my_locker_index = locker_names3.index(my_locker_name)

            if my_locker_index == 0:
                break

            result, watcher = await self._client.exists(self._path + "/" + locker_names3\
                [my_locker_index - 1], True, auto_retry=True)

            if result is not None:
                await watcher.wait_for_event()

            (locker_names,), _ = await self._client.get_children(self._path, auto_retry=True)

        self._my_locker_path = my_locker_path


_SHARED_LOCKER_NAME_PREFIX = "shared-"
