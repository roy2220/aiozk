import asyncio
import heapq
import logging
import re
import time
import typing

from . import errors
from . import protocol
from . import session


ServerAddress = typing.Tuple[str, int]


class _Server():
    def __init__(self, address: ServerAddress) -> None:
        self.address = address
        self.last_address_access_time = 0.0
        self.weight = 0.0

    def up(self) -> None:
        self.weight = 0.0

    def down(self) -> None:
        self.weight += 0.75

    async def get_address(self, loop: asyncio.AbstractEventLoop
                          , logger: logging.Logger) -> ServerAddress:
        now = time.time()
        wait_time = max(self.last_address_access_time + (1.5 ** self.weight - 1) - now, 0.0)
        logger.info("server address getting: server_address={!r} wait_time={!r}"
                    .format(self.address, wait_time))

        if wait_time > 0.001:
            await asyncio.sleep(wait_time, loop=loop)

        self.last_address_access_time = now
        return self.address

    def __lt__(self, other: "_Server") -> bool:
        return self.weight < other.weight


class Client:
    def __init__(self, *,
        loop=asyncio.get_event_loop(),
        logger=logging.getLogger(),
        session_timeout=4.0,
        server_addresses: typing.Iterable[ServerAddress]=(("127.0.0.1", 2181),),
        path_prefix = "/",
        auth_infos: typing.Iterable[session.AuthInfo]=(),
        default_acl: typing.Iterable[protocol.ACL]=(protocol.Ids.OPEN_ACL_UNSAFE,),
    ) -> None:
        self._session = session.Session(loop, logger, session_timeout)
        self._servers: typing.List[_Server] = list((_Server(server_address) for server_address \
            in set(server_addresses)))
        assert len(self._servers) >= 1, repr(self._servers)
        assert path_prefix.startswith("/"), repr(path_prefix)
        self._path_prefix = _RE1.sub("/", path_prefix + "/")
        self._auth_infos: typing.Set[session.AuthInfo] = set(auth_infos)
        self._default_acl = tuple(default_acl)
        assert len(self._default_acl) >= 1, repr(self._default_acl)
        self._task: asyncio.Future[None] = asyncio.Future(loop=self.get_loop())
        self._task.set_result(None)

    def add_session_listener(self) -> session.SessionListener:
        return self._session.add_listener()

    def remove_session_listener(self, session_listener: session.SessionListener) -> None:
        self._session.remove_listener(session_listener)

    async def start(self) -> None:
        assert not self.is_running()
        task = self.get_loop().create_task(self._run())
        session_listener = self._session.add_listener()
        await session_listener.get_state_change()
        self._session.remove_listener(session_listener)
        self._task = task

    def stop(self) -> None:
        assert self.is_running()
        self._task.cancel()

    def wait_for_stopped(self) -> "asyncio.Future[None]":
         return self._task if self._task.done() else asyncio.shield(self._task)

    def normalize_path(self, path: str) -> str:
        assert len(path) >= 1, repr(path)
        path = _RE1.sub("/", path + "/")

        if path[0] == "/":
            if path != "/":
                path = path[:-1]
        else:
            path = self._path_prefix + path[:-1]

        return path

    def is_running(self) -> bool:
        return not self._task.done()

    def create_op(self, path: str, data: typing.Union[bytes, bytearray]=b"", acl: typing.Iterable\
        [protocol.ACL]=(), ephemeral=False, sequential=False) -> protocol.Op:
        path = self.normalize_path(path)

        if acl is ():
            acl = self._default_acl

        if ephemeral:
            if sequential:
                flags = protocol.CreateMode.EPHEMERAL_SEQUENTIAL
            else:
                flags = protocol.CreateMode.EPHEMERAL
        else:
            if sequential:
                flags = protocol.CreateMode.PERSISTENT_SEQUENTIAL
            else:
                flags = protocol.CreateMode.PERSISTENT

        return protocol.OpCode.CREATE, protocol.CreateRequest(
            path=path,
            data=bytes(data),
            acl=tuple(acl),
            flags=flags,
        )

    async def create(self, *args, **kwargs) -> protocol.CreateResponse:
        assert self.is_running()
        auto_retry = kwargs.pop("auto_retry", False)
        return await self._session.execute_operation(*self.create_op(*args, **kwargs), auto_retry)

    def delete_op(self, path: str, version=-1) -> protocol.Op:
        path = self.normalize_path(path)

        return protocol.OpCode.DELETE, protocol.DeleteRequest(
            path=path,
            version=version,
        )

    async def delete(self, *args, **kwargs) -> None:
        assert self.is_running()
        auto_retry = kwargs.pop("auto_retry", False)
        await self._session.execute_operation(*self.delete_op(*args, **kwargs), auto_retry)

    def set_data_op(self, path: str, data: bytes, version=-1) -> protocol.Op:
        path = self.normalize_path(path)

        return protocol.OpCode.SET_DATA, protocol.SetDataRequest(
            path=path,
            data=data,
            version=version,
        )

    async def set_data(self, *args, **kwargs) -> protocol.SetDataResponse:
        assert self.is_running()
        auto_retry = kwargs.pop("auto_retry", False)
        return await self._session.execute_operation(*self.set_data_op(*args, **kwargs), auto_retry)

    def check_op(self, path: str, version=-1) -> protocol.Op:
        path = self.normalize_path(path)

        return protocol.OpCode.CHECK, protocol.CheckVersionRequest(
            path=path,
            version=version,
        )

    async def check(self, *args, **kwargs) -> None:
        assert self.is_running()
        auto_retry = kwargs.pop("auto_retry", False)
        await self._session.execute_operation(*self.check_op(*args, **kwargs), auto_retry)

    async def multi(self, ops: typing.Iterable[protocol.Op], *
                    , auto_retry=False) -> protocol.MultiResponse:
        assert self.is_running()

        return await self._session.execute_operation(
            protocol.OpCode.MULTI,

            protocol.MultiRequest(
                ops=tuple(ops),
            ),

            auto_retry,
        )

    async def exists(self, path: str, watch: bool=False, *, auto_retry=False) -> typing.Tuple\
        [typing.Optional[protocol.ExistsResponse], typing.Optional[session.Watcher]]:
        assert self.is_running()
        path = self.normalize_path(path)
        watcher = None

        if watch:
            def on_completed(non_error_class: typing.Optional[typing.Type[errors.Error]]) -> None:
                nonlocal watcher

                if non_error_class is None:
                    watcher_type = session.WatcherType.DATA
                elif non_error_class is errors.NoNodeError:
                    watcher_type = session.WatcherType.EXIST
                else:
                    assert False, repr(non_error_class)

                watcher = session.Watcher(watcher_type, path, self.get_loop())
                self._session.add_watcher(watcher)
        else:
            on_completed = None

        return await self._session.execute_operation(
            protocol.OpCode.EXISTS,

            protocol.ExistsRequest(
                path=path,
                watch=watch,
            ),

            auto_retry,
            on_completed,
            (errors.NoNodeError,),
        ), watcher

    async def get_data(self, path: str, watch: bool=False, *, auto_retry=False) -> typing.Tuple\
        [protocol.GetDataResponse, typing.Optional[session.Watcher]]:
        assert self.is_running()
        path = self.normalize_path(path)
        watcher = None

        if watch:
            def on_completed(non_error_class: typing.Optional[typing.Type[errors.Error]]) -> None:
                nonlocal watcher
                watcher = session.Watcher(session.WatcherType.DATA, path, self.get_loop())
                self._session.add_watcher(watcher)
        else:
            on_completed = None

        return await self._session.execute_operation(
            protocol.OpCode.GET_DATA,

            protocol.GetDataRequest(
                path=path,
                watch=watch,
            ),

            auto_retry,
            on_completed,
        ), watcher

    async def get_children(self, path: str, watch: bool=False, *, auto_retry=False) -> typing\
        .Tuple[protocol.GetChildrenResponse, typing.Optional[session.Watcher]]:
        assert self.is_running()
        path = self.normalize_path(path)
        watcher = None

        if watch:
            def on_completed(non_error_class: typing.Optional[typing.Type[errors.Error]]) -> None:
                nonlocal watcher
                watcher = session.Watcher(session.WatcherType.CHILD, path, self.get_loop())
                self._session.add_watcher(watcher)
        else:
            on_completed = None

        return await self._session.execute_operation(
            protocol.OpCode.GET_CHILDREN,

            protocol.GetChildrenRequest(
                path=path,
                watch=watch,
            ),

            auto_retry,
            on_completed,
        ), watcher

    async def get_children2(self, path: str, watch: bool=False, *, auto_retry=False) -> typing\
        .Tuple[protocol.GetChildren2Response, typing.Optional[session.Watcher]]:
        assert self.is_running()
        path = self.normalize_path(path)
        watcher = None

        if watch:
            def on_completed(non_error_class: typing.Optional[typing.Type[errors.Error]]) -> None:
                nonlocal watcher
                watcher = session.Watcher(session.WatcherType.CHILD, path, self.get_loop())
                self._session.add_watcher(watcher)
        else:
            on_completed = None

        return await self._session.execute_operation(
            protocol.OpCode.GET_CHILDREN2,

            protocol.GetChildrenRequest(
                path=path,
                watch=watch,
            ),

            auto_retry,
            on_completed,
        )

    async def get_acl(self, path: str, *, auto_retry=False) -> protocol.GetACLResponse:
        assert self.is_running()
        path = self.normalize_path(path)

        return await self._session.execute_operation(
            protocol.OpCode.GET_ACL,

            protocol.GetACLRequest(
                path=path,
            ),

            auto_retry,
        )

    async def set_acl(self, path: str, acl: typing.Iterable[protocol.ACL]=(), version=-1, *
                      , auto_retry=False) -> protocol.SetACLResponse:
        assert self.is_running()
        path = self.normalize_path(path)

        if acl is ():
            acl = self._default_acl

        return await self._session.execute_operation(
            protocol.OpCode.SET_ACL,

            protocol.SetACLRequest(
                path=path,
                acl=tuple(acl),
                version=version,
            ),

            auto_retry,
        )

    async def sync(self, path: str, *, auto_retry=False) -> protocol.SyncResponse:
        assert self.is_running()
        path = self.normalize_path(path)

        return await self._session.execute_operation(
            protocol.OpCode.SYNC,

            protocol.SyncRequest(
                path=path,
            ),

            auto_retry,
        )

    async def create_p(self, path: str) -> None:
        path = self.normalize_path(path)

        if path == "/":
            return

        node_names = path[1:].split("/")

        while True:
            path = ""

            try:
                for node_name in node_names:
                    path += "/" + node_name

                    try:
                        await self.create(path, auto_retry=True)
                    except errors.NodeExistsError:
                        pass
            except errors.NoNodeError:
                continue
            else:
                return

    async def delete_r(self, path: str) -> None:
        path = self.normalize_path(path)

        while True:
            try:
                (children,), _ = await self.get_children(path, auto_retry=True)
            except errors.NoNodeError:
                return

            for child in children:
                await self.delete_r(path + "/" + child)

            try:
                await self.delete(path, auto_retry=True)
            except errors.NotEmptyError:
                continue
            except errors.NoNodeError:
                return
            else:
                return

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._session.get_loop()

    def get_logger(self) -> logging.Logger:
        return self._session.get_logger()

    async def _run(self) -> None:
        while True:
            server = heapq.heappop(self._servers)

            try:
                host_name, port_number = await server.get_address(self.get_loop()
                                                                  , self.get_logger())
                await self._session.connect(host_name, port_number, self._auth_infos)
                server.up()
                await self._session.dispatch()
                assert False
            except (
                ConnectionRefusedError,
                ConnectionResetError,
                TimeoutError,
                asyncio.TimeoutError,
                asyncio.IncompleteReadError,
            ):
                server.down()
            except (
                asyncio.CancelledError,
                errors.SessionExpiredError,
                errors.AuthFailedError
            ):
                break
            except Exception:
                self.get_logger().exception("client running failure:")
                break
            finally:
                heapq.heappush(self._servers, server)

        if not self._session.is_closed():
            self._session.close()

        self._session.remove_all_listeners()


_RE1 = re.compile(r"//+")
