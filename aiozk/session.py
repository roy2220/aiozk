import asyncio
import enum
import logging
import time
import typing

from asyncio_toolkit import utils
from asyncio_toolkit.deque import Deque
import async_generator

from . import errors
from . import protocol
from .record import *
from .transport import Transport


class SessionState(enum.IntEnum):
    CONNECTING = enum.auto()
    CONNECTED = enum.auto()
    CLOSED = enum.auto()
    AUTH_FAILED = enum.auto()


class SessionEventType(enum.IntEnum):
    # for SessionState.CONNECTING
    CONNECTING = enum.auto()
    DISCONNECTED = enum.auto()

    # for SessionState.CONNECTED
    CONNECTED = enum.auto()

    # for SessionState.CLOSED
    CLOSED = enum.auto()
    SESSION_EXPIRED = enum.auto()

    # for SessionState.AUTH_FAILED
    AUTH_FAILED = enum.auto()


class SessionListener:
    def __init__(self) -> None:
        self._state_changes: typing.Optional[asyncio.Queue[typing.Optional[typing.Tuple\
            [SessionState, SessionEventType]]]] = None

    async def get_state_change(self) -> typing.Optional[typing.Tuple[SessionState
                                                                     , SessionEventType]]:
        if self._state_changes is None:
            return None

        return await self._state_changes.get()

    def is_active(self) -> bool:
        return self._state_changes is not None


class WatcherType(enum.IntEnum):
    DATA, EXIST, CHILD = range(3)


class Watcher:
    def __init__(self, type_: WatcherType, path: str, loop: asyncio.AbstractEventLoop) -> None:
        self._type = type_
        self._path = path
        self._event: asyncio.Future[protocol.WatcherEventType] = utils.Future(loop=loop)

    def get_type(self) -> WatcherType:
        return self._type

    def get_path(self) -> str:
        return self._path

    def wait_for_event(self) -> "asyncio.Future[protocol.WatcherEventType]":
        return self._event if self._event.done() else utils.shield(self._event)

    def remove(self):
        assert not self.is_removed()
        self._event.cancel()

    def is_removed(self) -> bool:
        return self._event.done()

    def __repr__(self) -> str:
        return "<Watcher: type={!r} path={!r}>".format(self._type, self._path)


OperationCompletionCallback = typing.Callable[[typing.Optional[typing.Type[errors.Error]]], None]


class _Operation(typing.NamedTuple):
    op_code: protocol.OpCode
    request: typing.Any
    auto_retry: bool
    non_error_classes: typing.Sequence[typing.Type[errors.Error]]
    on_completed: typing.Optional[OperationCompletionCallback]
    response: asyncio.Future


AuthInfo = typing.Tuple[str, bytes]


class Session:
    def __init__(self, loop: typing.Optional[asyncio.AbstractEventLoop]
                 , logger: typing.Optional[logging.Logger], timeout: float) -> None:
        self._transport = Transport(loop, logger)
        self._timeout = timeout
        self._state = SessionState.CLOSED
        self._listeners: typing.Set[SessionListener] = set()
        self._id = Long(0)
        self._password = b""
        self._last_zxid = Long(0)
        self._next_xid = 1
        self._pending_operations1: Deque[_Operation] = Deque(_MAX_NUMBER_OF_PENDING_OPERATIONS
                                                             , self.get_loop())
        self._pending_operations2: typing.Dict[int, _Operation] = {}
        self._watchers: typing.Tuple[typing.Dict[str, typing.Set[Watcher]]
                                     , typing.Dict[str, typing.Set[Watcher]]
                                     , typing.Dict[str, typing.Set[Watcher]]] = ({}, {}, {})

    def add_listener(self) -> SessionListener:
        listener = SessionListener()
        listener._state_changes = asyncio.Queue(loop=self.get_loop())
        self._listeners.add(listener)
        return listener

    def remove_listener(self, listener: SessionListener) -> None:
        assert listener.is_active()
        listener._state_changes.put_nowait(None)  # type: ignore
        listener._state_changes = None
        self._listeners.remove(listener)

    def remove_all_listeners(self) -> None:
        for listener in self._listeners:
            listener._state_changes.put_nowait(None)  # type: ignore
            listener._state_changes = None

        self._listeners.clear()

    async def connect(self, host_name: str, port_number: int, connect_deadline: float
                      , auth_infos: typing.Iterable[AuthInfo]) -> None:
        if self.is_closed():
            event_type = SessionEventType.CONNECTING
        else:
            event_type = SessionEventType.DISCONNECTED

        self._set_state(SessionState.CONNECTING, event_type)

        async with self._connect_transport(host_name, port_number, connect_deadline) as transport:
            async with self._do_connect(transport, connect_deadline):
                await self._authenticate(transport, connect_deadline, auth_infos)
                await self._rewatch(transport, connect_deadline)

        self._set_state(SessionState.CONNECTED, SessionEventType.CONNECTED)

    async def dispatch(self) -> None:
        assert self._state is SessionState.CONNECTED, repr(self._state)
        await utils.wait_for_any((self._send_requests(), self._receive_responses())
                                 , loop=self.get_loop())

    def close(self) -> None:
        assert not self.is_closed()
        self._reset(SessionState.CLOSED, SessionEventType.CLOSED)

    async def execute_operation(self, op_code: protocol.OpCode, request, auto_retry: bool
                                , non_error_classes: typing.Sequence[typing.Type[errors.Error]]=()
                                , on_operation_completed: typing.Optional\
        [OperationCompletionCallback]=None):
        assert isinstance(request, protocol.get_request_class(op_code)), repr((op_code, request))
        error_class = _FINAL_SESSION_STATE_2_ERROR_CLASS.get(self._state, None)

        if error_class is not None:
            error_message = "request: {!r}".format(request)
            raise error_class(error_message)

        operation = _Operation(
            op_code=op_code,
            request=request,
            auto_retry=auto_retry,
            non_error_classes=non_error_classes,
            on_completed=on_operation_completed,
            response=self.get_loop().create_future(),
        )

        try:
            await self._pending_operations1.insert_tail(operation)
        except errors.Error as error:
            error_message = "request: {!r}".format(request)
            error.args = error_message,
            raise

        try:
            return await operation.response
        except Exception:
            if not self._pending_operations1.is_closed():
                self._pending_operations1.try_remove_item(operation)

            raise

    def add_watcher(self, watcher: Watcher) -> None:
        path_2_watchers = self._watchers[watcher._type]
        watchers = path_2_watchers.get(watcher._path, None)

        if watchers is None:
            watchers = set()
            path_2_watchers[watcher._path] = watchers

        watchers.add(watcher)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._transport.get_loop()

    def get_logger(self) -> logging.Logger:
        return self._transport.get_logger()

    def get_timeout(self) -> float:
        return self._timeout

    def get_read_timeout(self) -> float:
        return self._timeout * 2 / 3

    def get_id(self) -> int:
        return self._id

    def is_closed(self) -> bool:
        return self._state in _FINAL_SESSION_STATES

    def _set_state(self, new_state: SessionState, event_type: SessionEventType) -> None:
        old_state = self._state
        error_class: typing.Optional[typing.Type[errors.Error]] = None

        if old_state is SessionState.CONNECTING:
            if new_state is old_state:
                return

            if new_state is SessionState.CONNECTED:
                pass
            elif new_state is SessionState.CLOSED:
                if event_type is SessionEventType.SESSION_EXPIRED:
                    error_class = errors.SessionExpiredError
                else:
                    error_class = errors.ConnectionLossError
            elif new_state is SessionState.AUTH_FAILED:
                error_class = errors.AuthFailedError
            else:
                assert False, repr(new_state)
        elif old_state is SessionState.CONNECTED:
            if new_state is SessionState.CONNECTING:
                error_class = errors.ConnectionLossError
            elif new_state is SessionState.CLOSED:
                error_class = errors.ConnectionLossError
            else:
                assert False, repr(new_state)
        elif old_state is SessionState.CLOSED:
            assert new_state is SessionState.CONNECTING, repr(new_state)
        elif old_state is SessionState.AUTH_FAILED:
            assert new_state is SessionState.CONNECTING, repr(new_state)
        else:
            assert False, repr(old_state)

        if error_class is not None:
            need_retry = error_class is errors.ConnectionLossError
            error_class2 = _FINAL_SESSION_STATE_2_ERROR_CLASS.get(new_state, None)
            operation: typing.Optional[_Operation]

            if error_class2 is None:
                self._pending_operations1.commit_item_removals(len(self._pending_operations2))

                for operation in self._pending_operations2.values():
                    if operation.response.cancelled():
                        continue

                    if need_retry and operation.auto_retry:
                        self._pending_operations1.try_insert_tail(operation)
                    else:
                        error_message = "request: {!r}".format(operation.request)
                        operation.response.set_exception(error_class(error_message))

                self._pending_operations2.clear()
            else:
                if not self._transport.is_closed():
                    self._do_close(self._transport)
                    self._transport.close()

                while True:
                    operation = self._pending_operations1.try_remove_head()

                    if operation is None:
                        break

                    if operation.response.cancelled():
                        continue

                    error_message = "request: {!r}".format(operation.request)
                    operation.response.set_exception(error_class2(error_message))

                self._pending_operations1.close(error_class2)

                for operation in self._pending_operations2.values():
                    if operation.response.cancelled():
                        continue

                    error_message = "request: {!r}".format(operation.request)

                    if need_retry and operation.auto_retry:
                        operation.response.set_exception(error_class2(error_message))
                    else:
                        operation.response.set_exception(error_class(error_message))

                self._pending_operations2.clear()

                for path_2_watchers in self._watchers:
                    for watchers in path_2_watchers.values():
                        for watcher in watchers:
                            if watcher.is_removed():
                                continue

                            error_message = "watcher: {!r}".format(watcher)
                            watcher._event.set_exception(error_class2(error_message))

                    path_2_watchers.clear()

        self._state = new_state
        self.get_logger().info("session state change: session_id={:#x} session_state={!r}"
                               " session_event_type={!r}".format(self._id, self._state, event_type))

        for listener in self._listeners:
            listener._state_changes.put_nowait((self._state, event_type))  # type: ignore

    @async_generator.asynccontextmanager
    async def _connect_transport(self, host_name: str, port_number: int
                                 , connect_deadline: float) -> typing.AsyncIterator[Transport]:
        transport = Transport(self.get_loop(), self.get_logger())
        connect_timeout = max(connect_deadline - time.monotonic(), 0.0)
        await transport.connect(host_name, port_number, connect_timeout)

        try:
            yield transport
        except Exception:
            transport.close()
            raise

        if not self._transport.is_closed():
            self._transport.close()

        self._transport = transport

    @async_generator.asynccontextmanager
    async def _do_connect(self, transport: Transport
                          , connect_deadline: float) -> typing.AsyncIterator[None]:
        buffer = bytearray()

        request = protocol.ConnectRequest(
            protocol_version=_PROTOCOL_VERSION,
            last_zxid_seen=self._last_zxid,
            time_out=int(self._timeout * 1000),
            session_id=self._id,
            passwd=self._password,
        )

        serialize_record(request, buffer)
        transport.write(buffer)
        connect_timeout = max(connect_deadline - time.monotonic(), 0.0)
        data = await transport.read(connect_timeout)
        response: protocol.ConnectResponse
        response, _ = deserialize_record(protocol.ConnectResponse, data)

        if response.time_out <= 0:
            self._reset(SessionState.CLOSED, SessionEventType.SESSION_EXPIRED)
            error_message = "request: {!r}".format(request)
            raise errors.SessionExpiredError(error_message)

        try:
            yield
        except Exception:
            if self._id == 0:
                self._do_close(transport)

            raise

        self._timeout = response.time_out / 1000
        self._id = response.session_id
        self._password = response.passwd

    def _do_close(self, transport: Transport) -> None:
        buffer = bytearray()
        request_header = protocol.RequestHeader(xid=self._get_xid()
                                                , type=protocol.OpCode.CLOSE_SESSION)
        serialize_record(request_header, buffer)
        transport.write(buffer)

    async def _authenticate(self, transport: Transport, connect_deadline: float
                            , auth_infos: typing.Iterable[AuthInfo]) -> None:
        for auth_info in auth_infos:
            connect_timeout = max(connect_deadline - time.monotonic(), 0.0)

            try:
                await self._execute_operation(transport, connect_timeout, -4, protocol.OpCode.AUTH
                                              , protocol.AuthPacket(
                    type=0,
                    scheme=auth_info[0],
                    auth=auth_info[1])
                )
            except errors.AuthFailedError:
                self._reset(SessionState.AUTH_FAILED, SessionEventType.AUTH_FAILED)
                raise

    async def _rewatch(self, transport: Transport, connect_deadline: float) -> None:
        requests = []
        request_size = _SETWATCHES_OVERHEAD_SIZE
        paths: typing.Tuple[typing.List[str], typing.List[str], typing.List[str]] = ([], [], [])

        for watcher_type, path_2_watchers in enumerate(self._watchers):
            for path, watchers in path_2_watchers.items():
                if all(watcher.is_removed() for watcher in watchers):
                    continue

                path_size = _STRING_OVERHEAD_SIZE + len(path.encode())

                if request_size + path_size > _MAX_SETWATCHES_SIZE:
                    requests.append(protocol.SetWatches(
                        relative_zxid=self._last_zxid,
                        data_watches=tuple(paths[WatcherType.DATA]),
                        exist_watches=tuple(paths[WatcherType.EXIST]),
                        child_watches=tuple(paths[WatcherType.CHILD]),
                    ))

                    request_size = _SETWATCHES_OVERHEAD_SIZE
                    paths[0].clear()
                    paths[1].clear()
                    paths[2].clear()

                paths[watcher_type].append(path)
                request_size += path_size

        if request_size > _SETWATCHES_OVERHEAD_SIZE:
            requests.append(protocol.SetWatches(
                relative_zxid=self._last_zxid,
                data_watches=tuple(paths[WatcherType.DATA]),
                exist_watches=tuple(paths[WatcherType.EXIST]),
                child_watches=tuple(paths[WatcherType.CHILD]),
            ))

        for request in requests:
            connect_timeout = max(connect_deadline - time.monotonic(), 0.0)
            await self._execute_operation(transport, connect_timeout, -8
                                          , protocol.OpCode.SET_WATCHES, request)

    async def _execute_operation(self, transport: Transport, read_timeout: float, xid: int
                                 , op_code: protocol.OpCode, request):
        assert isinstance(request, protocol.get_request_class(op_code)), repr((op_code, request))
        buffer = bytearray()
        request_header = protocol.RequestHeader(xid=xid, type=op_code)
        serialize_record(request_header, buffer)
        serialize_record(request, buffer)
        transport.write(buffer)

        while True:
            data = await transport.read(read_timeout)
            reply_header: protocol.ReplyHeader
            reply_header, data_offset = deserialize_record(protocol.ReplyHeader, data)

            if reply_header.zxid > 0:
                self._last_zxid = reply_header.zxid

            if reply_header.err != 0:
                error_class = errors.get_error_class(reply_header.err)
                error_message = "request: {!r}".format(request)
                raise error_class(error_message)

            if reply_header.xid == xid:
                break

            if reply_header.xid == -1:  # -1 means notification
                watcher_event: protocol.WatcherEvent
                watcher_event, _ = deserialize_record(protocol.WatcherEvent, data, data_offset)
                self._fire_watcher_event(protocol.WatcherEventType(watcher_event.type)
                                         , watcher_event.path)
            elif reply_header.xid == -2:  # -2 is the xid for pings
                pass
            else:
                self.get_logger().warn("ignored reply: reply_header={!r}".format(reply_header))

        response, _ = deserialize_record(protocol.get_response_class(op_code), data, data_offset)
        return response

    async def _send_requests(self) -> None:
        loop = self.get_loop()

        while True:
            operation = self._pending_operations1.try_remove_head(False)

            if operation is None:
                try:
                    operation = await utils.wait_for2(self._pending_operations1.remove_head(False)
                                                      , self._get_min_ping_interval(), loop=loop)
                except asyncio.TimeoutError:
                    buffer = bytearray()
                    request_header = protocol.RequestHeader(xid=-2, type=protocol.OpCode.PING)
                    serialize_record(request_header, buffer)
                    self._transport.write(buffer)
                    continue

            buffer = bytearray()
            xid = self._get_xid()
            request_header = protocol.RequestHeader(xid=xid, type=operation.op_code)  # type: ignore
            serialize_record(request_header, buffer)
            serialize_record(operation.request, buffer)  # type: ignore
            self._transport.write(buffer)
            self._pending_operations2[xid] = operation  # type: ignore

    async def _receive_responses(self) -> None:
        while True:
            data = await self._transport.read(self.get_read_timeout())
            reply_header: protocol.ReplyHeader
            reply_header, data_offset = deserialize_record(protocol.ReplyHeader, data)

            if reply_header.zxid > 0:
                self._last_zxid = reply_header.zxid

            if reply_header.xid < 0:
                if reply_header.xid == -1:  # -1 means notification
                    watcher_event: protocol.WatcherEvent
                    watcher_event, _ = deserialize_record(protocol.WatcherEvent, data, data_offset)
                    self._fire_watcher_event(protocol.WatcherEventType(watcher_event.type)
                                             , watcher_event.path)
                elif reply_header.xid == -2:  # -2 is the xid for pings
                    pass
                else:
                    self.get_logger().warn("ignored reply: reply_header={!r}".format(reply_header))
            else:
                operation = self._pending_operations2.pop(reply_header.xid, None)

                if operation is None:
                    self.get_logger().warn("missing operation: reply_header={!r}"
                                           .format(reply_header))
                    continue

                self._pending_operations1.commit_item_removals(1)

                if operation.response.cancelled():
                    continue

                if reply_header.err == 0:
                    non_error_class = None
                    response, _ = deserialize_record(protocol.get_response_class(operation.op_code)
                                                     , data, data_offset)
                else:
                    error_class = errors.get_error_class(reply_header.err)

                    if error_class not in operation.non_error_classes:
                        error_message = "request: {!r}".format(operation.request)
                        operation.response.set_exception(error_class(error_message))
                        continue

                    non_error_class = error_class
                    response = None

                if operation.on_completed is not None:
                    operation.on_completed(non_error_class)

                operation.response.set_result(response)

    def _get_xid(self) -> int:
        xid = self._next_xid
        self._next_xid = (xid + 1) & 0x7FFFFFFF
        return xid

    def _get_min_ping_interval(self) -> float:
        return self._timeout / 3

    def _fire_watcher_event(self, watcher_event_type: protocol.WatcherEventType, path: str) -> None:
        watcher_types = _WATCHER_EVENT_TYPE_2_WATCHER_TYPES[watcher_event_type]

        for watcher_type in watcher_types:
            path_2_watchers = self._watchers[watcher_type]
            watchers = path_2_watchers.pop(path, None)

            if watchers is None:
                self.get_logger().warn("missing watcher: watcher_event_type={!r} path={!r}"
                                       .format(watcher_event_type, path))
                continue

            for watcher in watchers:
                if watcher.is_removed():
                    continue

                watcher._event.set_result(watcher_event_type)

    def _reset(self, final_state: SessionState, event_type: SessionEventType) -> None:
        assert final_state in _FINAL_SESSION_STATES, repr(final_state)
        self._set_state(final_state, event_type)
        self._id = Long(0)
        self._password = b""
        self._last_zxid = Long(0)
        self._pending_operations1.reset(_MAX_NUMBER_OF_PENDING_OPERATIONS)


_MAX_NUMBER_OF_PENDING_OPERATIONS = 1 << 16

_FINAL_SESSION_STATE_2_ERROR_CLASS: typing.Dict[SessionState, typing.Type[errors.Error]] = {
    SessionState.CLOSED: errors.SessionExpiredError,
    SessionState.AUTH_FAILED: errors.AuthFailedError,
}

_FINAL_SESSION_STATES = _FINAL_SESSION_STATE_2_ERROR_CLASS.keys()

_PROTOCOL_VERSION = 0

_MAX_SETWATCHES_SIZE = 1 << 17
_SETWATCHES_OVERHEAD_SIZE = get_size(protocol.RequestHeader) + get_size(protocol.SetWatches)
_STRING_OVERHEAD_SIZE = get_size(protocol.String)

_WATCHER_EVENT_TYPE_2_WATCHER_TYPES: typing.Dict[protocol.WatcherEventType
                                                 , typing.Tuple[WatcherType, ...]] = {
    protocol.WatcherEventType.NODE_CREATED: (
        WatcherType.EXIST,
    ),

    protocol.WatcherEventType.NODE_DELETED: (
        WatcherType.DATA,
        WatcherType.CHILD,
    ),

    protocol.WatcherEventType.NODE_DATA_CHANGED: (
        WatcherType.DATA,
    ),

    protocol.WatcherEventType.NODE_CHILDREN_CHANGED: (
        WatcherType.CHILD,
    ),
}
