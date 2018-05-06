import enum
import typing

from .record import *


class Id(typing.NamedTuple):
    scheme: String
    id: String


class ACL(typing.NamedTuple):
    perms: Int
    id: Id


class Stat(typing.NamedTuple):
    czxid: Long
    mzxid: Long
    ctime: Long
    mtime: Long
    version: Int
    cversion: Int
    aversion: Int
    ephemeral_owner: Long
    data_length: Int
    num_children: Int
    pzxid: Long


class ConnectRequest(typing.NamedTuple):
    protocol_version: Int
    last_zxid_seen: Long
    time_out: Int
    session_id: Long
    passwd: Buffer

class ConnectResponse(typing.NamedTuple):
    protocol_version: Int
    time_out: Int
    session_id: Long
    passwd: Buffer


class SetWatches(typing.NamedTuple):
    relative_zxid: Long
    data_watches: Vector[String]
    exist_watches: Vector[String]
    child_watches: Vector[String]


class RequestHeader(typing.NamedTuple):
    xid: Int
    type: Int


class MultiHeader(typing.NamedTuple):
    type: Int
    done: Boolean
    err: Int


class AuthPacket(typing.NamedTuple):
    type: Int
    scheme: String
    auth: Buffer


class ReplyHeader(typing.NamedTuple):
    xid: Int
    zxid: Long
    err: Int


class GetDataRequest(typing.NamedTuple):
    path: String
    watch: Boolean


class SetDataRequest(typing.NamedTuple):
    path: String
    data: Buffer
    version: Int


class ReconfigRequest(typing.NamedTuple):
    joining_servers: String
    leaving_servers: String
    new_members: String
    cur_config_id: Long


class SetDataResponse(typing.NamedTuple):
    stat: Stat


class CreateRequest(typing.NamedTuple):
    path: String
    data: Buffer
    acl: Vector[ACL]
    flags: Int


class DeleteRequest(typing.NamedTuple):
    path: String
    version: Int


class GetChildrenRequest(typing.NamedTuple):
    path: String
    watch: Boolean


class CheckVersionRequest(typing.NamedTuple):
    path: String
    version: Int


class SyncRequest(typing.NamedTuple):
    path: String


class SyncResponse(typing.NamedTuple):
    path: String


class GetACLRequest(typing.NamedTuple):
    path: String


class SetACLRequest(typing.NamedTuple):
    path: String
    acl: Vector[ACL]
    version: Int


class SetACLResponse(typing.NamedTuple):
    stat: Stat


class WatcherEvent(typing.NamedTuple):
    type: Int
    state: Int
    path: String


class ErrorResponse(typing.NamedTuple):
    err: Int


class CreateResponse(typing.NamedTuple):
    path: String


class Create2Response(typing.NamedTuple):
    path: String
    stat: Stat


class ExistsRequest(typing.NamedTuple):
    path: String
    watch: Boolean


class ExistsResponse(typing.NamedTuple):
    stat: Stat


class GetDataResponse(typing.NamedTuple):
    data: Buffer
    stat: Stat


class GetChildrenResponse(typing.NamedTuple):
    children: Vector[String]


class GetChildren2Response(typing.NamedTuple):
    children: Vector[String]
    stat: Stat


class GetACLResponse(typing.NamedTuple):
    acl: Vector[ACL]
    stat: Stat


class RemoveWatchesRequest(typing.NamedTuple):
    path: String
    type: Int


class OpCode(enum.IntEnum):
    NOTIFICATION = 0
    CREATE = 1
    DELETE = 2
    EXISTS = 3
    GET_DATA = 4
    SET_DATA = 5
    GET_ACL = 6
    SET_ACL = 7
    GET_CHILDREN = 8
    SYNC = 9
    PING = 11
    GET_CHILDREN2 = 12
    CHECK = 13
    MULTI = 14
    CREATE2 = 15
    RECONFIG = 16
    REMOVE_WATCHES = 18
    AUTH = 100
    SET_WATCHES = 101
    CLOSE_SESSION = -11
    ERROR = -1


class WatcherEventType(enum.IntEnum):
    NODE_CREATED = 1
    NODE_DELETED = 2
    NODE_DATA_CHANGED = 3
    NODE_CHILDREN_CHANGED = 4


class CreateMode(enum.IntEnum):
    PERSISTENT = 0
    EPHEMERAL = 1
    PERSISTENT_SEQUENTIAL = 2
    EPHEMERAL_SEQUENTIAL = 3


class Perms(enum.IntEnum):
    READ = 1 << 0
    WRITE = 1 << 1
    CREATE = 1 << 2
    DELETE = 1 << 3
    ADMIN = 1 << 4
    ALL = READ | WRITE | CREATE | DELETE | ADMIN;


class Ids:
    ANYONE_ID_UNSAFE = Id("world", "anyone")
    AUTH_IDS = Id("auth", "")

    OPEN_ACL_UNSAFE = ACL(Perms.ALL, ANYONE_ID_UNSAFE)
    CREATOR_ALL_ACL = ACL(Perms.ALL, AUTH_IDS)
    READ_ACL_UNSAFE = ACL(Perms.READ, ANYONE_ID_UNSAFE)


Op = typing.Tuple[OpCode, typing.Any]


class MultiRequest(typing.NamedTuple):
    ops: Vector[Op]

    def serialize(self, buffer: bytearray) -> None:
        for op in self.ops:
            assert isinstance(op[1], get_request_class(op[0]))

            serialize_record(MultiHeader(
                type=op[0],
                done=False,
                err=-1,
            ), buffer)

            serialize_record(op[1], buffer)

        serialize_record(MultiHeader(
            type=-1,
            done=True,
            err=-1,
        ), buffer)

    @classmethod
    def deserialize(cls, data: bytes, data_offset) -> typing.Tuple["MultiRequest", int]:
        ops: typing.List[Op] = []
        data_offset = 0

        while True:
            multi_header: MultiHeader
            multi_header, data_offset = deserialize_record(MultiHeader, data, data_offset)

            if multi_header.done:
                break

            request_class = get_request_class(OpCode(multi_header.type))
            request, data_offset = deserialize_record(request_class, data, data_offset)
            ops.append((OpCode(multi_header.type), request))

        return cls(tuple(ops)), data_offset

    @classmethod
    def get_size(cls, value: typing.Optional["MultiRequest"]) -> int:
        size = get_size(MultiHeader)

        if value is not None:
            for op in value.ops:
                assert isinstance(op[1], get_request_class(op[0]))
                size += get_size(MultiHeader)
                get_size(type(op[1]), op[1])

        return size


OpResult = typing.Tuple[OpCode, typing.Any]


class MultiResponse(typing.NamedTuple):
    op_results: Vector[OpResult]

    def serialize(self, buffer: bytearray) -> None:
        for op_result in self.op_results:
            assert isinstance(op_result[1], get_request_class(op_result[0]))
            err = op_result[1].err if op_result[0] is OpCode.ERROR else 0

            serialize_record(MultiHeader(
                type=op_result[0],
                done=False,
                err=err,
            ), buffer)

            serialize_record(op_result[1], buffer)

        serialize_record(MultiHeader(
            type=-1,
            done=True,
            err=-1,
        ), buffer)

    @classmethod
    def deserialize(cls, data: bytes, data_offset: int) -> typing.Tuple["MultiResponse", int]:
        op_results: typing.List[OpResult] = []

        while True:
            multi_header: MultiHeader
            multi_header, data_offset = deserialize_record(MultiHeader, data, data_offset)

            if multi_header.done:
                break

            response_class = get_response_class(OpCode(multi_header.type))
            response, data_offset = deserialize_record(response_class, data, data_offset)
            op_results.append((OpCode(multi_header.type), response))

        return cls(tuple(op_results)), data_offset

    @classmethod
    def get_size(cls, value: typing.Optional["MultiResponse"]) -> int:
        size = get_size(MultiHeader)

        if value is not None:
            for op_result in value.op_results:
                assert isinstance(op_result[1], get_request_class(op_result[0]))
                size += get_size(MultiHeader)
                get_size(type(op_result[1]), op_result[1])

        return size


def get_request_class(op_code: OpCode) -> typing.Type:
    return _OP_CODE_2_MESSAGE_CLASSES[op_code][0]


def get_response_class(op_code: OpCode) -> typing.Type:
    return _OP_CODE_2_MESSAGE_CLASSES[op_code][1]


_OP_CODE_2_MESSAGE_CLASSES: typing.Dict[OpCode, typing.Tuple[typing.Type, typing.Type]] = {
    OpCode.CREATE: (CreateRequest, CreateResponse),
    OpCode.DELETE: (DeleteRequest, type(None)),
    OpCode.EXISTS: (ExistsRequest, ExistsResponse),
    OpCode.GET_DATA: (GetDataRequest, GetDataResponse),
    OpCode.SET_DATA: (SetDataRequest, SetDataResponse),
    OpCode.GET_ACL: (GetACLRequest, GetACLResponse),
    OpCode.SET_ACL: (SetACLRequest, SetACLResponse),
    OpCode.GET_CHILDREN: (GetChildrenRequest, GetChildrenResponse),
    OpCode.SYNC: (SyncRequest, type(None)),
    OpCode.GET_CHILDREN2: (GetChildrenRequest, GetChildren2Response),
    OpCode.CHECK: (CheckVersionRequest, type(None)),
    OpCode.MULTI: (MultiRequest, MultiResponse),
    OpCode.CREATE2: (CreateRequest, Create2Response),
    OpCode.RECONFIG: (ReconfigRequest, type(None)),
    OpCode.REMOVE_WATCHES: (RemoveWatchesRequest, type(None)),
    OpCode.AUTH: (AuthPacket, type(None)),
    OpCode.SET_WATCHES: (SetWatches, type(None)),
    OpCode.ERROR: (type(None), ErrorResponse),
}
