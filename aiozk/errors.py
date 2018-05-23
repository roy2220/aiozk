import typing


class Error(Exception):
    CODE: typing.ClassVar[int] = 0


_ERROR_CODE_2_ERROR_CLASS: typing.Dict[int, typing.Type[Error]] = {}


def _register_error(error_code: int) -> typing.Callable[[typing.Type[Error]], typing.Type[Error]]:
    def do(error_class: typing.Type[Error]) -> typing.Type[Error]:
        assert issubclass(error_class, Error), repr(error_class)
        assert error_class not in _ERROR_CODE_2_ERROR_CLASS.keys(), repr(error_class)
        error_class.CODE = error_code
        _ERROR_CODE_2_ERROR_CLASS[error_code] = error_class
        return error_class

    return do


@_register_error(-1)
class SystemError(Error):
    pass


@_register_error(-2)
class RuntimeInconsistencyError(Error):
    pass


@_register_error(-3)
class DataInconsistencyError(Error):
    pass


@_register_error(-4)
class ConnectionLossError(Error):
    pass


@_register_error(-5)
class MarshallingError(Error):
    pass


@_register_error(-6)
class UnimplementedError(Error):
    pass


@_register_error(-7)
class OperationTimeoutError(Error):
    pass


@_register_error(-8)
class BadArgumentsError(Error):
    pass


@_register_error(-9)
class InvalidStateError(Error):
    pass


@_register_error(-13)
class NewConfigNoQuorumError(Error):
    pass


@_register_error(-14)
class ReconfigInprogressError(Error):
    pass


@_register_error(-101)
class NoNodeError(Error):
    pass


@_register_error(-102)
class NoAuthError(Error):
    pass


@_register_error(-103)
class BadVersionError(Error):
    pass


@_register_error(-108)
class NoChildrenForEphemeralsError(Error):
    pass


@_register_error(-110)
class NodeExistsError(Error):
    pass


@_register_error(-111)
class NotEmptyError(Error):
    pass


@_register_error(-112)
class SessionExpiredError(Error):
    pass


@_register_error(-113)
class InvalidCallbackError(Error):
    pass


@_register_error(-114)
class InvalidACLError(Error):
    pass


@_register_error(-115)
class AuthFailedError(Error):
    pass


@_register_error(-116)
class ClosingError(Error):
    pass


@_register_error(-117)
class NothingError(Error):
    pass


@_register_error(-118)
class SessionMovedError(Error):
    pass


@_register_error(-119)
class NotReadOnlyError(Error):
    pass


@_register_error(-120)
class EphemeralOnLocalSessionError(Error):
    pass


@_register_error(-121)
class NoWatcherError(Error):
    pass


@_register_error(-123)
class ReconfigDisabledError(Error):
    pass


def get_error_class(error_code: int) -> typing.Type[Error]:
    return _ERROR_CODE_2_ERROR_CLASS[error_code]
