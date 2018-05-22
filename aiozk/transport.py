import asyncio
import logging
import typing


class Transport:
    def __init__(self, loop: typing.Optional[asyncio.AbstractEventLoop]
                 , logger: typing.Optional[logging.Logger]) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()

        if logger is None:
            logger = logging.getLogger()

        self._is_closed = True
        self._loop = loop
        self._logger = logger

    def connect(self, host_name: str, port_number: int
                , connect_timeout: float) -> "asyncio.Future[None]":
        assert self._is_closed
        return asyncio.wait_for(self._connect(host_name, port_number), connect_timeout)

    def write(self, message: typing.Union[bytes, bytearray]) -> None:
        assert not self._is_closed
        message_size = len(message)
        self._stream_writer.write(message_size.to_bytes(4, "big"))
        self._stream_writer.write(message)

    def read(self, read_timeout: float) -> "asyncio.Future[bytes]":
        assert not self._is_closed
        return asyncio.wait_for(self._read(), read_timeout)

    def close(self) -> None:
        assert not self._is_closed
        self._is_closed = True
        self._stream_writer.close()

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def get_logger(self) -> logging.Logger:
        return self._logger

    def is_closed(self) -> bool:
        return self._is_closed

    async def _connect(self, host_name: str, port_number: int) -> None:
        self._stream_reader, self._stream_writer = await asyncio.open_connection(host_name\
            , port_number, loop=self._loop)
        self._is_closed = False

    async def _read(self) -> bytes:
        message_size = int.from_bytes(await self._stream_reader.readexactly(4), "big")
        message = await self._stream_reader.readexactly(message_size)
        return message
