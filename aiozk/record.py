__all__ = (
    "Boolean",
    "Int",
    "Long",
    "Buffer",
    "String",
    "Vector",
    "serialize_record",
    "deserialize_record",
    "get_size",
)


import typing


_T = typing.TypeVar("_T")

Boolean = bool
Int = int
Long = typing.NewType("Long", int)
Buffer = bytes
String = str
Vector = typing.Tuple[_T, ...]


def serialize_record(record, buffer: bytearray) -> None:
    return _serialize_record(type(record), record, buffer)


def deserialize_record(record_class: typing.Type, data: bytes
                       , data_offset: int=0) -> typing.Tuple[typing.Any, int]:
    return _deserialize_record(record_class, data, data_offset)


def get_size(class_: typing.Type[_T], value: typing.Optional[_T]=None) -> int:
    if class_ is Boolean:
        size = 1
    elif class_ is Int:
        size = 4
    elif class_ is Long:
        size = 8
    elif class_ is Buffer:
        size = 4

        if value is not None:
            size += len(value)  # type: ignore
    elif class_ is String:
        size = 4

        if value is not None:
            size += len(value.encode())  # type: ignore
    else:
        if _test_vector_class(class_):
            size = 4

            if value is not None:
                element_class = _get_element_class(class_)

                for element_value in value:  # type: ignore
                    size += get_size(element_class, element_value)
        else:
            if class_ is type(None):
                size = 0
            elif hasattr(class_, "get_size"):
                size = class_.get_size(value)  # type: ignore
            else:
                size = 0

                for field_name, field_class in class_._field_types.items():  # type: ignore
                    if value is None:
                        field_value = None
                    else:
                        field_value = getattr(value, field_name)

                    size += get_size(field_class, field_value)

    return size


def _serialize_record(record_class: typing.Type, record, buffer: bytearray) -> None:
    if record_class is type(None):
        pass
    elif hasattr(record_class, "serialize"):
        record.serialize(buffer)
    else:
        for field_name, field_class in record_class._field_types.items():
            field_value = getattr(record, field_name)
            _serialize_value(field_class, field_value, buffer)


def _deserialize_record(record_class: typing.Type, data: bytes
                        , data_offset: int) -> typing.Tuple[typing.Any, int]:
    if record_class is type(None):
        record = None
    elif hasattr(record_class, "deserialize"):
        record, data_offset = record_class.deserialize(data, data_offset)
    else:
        field_values = []

        for field_name, field_class in record_class._field_types.items():
            field_value, data_offset = _deserialize_value(field_class, data, data_offset)
            field_values.append(field_value)

        record = record_class._make(field_values)

    return record, data_offset


def _serialize_vector(vector_class: typing.Type, vector: Vector, buffer: bytearray) -> None:
    _serialize_int(len(vector), buffer)
    element_class = _get_element_class(vector_class)

    for element_value in vector:
        _serialize_value(element_class, element_value, buffer)


def _deserialize_vector(vector_class: typing.Type, data: bytes
                        , data_offset: int) -> typing.Tuple[Vector, int]:
    number_of_elements, data_offset = _deserialize_int(data, data_offset)

    if number_of_elements < 0:
        raise ValueError("number_of_elements={!r}".format(number_of_elements))

    element_values = []
    element_class = _get_element_class(vector_class)

    for _ in range(number_of_elements):
        element_value, data_offset = _deserialize_value(element_class, data, data_offset)
        element_values.append(element_value)

    vector = tuple(element_values)
    return vector, data_offset


def _serialize_value(class_: typing.Type, value, buffer: bytearray) -> None:
    serdes = _PRIMITIVE_CLASS_2_SERDES.get(class_, None)

    if serdes is None:
        if _test_vector_class(class_):
            _serialize_vector(class_, value, buffer)
        else:
            _serialize_record(class_, value, buffer)
    else:
        serdes[0](value, buffer)


def _deserialize_value(class_: typing.Type, data: bytes
                       , data_offset: int) -> typing.Tuple[typing.Any, int]:
    serdes = _PRIMITIVE_CLASS_2_SERDES.get(class_, None)

    if serdes is None:
        if _test_vector_class(class_):
            value, data_offset = _deserialize_vector(class_, data, data_offset)
        else:
            value, data_offset = _deserialize_record(class_, data, data_offset)
    else:
        value, data_offset = serdes[1](data, data_offset)

    return value, data_offset


def _serialize_boolean(boolean: Boolean, buffer: bytearray) -> None:
    if boolean:
        buffer.append(1)
    else:
        buffer.append(0)


def _deserialize_boolean(data: bytes, data_offset: int) -> typing.Tuple[Boolean, int]:
    next_data_offset = data_offset + 1

    if next_data_offset > len(data):
        raise ValueError("next_data_offset={!r} data_size={!r}".format(next_data_offset, len(data)))

    boolean = bool(data[data_offset])
    data_offset = next_data_offset
    return boolean, data_offset


def _serialize_int(int_: Int, buffer: bytearray) -> None:
    buffer.extend(int_.to_bytes(4, "big", signed=True))


def _deserialize_int(data: bytes, data_offset: int) -> typing.Tuple[Int, int]:
    next_data_offset = data_offset + 4

    if next_data_offset > len(data):
        raise ValueError("next_data_offset={!r} data_size={!r}".format(next_data_offset, len(data)))

    int_ = int.from_bytes(data[data_offset:next_data_offset], "big", signed=True)
    data_offset = next_data_offset
    return int_, data_offset


def _serialize_long(long: Long, buffer: bytearray) -> None:
    buffer.extend(long.to_bytes(8, "big", signed=True))


def _deserialize_long(data: bytes, data_offset: int) -> typing.Tuple[Long, int]:
    next_data_offset = data_offset + 8

    if next_data_offset > len(data):
        raise ValueError("next_data_offset={!r} data_size={!r}".format(next_data_offset, len(data)))

    long = Long(int.from_bytes(data[data_offset:next_data_offset], "big", signed=True))
    data_offset = next_data_offset
    return long, data_offset


def _serialize_buffer(buffer1: Buffer, buffer2: bytearray) -> None:
    _serialize_int(len(buffer1), buffer2)
    buffer2.extend(buffer1)


def _deserialize_buffer(data: bytes, data_offset: int) -> typing.Tuple[Buffer, int]:
    number_of_bytes, data_offset = _deserialize_int(data, data_offset)

    if number_of_bytes < 0:
        raise ValueError("number_of_bytes={!r}".format(number_of_bytes))

    next_data_offset = data_offset + number_of_bytes

    if next_data_offset > len(data):
        raise ValueError("next_data_offset={!r} data_size={!r}".format(next_data_offset, len(data)))

    buffer = data[data_offset:next_data_offset]
    data_offset = next_data_offset
    return buffer, data_offset


def _serialize_string(string: String, buffer: bytearray) -> None:
    raw_value = string.encode()
    _serialize_int(len(raw_value), buffer)
    buffer.extend(raw_value)


def _deserialize_string(data: bytes, data_offset: int) -> typing.Tuple[String, int]:
    number_of_bytes, data_offset = _deserialize_int(data, data_offset)

    if number_of_bytes < 0:
        raise ValueError("number_of_bytes={!r}".format(number_of_bytes))

    next_data_offset = data_offset + number_of_bytes

    if next_data_offset > len(data):
        raise ValueError("next_data_offset={!r} data_size={!r}".format(next_data_offset, len(data)))

    raw_value = data[data_offset:next_data_offset]
    string = raw_value.decode()
    data_offset = next_data_offset
    return string, data_offset


def _test_vector_class(class_: typing.Type) -> bool:
    return hasattr(class_, "_subs_tree")


def _get_element_class(vector_class: typing.Type) -> typing.Type:
    element_class = vector_class._subs_tree()[1]

    if isinstance(element_class, tuple):
        element_class = element_class[0][element_class[1:]]

    return element_class


_PRIMITIVE_CLASS_2_SERDES: typing.Dict[typing.Any, typing.Tuple[typing.Callable
                                                                , typing.Callable]] = {
    Boolean: (_serialize_boolean, _deserialize_boolean),
    Int: (_serialize_int, _deserialize_int),
    Long: (_serialize_long, _deserialize_long),
    Buffer: (_serialize_buffer, _deserialize_buffer),
    String: (_serialize_string, _deserialize_string),
}
