# Pure Python bencode implementation
# Source: https://github.com/trim21/bencode-py

from __future__ import annotations

import io
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from types import MappingProxyType
from typing import Any, Final

from typing_extensions import Buffer

# Common constants
char_l: Final = ord("l")
char_i: Final = ord("i")
char_e: Final = ord("e")
char_d: Final = ord("d")
char_0: Final = ord("0")
char_9: Final = ord("9")
char_dash: Final = ord("-")
char_colon: Final = ord(":")

# --- Decoder ---

class BencodeDecodeError(ValueError):
    """Bencode decode error."""


def bdecode(value: Buffer, /) -> Any:
    """Decode bencode formatted bytes to python value."""
    return Decoder(memoryview(value).cast("B")).decode()


class Decoder:
    value: memoryview
    index: int
    size: int

    __slots__ = ("value", "index", "size")

    def __init__(self, value: memoryview) -> None:
        self.size = len(value)
        if self.size == 0:
            raise BencodeDecodeError("empty input")

        self.value = value
        self.index = 0

    def decode(self) -> object:
        data = self.__decode()

        if self.index != self.size:
            raise BencodeDecodeError("invalid bencode value (data after valid prefix)")

        return data

    def __decode(self) -> object:
        if char_0 <= self.value[self.index] <= char_9:
            return self.__decode_bytes()
        if self.value[self.index] == char_i:
            return self.__decode_int()
        if self.value[self.index] == char_d:
            return self.__decode_dict()
        if self.value[self.index] == char_l:
            return self.__decode_list()

        raise BencodeDecodeError(
            f"unexpected token {self.value[self.index:self.index + 1].tobytes()}. "
            f"index {self.index}"
        )

    def __decode_int(self) -> int:
        self.index += 1
        for i, c in enumerate(self.value[self.index :]):
            if c == char_e:
                index_end = i + self.index
                break
        else:
            raise BencodeDecodeError(
                f"invalid int, failed to found end. index {self.index}"
            )

        if index_end == self.index:
            raise BencodeDecodeError(f"invalid int, found 'ie': {self.index}")

        n: int = 1
        offset: int = 0

        if self.value[self.index] == char_dash:
            n = -1
            offset = 1

        total: int = 0
        for c in self.value[self.index + offset : index_end]:
            if not (char_0 <= c <= char_9):
                raise BencodeDecodeError(
                    f"malformed int {self.value[self.index:index_end].tobytes()}. index {self.index}"
                )
            total = total * 10 + (c - char_0)

        n = total * n

        if self.value[self.index] == char_dash:
            if self.value[self.index + 1] == char_0:
                raise BencodeDecodeError(
                    f"-0 is not allowed in bencoding. index: {self.index}"
                )
        elif self.value[self.index] == char_0 and index_end != self.index + 1:
            raise BencodeDecodeError(
                f"integer with leading zero is not allowed. index: {self.index}"
            )
        self.index = index_end + 1
        return n

    def __decode_list(self) -> list[Any]:
        r: list[Any] = []
        self.index += 1

        while True:
            if self.index >= self.size:
                raise BencodeDecodeError(
                    f"buffer overflow when decoding array, index {self.index}"
                )

            if self.value[self.index] == char_e:
                break

            v = self.__decode()
            r.append(v)

        self.index += 1
        return r

    def __decode_bytes(self) -> bytes:
        for i, c in enumerate(self.value[self.index :]):
            if c == char_colon:
                index_colon = i + self.index
                break
        else:
            raise BencodeDecodeError(
                f"invalid bytes, failed find expected char ':'. index {self.index}"
            )

        if self.value[self.index] == char_0:
            if index_colon != self.index + 1:
                raise BencodeDecodeError(
                    f"malformed str/bytes length with leading 0. index {self.index}"
                )

        n: int = 0
        for c in self.value[self.index : index_colon]:
            if not (char_0 <= c <= char_9):
                raise BencodeDecodeError(
                    f"malformed str/bytes length {self.value[self.index:index_colon].tobytes()!r}."
                    f" index {self.index}"
                )

            n = n * 10 + (c - char_0)

        if index_colon + n >= self.size:
            raise BencodeDecodeError(
                f"malformed str/bytes length, buffer overflow. index {self.index}"
            )

        index_colon += 1
        s = self.value[index_colon : index_colon + n]
        self.index = index_colon + n
        return s.tobytes()

    def __decode_dict(self) -> dict[str | bytes, Any]:
        start_index = self.index
        self.index += 1

        items: list[tuple[bytes, Any]] = []

        while True:
            if self.index >= self.size:
                raise BencodeDecodeError(
                    f"buffer overflow when decoding bytes, index {self.index}"
                )
            if self.value[self.index] == char_e:
                break
            if not (char_0 <= self.value[self.index] <= char_9):
                raise BencodeDecodeError(
                    f"directory only allow str as keys, "
                    f"found unexpected char "
                    f"'{self.value[self.index]:c}', index {self.index}"
                )
            k = self.__decode_bytes()
            v = self.__decode()
            items.append((k, v))

        if not items:
            self.index += 1
            return {}

        _check_sorted(items, start_index)
        self.index += 1
        return dict(items)


def _check_sorted(s: list[tuple[bytes, Any]], idx: int) -> None:
    i = 1
    while i < len(s):
        if s[i][0] < s[i - 1][0]:
            raise BencodeDecodeError(f"directory keys is not sorted, index {idx}")
        if s[i][0] == s[i - 1][0]:
            raise BencodeDecodeError(f"found duplicated keys in directory, index {idx}")
        i += 1

# --- Encoder ---

def bencode(value: Any, /) -> bytes:
    """Encode value into the bencode format."""
    with io.BytesIO() as w:
        __encode(w, value, set(), stack_depth=0)
        return w.getvalue()


def __encode(w: io.BytesIO, value: Any, seen: set[int], stack_depth: int) -> None:
    if isinstance(value, str):
        return __encode_bytes(w, value.encode("UTF-8"))

    if isinstance(value, int):
        w.write(b"i")
        w.write(str(int(value)).encode())
        w.write(b"e")
        return

    if isinstance(value, bytes):
        return __encode_bytes(w, value)

    stack_depth += 1
    i = id(value)
    if isinstance(value, (dict, OrderedDict, MappingProxyType)):
        if stack_depth >= 100:
            if i in seen:
                raise BencodeEncodeError(f"circular reference found {value!r}")
            seen.add(i)
        __encode_mapping(w, value, seen, stack_depth=stack_depth)
        if stack_depth >= 100:
            seen.remove(i)
        stack_depth -= 1
        return

    if isinstance(value, (list, tuple)):
        if stack_depth >= 100:
            if i in seen:
                raise BencodeEncodeError(f"circular reference found {value!r}")
            seen.add(i)

        w.write(b"l")
        for item in value:
            __encode(w, item, seen, stack_depth=stack_depth)
        w.write(b"e")

        if stack_depth >= 100:
            seen.remove(i)
        stack_depth -= 1
        return

    if isinstance(value, bytearray):
        __encode_bytes(w, bytes(value))
        return

    if isinstance(value, memoryview):
        w.write(str(len(value)).encode())
        w.write(b":")
        w.write(value)
        return

    if is_dataclass(value) and not isinstance(value, type):
        if stack_depth >= 100:
            if i in seen:
                raise BencodeEncodeError(f"circular reference found {value!r}")
            seen.add(i)
        __encode_dataclass(w, value, seen, stack_depth=stack_depth)
        if stack_depth >= 100:
            seen.remove(i)
        stack_depth -= 1
        return

    raise TypeError(f"type '{type(value)!r}' not supported by bencode")


def __encode_bytes(w: io.BytesIO, val: bytes) -> None:
    w.write(str(len(val)).encode())
    w.write(b":")
    w.write(val)


def __encode_mapping(
    w: io.BytesIO,
    val: Mapping[Any, Any],
    seen: set[int],
    stack_depth: int,
) -> None:
    w.write(b"d")

    i_list: list[tuple[bytes, object]] = [(to_binary(k), v) for k, v in val.items()]
    if not i_list:
        w.write(b"e")
        return
    i_list.sort(key=lambda kv: kv[0])
    __check_duplicated_keys(i_list)

    for k, v in i_list:
        __encode_bytes(w, k)
        __encode(w, v, seen, stack_depth=stack_depth)

    w.write(b"e")


def __encode_dataclass(w: io.BytesIO, x: Any, seen: set[int], stack_depth: int) -> None:
    keys = fields(x)
    if not keys:
        w.write(b"de")
        return
    w.write(b"d")
    ks = sorted([k.name for k in keys])
    for k in ks:
        __encode_bytes(w, k.encode())
        __encode(w, getattr(x, k), seen, stack_depth=stack_depth)
    w.write(b"e")


def __check_duplicated_keys(s: list[tuple[bytes, object]]) -> None:
    last_key: bytes = s[0][0]
    for current, _ in s[1:]:
        if last_key == current:
            raise BencodeEncodeError(
                f"find duplicated keys {last_key!r} and {current.decode()}"
            )
        last_key = current


def to_binary(s: str | bytes) -> bytes:
    if isinstance(s, bytes):
        return s
    if isinstance(s, str):
        return s.encode("utf-8", "strict")
    raise TypeError(f"expected binary or text (found {type(s)})")
