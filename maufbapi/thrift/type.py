# mautrix-facebook - A Matrix-Facebook Messenger puppeting bridge.
# Copyright (C) 2022 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from __future__ import annotations

from typing import Any, NamedTuple, Type, TypeVar
from enum import IntEnum

from .. import thrift


class TType(IntEnum):
    """
    Thrift type identifiers for the compact struct encoding.

    https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#struct-encoding
    """

    STOP = 0
    TRUE = 1
    FALSE = 2
    BYTE = 3
    I16 = 4
    I32 = 5
    I64 = 6
    DOUBLE = 7
    BINARY = 8
    LIST = 9
    SET = 10
    MAP = 11
    STRUCT = 12
    # Facebook-specific: https://github.com/facebook/fbthrift/blob/v2021.03.22.00/thrift/lib/cpp/protocol/TCompactProtocol-inl.h#L57
    FLOAT = 13

    # Used internally to represent booleans in schemas.
    BOOL = 0xA1


class RecursiveType(NamedTuple):
    """A type container that can exactly specify the expected types for nested lists/maps."""

    type: TType
    python_type: Type[Any] | None = None
    item_type: RecursiveType | None = None
    key_type: RecursiveType | None = None
    value_type: RecursiveType | None = None


T = TypeVar("T")


class ThriftField(NamedTuple):
    name: str
    rtype: RecursiveType


class ThriftObject:
    thrift_spec: dict[int, ThriftField]

    def to_thrift(self) -> bytes:
        buf = thrift.ThriftWriter()
        buf.write_struct(self)
        return buf.getvalue()

    @classmethod
    def from_thrift(cls: Type[T], data: bytes) -> T:
        return thrift.ThriftReader(data).read_struct(cls)
