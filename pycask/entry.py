import struct
import zlib
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Final, NamedTuple, Tuple

__all__ = ['Entry', 'HEADER_SIZE', 'EntryType']

# The entry header format looks like this:
#
#   ┌─────────┬─────────┬───────────────┬──────────────┬────────────────┐
#   │ crc(4B) │ type(1B)│ timestamp(4B) │ key_size(4B) │ value_size(4B) │
#   └─────────┴─────────┴───────────────┴──────────────┴────────────────┘
#
# `!` - represents network(= big-endian) byte order.
# `B` - represents unsigned char (1 bytes).
# `I` - represents unsigned int (4 bytes).
#
# See the :class: `EntryHeader` for the implementation.
#
# Because the crc field don't join the cyclic redundancy check,
# the entry crc header format look like this:
#   ┌─────────┬───────────────┬──────────────┬────────────────┐
#   │ type(1B)│ timestamp(4B) │ key_size(4B) │ value_size(4B) │
#   └─────────┴───────────────┴──────────────┴────────────────┘
HEADER_FORMAT: Final[str] = '!B3I'
CRC_FORMAT: Final[str] = '!I'

# These four fields occupies `4 + 1 + 4 + 4 + 4 = 17` bytes.
HEADER_SIZE: Final[int] = 17
HEADER_INDEX: Final[int] = 4


class EntryType(IntEnum):
    NORMAL = 0
    DELETED = 1


class EntryHeader(NamedTuple):
    crc: int
    typ: int
    timestamp: int
    key_size: int
    value_size: int


@dataclass
class Entry:
    """The log entry."""

    #: The entry's key.
    key: str

    #: The entry's value.
    value: str

    #: Timestamp at which wrote the KV pair to the disk.
    #: The value is current time in seconds since the epoch.
    timestamp: int

    #: The entry's type.
    #: See the :class: `EntryType` for more information.
    typ: int = field(default=EntryType.NORMAL.value)

    def encode(self) -> Tuple[bytes, int]:
        """Encode the entry into bytes.

        Returns:
            The byte object and the size of encoded bytes.

        """
        header_bytes: bytearray = bytearray(HEADER_SIZE)
        header_bytes[HEADER_INDEX:] = struct.pack(
            HEADER_FORMAT, self.typ, self.timestamp, len(self.key), len(self.value)
        )
        payload_bytes: bytes = f'{self.key}{self.value}'.encode()

        # Cyclic redundancy check (CRC) field is an error-detecting code.
        # It uses all the other fields to generate the checksum.
        crc: int = zlib.crc32(header_bytes[HEADER_INDEX:] + payload_bytes)
        header_bytes[:HEADER_INDEX] = struct.pack(CRC_FORMAT, crc)
        return bytes(header_bytes) + payload_bytes, len(header_bytes) + len(
            payload_bytes
        )

    @classmethod
    def decode_header(cls, data: bytes) -> EntryHeader:
        """Decodes the bytes into header."""
        header_bytes: bytes = data[:HEADER_SIZE]
        (crc,) = struct.unpack(CRC_FORMAT, header_bytes[:HEADER_INDEX])
        typ, timestamp, key_size, value_size = struct.unpack(
            HEADER_FORMAT, header_bytes[HEADER_INDEX:]
        )
        return EntryHeader(crc, typ, timestamp, key_size, value_size)

    @classmethod
    def decode(cls, data: bytes) -> 'Entry':
        """Decodes the bytes into `Entry` instance."""
        header: EntryHeader = cls.decode_header(data)
        key_bytes: bytes = data[HEADER_SIZE : HEADER_SIZE + header.key_size]
        value_bytes: bytes = data[HEADER_SIZE + header.key_size :]
        key: str = key_bytes.decode()
        value: str = value_bytes.decode()
        return Entry(key, value, header.timestamp, typ=header.typ)
