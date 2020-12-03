import struct
import zlib
from dataclasses import dataclass
from typing import Final, NamedTuple, Tuple

__all__ = ['Entry']

# The entry header format looks like this:
#
#   ┌─────────┬───────────────┬──────────────┬────────────────┐
#   │ crc(4B) │ timestamp(4B) │ key_size(4B) │ value_size(4B) │
#   └─────────┴───────────────┴──────────────┴────────────────┘
#
# `!` - represents network(= big-endian) byte order.
# `I` - represents unsigned int (4 bytes).
#
# See the :class: `EntryHeader` for the implementation.
#
#
# Because the crc field don't join the cyclic redundancy check,
# the entry crc header format look like this:
#   ┌───────────────┬──────────────┬────────────────┐
#   │ timestamp(4B) │ key_size(4B) │ value_size(4B) │
#   └───────────────┴──────────────┴────────────────┘
HEADER_FORMAT: Final[str] = '!3I'
CRC_FORMAT: Final[str] = '!I'

# These four fields occupies `4 + 4 + 4 + 4 = 16` bytes.
HEADER_SIZE: Final[int] = 16
HEADER_INDEX: Final[int] = 4


class EntryHeader(NamedTuple):
    crc: int
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

    def encode(self) -> Tuple[bytes, int]:
        """Encode the entry into bytes.

        Returns:
            The byte object and the size of encoded bytes.

        """
        header_bytes: bytearray = bytearray(HEADER_SIZE)
        header_bytes[HEADER_INDEX:] = struct.pack(
            HEADER_FORMAT, self.timestamp, len(self.key), len(self.value)
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
        timestamp, key_size, value_size = struct.unpack(
            HEADER_FORMAT, header_bytes[HEADER_INDEX:]
        )
        return EntryHeader(crc, timestamp, key_size, value_size)

    @classmethod
    def decode(cls, data: bytes) -> 'Entry':
        """Decodes the bytes into `Entry` instance."""
        header: EntryHeader = cls.decode_header(data)
        key_bytes: bytes = data[HEADER_SIZE: HEADER_SIZE + header.key_size]
        value_bytes: bytes = data[HEADER_SIZE + header.key_size:]
        key: str = key_bytes.decode()
        value: str = value_bytes.decode()
        return Entry(key, value, header.timestamp)
