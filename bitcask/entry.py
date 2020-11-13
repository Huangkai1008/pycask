import struct
import zlib
from dataclasses import dataclass
from typing import Final, Tuple

__all__ = ['Entry']

# The entry header format look like this:
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
        payload: bytes = f'{self.key}{self.value}'.encode()

        # Cyclic redundancy check (CRC) field is an error-detecting code.
        # It uses all the other fields to generate checksum.
        crc: int = zlib.crc32(header_bytes[HEADER_INDEX:] + payload)
        header_bytes[:HEADER_INDEX] = struct.pack(CRC_FORMAT, crc)
        return bytes(header_bytes) + payload, len(header_bytes) + len(payload)
