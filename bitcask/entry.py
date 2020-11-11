from dataclasses import dataclass
from typing import Final, final

__all__ = ['EntryHeader']

# The entry header format look like this:
#
#     ┌─────────┬───────────────┬──────────────┬────────────────┐
#     │ crc(4B) │ timestamp(4B) │ key_size(4B) │ value_size(4B) │
#     └─────────┴───────────────┴──────────────┴────────────────┘
#
# `!` - represents network(= big-endian) byte order.
# `I` - represents unsigned int (4 bytes).
#
# See the :class: `EntryHeader` for the implementation.
HEADER_FORMAT: Final[str] = '!IIII'

# These four fields occupies `4 + 4 + 4 + 4 = 16` bytes.
HEADER_SIZE: Final[int] = 16


@final
@dataclass(frozen=True)
class EntryHeader:
    """The log entry header."""

    #: Cyclic redundancy check (CRC) field is an error-detecting code.
    crc: int

    #: Timestamp field stores the time the record we inserted in unix epoch seconds.
    timestamp: int

    #: Key size field stores the length of bytes occupied by the key.
    key_size: int

    #: Value size field stores the length of bytes occupied by the value.
    value_size: int
