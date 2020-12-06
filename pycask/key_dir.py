from dataclasses import dataclass
from typing import Dict

__all__ = ['KeyEntry', 'KeyDir']


@dataclass(frozen=True)
class KeyEntry:
    """The value structure of `KeyDir`.

    The `KeyEntry` keeps the metadata about the KV, includes the file_id, offset,
    timestamp, and size of the most recently written entry for that key.

    """

    #: The ID of target file.
    file_id: int

    #: The byte offset in the file where the data exists.
    offset: int

    #: Total size of bytes of the value.
    entry_size: int

    #: Timestamp at which we wrote the KV pair to the disk. The value
    #: is current time in seconds since the epoch.
    timestamp: int


# KeyDir is an in-memory hash table that stores all the keys present in the storage instance
# and maps it to the offset in the datafile where the log entry (value) resides;
# thus facilitating the point lookups.
KeyDir = Dict[str, KeyEntry]
