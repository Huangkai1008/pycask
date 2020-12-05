# mypy: no-strict-optional
import time
from pathlib import Path
from typing import Generator, Optional

from bitcask.file import LogFile
from bitcask.key_dir import KeyDir, KeyEntry

__all__ = ['DataStore']


class DataStore:
    """Implements the KV store on disk.

    Attributes:
        dir_name:
            The directory of data storage.

        key_dir:
            A hash table that maps every key to an entry.

        active_datafile:
            The active data file accepts all write requests.

    """

    def __init__(self, dir_name: str) -> None:
        self.dir_name: str = dir_name
        self.key_dir: KeyDir = dict()
        self.active_datafile: Optional[LogFile] = None

        # Initialise data directory.
        self._init_data_dir()

        # Initialise data files.
        self._init_data_files()

        # Initialise the key dir.
        self._init_key_dir()

    def get(self, key: str) -> Optional[str]:
        """Retrieve a value by key from datastore.

        If the key does not exist, return `None`.

        Args:
            key: The given key.

        Returns:
            The value if key exists, `None` otherwise.

        """
        key_entry: Optional[KeyEntry] = self.key_dir.get(key)
        if not key_entry:
            return None

        # FIXME: Only one data file at first.
        return self.active_datafile.get_entry_value(
            key_entry.offset, key_entry.entry_size
        )

    def put(self, key: str, value: str) -> None:
        """Store a key and value in datastore.

        Args:
            key: The key to store.
            value: The value to store.

        """
        # Persistent data to disk.
        timestamp = int(time.time())
        #: Note: get the write position before data is written to disk.
        offset: int = self.active_datafile.write_position
        entry_size: int = self.active_datafile.append(key, value, timestamp)

        # Update `KeyDir` if the key-value pair stored to disk successfully.
        self.key_dir[key] = KeyEntry(
            self.active_datafile.file_id,
            offset,
            entry_size,
            timestamp,
        )

    def delete(self, key: str) -> None:
        """Delete a key from datastore.

        If the key does not exist, do nothing.

        Args:
            key: The key to delete.

        """
        ...

    def keys(self) -> Generator[str, None, None]:
        """Generate all keys in datastore.

        Yields: The next key in the range of all keys.

        """
        yield from self.key_dir.keys()

    def __getitem__(self, key: str) -> Optional[str]:
        return self.get(key)

    def __setitem__(self, key: str, value: str) -> None:
        return self.put(key, value)

    def __delitem__(self, key: str) -> None:
        return self.delete(key)

    def _init_data_dir(self) -> None:
        """Initialise the data directory.

        If the directory does not exist, recursively create all folders.

        """
        path = Path(self.dir_name)
        if not path.exists():
            path.mkdir(parents=True)

    def _init_data_files(self) -> None:
        """Initialise data files.

        FIXME: Only one data file at first.

        """
        self.active_datafile = LogFile(self.dir_name)

    def _init_key_dir(self) -> None:
        """Initialise the key dir.

        If the file exists, then the `key_dir` should be initialized with the
        contents of the file read.

        """
        self.key_dir = self.active_datafile.get_key_dir()
