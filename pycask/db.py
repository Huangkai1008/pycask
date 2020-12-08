# mypy: no-strict-optional
import time
from itertools import chain
from pathlib import Path
from typing import Any, Final, Generator, List, Optional, TypedDict, Union

from pycask.entry import EntryType
from pycask.file import LOG_FILE_PREFIX, LogFile
from pycask.key_dir import KeyDir, KeyEntry

__all__ = ['DataStore']

# The default threshold size is 64MB.
LOG_FILE_DEFAULT_THRESHOLD_SIZE: Final[int] = 64 << 20

# The default merge interval is 8H.
LOG_FILE_DEFAULT_MERGE_INTERVAL: Final[int] = 3600 * 8


class Options(TypedDict, total=False):
    #: Threshold size of each log file,
    #: active log file will be closed if reach the threshold.
    #: The default value is :const:`LOG_FILE_DEFAULT_THRESHOLD_SIZE`.
    log_file_threshold_size: int

    #: Background thread will merge archived log files periodically according to the interval.
    #: The interval measured in seconds.
    #: The default value is :const:`LOG_FILE_MERGE_INTERVAL`.
    log_file_merge_interval: int


class DataStore:
    """Implements the KV store on disk.

    Attributes:
        dir_name:
            The directory of data storage.

        _key_dir:
            A hash table that maps every key to an entry.

        _active_datafile:
            The active data file accepts all write requests.

        _archived_datafiles:
            The non-active (i.e. immutable) files accepts read requests only.

    Keyword Args:
        options: See more in :class:`Options`.

    """

    MERGE_FILE_START_ID = -1

    def __init__(self, dir_name: str, **options: Union[Any, Options]) -> None:
        self._dir_name: str = dir_name
        self._key_dir: KeyDir = dict()
        self._active_datafile: Optional[LogFile] = None
        self._archived_datafiles: List[LogFile] = []
        self._options: dict = options

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
        key_entry: Optional[KeyEntry] = self._key_dir.get(key)
        if not key_entry:
            return None

        return self._get_log_file(key_entry.file_id).get_entry_value(
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
        data, entry_size = self._active_datafile.encode_entry(key, value, timestamp)
        offset: int = self._append(data, entry_size)

        # Update `KeyDir` if the key-value pair stored to disk successfully.
        self._key_dir[key] = KeyEntry(
            self._active_datafile.file_id,
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
        timestamp = int(time.time())
        data, entry_size = self._active_datafile.encode_entry(
            key, '', timestamp, EntryType.DELETED.value
        )
        self._append(data, entry_size)

        # Delete key in the `KeyDir`.
        self._key_dir.pop(key, None)

    def keys(self) -> Generator[str, None, None]:
        """Generate all keys in datastore.

        Yields: The next key in the range of all keys.

        """
        yield from self._key_dir.keys()

    def __getitem__(self, key: str) -> Optional[str]:
        return self.get(key)

    def __setitem__(self, key: str, value: str) -> None:
        return self.put(key, value)

    def __delitem__(self, key: str) -> None:
        return self.delete(key)

    def __contains__(self, key: str) -> bool:
        return key in self._key_dir

    @property
    def log_file_threshold_size(self) -> int:
        return self._options.get(
            'log_file_threshold_size', LOG_FILE_DEFAULT_THRESHOLD_SIZE
        )

    @property
    def log_file_merge_interval(self) -> int:
        return self._options.get(
            'log_file_merge_interval', LOG_FILE_DEFAULT_MERGE_INTERVAL
        )

    def _init_data_dir(self) -> None:
        """Initialise the data directory.

        If the directory does not exist, recursively create all folders.

        """
        path = Path(self._dir_name)
        if not path.exists():
            path.mkdir(parents=True)

    def _init_data_files(self) -> None:
        """Initialise data files."""
        p: Path = Path(self._dir_name)
        datafiles: List[Path] = list(p.glob(f'{LOG_FILE_PREFIX}.*'))
        file_ids: List[int] = []
        for datafile in datafiles:
            file_id: int = int(datafile.name.split('.')[-1])
            file_ids.append(file_id)
        # The newer one has a larger number.
        file_ids.sort()

        if file_ids:
            max_file_id: int = file_ids[-1]
            self._active_datafile = LogFile(self._dir_name, file_id=max_file_id)
            self._archived_datafiles = [
                LogFile(self._dir_name, file_id)
                for file_id in file_ids
                if file_id != max_file_id
            ]
        else:
            self._active_datafile = LogFile(self._dir_name)

    def _init_key_dir(self) -> None:
        """Initialise the key dir.

        If the file exists, then the `key_dir` should be initialized with the
        contents of the file read.

        """
        for data_file in chain(self._archived_datafiles, (self._active_datafile,)):
            self._key_dir.update(data_file.get_key_dir())

    def _get_log_file(self, file_id: int) -> LogFile:
        return LogFile(self._dir_name, file_id)

    def _append(self, data: bytes, entry_size: int) -> int:
        """Append entry data bytes to the store.

        Returns:
            The offset.

        """
        # Switch to new active datafile if the size meets the threshold.
        if (
            self._active_datafile.write_position + entry_size
            > self.log_file_threshold_size
        ):
            self._active_datafile.close()
            self._archived_datafiles.append(self._active_datafile)
            self._active_datafile = LogFile(
                self._dir_name, self._active_datafile.file_id + 1
            )

        #: Note: get the write position before data is written to disk.
        offset: int = self._active_datafile.write_position
        self._active_datafile.append(data)
        return offset

    def merge(self) -> None:
        """Merge archived data files.

        Iterates over all non-active (i.e. immutable) files
        and produces as output a set of data files containing only the live
        or latest versions of each present key.

        """
        archived_datafiles: List[LogFile] = self._archived_datafiles
        merge_file_ids: List[int] = [self.MERGE_FILE_START_ID]
        merge_file: LogFile = LogFile(self._dir_name, self.MERGE_FILE_START_ID)
        for data_file in archived_datafiles:
            for entry, offset in data_file:
                key_entry: KeyEntry = self._key_dir.get(entry.key)
                if (
                    key_entry
                    and key_entry.file_id == data_file.file_id
                    and key_entry.offset == offset
                ):
                    data, entry_size = entry.encode()
                    if (
                        merge_file.write_position + entry_size
                        > self.log_file_threshold_size
                    ):
                        # Switch to new merge file if the size meets the threshold.
                        # Close last merge file first.
                        merge_file.close()
                        merge_file_id: int = merge_file_ids[-1] - 1
                        merge_file_ids.append(merge_file_id)
                        merge_file = LogFile(self._dir_name, merge_file_id)
                    merge_file.append(data)

        merge_file.close()

        for data_file in archived_datafiles:
            data_file.delete()

        # Rename all merge files.
        # The rename file id is `abs(old_file_id) - 1`.
        for merge_file_id in merge_file_ids:
            file_name: str = LogFile.get_file_name(self._dir_name, merge_file_id)
            path: Path = Path(file_name)
            prefix, file_id = file_name.rsplit('.', maxsplit=1)
            new_file_id = abs(int(file_id)) - 1
            path.rename(f'{prefix}.{new_file_id}')
