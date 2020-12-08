import os
from pathlib import Path
from types import TracebackType
from typing import IO, Final, Generator, Optional, Tuple, Type, Union

from pycask.entry import HEADER_SIZE, Entry, EntryType
from pycask.key_dir import KeyDir, KeyEntry

__all__ = ['LogFile', 'LOG_FILE_PREFIX']

LOG_FILE_PREFIX: Final[str] = 'cask.db'


class LogFile:
    """The log file.

    Attributes:
        dir_name:
            The directory of data storage.

        file_id:
            The ID of current file.

        file_name:
            Name of the file where all the data will be written.

        write_position:
            Current cursor position in the file where the data can be written.

        file:
            File object pointing the file_name.

    """

    def __init__(
        self, dir_name: Union[str, Path], file_id: int = 0, mode: str = 'a+b'
    ) -> None:
        self._file_id: int = file_id
        self.file_name: str = self.get_file_name(dir_name, file_id)
        self.mode: str = mode
        self.file: IO[bytes] = open(self.file_name, mode)
        self.write_position: int = Path(self.file_name).stat().st_size

    @property
    def file_id(self) -> int:
        """Returns the ID of current file."""
        return self._file_id

    def get_entry_value(self, offset: int, entry_size: int) -> str:
        """Get the value of entry with given offset and entry_size."""
        return self.read_log_entry(offset, entry_size).value

    def get_key_dir(self) -> KeyDir:
        """Get key dir from the current file."""
        key_dir: KeyDir = dict()
        with open(self.file_name, 'rb') as f:
            offset: int = 0
            while header_bytes := f.read(HEADER_SIZE):
                entry_header = Entry.decode_header(header_bytes)
                key_bytes: bytes = f.read(entry_header.key_size)
                _ = f.read(entry_header.value_size)
                key: str = key_bytes.decode()
                entry_size: int = (
                    entry_header.key_size + entry_header.value_size + HEADER_SIZE
                )
                key_entry: KeyEntry = KeyEntry(
                    self.file_id,
                    offset,
                    entry_size,
                    entry_header.timestamp,
                )
                offset += entry_size
                if entry_header.typ == EntryType.NORMAL.value:
                    key_dir[key] = key_entry

        return key_dir

    @classmethod
    def encode_entry(
        cls,
        key: str,
        value: str,
        timestamp: int,
        typ: int = EntryType.NORMAL.value,
    ) -> Tuple[bytes, int]:
        """Encode entry into bytes.

        Args:
            key: The entry's key.
            value: The entry's value.
            timestamp: The given timestamp.
            typ: The entry's type.

        Returns:
            The byte object and the size of entry in bytes.

        """
        entry: Entry = Entry(key, value, timestamp, typ=typ)
        data, entry_size = entry.encode()
        return data, entry_size

    def append(self, data: bytes) -> None:
        """Append entry bytes to the log file.

        Args:
            data: The entry bytes.

        """
        self.write_log_entry(data)

    def read_log_entry(self, offset: int, entry_size: int) -> Entry:
        """Read a LogEntry from log file at offset."""
        with self.file as f:
            f.seek(offset)
            data: bytes = f.read(entry_size)
        entry: Entry = Entry.decode(data)
        return entry

    def write_log_entry(self, data: bytes) -> None:
        """Write log entry(bytes format) to log file."""
        self.file.write(data)
        self.file.flush()
        os.fsync(self.file.fileno())
        # Update last write position so that next record can be written from this point.
        self.write_position += len(data)

    @classmethod
    def get_file_name(cls, dir_name: Union[str, Path], file_id: int) -> str:
        return f'{dir_name}/{LOG_FILE_PREFIX}.{file_id}'

    @property
    def closed(self) -> bool:
        return self.file.closed

    def close(self) -> None:
        """Close the file.

        Before we close the file, we need to safely write the contents in the buffers
        to the disk.

        """
        if self.closed:
            return

        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()

    def delete(self) -> None:
        self.file.close()
        Path(self.file_name).unlink(missing_ok=True)

    def __enter__(self) -> 'LogFile':
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if not self.closed:
            self.close()

    def __del__(self) -> None:
        self.close()
        del self.file

    def __iter__(self) -> Generator[Tuple[Entry, int], None, None]:
        with open(self.file_name, 'rb') as f:
            offset: int = 0
            while header_bytes := f.read(HEADER_SIZE):
                entry_header = Entry.decode_header(header_bytes)
                payload_size: int = entry_header.key_size + entry_header.value_size
                payload_bytes: bytes = f.read(payload_size)
                entry: Entry = Entry.decode(header_bytes + payload_bytes)
                if entry_header.typ == EntryType.NORMAL.value:
                    yield entry, offset

                offset += HEADER_SIZE + payload_size

    def __repr__(self) -> str:
        return f'LogFile<file_name={self.file_name}>'
