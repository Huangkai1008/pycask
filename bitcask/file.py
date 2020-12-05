import os
from pathlib import Path
from typing import BinaryIO, Union

from bitcask.entry import HEADER_SIZE, Entry
from bitcask.key_dir import KeyDir, KeyEntry

__all__ = ['LogFile']


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
        self,
        dir_name: Union[str, Path],
        file_id: int = 0,
    ) -> None:
        self._file_id: int = file_id
        self.file_name: str = self._get_file_name(dir_name, file_id)
        self.file: BinaryIO = open(self.file_name, 'a+b')
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
                key_dir[key] = key_entry
        return key_dir

    def append(self, key: str, value: str, timestamp: int) -> int:
        """Append entry to the log file.

        Args:
            key: The entry's key.
            value: The entry's value.
            timestamp: The given timestamp.

        Returns:
            The size of entry in bytes.

        """
        entry: Entry = Entry(key, value, timestamp)
        data, entry_size = entry.encode()
        self.write_log_entry(data)
        return entry_size

    def read_log_entry(self, offset: int, entry_size: int) -> Entry:
        """Read a LogEntry from log file at offset."""
        self.file.seek(offset)
        data: bytes = self.file.read(entry_size)
        entry: Entry = Entry.decode(data)
        return entry

    def write_log_entry(self, data: bytes) -> None:
        """Write log entry(bytes format) to log file."""
        self.file.write(data)
        self.file.flush()
        os.fsync(self.file.fileno())
        # Update last write position so that next record can be written from this point.
        self.write_position += len(data)

    def close(self) -> None:
        """Close the file.

        Before we close the file, we need to safely write the contents in the buffers
        to the disk.

        """
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()

    def __del__(self) -> None:
        self.close()
        del self.file

    @staticmethod
    def _get_file_name(dir_name: Union[str, Path], file_id: int) -> str:
        return f'{dir_name}/cask.db.{file_id}'
