import os
from pathlib import Path
from typing import BinaryIO

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
        dir_name: str,
        file_id: int = 0,
    ) -> None:
        self._file_id: int = file_id
        self.file_name: str = self._get_file_name(dir_name, file_id)
        self.file: BinaryIO = open(self.file_name, 'a+b')
        self.write_position: int = Path(self.file_name).stat().st_size

    @property
    def file_id(self) -> int:
        """Returns the ID of current file."""
        return self.file_id

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
    def _get_file_name(dir_name: str, file_id: int) -> str:
        return f'{dir_name}/cask.db.{file_id}'
