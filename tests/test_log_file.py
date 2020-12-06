from pathlib import Path

from pycask.file import LogFile


class TestCreateLogFile:
    def test_create_default_file(self, data_dir: Path) -> None:
        log_file = LogFile(data_dir)
        assert log_file.file_id == 0
        assert Path(log_file.file_name) == Path(data_dir, f'cask.db.{0}')
