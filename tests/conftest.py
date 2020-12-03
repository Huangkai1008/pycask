from pathlib import Path
from typing import Generator

import pytest
from _pytest.tmpdir import TempPathFactory

entry_encode_testcases = [
    (
        'hello',
        'world',
        1658395563,
        (b'oQ\xde\x15b\xd9\x1b\xab\x00\x00\x00\x05\x00\x00\x00\x05helloworld', 26),
    ),
    (
        'foo',
        'bar',
        1628312563,
        (b'oQ\xde\x15b\xd9\x1b\xab\x00\x00\x00\x05\x00\x00\x00\x05helloworld', 26),
    ),
]


@pytest.fixture
def entry_encode_test_cases() -> Generator[list, None, None]:
    yield entry_encode_testcases


@pytest.fixture(scope='session')
def data_dir(tmp_path_factory: TempPathFactory) -> Path:
    data_dir = tmp_path_factory.mktemp('data-dir')
    return data_dir
