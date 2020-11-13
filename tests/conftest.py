import pytest

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
def entry_encode_test_cases():
    yield entry_encode_testcases
