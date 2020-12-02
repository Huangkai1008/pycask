import pytest

from bitcask.entry import Entry


class TestEncodeEntry:
    testcases = [
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
            (b'?\xd1A\xc2a\x0e\x13\xf3\x00\x00\x00\x03\x00\x00\x00\x03foobar', 22),
        ),
    ]

    @pytest.mark.parametrize('key,value,timestamp,expected', testcases)
    def test_encode_entry(
        self, key: str, value: str, timestamp: int, expected: tuple
    ) -> None:
        entry = Entry(key, value, timestamp)
        assert entry.encode() == expected
