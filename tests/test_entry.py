import pytest

from pycask.entry import Entry


class TestEncodeEntry:
    testcases = [
        (
            'hello',
            'world',
            1658395563,
            (
                b'\xb5\xd8K\xb1\x00b\xd9\x1b\xab\x00\x00\x00\x05\x00\x00\x00\x05helloworld',
                27,
            ),
        ),
        (
            'foo',
            'bar',
            1628312563,
            (
                b'\x82\xc2G\xe0\x00a\x0e\x13\xf3\x00\x00\x00\x03\x00\x00\x00\x03foobar',
                23,
            ),
        ),
    ]

    @pytest.mark.parametrize('key,value,timestamp,expected', testcases)
    def test_encode_entry(
        self, key: str, value: str, timestamp: int, expected: tuple
    ) -> None:
        entry = Entry(key, value, timestamp)
        assert entry.encode() == expected


class TestDncodeEntry:
    testcases = [
        (
            b'\xb5\xd8K\xb1\x00b\xd9\x1b\xab\x00\x00\x00\x05\x00\x00\x00\x05helloworld',
            'hello',
            'world',
            1658395563,
        ),
        (
            b'\x82\xc2G\xe0\x00a\x0e\x13\xf3\x00\x00\x00\x03\x00\x00\x00\x03foobar',
            'foo',
            'bar',
            1628312563,
        ),
    ]

    @pytest.mark.parametrize('data, key,value,timestamp', testcases)
    def test_encode_entry(
        self, data: bytes, key: str, value: str, timestamp: int
    ) -> None:
        assert Entry.decode(data) == Entry(key, value, timestamp)
