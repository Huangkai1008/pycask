import struct
import time
import zlib

header_bytes = bytearray(16)
header_bytes[4:] = struct.pack('!3I', int(time.time()), len('hello'), len('world'))
data: bytes = 'helloworld'.encode()
crc: int = zlib.crc32(header_bytes[4:] + data)
header_bytes[:4] = struct.pack('!I', crc)
timestamp, key_size, value_size = struct.unpack('!3I', bytes(header_bytes[4:]))
print(timestamp, key_size, value_size)
