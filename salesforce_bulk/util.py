from io import IOBase
from itertools import chain, islice, takewhile


class IteratorBytesIO(IOBase):
    def __init__(self, iterator):
        self.iterator = chain.from_iterable(iterator or [])

    def readable(self):
        return True

    def read(self, n=None):
        return bytes(bytearray(islice(self.iterator, None, n)))

    def readline(self, limit=None):
        line = takewhile(lambda x: x != b'\n', self.iterator)
        b = bytearray(islice(line, None, limit))
        if not b or len(b) == limit:
            return bytes(b)
        else:
            b.extend(b'\n')
            return bytes(b)
