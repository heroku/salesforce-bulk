from io import IOBase
from itertools import chain, islice


class IteratorBytesIO(IOBase):
    def __init__(self, iterator):
        self.iterator = chain.from_iterable(iterator or [])

    def readable(self):
        return True

    def read(self, n=None):
        return bytes(bytearray(islice(self.iterator, None, n)))
