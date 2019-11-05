import datetime
import functools
import hashlib
import logging
import os
import pickle
import shelve
import struct

import crayons

logger = logging.getLogger(__name__)

def _address_1(v):
    
    if isinstance(v, dict):
        yield from  _address(v)

    elif isinstance(v, list):
        yield from _address_list(v)

    else:
        hash(v)
        yield v
       
def _address_list(l):

    yield "LIST"

    for v in l:
        yield "LISTELEMENT"
        yield from _address_1(v)

def _address(d):

    assert isinstance(d, dict)

    def _process_key(s):
        if isinstance(s, str): return s
        if s is None: return str(s)
        if isinstance(s, (float, int)): return str(s)
        raise TypeError(f'unexpected key type {type(s)} {s!r}')

    class Key:
        def __init__(self, s):
            self.s0 = s
            self.s1 = _process_key(s)

        def __lt__(self, other):
            return (self.s1 < other.s1)

    keys = list(sorted([Key(s) for s in d.keys()]))

    while keys:
        k = keys.pop(0)

        yield k.s1

        v = d[k.s0]

        yield from _address_1(v)


class Address:
    """
    takes a dict-like object and creates an ordered sequence of hashable objects
    that serves as an address into a tree
    """
    def __init__(self, d):
        """
        d - a dict-like object
        """
        assert isinstance(d, dict)
        l = list(_address(d))

        #h = functools.reduce(lambda x, y: (hash(x) * hash(y)) % 2**(8*7), l)
        #h = h % 1024
        #h = struct.pack('h', h).hex()

        #self.l = [h] + l

        self.l = l

        self.s = "".join(str(_) for _ in l)

        #self.h = hash(tuple(l))


        m = hashlib.sha256()
        m.update(pickle.dumps(l))
        self.h = m.hexdigest()




