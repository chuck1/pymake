import datetime
import functools
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

    keys = list(sorted(d.keys()))

    while keys:

        k = keys.pop(0)

        yield k

        v = d[k]

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

        h = functools.reduce(lambda x, y: (hash(x) * hash(y)) % 2**(8*7), l)
        h = h % 1024
        h = struct.pack('h', h).hex()

        self.l = [h] + l





