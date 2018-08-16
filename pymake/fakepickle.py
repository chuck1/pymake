import logging
import time
"""
import contextlib
import datetime
import functools
import inspect
import pickle
import re
import os
import traceback
import pymongo
import bson
import json
import pprint

import pymake
import cached_property
from mybuiltins import *
from mybuiltins import ason
from .exceptions import *
from .util import *
from .result import *
"""

logger = logging.getLogger(__name__)

class FakePickleArchive:

    def __init__(self):
        self._next_id = int(time.time()*1000)

        self._map = {}

    def next_id(self):
        i = self._next_id
        self._next_id += 1
        return i

    def write(self, o):

        i = self.next_id()

        p = FakePickle(i)

        self._map[i] = o

        logger.debug(f'fake pickle archive write: {o} {i} {p}')

        return p

    def contains(self, p):
        assert isinstance(p, FakePickle)
        return (p.i in self._map)

    def read(self, p):
        assert isinstance(p, FakePickle)
        return self._map[p.i]

fake_pickle_archive = FakePickleArchive()

class FakePickle:
    def __init__(self, i):
        self.i = i


