import contextlib
import collections
import functools
import inspect
import re
import os
import logging
import traceback

import crayons
import pygraphviz as gv
from mybuiltins import *
logger = logging.getLogger(__name__)

"""
The pymake module
"""

MONGO_COLLECTION = None

def dict_get(d, k, de):
    if not k in d:
        d[k] = de
    return d[k]

def clean_doc(d0):
    d1 = dict(d0)

    keys_to_delete = [k for k in d1.keys() if k.startswith("_")]
    
    for k in keys_to_delete:
        del d1[k]

    return d1


def bin_compare(b0,b1):
    for c0,c1 in zip(b,b1):
        try:
            s0 = chr(c0)
            s1 = chr(c1)
        except Exception as e:
            logger.error('error in bin_compare')
            logger.error(e)
            s0 = ''
            s1 = ''
        
        msg = '' if c0==c1 else 'differ'
        
        print("{:02x} {:02x} {:6} {:6} {}".format(c0,c1,repr(s0),repr(s1),msg))

def check_existing_binary_data(filename, b0):
    if os.path.exists(filename):
        with open(filename, 'rb') as f:
            b1 = f.read()
        
        if b0 == b1:
            return False
        else:
            #bin_compare(b0,b1)
            return True
    else:
        return True

class BuildError(Exception):
    def __init__(self, message):
        super(BuildError, self).__init__(message)

class NoTargetError(Exception):
    def __init__(self, message):
        super(NoTargetError, self).__init__(message)

class MakeContext:
    def __init__(self, l, x):
        self.l = l
        self.x = x

    def __enter__(self):
        self.l.append(self.x)
        return self

    def __exit__(self, exc_type, exc_value, traceback):


        if exc_type is not None:

            logger.warning(crayons.green('stack:'))
            for i in self.l:
                logger.warning(crayons.green(f'\t{i!r}'))
                #i.print_long()
        
        self.l.pop()

def makedirs(d):
    #d = os.path.dirname(f)
    try:
        os.makedirs(d)
    except OSError:
        pass
    except Exception as e:
        logger.error(e)
        logger.error(d)
        raise

class PickleError(Exception): pass

def _isinstance(v, cls):
    if isinstance(v, cls): return True
    if not inspect.isclass(v): return False
    if not issubclass(v, cls): return False
    if v is cls: return False
    return True

def _sort(a):
    """
    yield the key-value pairs of dict-like `a` with keys in order
    """

    keys = sorted(a.keys())

    for k in keys:

        if isinstance(a[k], dict):
            yield k, sort(a[k])
        else:
            yield k, a[k]


def sort(a):
    assert isinstance(a, dict)

    return collections.OrderedDict(list(_sort(a)))


def clean(a):
    b = dict(a)
    keys = [k for k in a.keys() if k.startswith("_")]
    for k in keys:
        del b[k]
    return sort(b)



