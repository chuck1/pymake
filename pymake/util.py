import asyncio
import contextlib
import functools
import inspect
import re
import os
import logging
import traceback
import pickle

import crayons
import pygraphviz as gv
from mybuiltins import *

import pymake.req

logger = logging.getLogger(__name__)
logger_queue = logging.getLogger(__name__+'-queue')

"""
The pymake module
"""

MONGO_COLLECTION = None

def can_pickle(o, l=[]):
    if isinstance(o, str):
        return

    if isinstance(o, list):
        for v in o:
            can_pickle(v, l + [o])
        return

    if hasattr(o, '__dict__'):
        for k, v in o.__dict__.items():
            can_pickle(v, l + [(o, k)])
        return

    try:
        pickle.dumps(o)
    except:
        raise Exception(f'cannot pickle {o!r} in {l!r}')


def dict_get(d, k, de):
    if not k in d:
        d[k] = de
    return d[k]


def bin_compare(b0,b1):
    for c0,c1 in zip(b,b1):
        try:
            s0 = chr(c0)
            s1 = chr(c1)
        except Exception as e:
            print('error in bin_compare')
            print(e)
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
    def __init__(self, makecall, stack, target):
        self.makecall = makecall
        self.l = stack
        self.target = target

    async def __do_queue(self):
        
        if isinstance(self.target, pymake.req.ReqFake): 
            self.q = None
            return

        filename = self.target.fn
        
        if filename in self.makecall.makefile._file_queue:
            self.q = self.makecall.makefile._file_queue[filename]
            logger_queue.info(f'join queue for: {filename}')
            
            while not self.q.empty():
                await self.q.join()

            logger_queue.info(f'join success:   {filename}')
        else:
            self.q = asyncio.Queue()
            self.makecall.makefile._file_queue[filename] = self.q
        
        self.q.put_nowait(None)

    async def __aenter__(self):
        self.l.append(self.target)

        await self.__do_queue()
        
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            if False:
                logger.warning(crayons.yellow('stack:'))
                for i in self.l:
                    logger.warning(crayons.yellow(f'\t{i!r}'))
            
        self.l.pop()

        if self.q is not None:
            logger_queue.info(f'task done for:  {self.target.fn}')
            self.q.get_nowait()
            self.q.task_done()

def makedirs(d):
    #d = os.path.dirname(f)
    try:
        os.makedirs(d)
    except OSError:
        pass
    except Exception as e:
        print(e)
        print(d)
        raise

