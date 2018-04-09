import contextlib
import functools
import inspect
import pickle
import re
import os
import logging
import traceback

from cached_property import cached_property
from mybuiltins import *
from .exceptions import *
from .util import *
from .result import *
from .file_index import *

logger = logging.getLogger(__name__)

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class Req:
    def output_exists(self):
        return None

    def output_mtime(self):
        return None

    async def make(self, loop, makefile, mc, ancestor):

        #if not mc.args['test']:
        if True:
            if self in makefile._cache_req:
                #print('{} is in cache'.format(target))
                return ResultNoBuild()
       
        makefile._cache_req.append(self)
    
        mc.add_edge(ancestor, self)

        rules = makefile.rules_sorted(self)

        if len(rules) == 0:
            b = self.output_exists()
            if b:
                return ResultNoRuleFileExists()
            else:
                raise NoTargetError("no rules to make {}".format(repr(self)))
       
        rule = rules[0]

        #if self.touch_maybe(mc): return

        #mc.add_edge(ancestor, rule)

        #for rule in rules:

        try:
            ret = await rule._make(loop, mc, self)
        except NoTargetError as e:
            print('while building', repr(self))
            print(' ',e)
            raise
        
        return ret

    def would_touch(self, mc):
        if not os.path.exists(self.fn):
            return False

        for touch_str in mc.args.get('touch', []):
            if touch_str:
                #print(crayons.yellow(f'touch: {touch_str}'))
                pat = re.compile(touch_str)
                m = pat.match(self.fn)
                if m:
                    return True
        return False

    def touch_maybe(self, mc):
        if self.would_touch(mc):
            print(crayons.yellow(f'touch: {self.fn}'))
            touch(self.fn)
            return True
        return False

class ReqFake(Req):
    def __init__(self, fn):
        self.fn = fn

    def make(self, makefile, mc, ancestor):
        return ResultNoBuild()

    def __repr__(self):
        return f'<{self.__class__.__name__} fn={self.fn!r}>'

class ReqFile(Req):
    """
    simple file requirement

    :param fn: relative path to file
    """
    def __init__(self, fn):
        assert isinstance(fn, str)
        self.fn = fn

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.fn == other.fn

    def output_exists(self):
        """
        check if the file exists
        """
        return os.path.exists(self.fn)

    def output_mtime(self):
        """
        return the mtime of the file
        """
        return os.path.getmtime(self.fn)

    def __repr__(self):
        return 'pymake.ReqFile({})'.format(repr(self.fn))

    def load_object(self):
        try:
            with open(self.fn, 'rb') as f:
                return pickle.load(f)
        except:
            logger.error(f'error loading: {self.fn!r}')
            raise

    def load_json(self):
        with open(self.fn, 'r') as f:
            s = f.read()
        try:
            return json.loads(s)
        except:
            logger.error(f'error loading: {self.fn!r}')
            breakpoint()
            raise

    def graph_string(self):
        return self.fn

class ReqFileDescriptor(ReqFile):
    def __init__(self, d):
        self.d = d

        self.fn = manager.get_filename(self.d)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.d})'

    def graph_string(self):
        return json.dumps(self.d, indent=2)


