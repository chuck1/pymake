import functools
import logging
import re

from mybuiltins import *

logger = logging.getLogger(__name__)

# dont create a PatTuple because
# json cant encode tuples

class Pat:
    pass

class PatOr(Pat):
    def __init__(self, *patterns):
        for _ in patterns:
            assert isinstance(_, Pat)

        self.patterns = patterns

    def match(self, s):
        for pattern in self.patterns:
            if pattern.match(s): return True
        return False

class PatAny(Pat):
    def match(self, s):
        return True

class PatString(Pat):
    def __init__(self, pattern_string=None):
        if pattern_string is not None:
            self.pat = re.compile(pattern_string)
        else:
            self.pat = None

    def match(self, s):
        if not isinstance(s, str): return False
        
        if self.pat is not None:
            return bool(self.pat.match(s))

        return True

class PatInt(Pat):
    def __init__(self, start=None, stop=None):
        self.start = start
        self.stop = stop

    def match(self, i):
        try:
            i = int(i)
        except:
            return False

        if self.start is not None:
            if i < self.start:
                return False
            
        if self.stop is not None:
            if i >= self.stop:
                return False

        return True

class PatList(Pat):
    def __init__(self, _pat=None):
        self._pat = _pat

    def match(self, x):
        if not isinstance(x, list): 
            logger.debug(f'PatList x is not a list')
            return False

        if self._pat is None: return True

        for y in x:
            if not self._pat.match(y): 
                logger.debug(f'PatList element {y!r} does not match pat {self._pat!r}')
                return False

        return True

class PatFloat(Pat):
    def match(self, x):
        try:
            float(x)
            return True
        except:
            return False

class PatDict(Pat):
    def __init__(self, _pat=None):
        self._pat = _pat

    def match(self, x):
        if not isinstance(x, dict):
            return False

        if self._pat is not None:
            if not match_dict(self._pat, x):
                return False

        return True

class PatBool(Pat):
    def match(self, x):
        return isinstance(x, bool)

class PatNullable(Pat):
    def __init__(self, pat=PatAny(), default=None):
        self.pat = pat
        self.default = default

    def match(self, thing):
        if thing is None:
            return True

        return self.pat.match(thing)

class PatInstance(Pat):
    def __init__(self, _types):
        self._types = _types

    def match(self, x):
        return isinstance(x, self._types)

def match_dict(a, b, b1=False):


        set_a = set(a.keys())
        set_b = set(b.keys())
        
        a_and_b = set_a & set_b

        just_pat = set_a - set_b
        just_dsc = set_b - set_a
        
        b0 = False #"type" in a_and_b and a["type"] == b["type"]
        #b0 = "type" in a_and_b and a["type"] == b["type"]
        
        b0 = b0 or b1
        with context_if(functools.partial(logger_level_context, logger, logging.DEBUG), b0):


            logger.debug('match_dict')
            logger.debug('a')
            for k, v in a.items():
                logger.debug(f'  {k!r}: {v!r}')
            logger.debug('b')
            for k, v in b.items():
                logger.debug(f'  {k!r}: {v!r}')



            for k in a_and_b:
                if isinstance(a[k], Pat):
                    if not a[k].match(b[k]):
                        logger.debug(f'{k!r} does not match pattern {a[k]!r} {b[k]!r}')
                        return False
    
                    if isinstance(a[k], PatNullable) and b[k] is None:
                        b[k] = a[k].default
                    
                elif isinstance(a[k], dict) and isinstance(b[k], dict):
                    if not match_dict(a[k], b[k], b0):
                        return False

                else:
                    if not (a[k] == b[k]):
                        logger.debug(f'{k!r} differs')
                        return False
    
            # attributes in the pattern but not in the descriptor must be nullable
            for k in just_pat:
                if not isinstance(a[k], PatNullable):
                    logger.debug(f'{k!r} is in pattern but not descriptor and is not nullable')
                    return False
                else:
                    b[k] = a[k].default
            
            # attributes in the descriptor but not in the pattern must be null
            for k in just_dsc:
                if b[k] is not None:
                    logger.debug(f'{k!r} is in descriptor but not in pattern and is not None')
                    return False
   
            #logger.debug(crayons.green(f'match {a} and {b}'))

        return True


