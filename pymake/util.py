
import functools
import inspect
import re
import os
import logging
import traceback

import crayons


red = functools.partial(crayons.red, bold=True)
yellow = functools.partial(crayons.yellow, bold=True)
green = functools.partial(crayons.green, bold=True)

logger = logging.getLogger(__name__)

"""
The pymake module
"""

MONGO_COLLECTION = None

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

class MakeCall(object):
    def __init__(self, makefile, test=False, force=False, show_plot=False):
        self.makefile = makefile
        self.test = test
        self.force = force
        self.show_plot = show_plot
    
    def make(self,t):
        self.makefile.make(target=t, test=self.test, force=self.force)


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

