__version__ = '0.2'

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

from .makefile import *
from .rules import *
from .req import *
from .util import *

