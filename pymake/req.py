
import functools
import inspect
import pickle
import re
import os
import logging
import traceback

from .colors import *
from .exceptions import *

logger = logging.getLogger(__name__)

class Req(object):
    def output_exists(self):
        return None

    def output_mtime(self):
        return None

class ReqFile(Req):
    """
    simple file requirement

    :param fn: relative path to file
    """
    def __init__(self, fn):
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

class ReqDocAttr(Req):
    """
    :param str id_: document id
    :param set attrs: set of attributes
    """

    def __init__(self, id_, attrs):
        self.id_ = id_
        self.attrs = set(attrs)
        self.attrs_remain = set(attrs)
        self._attrs_remain = set(attrs)

    def __repr__(self):
        return '<{}.{} id_={} attrs={}>'.format(self.__class__.__module__, self.__class__.__name__, self.id_, self.attrs)

class NoMeta(Exception): pass

def _get_meta1(o, l, name):
    """
    :param o: object
    :param l: list of attribute names as strings representing chain of getattrs
    :param name: name of final attribute
    """
    if l:
        return _get_meta1(getattr(o, l.pop(0)), l, name)

    if not hasattr(o, '_meta_attr'):
        red('_get_meta1: {} has no _meta_attr'.format(o))
    
    if name not in o._meta_attr:
        raise NoMeta("no meta for {} in {}".format(repr(name), repr(o)))

    return o._meta_attr[name]

def _set_meta1(o, l, name, m):
    """
    :param o: object
    :param l: list of attribute names as strings representing chain of getattrs
    :param name: name of final attribute
    """
    if l:
        return _set_meta1(getattr(o, l.pop(0)), l, name, m)

    o._meta_attr[name] = m

def get_meta(o, a):
    l = a.split('.')
    #red('attr split {}'.format(l))
    return _get_meta1(o, l, l.pop())

def set_meta(o, a, m):
    l = a.split('.')
    #red('attr split {}'.format(l))
    return _set_meta1(o, l, l.pop(), m)

class ReqFileAttr(Req):
    """
    :param str id_: filename
    :param set attrs: set of attributes
    """

    def __init__(self, id_, attrs):
        self.id_ = id_
        self.attrs = set(attrs)
        self.attrs_remain = set(attrs)
        self._attrs_remain = set(attrs)

    def __repr__(self):
        return '<{}.{} id_={} attrs={}>'.format(self.__class__.__module__, self.__class__.__name__, self.id_, self.attrs)

    def reset_remain(self):
        self.attrs_remain = set(self._attrs_remain)

    @property
    def obj(self):
        if not hasattr(self, '_obj'):
            with open(self.id_, 'rb') as f:
                self._obj = pickle.load(f)
        return self._obj

    def output_exists(self):
        """
        check if the file exists
        """
        if not os.path.exists(self.id_):
            #red('does not exist: {}'.format(self.id_))
            return False

        for a in self.attrs:
            try:
                m = get_meta(self.obj, a)
                if not isinstance(m, dict):
                    set_meta(self.obj, a, {'mtime':0})
                    #red('meta for {} = {}'.format(a, m))
            except NoMeta:
                raise OutputNotExists("No meta data for {} in {}".format(repr(a), repr(self.obj)))
        
        return True

    def output_mtime(self):
        """
        return the mtime of the file
        """
        mtimes = []

        for a in self.attrs:
            try:
                m = get_meta(self.obj, a)
            except Exception as e:
                red("error in output_mtime of {}".format(repr(self)))
                red(repr(e))
                raise

            #red('meta for {} = {}'.format(a, m))
            mtimes.append(m['mtime'])

        ret = max(mtimes)

        #red('{} mtimes is {}...{}'.format(self, mtimes, ret))

        return ret





