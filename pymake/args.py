from mybuiltins import *

class ReadOnlyArg:

    def __init__(self, key, default):
        self.key = key
        self.default = default

    def __get__(self, instance, owner):
        return instance._args.get(self.key, self.default)

    def __set__(self, instance, value):
        raise Exception('readonly')

class Args:
    
    test        = ReadOnlyArg('test',       False)
    force       = ReadOnlyArg('force',      False)
    show_plot   = ReadOnlyArg('show_plot',  False)

    def __init__(self, **kwargs):
        self._args = kwargs



