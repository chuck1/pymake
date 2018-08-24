
class ReadOnlyArg:

    def __init__(self, key, default):
        self.key = key
        self.default = default

    def __get__(self, instance, owner):
        if not hasattr(self, 'value'):
            self.value = instance._args.get(self.key, self.default)

        return self.value

    def __set__(self, instance, value):
        raise Exception('readonly')

class Args:
    
    test        = ReadOnlyArg('test',       False)
    force       = ReadOnlyArg('force',      False)
    show_plot   = ReadOnlyArg('show_plot',  False)

    def __init__(self, **kwargs):
        self._args = kwargs



