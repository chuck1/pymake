
import copy
from mybuiltins import *
import jelly

class Desc(jelly.Serializable):
    """
    This class conforms to the following model.
    The data passed to the constructor shall be the data that will be stored by the object and
    needed to characterize it.
    It shall be all the data needed to recreate the object by pickling.
    """
    
    # required keys
    _keys = None

    # additional keys that should be part of return of encoded()
    # which is used by pymake. not for serialization
    _keys_encode = ("type_",)

    def __init__(self, **kwargs):

        self._kwargs = copy.deepcopy(kwargs)

        # only keys in _keys should appears in kwargs
        if self._keys is not None:
            for k in kwargs:
                if k not in self._keys:
                    raise Exception(f'invalid kwarg {k!r}')

        # because I dont want to type out all parameters in constructor
        if self._keys is not None:
            for key in self._keys:
                if key not in self._kwargs:
                    self._kwargs[key] = None

        assert ('type' in self._kwargs) or ('type_' in self._kwargs) or hasattr(self, 'type_')

        #for k, v in self._kwargs.items():
        #    setattr(self, k, v)

        if not hasattr(self, 'type_'):
            if 'type_' in self._kwargs:
                self.type_ = self._kwargs['type_']
            else:
                self.type_ = self._kwargs['type']
            
    def __getattribute__(self, name):
        if name.startswith('_'):
            return super().__getattribute__(name)
        if name in self._kwargs:
            return self._kwargs[name]
        return super().__getattribute__(name)

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
            return

        self._kwargs[name] = value

    def __deepcopy__(self, memo):
        return self.__class__(**copy.deepcopy(self._kwargs, memo))

    def __contains__(self, key):
        return (key in self._kwargs)

    def keys(self):
        """
        keys that this will have when encoded
        """
        lst = self._kwargs.keys()
        yield from lst
        
        for k in self._keys_encode:
            if k in lst:
                continue
            yield k

    def encoded(self):
        dct = jelly.encode(self._kwargs)

        for key in self._keys_encode:
            if key not in dct:
                dct[key] = getattr(self, key)

        return dct

    def _print(self):
        print(Indent.s() + str(self))
        with Indent() as indent:
            for k, v in self._kwargs.items():
                if isinstance(v, Desc):
                    print(str(indent) + f'{k!s}:')

                    with Indent():
                        v._print()

                else:
                    print(str(indent) + f'{k!s}: {v!r}')





