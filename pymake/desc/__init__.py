import copy
from mybuiltins import *
import ason

class Desc:

    _keys = tuple()

    @classmethod
    async def decode(cls, decoder, kwargs):
        kwargs = await decoder.decode(kwargs)
        return cls(**kwargs)

    def __init__(self, **kwargs):
        self._kwargs = kwargs

        for key in self._keys:
            if key not in self._kwargs:
                self._kwargs[key] = None

        assert ('type' in self._kwargs) or ('type_' in self._kwargs) or hasattr(self, 'type_')

        #if 'type_' not in self._kwargs:
        #    self._kwargs['type_'] = self.type_

        for k, v in self._kwargs.items():
            #if k == 'type_': continue
            setattr(self, k, v)

        if not hasattr(self, 'type_'):
            if 'type_' in self._kwargs:
                self.type_ = self._kwargs['type_']
            else:
                self.type_ = self._kwargs['type']
            

        #    def type_(self):
        #return self._kwargs['type_']

    def __encode__(self):
        return {'Desc': ason.encode(self._kwargs)}

    def __deepcopy__(self, memo):
        return self.__class__(**copy.deepcopy(self._kwargs, memo))

    def __getitem__(self, key):
        return self._kwargs[key]

    def __contains__(self, key):
        return (key in self._kwargs)

    def keys(self):
        return self._kwargs.keys()

    def encoded(self):
        return ason.encode(self._kwargs)


