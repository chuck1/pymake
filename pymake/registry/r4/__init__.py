import asyncio
import collections
import datetime
import logging
import os
import pickle
import pprint
import shelve
import json

import bson
import crayons
import pymake.registry
from pymake.util import clean
import pymake.registry

logger = logging.getLogger(__name__)


class Registry(pymake.registry.Registry):

    def __init__(self, folder, filename_meta):
        super().__init__()

        try:
            os.makedirs(os.path.dirname(filename_meta))
        except OSError:
            pass

        try:
            os.makedirs(folder)
        except OSError:
            pass

        self.folder = folder

        self._db_meta = shelve.open(filename_meta, writeback=True)

    async def get_lock(self, req):

        i = req.hash2

        logger.debug(f'i = {i!s}')

        async with self.__lock:

            if i not in self._locks:
                
                self._locks[i] = asyncio.Lock()

            return self._locks[i]

    async def get_subregistry(self, req, f=None):
        h1 = req.hash1
        h2 = req.hash2
        
        if not os.path.exists(os.path.join(self.folder, h1)):
            os.makedirs(os.path.join(self.folder, h1))

        filename = os.path.join(self.folder, h1, h2)

        if not os.path.exists(filename):
            return pymake.registry.SubRegistry()


        try:

            with open(filename, 'rb') as f:
                s = f.read()

            return pickle.loads(s)

        except (AttributeError, ModuleNotFoundError) as e:

            # indicates problem with pickled object, need to delete it
            logger.error(crayons.red(f'Unpickle error for {h2}. delete'))
            
            os.remove(filename)

            logger.error(crayons.red(f'delete successful'))

            raise
        
        except RuntimeError as exc:

            if exc.args[0] == "input stream error":
                logger.error(crayons.red(f'Unpickle error for {_id}. delete'))

                os.remove(filename)

            raise

    async def get_subregistry_meta(self, req, f=None):

        h1 = req.hash1
        h2 = req.hash2

        if h1 not in self._db_meta:

            self._db_meta[h1] = pymake.registry.SubRegistry()

        return self._db_meta[h1][h2]


    def close(self):
        print("closing database")
        self._db_meta.close()

    def write_subregistry(self, req, r):
        
        h1 = req.hash1
        h2 = req.hash2

        filename = os.path.join(self.folder, h1, h2)

        s = pickle.dumps(r)

        with open(filename, 'wb') as f:
            f.write(s)






