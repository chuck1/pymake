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

    def __init__(self, filename, filename_meta):
        super().__init__()

        self._db = shelve.open(filename, writeback=True)
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
        
        if h1 not in self._db:
            self._db[h1] = SubRegistry()

        try:

            return self._db[h1][h2]

        except (AttributeError, ModuleNotFoundError) as e:

            # indicates problem with pickled object, need to delete it
            logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
            
            del db[str(_id)]

            raise
        
        except RuntimeError as exc:

            if exc.args[0] == "input stream error":
                logger.error(crayons.red(f'Unpickle error for {_id}. delete'))
                del db[str(_id)]

            raise

    def get_subregistry_meta(self, req, f=None):
        h1 = req.hash1
        h2 = req.hash2

        if h1 not in self._db_meta:

            self._db_meta[h1] = pymake.registry.SubRegistry()

        return self._db_meta[h1][h2]


    def close(self):
        self._db.close()
        self._db_meta.close()


