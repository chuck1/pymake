import asyncio
import logging

import pymake
import pymake.doc_registry
from pymake.util import clean

logger = logging.getLogger(__name__)

"""
this module contains multiple implementations of a registry for storing and retrieving pickleable objects
"""

class SubRegistry(pymake.doc_registry.SubRegistry): pass

def _lock(f):
    async def wrapped(self, req, *args):
        #d = clean(d)

        assert isinstance(req, pymake.req.req_doc.ReqDocBase)

        l = await self.get_lock(req)
 
        logger.debug('aquire lock')
        async with l:
            logger.debug('lock aquired')
            return await f(self, req, *args)

    return wrapped


class Registry:

    def __init__(self):

        self._locks = {}

        self.__lock = asyncio.Lock()


    @_lock
    async def read(self, req):

        #d = clean(req.encoded)

        #_id = await self.get_id_cached(req)

        logger.debug(f"read {req}")

        if not (await self.__exists(req)):
            logger.error(f'{req}')
            logger.error(f'{req.d}')
            logger.error(f'{req.d.encoded()}')
            raise Exception(f"Object not found: {repr(req)[:1000]}")

        r = await self.get_subregistry(req)

        logger.debug(f"read from {type(r)} {id(r)}")

        if r.doc is None:
            raise Exception(f"Object not found: {d!r}")

        return r.doc.doc

    @_lock
    async def read_mtime(self, req):

        r = await self.get_subregistry_meta(req)

        return r.doc.mtime

    @_lock
    async def write(self, req, doc):
        """
        :param _id: unique id for address
        :param d: dict describing address
        :param doc: contents to save
        """


        logger.debug(f"write {req!r} {doc!r}")

        assert not asyncio.iscoroutine(doc)

        r = await self.get_subregistry(req)
        r_meta = await self.get_subregistry_meta(req)


        d = clean(req.encoded)

        r.doc = pymake.doc_registry.Doc(doc, d=d)
        r_meta.doc = pymake.doc_registry.DocMeta(d=d)

        self.write_subregistry(req, r)

    @_lock
    async def delete(self, req):
        await self.__delete(req)

    async def __delete(self, d):
        logger.warning(crayons.yellow(f"delete {d}"))

        r = await self.get_subregistry(req)
        
        r.doc = None

    @_lock
    async def exists(self, req):
        return await self.__exists(req)

    async def __exists(self, req):

        r = await self.get_subregistry_meta(req)

        if not hasattr(r, "doc"): 

            logger.info('subregistry_meta does not have "doc" attribute')
            return False

        if r.doc is None: 
            logger.info('subregistry_meta "doc" is None')
            return False

        r1 = await self.get_subregistry(req)


        if not hasattr(r1, "doc"): 
            logger.info('subregistry does not have "doc" attribute')
            return False

        if r1.doc is None: 
            logger.info('subregistry "doc" is None')
            return False

        return True

    def write_subregistry(self, req, r):
        pass

