import logging
import pprint

import cached_property
import crayons
import bson
from mybuiltins import ason

import pymake.req

logger = logging.getLogger(__name__)



class ReqDocBase(pymake.req.Req):
    def __init__(self, d, build=True):
        """
        d     - bson-serializable object. once initialized, MUST NOT CHANGE
        build - flag is this should be built or just read
        """

        if not isinstance(d, dict):
            raise Exception()

        assert 'type' in d
        self.d = d

        self.build = build

    def __encode__(self):
        return {'/ReqDoc': {'args': [ason.encode(self.d)]}}

    def __repr__(self):
        if 'type' not in self.d:
            print(self.d)
            breakpoint()
        return f'{self.__class__.__name__} id = {self._id} {{"type":{self.d["type"]!r}}}'

    @cached_property.cached_property
    def key_set(self):
        return set(self.d.keys())

    @cached_property.cached_property
    def encoded(self):
        _ = ason.encode(self.d)
        return _

    def print_long(self):
        print(f'id: {self._id}')
        s = bson.json_util.dumps(self.encoded)
        print(s)
        pprint.pprint(self.encoded)
        #pprint.pprint(self.get_encoded())

    def get_doc(self):
        d = pymake.req.client.find_one(self.encoded)
        return d

    @cached_property.cached_property
    def _id(self):
        d = pymake.req.client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        if d is None:
            res = pymake.req.client.insert_one(self.encoded)
            return res.inserted_id

        return str(d["_id"])

    def _read_mtime(self, d):
        if d is None: return 0
        if '_last_modified' not in d: return 0
        return d['_last_modified'].timestamp()

    def graph_string(self):
        return bson.json_util.dumps(self.encoded, indent=2)

    async def delete(self):
        res = pymake.req.client._coll.update_one(self.encoded, {'$unset': {'_last_modified': 1}})
        if res.modified_count != 1:
            raise Exception(f"document: {self.d!r}. modified count should be 1 but is {res.modified_count}")

    def would_touch(self, mc):
        return False

    def write_binary(self, b):
        self.write_contents(b)

    def write_json(self, b):
        self.write_contents(b)

    def write_text(self, b):
        assert isinstance(b, str)
        self.write_contents(b)

    def write_contents(self, b):
        # make sure is compatible
        #bson.json_util.dumps(b)
        t = pymake.req.client.update_one(self.encoded, {'$set': {'_contents': b}})
        self._mtime = t.timestamp()

    def read_contents(self):
        assert self.output_exists()
        d = pymake.req.client.find_one(self.encoded)
        #if "_contents" not in d:
        #    breakpoint()
        return d["_contents"]

    def read_json(self):
        return self.read_contents()

    def read_text(self):
        s = self.read_contents()
        if isinstance(s, bytes):
            s = s.decode()
        assert isinstance(s, str)
        return s

    def read_binary(self):
        b = self.read_contents()
        assert isinstance(b, bytes)
        return b

class ReqDoc(ReqDocBase):
    """
    use mongodb to store pickled binary data
    """

    def output_exists(self):
        d = pymake.req.client.find_one(self.encoded)
        if d is None: return False
        b = bool('_last_modified' in d)
        
        if b:
            # look for FakePickle object

            s = d["_contents"] #self.read_contents()
            try:
                o = pickle.loads(s)
            except Exception as e:
                #logger.warning(f"pickle load error: {e!r}")
                pass
            else:
                if isinstance(o, FakePickle):
                    if not fake_pickle_archive.contains(o):
                        return False

        return b

    def output_mtime(self):

        if hasattr(self, '_mtime'):
            logger.debug(crayons.blue('USING SAVED MTIME'))
            return self._mtime

        d = pymake.req.client.find_one(self.encoded)

        self._mtime = self._read_mtime(d)

        return self._mtime

    async def read_pickle(self, mc=None):
 
        #logger.info(f"read pickle {self.encoded}")
   
        return await super().read_pickle(mc)


class ReqDoc1(ReqDocBase):

    def output_exists(self):

        try:
            pymake.req.registry.read(self.encoded)
        except:
            return False
        else:
            return True
    
    def output_mtime(self):

        return pymake.req.registry.read_mtime(self.encoded)

    def write_pickle(self, o):

        #logger.info("write pickle")

        pymake.req.registry.write(self.encoded, o)

    async def read_pickle(self, mc=None):
 
        #logger.info("read pickle")
   
        return pymake.req.registry.read(self.encoded)











