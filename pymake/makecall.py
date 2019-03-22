import asyncio
import contextlib
import functools
import hashlib
import inspect
import re
import os
import logging
import traceback
import json

import crayons
import pygraphviz as gv

from mybuiltins import *
from .util import *

import pymake.args
import pymake.rules

logger = logging.getLogger(__name__)

class MakeCall:
    def __init__(self, makefile, args={}, stack=[], thread_depth=0):
        self.makefile = makefile
        self.decoder = makefile.decoder

        self.args = pymake.args.Args(**args)

        self.stack = stack

        self.thread_depth = thread_depth

    @property
    def show_plot(self):
        return self.args.show_plot

    def copy(self, **kwargs):
        args1 = dict(self.args._args)
        args1.update(kwargs)
        return MakeCall(self.makefile, args1, self.stack)

    def make_threadsafe(self, *args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(self.make(*args, **kwargs))

    async def make(self, req_0, test=None, ancestor=None, **kwargs):
        """
        this is the ONLY make function that should be called outside the pymake module
        """

        # validate
        if req_0 is None:
            return pymake.result.ResultNoBuild("req is None")

        if not isinstance(req_0, pymake.req.Req):
            raise Exception(f"req should be a pymake.req.Req object, not {req_0!r}")

        if isinstance(req_0, pymake.req.ReqFake):
            return pymake.result.ResultNoBuild("fake")

        # debug
        if isinstance(req_0, pymake.req.ReqFile):
            if 'node_20' in req_0.fn:
                #breakpoint()
                pass

        # get equivalent req object from cache or add to cache
        req = self.makefile.cache_get(req_0)

        logger.debug(f'makefile: {id(self.makefile)} req: {id(req)} {req!r}')

        # added this because needed to make a file when test was True
        if test is None: test = self.args.test

        makecall = self.copy(test=test, **kwargs)

        with MakeContext(makecall.stack, req):

            if not req.build:
                if req.output_exists():
                    ret = pymake.result.ResultNoBuild()
                    logger.info(f"make {req} result = {ret}")
                    return ret

            ret = await req._make(makecall, ancestor)

            logger.debug(f"make {req} result = {ret}")
            
            return ret

    async def decode(self, a):
        return await self.makefile.decoder.decode(a, (self.copy(force=False),))




