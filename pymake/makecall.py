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
    def __init__(self, makefile, args={}, stack=[]):
        self.makefile = makefile
        self.decoder = makefile.decoder

        self.args = pymake.args.Args(**args)

        self.stack = stack

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

    async def make(self, req, test=None, ancestor=None, **kwargs):
        """
        this is the ONLY make function that should be called outside the pymake module
        """
        if req is None:
            raise Exception("req is None")

        if not isinstance(req, pymake.req.Req):
            raise Exception(f"req should be a pymake.req.Req object, not {req!r}")

        if isinstance(req, pymake.req.ReqFake):
            return pymake.result.ResultNoBuild("fake")

        req = self.makefile.cache_get(req)

        # TODO we may need to still check requirements that can change at runtime
        if req.up_to_date: 
            return pymake.result.ResultNoBuild("up to date")

        logger.debug(repr(req))

        # added this because needed to make a file when test was True
        if test is None:
            test = self.args.test

        makecall = self.copy(test=test, **kwargs)

        with MakeContext(makecall.stack, req):

            if not req.build:
                if req.output_exists():
                    ret = pymake.result.ResultNoBuild()
                    logger.info(f"make {req} result = {ret}")
                    return ret

            if isinstance(req, pymake.rules.Rule):
                return await req.make(mc, None)

            req = self.ensure_is_req(req)
    
            ret = await req._make(makecall, ancestor)

            logger.debug(f"make {req} result = {ret}")
            return ret

    def ensure_is_req(self, target):
        if isinstance(target, str):
            target = pymake.req.ReqFile(target)

        if not isinstance(target, pymake.req.Req):
            raise Exception('Excepted Req, got {}'.format(repr(target)))

        return target


