import asyncio

import pytest

import jelly
import pymake
import pymake.client
import pymake.req.req_doc

class A(pymake.rules.RuleDoc):
    @classmethod
    def descriptor_pattern(cls):
        return {
                "type_": "parallel A",
                }

    async def requirements_0(self, mc, func):
        return
        yield

    async def requirements_1(self, mc, func):
        return
        yield

    async def build(self, *args):
    
        pass

class B(pymake.rules.RuleDoc):
    @classmethod
    def descriptor_pattern(cls):
        return {
                "type_": "parallel B",
                "a": pymake.pat.PatInt(),
                }

    async def requirements_0(self, mc, func):
        yield await func(pymake.req.req_doc.ReqDoc1({"type_": "parallel A"}))

    async def requirements_1(self, mc, func):
        return
        yield

    async def build(self, *args):
    
        pass

class C(pymake.rules.RuleDoc):
    @classmethod
    def descriptor_pattern(cls):
        return {
                "type_": "parallel C",
                }

    async def requirements_0(self, mc, func):
        for i in range(10):
            yield await func(pymake.req.req_doc.ReqDoc1({
                "type_": "parallel B",
                "a": i,
                }))

    async def requirements_1(self, mc, func):
        return
        yield

    async def build(self, *args):
    
        pass


@pytest.mark.asyncio
async def test_parallel_0(event_loop, makefile):

    pymake.client.USE_ASYNC = True
    pymake.client.client = pymake.client.Client(event_loop)

    makefile.add_rules([A, B, C])

    pymake.rules.USE_TASKS = True

    mc = pymake.makecall.MakeCall(makefile, classDecoder=jelly.Decoder)


    r_0 = pymake.req.req_doc.ReqDoc1({"type_":"parallel C"})

    await mc.make(r_0)

@pytest.mark.asyncio
async def test_parallel_1(event_loop, makefile):

    pymake.client.USE_ASYNC = True
    pymake.client.client = pymake.client.Client(event_loop)

    n = 20

    reqs = [pymake.req.req_doc.ReqDoc1({"type_":"A"}) for i in range(n)]

    async def f(req):

        return await req._id()

    
    def get_tasks():

        for req in reqs:

            yield asyncio.ensure_future(f(req))


    tasks = list(get_tasks())

    done, pending = await asyncio.wait(tasks)

    for t in done:
        print(t.result())







