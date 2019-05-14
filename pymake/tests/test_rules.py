import pytest

import jelly
import pymake.makecall
import pymake.req.req_doc

@pytest.mark.asyncio
async def test_0(makefile):

    pymake.client.client = pymake.client.Client()

    mc = pymake.makecall.MakeCall(makefile, classDecoder=jelly.Decoder)

    req = pymake.req.req_doc.ReqDoc1({"type_": "A"})

    await mc.make(req)


