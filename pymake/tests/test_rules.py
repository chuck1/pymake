import pytest
import pymake.makecall
import pymake.req.req_doc

@pytest.mark.asyncio
async def test_0(makefile):

    mc = pymake.makecall.MakeCall(makefile)

    req = pymake.req.req_doc.ReqDoc1({"type": "A"})

    await mc.make(req)


