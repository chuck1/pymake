
import pytest

import pymake.req.req_doc

def _test_req():

    d = {
            "type": "test",
            "a":    0,
            "b":    "hello",
            "c": {
                "d": 0,
                },
            "d": [
                0,
                ],
            }
    
    req = pymake.req.req_doc.ReqDoc1(d)

    return req

@pytest.mark.asyncio
async def test_string(makefile):

    req = _test_req()

    s = "hello"

    await req.write_string(s)

    assert (await req.read_string()) == s

    
@pytest.mark.asyncio
async def test_binary(makefile):

    req = _test_req()

    s = b"hello"

    await req.write_binary(s)

    assert (await req.read_binary()) == s

@pytest.mark.asyncio
async def test_object(makefile):

    req = _test_req()

    class Foo: pass

    s = Foo()

    await req.write_pickle(s)

    assert (await req.read_pickle()) == s

@pytest.mark.asyncio
async def test_json(makefile):

    req = _test_req()

    class Foo: pass

    s = dict()

    await req.write_json(s)

    assert (await req.read_json()) == s








