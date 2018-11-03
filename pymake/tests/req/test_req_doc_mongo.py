
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
    
    req = pymake.req.req_doc.ReqDoc0(d)

    return req

@pytest.mark.asyncio
async def test_string():

    req = _test_req()

    s = "hello"

    req.write_string(s)

    assert req.read_string() == s

    
@pytest.mark.asyncio
async def test_binary():

    req = _test_req()

    s = b"hello"

    req.write_binary(s)

    assert req.read_binary() == s

@pytest.mark.asyncio
async def test_object():

    req = _test_req()

    class Foo: pass

    s = Foo()

    req.write_pickle(s)

    assert req.read_pickle() == s

@pytest.mark.asyncio
async def test_json():

    req = _test_req()

    class Foo: pass

    s = dict()

    req.write_json(s)

    assert req.read_json() == s








