import pprint
import pytest
import pymake.doc_registry

class Foo:
    pass

@pytest.mark.asyncio
async def test_0(db, db_meta, client):

    r = pymake.doc_registry.DocRegistry(db, db_meta, client)

    d = {
            "type_":"A",
            "a": 1,
            "b": "hello",
            "c": {
                "a": 1,
                },
            "d": [
                "a",
                1,
                ],
            "e": [
                {
                    "a": 1,
                    },
                    "hey",
                ]
            }

    req = pymake.req.req_doc.ReqDoc1(d)

    doc = Foo()

    await r.write(await req._id(mc), req.encoded, doc)

    assert (await r.read(await req._id(), d)) is doc

    print()
    print(r._registry)
    print()

@pytest.mark.asyncio
async def test_1(db, db_meta, client):

    r = pymake.doc_registry.DocRegistry(db, db_meta, client)

    d = {
            "type_":"A",
            "a": 1,
            "b": 2,
            }

    req = pymake.req.req_doc.ReqDoc1(d)

    doc = Foo()

    await r.write(await req._id(), d, doc)

    assert (await r.read(await req._id(), d)) is doc

    ############################

    d = {
            "type_":"A",
            "a": 1,
            "b": 3,
            }

    req = pymake.req.req_doc.ReqDoc1(d)

    doc = Foo()

    await r.write(await req._id(), d, doc)

    assert (await r.read(await req._id(), d)) is doc
    
    d = {
            "type_":"A",
            "a": 1,
            }

    ############################

    req = pymake.req.req_doc.ReqDoc1(d)

    doc = Foo()

    await r.write(await req._id(), d, doc)

    assert (await r.read(await req._id(), d)) is doc

 



