import pprint
import pytest
import pymake.doc_registry

class Foo:
    pass

@pytest.mark.asyncio
async def test_0(db, db_meta):

    pymake.client.USE_ASYNC = False

    r = pymake.doc_registry.DocRegistry(db, db_meta)

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

    await r.write(await req._id(), req.encoded, doc)

    assert (await r.read(d)) is doc

    print()
    print(r._registry)
    print()

@pytest.mark.asyncio
async def test_1(db, db_meta):

    r = pymake.doc_registry.DocRegistry(db, db_meta)

    d = {
            "type_":"A",
            "a": 1,
            "b": 2,
            }

    doc = Foo()

    await r.write(d, doc)

    assert (await r.read(d)) is doc
    
    d = {
            "type_":"A",
            "a": 1,
            "b": 3,
            }

    doc = Foo()

    await r.write(d, doc)

    assert (await r.read(d)) is doc
    
    d = {
            "type_":"A",
            "a": 1,
            }

    doc = Foo()

    await r.write(d, doc)

    assert (await r.read(d)) is doc

 



