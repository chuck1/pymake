import pprint
import pymake.doc_registry

class Foo:
    pass

def test_0(db, db_meta):

    r = pymake.doc_registry.DocRegistry(db, db_meta)

    d = {
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

    doc = Foo()

    r.write(d, doc)

    assert r.read(d) is doc

    print()
    print(r._registry)
    print()
    



