import pprint
import pymake.doc_registry

class Foo:
    pass

def test_0():

    r = pymake.doc_registry.DocRegistry()

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
            }

    doc = Foo()

    r.write(d, doc)

    assert r.read(d) is doc

    print()
    print(r._registry)
    print()
    


