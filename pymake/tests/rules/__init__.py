
class A(pymake.rules.RuleDoc):

    @classmethod
    def descriptor_pattern(cls):
        return {"type": "A"}

    async def build_requirements(self, mc, func):
        yield await func(pymake.req.req_doc.ReqDoc1({"type": "B"}))

    async def build(self, mc, _1, reqs):
        print(reqs)

        print(reqs[0].read_string())

        self.req.write_string("hello from A")

class B(pymake.rules.RuleDoc):

    @classmethod
    def descriptor_pattern(cls):
        return {"type": "B"}

    async def build_requirements(self, mc, func):
        return
        yield await func(pymake.req.req_doc.ReqDoc1({"type": "B"}))

    async def build(self, mc, _1, reqs):
        self.req.write_string("hello from B")

