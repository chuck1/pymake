
class Result:
    def is_build_required(self):
        raise NotImplementedError()

class ResultBuild(Result):
    def is_build_required(self):
        return True

class ResultNoBuild(Result):
    def is_build_required(self):
        return False

class ResultTestBuild(ResultBuild): pass

class ResultTestNoBuild(ResultNoBuild): pass

class ResultNoRuleFileExists(ResultNoBuild): pass

