
import os

class BuildError(Exception):
    def __init__(self, message):
        super(BuildError, self).__init__(message)

"""
manages the building of targets
"""
class Makefile(object):
    def __init__(self):
        self.rules = []
    def find_rule(self, target):
        for rule in self.rules:
            f_out = list(rule.f_out())
            if target in f_out:
                return rule
        return None

    def make(self, t, test):

        if isinstance(t, list):
            for t1 in t: self.make(t1, test)
            return
        
        if t is None:
            raise Exception('target is None'+str(t))

        if isinstance(t, Rule):
            t.make(self, test)
            return

        rule = self.find_rule(t)
        if rule is None:
            if os.path.exists(t):
                pass
            else:
                for r in self.rules:
                    print(r,list(r.f_out()))
                raise Exception("no rules to make {}".format(t))
        else:
            rule.make(self, test)


"""
a rule
f_out and f_in are generator functions that return a list of files
func is a function that builds the output
"""
class Rule(object):

    def __init__(self, f_out, f_in, func):
        self.f_out = f_out
        self.f_in = f_in
        self.func = func

        self.up_to_date = False
        
    def check(self, makefile, f_out, f_in, test):

        if None in f_in:
            raise Exception('None in f_in ' + str(self))
        
        for f in f_in:
            makefile.make(f, test)
        
        for f in f_out:
            if not os.path.exists(f): return True, "{} does not exist".format(f)
        
        mtime = [os.path.getmtime(f) for f in f_out]
        
        for f in f_in:
            if isinstance(f, Rule):
                #return True
                continue

            for t in mtime:
                if os.path.exists(f):
                    if os.path.getmtime(f) > t:
                        return True, f

        return False, None

    def make(self, makefile, test):

        if self.up_to_date: return
        
        f_in = list(self.f_in(makefile))
        f_out = list(self.f_out())
        
        should_build, f = self.check(makefile, f_out, f_in, test)
        if should_build:
            if test:
                print('build',f_out,'because',f)
            else:
                ret = self.func(f_out, f_in)

                if ret != 0:
                    raise BuildError(str(self) + ' return code ' + str(ret))

        self.up_to_date = True


"""
a rule to which we can pass a static list of files for f_out and f_in
"""
class RuleStatic(Rule):
    def __init__(self, static_f_out, static_f_in, func):
        super(RuleStatic, self).__init__(
                lambda: static_f_out,
                lambda makefile: static_f_in,
                func)



