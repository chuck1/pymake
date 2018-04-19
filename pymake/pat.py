import re


class Pat:
    pass

class PatAny(Pat):
    def match(self, s):
        return True

class PatString(Pat):
    def __init__(self, pattern_string=None):
        if pattern_string is not None:
            self.pat = re.compile(pattern_string)
        else:
            self.pat = None

    def match(self, s):
        if not isinstance(s, str): return False
        
        if self.pat is not None:
            return bool(self.pat.match(s))

        return True

class PatInt(Pat):
    def __init__(self, start=None, stop=None):
        self.start = start
        self.stop = stop

    def match(self, i):
        try:
            i = int(i)
        except:
            return False

        if self.start is not None:
            if i < self.start:
                return False
            
        if self.stop is not None:
            if i >= self.stop:
                return False

        return True

class PatFloat(Pat):
    def match(self, x):
        try:
            float(x)
            return True
        except:
            return False

class PatDict(Pat):
    def match(self, x):
        return isinstance(x, dict)

class PatBool(Pat):
    def match(self, x):
        return isinstance(x, bool)

class PatNullable(Pat):
    def __init__(self, pat, default=None):
        self.pat = pat
        self.default = default

    def match(self, thing):
        if thing is None:
            return True

        return self.pat.match(thing)




