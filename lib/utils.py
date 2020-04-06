from time import clock
from configparser import ConfigParser
from configparser import RawConfigParser

from configparser import NoOptionError
from configparser import NoSectionError

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
 
    def __enter__(self):
        self.start = clock()
        return self
 
    def __exit__(self, *args):
        self.end = clock()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs

try:
    from line_profiler import LineProfiler
    #from functools import wraps
    #raise Exception('cust')
    def func_line_time(f):
        #@wraps(f)
        def decorator(*args, **kwargs):
            lp = LineProfiler()
            lp_wrap = lp(f)
            func_return = lp_wrap(*args, **kwargs) 
            #func_return = f(*args, **kwargs)
            lp.print_stats() 
            return func_return 
        return decorator 
except:
    def func_line_time(f):
        def decorator(*args, **kwargs):
            with Timer() as t:
                func_return = f(*args, **kwargs)
            if t.verbose:
                print('function %s running for %d'%(f.__name__, t.msecs))
            return func_return 
        return decorator 

class MyConf:
    def getConf(self, section, key, default=None):
        value = default
        try:
            value = self.config.get(section, key)
        except NoSectionError:
            pass 
        except NoOptionError:
            pass        
        return value
    @staticmethod
    def initConf(file, type=None):
        cp = None
        if type == 'raw':
            cp = RawConfigParser()
        else:
            cp = ConfigParser()
        cp.read(file)
        return cp
    def __init__(self, filename, type=None): 
        self.config = MyConf.initConf(filename, type)


@func_line_time
def testrun(abc):
    for i in range(0, 500000):
        if i == 0:
            print('hello ' + abc)
    return False

if __name__ == '__main__':
    result = testrun('good')
    print(result)    