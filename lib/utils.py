from time import clock
 
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
        if self.verbose:
            print('elapsed time: %f ms' %(self.msecs))
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
            print('function %s running for %d'%(f.__name__, t.msecs))
            return func_return 
        return decorator 

@func_line_time
def testrun(abc):
    for i in range(0, 500000):
        if i == 0:
            print('hello ' + abc)
    return False

if __name__ == '__main__':
    result = testrun('good')
    print(result)    