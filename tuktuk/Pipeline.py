
from . import utils


class SerialExecutor(object):
    ''' Execution context for a Serial executor that is compatible with those 
    defined inside concurrent.futures.
    '''
    def submit(self, func, *args, **kwargs):
        return func(*args, **kwargs)

    def collect(self, res):
        # Serial Executor doesn't have to manage the collection results, just
        # return them
        return res

    def __enter__(self):
        return _Executor()

    def __exit__(self, *args):
        pass


def concurrent_collector(r):
    ''' Pipelines with a concurrent.futures context need to gather results
    through the result() function.
    '''
    return r.result()


def serial_collector(r):
    ''' Just a dummy collector for the serial pipeline
    '''
    return r


class Pipeline(object):

    def __init__(tasklist, results=None):
        self.tasklist = tasklist
        self.results = results or FuncDict()

