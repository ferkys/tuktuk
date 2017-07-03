import re
import pkg_resources as pkg
import collections
import string


class FuncDict(collections.MutableMapping):
    '''Dict to store function results. Keys can be function
    objects (or function name). If it's indexed by string name,
    module should be included.
    '''
    def key(self, func):
        return (
            '.'.join([func.__module__, func.__name__])
            if callable(func) else func
        )

    def _assert(self, func):
        assert callable(func), (
            '"%s" not a function' % func
        )

    def __init__(self):
        self._val = collections.OrderedDict()

    def __getitem__(self, func):
        return self._val[self.key(func)]

    def __setitem__(self, func, result):
        self._assert(func)
        self._val[self.key(func)] = result

    def __getattr__(self, name):
        return getattr(self._val, name)

    def __iter__(self):
        return iter(self._val)

    def __delitem__(self, func):
        del self._val[self.key(func)]

    def __len__(self):
        return len(self._val)

    def get_list(self, keylist, default=None):
        return self._val.values()

    def __str__(self):
        return "{}".format(self._val)

    def template(self, tplstr, **kwargs):
        tpl = dict()
        assert kwargs, "Need to specifying subsitute mappings"
        for k,v in kwargs.iteritems():
            assert ':' in v and '.' in v
            mod, funcarg = v.split(':')
            func, arg = funcarg.split('.')
            fullfunc = '.'.join([mod, func])

            if arg == '*':
                tpl[k] = self._val[fullfunc]
            else:
                tpl[k] = self._val[fullfunc][int(arg)]

        tmpl = string.Template(tplstr)
        return tmpl.substitute(tpl)





class _Executor(object):
    '''Just a dummy executor. Execution contexts need a submit function
    in order to run a function inside a pipeline.
    '''
    def submit(self, func, *args, **kwargs):
        return func(*args, **kwargs)


class SerialExecutor(object):
    '''Execution context for a Serial executor.
    '''
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



def _parsedocline(doc):
    regex = r'^\s*@from\s+(\w+):\s*((?:\w*\.)*\w+):(\w+)\.(\d+|\*)\s*.*'
    match = re.compile(regex).match(doc)

    if match:
        _, pkg, func, pos = match.groups()
        pos = '-1' if pos == '*' else pos
        assert re.findall('^(-\d+|\d+)$', pos), (
            'Position not an integer "%s"' % doc)
        pos = int(pos)
        return ('.'.join([pkg, func]), pos)
    else:
        return None


def _doc(f):
    return '' if not f.__doc__ else f.__doc__.split('\n')


# doc  = lambda f: '' if not f.__doc__ else f.__doc__.split('\n')

def _parse_func_docstring(func):
    assert callable(func)
    return [
        x
        for x in (_parsedocline(lin) for lin in _doc(func))
        if x
    ]


def make_args(task, results):
    args = list()
    for func, idx in _parse_func_docstring(task):
        if idx == -1:
            args.append(results[func])
        else:
            args.append(results[func][idx])
    return args


def load_entry_point(entry_point):
    ep = pkg.EntryPoint.parse("namep={}".format(entry_point))
    return ep.resolve()


def load_collections():
    d = dict()
    for ep in pkg.iter_entry_points('tuktuk.collections'):
        d[ep.name] = ep.load()
    return d
