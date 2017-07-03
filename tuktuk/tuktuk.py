from collections import namedtuple
from cytoolz import curry
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import time
from . import utils


_results = utils.FuncDict()
_task_collection = None


Pipe = namedtuple(
    'Pipe',
    'tasklist init destroy'
)


def pipeline_args():
    return _results[pipeline_args]


def runpipe(pipe, context, collector, pipeargs=None, **kwargs):
    start = time.time()
    '''Loops a collection of tasks inside a pipe executing them within
    a specific execution context.
    Extra named args are passed to the execution context.
    '''
    # pipeargs is the input to the pipeline. We first store it in our
    # results global so every task has access to ir through the special
    # input word: tuktuk.tuktuk:pipeline_args.*
    if pipeline_args not in _results:
        print 'Init args no esta definido'
        _results[pipeline_args] = pipeargs

    if pipe.init:
        pipe.init(**pipeargs)

    d = utils.FuncDict()
    with context(**kwargs) as c:
        assert getattr(c, 'submit')

        for t in pipe.tasklist:
            args = utils.make_args(t, _results)
            _results[t] = c.submit(t, *args)

    for t in pipe.tasklist:
        d[t] = collector(_results[t])
        _results[t] = d[t]

    if pipe.destroy:
        pipe.destroy(**pipeargs)

    return d.get_list(pipe.tasklist)

'''serial, processes or threaded pipes are specified by fixing the
corresponding context and result collectors
'''
runpipe_serial = curry(
    runpipe, context=utils.SerialExecutor, collector=utils.serial_collector)
runpipe_process = curry(
    runpipe, context=ProcessPoolExecutor, collector=utils.concurrent_collector)
runpipe_thread = curry(
    runpipe, context=ThreadPoolExecutor, collector=utils.concurrent_collector)


def run(pipe, pipeargs=None, **kwargs):
    global _results

    _results = utils.FuncDict()
    return runpipe_serial(pipe=pipe, pipeargs=pipeargs, **kwargs)

''' _insertpipe function allows to insert a pipe as a task inside other pipe.
Same as before: for each execution context only make functions with fixed
contexts.
'''

def _insertpipe(tasklist, name, context, collector, **kwargs):
    rpipe = curry(runpipe, pipe=Pipe(tasklist, init=None, destroy=None), context=context, collector=collector, **kwargs)
    rpipe.__name__ = name
    rpipe.__module__ = name
    return rpipe


serial = curry(_insertpipe, context=utils.SerialExecutor, collector=utils.serial_collector)
parallelp = curry(
    _insertpipe, context=ProcessPoolExecutor, collector=utils.concurrent_collector)
parallelt = curry(
    _insertpipe, context=ThreadPoolExecutor, collector=utils.concurrent_collector)


def parsedict(dictio):
    assert 'Pipeline' in dictio

    # get the list of tasks
    pipe = dictio['Pipeline']
    tasklist = [parse_task(t) for t in pipe]
    p = Pipe(tasklist, init=None, destroy=None)
    return p


def parse_task(task):
    # each task in the list should have only one main key in the dictionary
    assert len(task.keys()) == 1
    ttype = task.keys()[0]

    # each task type should be registered in our task collection
    assert ttype in _task_collection
    return _task_collection[ttype].fromdict(task)

