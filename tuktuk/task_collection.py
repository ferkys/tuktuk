import yaml
from . import utils
from . import tuktuk
from sklearn.externals import joblib



''' For defining specific tasks the Task class is provided.
'''

class Task(object):
    def __init__(self, name, ifrom=None):
        self.__name__ = name
        self.__doc__ = str(self.__doc__) + '\n@from arg: {}'.format(ifrom)
        self.ifrom = ifrom

    def __call__(self, *args, **kwargs):
        pass


class Function(Task):
    def __init__(self, func, ifrom=None):
        super(Function, self).__init__(func.__name__, ifrom)
        self.func = func
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __str__(self):
        return 'Function({})'.format(self.func)

    @staticmethod
    def fromdict(dictio):
        assert 'Function' in dictio
        func_def = dictio['Function']

        if type(func_def) == str:
            func = utils.load_entry_point(func_def)
            ifrom = None
        else:
            assert type(func_def) == dict
            func = utils.load_entry_point(func_def['name'])
            ifrom = func_def.get('ifrom')
        return Function(func, ifrom)


class Parallel(Task):
    def __init__(self, name, parallel_type, tasklist, max_workers, ifrom=None):
        super(Parallel, self).__init__(name, ifrom)
        self.func = tuktuk.parallelp(
            tasklist, name=name, max_workers=max_workers
        )

    def __call__(self):
        return self.func()

    @staticmethod
    def fromdict(dictio):
        assert 'Parallel' in dictio
        paral = dictio['Parallel']
        assert 'max_workers' in paral
        assert 'name' in paral
        assert 'pipe' in paral

        pipe = paral['pipe']
        tasklist = [tuktuk.parse_task(t) for t in pipe]

        return Parallel(paral['name'], 'process', tasklist, paral['max_workers'])


class SKlearnModel(Task):
    def __init__(self, name, ifrom, binfile, featurelist, featurefile=None):
        super(SKlearnModel, self).__init__(name, ifrom)

        assert featurelist or featurefile

        self.bin = binfile
        self.model = joblib.load(binfile)

        self.features = featurelist
        self.featurefile = featurefile

        if featurefile:
            with open(featurefile, 'r') as f:
                self.features = yaml.load(f)

    def __call__(self, X):
        print 'calling task SKLearnModel X = {}'.format(X)

    @staticmethod
    def fromdict(dictio):
        print 'from dict sklearn {}'.format(dictio)
        assert 'SKLearnModel' in dictio
        model = dictio['SKLearnModel']

        assert 'binfile' in model
        assert 'name' in model
        assert 'features' in model or 'featurefile' in model

        return SKlearnModel(
            name=model['name'],
            ifrom=model.get('ifrom'),
            binfile=model['binfile'],
            featurelist=model.get('featurelist'),
            featurefile=model.get('featurefile')
        )


