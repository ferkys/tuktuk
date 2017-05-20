from setuptools import setup, find_packages

setup(
    name='tuktuk',
    version='0.1',
    packages=find_packages(),
    entry_points='''
        [tuktuk.collections]
        SKLearnModel=tuktuk.task_collection:SKlearnModel
        Function=tuktuk.task_collection:Function
        Parallel=tuktuk.task_collection:Parallel
        
    ''',
    )
