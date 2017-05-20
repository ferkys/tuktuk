import cProfile
import tuktuk
import tuktuk.tuktuk as tk
import yaml

if __name__ == '__main__':
    tuktuk.load_collections()

    data = [1,2,3,4, 'initial']
    print 'collections = {}'.format(tk._task_collection)
    ysrt = '''
Pipeline:
  - Function: testdefs:func0
      
  - Parallel:
      max_workers: 4
      name: MyParallelPipe
      pipe:
        - Function: testdefs:func1
        - Function: testdefs:func3
        - Function: testdefs:func4
    '''

    dictio = yaml.load(ysrt)

    pipe = tk.parsedict(dictio)
    #cProfile.run('tk.runpipe_serial(pipe, pipeargs=data)')

    tk.run(pipe, pipeargs=data)
    print tk._results.template(
        '''
        resultado func0: $func0
        primer resultado func0: $func00
        resultado func3: $func3        
        ''', 
        func0='testdefs:func0.*',
        func00='testdefs:func0.0',
        func3='testdefs:func3.*'
    )
    print '''

    NEW EXECUTION

    '''
    cProfile.run('tk.run(pipe, pipeargs=data)')

        