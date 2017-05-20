import time

def func0(args):
    '''
    @from args:tuktuk.tuktuk:pipeline_args.*
    '''
    print 'start func0 with args {}'.format(args)
    print 'module: {}'.format(func0.__module__)
    #time.sleep(2)
    print 'end func0'
    return args


def func1(arg):
    '''
    @from arg: testdefs:func0.*
    '''
    print 'start func1 arg {}'.format(arg)
    #time.sleep(2)
    print 'end func1'
    

def func3(arg):
    '''
    @from arg: testdefs:func0.2
    '''    
    print 'start func3 arg {}'.format(arg)
    #time.sleep(2)
    print 'end func3'    
    return 2


def func4(arg):
    '''
    @from arg: testdefs:func0.3
    '''    
    print 'start func4 arg {}'.format(arg)
    #time.sleep(2)
    print 'end func4'    


def func5(arg):
    '''
    @from arg: testdefs:func0.*
    '''
    print 'start func5 arg {}'.format(arg)
    #time.sleep(2)
    print 'end func5'
    

def func6(arg):
    '''
    @from arg: testdefs:func0.2
    '''    
    print 'start func6 arg {}'.format(arg)
    #time.sleep(2)
    print 'end func6'    
    return 2


def func7(arg):
    '''
    @from arg: testdefs:func0.3
    '''    
    print 'start func7 arg {}'.format(arg)
    #time.sleep(2)
    print 'end func7' 