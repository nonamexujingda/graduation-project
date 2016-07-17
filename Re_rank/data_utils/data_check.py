__author__ = 'jingda'

import json,time,csv,functools

def run_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        t0 = time.time()
        result =  func(*args, **kw)
        t1 = time.time()-t0
        print('function %s() ,running time:%s'%(func.__name__,t1))
        return result
    return wrapper

@run_time
def check_testSession_repeat_(path):
    '''
    检查是否测试集中是否有重复的session id
    :param path:
    :return:
    '''
    session_lists = []
    with open(path,'r',newline='') as file:
        for line in file:
            s_id = json.loads(line)['session_id']
            session_lists.append(s_id)
    session_set = set(session_lists)
    if len(session_lists) != len(session_set):
        print('repeat')



