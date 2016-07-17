__author__ = 'jingda'

import  random,json,functools,time

def run_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        t0 = time.time()
        result =  func(*args, **kw)
        t1 = time.time()-t0
        print('function %s() ,running time:%s'%(func.__name__,t1))
        return result
    return wrapper

def parse_test_session(test_path):
    with open(test_path,'r',newline='') as test:
        for line in test:
            session = json.loads(line)
            yield  session

def extract_baseline(r_path,s_path):
    line_set = set()
    while len(line_set) < 2000:
        line_set.add(random.randint(0,226181))
    sorted(line_set)
    i,n = 0,0
    with open (r_path,'r',newline='') as baseline:
        with open(s_path,'w',newline='') as save_file:
            for line in baseline:
                i+=1
                if i in line_set:
                    entry = json.loads(line)
                    json.dump(entry,save_file)
                    save_file.write('\n')
                    n+=1
                if n == 2000:
                    break

def sample_test_set(test_path,base_path,save_path):
    s = parse_test_session(test_path)
    with open(base_path,'r',newline='') as baseline:
        with open(save_path,'w',newline='') as new_test:
            for line in baseline:
                baseline = json.loads(line)
                s_id = baseline['session_id']
                for session in s:
                    if session['session_id'] == s_id:
                        json.dump(session,new_test)
                        new_test.write('\n')
                        break

@run_time
def extract_test_url_domain(test_path,base_path,save_path):
    sid_list = generate_sid_list(base_path)
    with open(test_path,'r',newline='') as tests:
        with open(save_path,'w',newline='') as save_file:
            for line in tests:
                session = json.loads(line)
                s_id = session['session_id']
                if s_id in sid_list:
                    json.dump(session,save_file)
                    save_file.write('\n')

def generate_sid_list(base_path):
    sid_lists =[]
    with open(base_path,'r',newline='') as base_set:
        for line in base_set:
            baseline = json.loads(line)
            s_id = baseline['session_id']
            sid_lists.append(s_id)
    return sid_lists

def sample_data(path):
    i = 0
    with open(path,'r',newline='') as file:
        for line in file:
            print(line)
            i +=1
            if i >10 :
                break

