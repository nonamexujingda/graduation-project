__author__ = 'jingda'

import json,time,csv,functools,Global_Variable as gl

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
def train_UserSet(path):
    '''
    从训练集中产生用户列表
    :param path: 训练集路径
    :return:
    '''
    user_lists = set()
    with open(path,'r',newline='') as file:
        reader = csv.reader(file)
        for line in reader:
            try:
                user_id = int(line[1])  #SessionID TypeOfRecord Day USERID
                user_lists.add(user_id)
            except:
                pass
    return user_lists

@run_time
def test_UserSet(path):
    '''
    从测试集中产生用户列表
    :param path: 测试集路径
    :return:
    '''
    user_lists = set()
    with open(path,'r',newline='') as file:
        for line in file:
            user_id = json.loads(line)['user']
            user_lists.add(user_id)
    return user_lists

@run_time
def delete_testSession(train_path,base_path,save_path):
    '''
    删除测试集中的陌生用户的session，然后产生新的测试集
    :param train_path:训练集
    :param base_path:旧测试集
    :param save_path:新测试集地址
    :return:
    '''
    train_user_set = train_UserSet(train_path)
    with open(base_path,'r',newline='') as old:
        with open(save_path,'w',newline='') as new:
            for line in old:
                session = json.loads(line)
                user_id = session['user_id']
                if user_id in train_user_set:
                    json.dump(session,new)
                    new.write('\n')

@run_time
def delete_trainSession(train_path,save_path):
    num=0
    with open(train_path,'r',newline='') as train:
        with open(save_path,'w',newline='') as train_clean:
            reader = csv.reader(train)
            csv_writer = csv.writer(train_clean)
            for line in reader:
                flag = False
                try:
                    url_list = line[3:] #SessionID TypeOfRecord Day USERID
                    for url in url_list:
                        if int(str(url)[0]) > 1 :
                            flag = True
                            num+=1
                            break
                except:
                    pass
                if flag:
                    csv_writer.writerow(line)
    print('num',num)

def sort_test_set(path,save_path):
    with open(path,'r',newline='') as test_set:
        with open(save_path,'w',newline='') as new_test:
            for line in test_set:
                session = json.loads(line)
                serp = 0
                q_list = []
                for query in session['query_list']:
                    if query['serp'] == serp:
                        q_list.append(query)
                        serp+=1

