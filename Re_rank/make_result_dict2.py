__author__ = 'jingda'

import time,pandas,json,functools

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
def parse(path):
    session_list = []
    with open(path,'r',newline='') as file:
        for line in file:
            session = json.loads(line)
            url_list = session['url_list']
            user_id = session['user_id']
            session_id = session['session_id']
            query_id = session['query_id']
            new_url_list = [[int('%s%s'%(str(i),url[1])) for i in range(1,4)] for url in url_list]
            session_list.append(create_session(session_id,query_id,new_url_list,user_id))
        return  session_list

def create_session(session_id,query_id,url_list,user_id):
    total_dict=[]
    for idx in range(10):
            dict = {
                'url':int(str(url_list[idx][0])[1:]),
                'q':0,
                'd':0,
                'sat':0,
                'click':0,
                'ud':0,
                'uc':0
            }
            total_dict.append(dict)
    return {
        'session_id':session_id,
        'query':int(query_id),
        'url_list':url_list,
        'user_id':int(user_id),
        'dict':total_dict
    }

@run_time
def update(session_list,chunk):
    i = 0
    start = time.time()
    row_name = ['%s_url'%k for k in range(10)]
    for session in session_list:
        query,sessionID,url_list,total_dict = \
            session['query'],session['session_id'],session['url_list'],session['dict']

        user = session['user_id']
        line_q = chunk['query_id'].isin([query])
        line_u = chunk['user_id'].isin([user])

        for idx in range(10):
            if not chunk[line_u].empty :#如果有该用户的历史记录
                line_ud = chunk[line_u][row_name[0]].isin(url_list[idx])
                index_ud = chunk[line_u][line_ud].index
                for m in range(1,10):
                    u_line = chunk[line_u][row_name[m]].isin(url_list[idx])
                    index_ud = index_ud.append(chunk[line_u][u_line].index)
                line_uclick = chunk.ix[index_ud].isin(url_list[idx][1:]).any(1)#如果有相关的d

                total_dict[idx]['ud'] += len(index_ud)#累加d出现的次数
                total_dict[idx]['uc'] += len(chunk.ix[index_ud][line_uclick].index)#增加click的次数

            if chunk[line_q].empty:#如果没有相同query
                line_d = chunk[row_name[0]].isin(url_list[idx])
                index_d = chunk[line_d].index
                for m in range(1,10):
                    line = chunk[row_name[m]].isin(url_list[idx])
                    index_d = index_d.append(chunk[line].index)
                line_click = chunk.ix[index_d].isin(url_list[idx][1:]).any(1)#如果有相关的d
                total_dict[idx]['d'] += len(index_d)#累加d出现的次数
                total_dict[idx]['click'] += len(chunk.ix[index_d][line_click].index)#增加click的次数
            else :#如果有相同的query在当前chunk中
                line_qd = chunk[line_q].isin(url_list[idx]).any(1)
                line_sat = chunk[line_q].isin(url_list[idx][1:]).any(1)
                if chunk[line_q][line_sat].empty:
                    line_d = chunk[row_name[0]].isin(url_list[idx])
                    index_d = chunk[line_d].index
                    for m in range(1,10):
                        line = chunk[row_name[m]].isin(url_list[idx])
                        index_d = index_d.append(chunk[line].index)
                    total_dict[idx]['d'] += len(index_d)#累加d出现的次数
                    line_click = chunk.ix[index_d].isin(url_list[idx][1:]).any(1)#如果有相关的d
                    total_dict[idx]['click'] += len(chunk.ix[index_d][line_click].index)#增加click的次数
                else:
                    total_dict[idx]['sat'] += len(chunk[line_q][line_sat].index)
                    total_dict[idx]['q'] += len(chunk[line_q][line_qd].index)

        session['dict'] = total_dict
        i+=1
        print('session',i,time.time()-start)

def save(session_list,path):
    with open(path,'w',newline='') as result_dict:
        for session in session_list:
            s = {
                'session_id' :session['session_id'],
                'dict':session['dict']
            }
            json.dump(s,result_dict)
            result_dict.write('\n')

def generate_dict(session_list,csv_path,save_path):
    start = time.time()
    reader = pandas.read_csv(csv_path,engine = 'c', iterator =True,chunksize = 40000000)
    i=0
    start2 = time.time()
    for chunk in reader:
        print('chunk is read')
        i+=1
        update(session_list,chunk)
        end2 = time.time()
        print(i,'_time_passed:',end2-start2)

    save(session_list,save_path)
    print('total_time_passed:',time.time()-start)


