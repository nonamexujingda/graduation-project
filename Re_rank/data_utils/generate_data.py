__author__ = 'jingda'

from  data_utils import  preprocess as pre
import  functools,time,csv,collections,json,Global_Variable as gl

def run_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        t0 = time.time()
        result =  func(*args, **kw)
        t1 = time.time()-t0
        print('function %s() ,running time:%s'%(func.__name__,t1))
        return result
    return wrapper


def rowName():
    row_name = ['info','user_id','query_id']
    for i in range(10):
        row_name.append(str(i)+'_url')
    return row_name

def create_session(session_id,user_id,day):
    return{
        'session_id':session_id,
        'user_id':user_id,
        'day':day,
        'query_list':[]
    }

def create_query(query_id,serp,url_list):
    return{
        'query_id':query_id,
        'serp':serp,
        'url_list':url_list
    }

def create_baseline(session_id,user_id):
    return{
        'session_id':session_id,
        'user_id':user_id,
        'query_id':0,
        'serp':0,
        'url_list':[]
    }

def train_sessions(session,csv_writer):
    s_id = session['SessionID']
    u_id = session['USERID']
    for query in session['Query']:
        q_id = query['QueryID']
        ranks = collections.defaultdict(lambda :1)
        for idx in range(len(query['Clicks'])-1):#遍历click
            rating = query['Clicks'][idx + 1]['TimePassed'] - query['Clicks'][idx]['TimePassed']
            if (rating < 50):
                rating = 1
            elif (rating <400):
                rating = 2
            else: rating = 3
            if rating > ranks[query['Clicks'][idx]['URLID']]:
                ranks[query['Clicks'][idx]['URLID']] = rating
        if query['Clicks']:#最后一次click
            ranks[query['Clicks'][len(query['Clicks'])-1]['URLID']] = 3
        train_entry = [s_id,u_id,q_id]
        for item in query['URL_DOMAIN']:
            train_entry.append(''.join((str(ranks[item[0]]),str(item[0]))))
        csv_writer.writerow(train_entry)

def test_sessions(session,session_path,baseline_path):
    session_id,day,user_id = session['SessionID'],session['Day'],session['USERID']
    test_session = create_session(session_id,user_id,day)
    baseline = create_baseline(session_id,user_id)
    max_serp = 0
    for query in session['Query']:
        serp_id,query_id = query['SERPID'],query['QueryID']
        flag = False#判断是否至少有1个文档是相关或者是高度相关的
        ranks = collections.defaultdict(lambda :1)
        for idx in range(len(query['Clicks'])-1):#遍历click
            rating = query['Clicks'][idx]['TimePassed'] - query['Clicks'][idx-1]['TimePassed']
            if (rating < 50):
                rating = 1
            elif (rating <400):
                rating = 2
            else: rating = 3
            if rating > ranks[query['Clicks'][idx]['URLID']]:#如果有两次相同的点击，取时间最长的
                ranks[query['Clicks'][idx]['URLID']] = rating
                flag = True
        if query['Clicks']:#最后一次click
            ranks[query['Clicks'][len(query['Clicks'])-1]['URLID']] = 3
            flag = True
        url_list = [(ranks[item[0]],item[0]) for item in query['URL_DOMAIN']]
        test_query = create_query(query_id,serp_id,url_list)
        if flag:#当前query有click
            test_session['query_list'].append(test_query)
            if serp_id > max_serp:#选择最后的serp作为test,至少有两次查询
                max_serp = serp_id
                baseline['url_list'] = test_query['url_list']
                baseline['query_id'] = query_id
                baseline['serp'] = max_serp
    if len(test_session['query_list']) > 1:#this session at least has 2 query
        json.dump(test_session,session_path)
        session_path.write('\n')
        json.dump(baseline,baseline_path)
        baseline_path.write('\n')


def test_sessions_url_domain(session,session_path):
    session_id,day,user_id = session['SessionID'],session['Day'],session['USERID']
    test_session = create_session(session_id,user_id,day)
    for query in session['Query']:
        serp_id,query_id = query['SERPID'],query['QueryID']
        flag = False#判断是否至少有1个文档是相关或者是高度相关的
        ranks = collections.defaultdict(lambda :1)
        for idx in range(len(query['Clicks'])-1):#遍历click
            rating = query['Clicks'][idx]['TimePassed'] - query['Clicks'][idx-1]['TimePassed']
            if (rating < 50):
                rating = 1
            elif (rating <400):
                rating = 2
            else: rating = 3
            if rating > ranks[query['Clicks'][idx]['URLID']]:#如果有两次相同的点击，取时间最长的
                ranks[query['Clicks'][idx]['URLID']] = rating
                flag = True
        if query['Clicks']:#最后一次click
            ranks[query['Clicks'][len(query['Clicks'])-1]['URLID']] = 3
            flag = True
        url_list = [(ranks[item[0]],item[0],item[1]) for item in query['URL_DOMAIN']]
        test_query = create_query(query_id,serp_id,url_list)
        if flag:#当前query有click
            test_session['query_list'].append(test_query)
    if len(test_session['query_list']) > 1:#this session at least has 2 query
        json.dump(test_session,session_path)
        session_path.write('\n')

@run_time
def generate_baseline(test_set,base_path):
    with open(test_set,'r',newline='') as test:
        with open(base_path,'w',newline='') as basefile:
            for line in test:
                session = json.loads(line)
                s_id,u_id = session['session_id'],session['user_id']
                baseline = create_baseline(s_id,u_id)
                serp_max = 0
                for query in session['query_list']:
                    if query['serp'] > serp_max:
                        serp_max = query['serp']
                        baseline['query_id'] = query['query_id']
                        baseline['url_list'] = query['url_list']
                        baseline['serp'] = serp_max
                json.dump(baseline,basefile)
                basefile.write('\n')

@run_time
def generate_test_set(train,test_session_path,baseline_path):
    file = open(train,'r')
    sessions = pre.parse(file)
    with open(test_session_path,'w',newline='') as test_session:
        with open(baseline_path,'w',newline='') as baseline:
            for session in sessions:
                day = session['Day']
                if day >26:
                    test_sessions(session,test_session,baseline)

@run_time
def generate(train,train_session_path,test_session_path,baseline_path):
    file = open(train,'r')
    sessions = pre.parse(file)
    with open(train_session_path,'w',newline='') as train_session:
        csv_writer = csv.writer(train_session)
        csv_writer.writerow(rowName())
        trainN,testN= 0,0
        with open(test_session_path,'w',newline='') as test_session:
            with open(baseline_path,'w',newline='') as baseline:
                for session in sessions:
                    day = session['Day']
                    if day < 27:
                        train_sessions(session,csv_writer)
                        trainN+=1
                    elif day >26:
                        test_sessions(session,test_session,baseline)
                        testN+=1
    print('train_number:',trainN,'test_num',testN)

@run_time
def generate_test_url_set(path):
    url_set = set()
    with open(path,'r',newline='') as test_session:
        for line in test_session:
            session = json.loads(line)
            for query in session['query_list']:
                url_list = query['url_list']
                for url in url_list:
                    url_set.add(url[1])
    print(len(url_set))

@run_time
def generate_test_set_url_domain(train_path,save_path):
    file = open(train_path,'r')
    sessions = pre.parse(file)
    with open(save_path,'w',newline='') as test_session:
        for session in sessions:
            day = session['Day']
            if day >26:
                test_sessions_url_domain(session,test_session)


