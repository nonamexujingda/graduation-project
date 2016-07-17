__author__ = 'jingda'

import  csv,json,pandas,time,Global_Variable as gl,NDCG,matplotlib.pyplot as plt
from  data_utils import  preprocess as pre


def line_number(file):
    with open(file,'r',newline='') as f:
        line_num = sum(1 for line in f )
        print('line number',line_num)

def sample_data(path,type,num):
    '''
    :param path:数据地址
    :param type: 0是csv类型，1是json类型
    :param num: 行数
    '''
    with open(path,'r') as file:
        i = 0
        if type ==0:
            for line in file:
                print(line)
                i+=1
                if i > num:
                    break
        elif type ==1:
            for line in file:
                print(json.loads(line))
                i+=1
                if i > num:
                    break


def ndcg_distribute(path):
    ndcg_list = [[i/10,0] for i in range(1,11)]
    with open(path,'r') as baseline:
        for line in baseline:
            url_list = json.loads(line)['url_list']
            ndcg = NDCG.computeNDCG(url_list)
            for item in ndcg_list:
                if ndcg < item[0]:
                    item[1]+=1
                    break
    print(ndcg_list)
    x = [i[0]for i in ndcg_list]
    y = [i[1]for i in ndcg_list]
    plt.bar(x, y,-0.1,color='y', edgecolor='g', linewidth=1, align='edge')
    plt.show()

def train_SAT_click(train_path):
    num=0
    total=0
    with open(train_path,'r',newline='') as train:
            reader = csv.reader(train)
            for line in reader:
                total+=1
                try:
                    url_list = line[3:] #SessionID TypeOfRecord Day USERID
                    for url in url_list:
                        if int(str(url)[0]) == 3 :
                            num+=1
                            break
                except:
                    pass
    print('num:',num,'total:',total)
    print(num/total)

def user_number(path):
    user_lists= []
    with open(path,'r',newline='') as file:
        for line in file:
            session = json.loads(line)
            user_lists.append(session['user_id'])
    print(len(user_lists))
    print(len(set(user_lists)))

def total_session_num(path):
    file = open(path,'r')
    sessions = pre.parse(file)
    s_set = set()
    for s in sessions:
        sid= s['session_id']
        s_set.add(sid)
    print('total_session_num:',len(s_set))

def train_session_num(train_path):
    s_set = set()
    with open(train_path,'r',newline='') as train:
            reader = csv.reader(train)
            for line in reader:
                try:
                    sid = line[0] #SessionID TypeOfRecord Day USERID
                    s_set.add(sid)
                except:
                    pass
    print('train_num:',len(s_set))

def pandas_readTime(path):
    idx = 0
    start = time.time()
    reader = pandas.read_csv(path,engine = 'c', iterator =True,chunksize = 3000000)
    for chunk in reader:
        idx+=1
        end2 = time.time()
        print(idx,'_time_passed:',end2-start)

# pandas_readTime(gl.train_set)

# ndcg_distribute(gl.baseline)
train_SAT_click(gl.train_set)
# line_number(gl.baseline_sample_url_domain)
# sample_data(gl.baseline_clean_sample,0,10)
# user_number(gl.test_clean_sample)