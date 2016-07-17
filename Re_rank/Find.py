__author__ = 'jingda'

from data_utils import preprocess as pre
import time,pandas,json,Global_Variable as gl

def find_session_frm_file_by_id(path,session_id):
    file = open(path,'r')
    s = None
    flag = session_id
    # flag = 34596077
    flag1,flag2 = 0,0
    for line in file:
        line2 = line.strip().split('\t')
        # print( line2)
        sid = int(line2[0])  #SessionID TypeOfRecord Day USERID
        flag1 = sid
        # if sid == 34596077:
        if sid == flag:
            flag2 = 1
            print('found')
            if len(line2) == 4:
                sid, tor, day, uid = line2  #SessionID TypeOfRecord Day USERID
                s = pre.new_session(sid, day, uid)
            elif len(line2) == 16:
                sid, tp, tor, serpid, quid, lot = line2[:6]
                lou = line2[6:]
                s['Query'].append(pre.new_query(sid,tp, tor, serpid, quid, lot, lou))
            elif len(line2) == 5:
                sid, tp, tor, serpid, urlid = line2
                s['Query'][-1]['Clicks'].append(pre.new_click(tp, serpid, urlid))
        if flag2 ==1 and  flag1 != flag:
            break
    return  s

def find_click_frm_file_by_s_id(path,session_id):
    file = open(path,'r')
    s = None
    flag = session_id
    # flag = 34596077
    flag1,flag2 = 0,0
    for line in file:
        line2 = line.strip().split('\t')
        # print( line2)
        sid = int(line2[0])  #SessionID TypeOfRecord Day USERID
        flag1 = sid
        # if sid == 34596077:
        if sid == flag:
            flag2 = 1
            # print('found')
            if len(line2) == 5:
                sid, tp, tor, serpid, urlid = line2
                s= pre.new_click(tp, serpid, urlid)
                print(s)
        # if flag2 ==1 and  flag1 != flag:
        #     break
    # return  s

def find_session_from_train_by_query(train,queryItem,flag):
    i = 0
    for session in train:
        for query in session['Query']:
            i+=1
            if queryItem == query['ListOfTerms']:
                print('found')
                print(session)
            if flag:
                continue

def find_query_doc_from_train_by_usr(train_path,usr_id,document):
    f = open(train_path,'r')
    train = pre.parse(f)
    for session in train:
        if usr_id == session['USERID']:
            # print('found')
            for query in session['Query']:
                for url in query['URL_DOMAIN']:
                    if url[0] == document:
                        print(query)

def find_session_from_train_by_usr(train_path,usr_id):
    f = open(train_path,'r')
    train = pre.parse(f)
    for session in train:
        if usr_id == session['USERID']:
            print('found')
            print(session)



def find_query(query,csv_path,save_file):
    start = time.time()
    reader = pandas.read_csv(csv_path,engine = 'c', iterator =True,chunksize = 3000000)
    i=0
    start2 = time.time()
    result_lists=[]
    lists = [query]
    for chunk in reader:
        i+=1
        num = chunk['query_id'].isin(lists)
        result_lists.append(chunk[num])
        end2 = time.time()
        print(i,'_time_passed:',end2-start2)
        # if i>1:
        #     break
    df = pandas.concat(result_lists,ignore_index=True,copy=False)
    df.to_csv(save_file,index=False)
    print('total_time_passed:',time.time()-start)


def find_document(document,csv_path,save_file):
    start = time.time()
    reader = pandas.read_csv(csv_path,engine = 'c', iterator =True,chunksize = 3000000)
    idx  =0
    start2 = time.time()
    result_lists=[]
    row_name = []
    for k in range(10):
        row_name.append(str(k)+'_url')
    entry = []
    for i in range(1,4):
        entry.append(int('%s%s'%(str(i),document)))

    for chunk in reader:
        idx+=1
        line_d = chunk[row_name[0]].isin(entry)
        index_d = chunk[line_d].index
        for m in range(1,10):
            line = chunk[row_name[m]].isin(entry)
        #     # index_d.append(chunk[line].index.difference(index_d))
            index_d = index_d.append(chunk[line].index)# very important!!! BIG BUG

        # num = chunk.isin(entry).any(1)
        result_lists.append(chunk.ix[index_d])
        end2 = time.time()
        print(idx,'_time_passed:',end2-start2)
        # if i>1:
        #     break
    # print(result_lists)
    df = pandas.concat(result_lists,ignore_index=True,copy=False)
    df.to_csv(save_file,index=False)
    print('total_time_passed:',time.time()-start)

def find_user(uid,csv_path,save_file):
    start = time.time()
    reader = pandas.read_csv(csv_path,engine = 'c', iterator =True,chunksize = 3000000)
    idx  =0
    start2 = time.time()
    result_lists=[]
    for chunk in reader:
        idx+=1
        line_d = chunk['user_id'].isin([int(uid)])
        index_d = chunk[line_d].index
        result_lists.append(chunk.ix[index_d])
        end2 = time.time()
        print(idx,'_time_passed:',end2-start2)
    df = pandas.concat(result_lists,ignore_index=True,copy=False)
    df.to_csv(save_file,index=False)
    print('total_time_passed:',time.time()-start)


def find_user_frm_train13(base_path,train_path):
    start = time.time()
    reader = pandas.read_csv(train_path,engine = 'c', iterator =True,chunksize = 3000000)
    idx  =0
    start2 = time.time()
    user_list = []
    with open(base_path,'r',newline='') as file:
        for line in file:
            session = json.loads(line)
            user_id = session['user_id']
            user_list.append(user_id)
    print('user_list_length:',len(user_list))

    f_list = []
    for chunk in reader:
        idx+=1
        for user in user_list:
            num = chunk['user'].isin([user])
            if not chunk[num].empty:
                f_list.append(user)
        end2 = time.time()
        print(idx,'_time_passed:',end2-start2)
        # if i>1:
        #     break
    f_set = set(f_list)
    print(len(f_set))
    print('total_time_passed:',time.time()-start)


def find_test_session(test_path,session_id):
    with open(test_path,'r',newline='') as test_file:
        for line in test_file:
            session = json.loads(line)
            s_id =session['session_id']
            if s_id == int(session_id):
                print(session)
                break

# find_test_session(gl.test_sample_url_domain,9583)
find_document('2902815',gl.train_set_clean,'temp_find')
# find_user(1677,gl.train_set_clean,'temp_find')
# find_query_doc_from_train_by_usr('..\data\\train',5714923,48687351)
# find_user_frm_train13('data_run\\random_sample_baseline_usr','data_run\\train_set_csv_13')
# s = find_session_frm_file_by_id(gl.original_train_path,27558)
# print(s)
# find_session_from_train_by_usr('..\data\\train',1677)
# find_click_frm_file_by_s_id('..\data\\train',56)
# find_query(7444384,'train_csv_13_error','find_document_savefile')