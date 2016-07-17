__author__ = 'jingda'

import  json,collections,math,NDCG
import  Global_Variable as gl

def parameter_dict(bs,s_rel,s_irel,u,q,d,dr,ur,gama,bi,p,bf):
    return {
        'bs':bs,
        's_rel':s_rel,
        's_irel':s_irel,
        'u_weight':u,
        'q_weight':q,
        'd_weight':d,
        'domain_reward':dr,
        'url_reward':ur,
        'gama':gama,
        'base_idx':bi,
        'prb':p,
        'base_factor':bf
    }


def parse_test_session(test_path):
    with open(test_path,'r',newline='') as test:
        for line in test:
            test_session = json.loads(line)
            yield  test_session

def parse_baseline(base_path):
    with open(base_path,'r',newline='') as baseline:
        for line in baseline:
            baseline = json.loads(line)
            yield  baseline

def recover_rel(old_list,new_list):
    dict = collections.defaultdict(int)
    for url in old_list:
        dict[url[1]] = url[0]
    rel_list = []
    for url in new_list:
        rel_list.append([dict[url[1]], url[1]] )
    return rel_list


def compute_reward(rel,idx,parameter,serp):
    score = (rel-1)/math.log(idx+2,2)
    score = score*math.pow(parameter,serp)
    return  score


def session_GR(r_path,parameter):
    s = None
    with open(r_path,'r',newline='') as result:
        for line in result:
            session = json.loads(line)
            session_id,total_dict = session['session_id'],session['dict']
            url_list =[]
            lists = [session_id]
            probabilistic = parameter['prb']
            for idx in range(10):
                base = 1/math.log(idx+2,2)
                if idx>=parameter['base_idx']:
                    base = base/(idx)
                q,d,sat,click = total_dict[idx]['q'],total_dict[idx]['d'],total_dict[idx]['sat'],total_dict[idx]['click']
                if q != 0 and sat != 0:
                    score = (sat/q)*parameter['q_weight']+base*(1-parameter['q_weight'])
                    if (sat/q) > probabilistic:
                        probabilistic = sat/q
                elif d !=0 and click !=0 :
                    score = (click/d)*parameter['d_weight']+base*(1-parameter['d_weight'])
                else:
                    score = base*parameter['base_factor']
                url_list.append( [round(score,6),str(total_dict[idx]['url'])] )
            lists.append(url_list)
            lists.append(0.59)
            s = lists
            yield  s

def session_LR(test_path,base_path,param):
    ts = parse_test_session(test_path)
    bl = parse_baseline(base_path)
    # test_session = ts.__next__()
    for test_session in ts:
        baseline = bl.__next__()
        lists = [baseline['session_id']]
        if test_session['session_id'] == baseline['session_id']:
            url_dict = collections.defaultdict(int)#储存之前查询的所有url的score
            domain_dict = collections.defaultdict(int)#储存之前查询的所有url的score
            base_serp, base_url_list = baseline['serp'], baseline['url_list']
            for query in test_session['query_list']:
                q_url_list = query['url_list']
                serp_now = query['serp']
                if serp_now != base_serp:
                    for i in range(len(q_url_list)):
                        if q_url_list[i][0] > 1:
                            rel,url,domain = q_url_list[i][0],q_url_list[i][1],q_url_list[i][2]
                            if rel > 1:
                                url_score = compute_reward(rel,i,param['gama'],base_serp-serp_now)
                                if url_score > url_dict[url]:
                                    url_dict[url] = url_score
                                # domain_score = compute_reward(rel,i,param['gama'],base_serp-serp_now)
                                # if domain_score > domain_dict[domain]:
                                #     domain_dict[domain] = domain_score
            url_list = []
            for idx in range(len(base_url_list)):
                # d_reward = domain_dict[base_url_list[idx][2]]
                u_reward = url_dict[base_url_list[idx][1]]
                # if d_reward > 0 and u_reward == 0:
                #     reward = d_reward*param['domain_reward']
                if u_reward > 0:
                    reward = u_reward*param['url_reward']
                else:
                    reward = 0
                # reward = d_reward - u_reward*0.3
                # if reward < 0:
                #     reward = 0
                url_list.append( [round(reward,6),base_url_list[idx][1]] )
            lists.append(url_list)
        yield lists

def compute_total_reward(test_path,result_path,base_path,parameter):
    gr = session_GR(result_path,parameter)
    lr = session_LR(test_path,base_path,parameter)
    ndcg = 0
    num = 0
    with open(base_path,'r',newline='') as base:
        for line in base:
            baseline = json.loads(line)
            base_url_list = baseline['url_list']
            global_reward = gr.__next__()
            local_reward = lr.__next__()
            new_url_list = []
            if int(global_reward[0]) == int(local_reward[0]):
                gr_list = global_reward[1]
                lr_list = local_reward[1]

                for lr_item in lr_list: #test
                    if lr_item[0] > 0:
                        flag = True
                        break

                for i in range(len(gr_list)):
                    if int(gr_list[i][1]) == int(lr_list[i][1]):
                        # score = (gr_list[i][0])*0.7+lr_list[i][0]*0.3*0.51
                        score = (gr_list[i][0])*parameter['bs']\
                                +lr_list[i][0]*(1-parameter['bs'])\
                                 *((global_reward[2]*parameter['s_rel'])+((1-global_reward[2])*parameter['s_irel']))
                        new_url_list.append([round(score,6),int(lr_list[i][1])])
                new_url_list.sort(reverse = True)
                # print(new_url_list)
            final_url_list = recover_rel(base_url_list,new_url_list)

            num+=1
            ndcg+=NDCG.computeNDCG(final_url_list)

    return (ndcg/num)

def tune_parameter1():
    bss = [i/10 for i in range(1,11)]
    srs = [i/10 for i in range(1,11)]
    sirs = [i/10 for i in range(1,11)]
    maxN = 0
    a,b,c =0,0,0
    for bs in bss:
        for sr in srs:
            for sir in sirs:
                dict = parameter_dict(bs,sr,sir,0.7,0.3,0.4,0.6,0.1,0.9,5,0.15,0.72)
                n = compute_total_reward(gl.test_sample_url_domain,'data\\result_dict2',gl.baseline_sample_url_domain,dict)
            # print(dict)
                if n > maxN:
                    maxN = n
                    a,b,c = dict['bs'],dict['s_rel'],dict['s_irel']
    print(maxN)
    print(a,b,c)

def tune_parameter2():
    us = [i/10 for i in range(1,11)]
    qs = [i/10 for i in range(1,11)]
    ds = [i/10 for i in range(1,11)]
    maxN = 0
    a,b,c =0,0,0
    for u in us:
        for q in qs:
            for d in ds:
                p_d = {
                    'bs':0.7,'s_rel':0.8,'s_irel':0.3,
                    'u_weight':u,'q_weight':q,'d_weight':d,
                    'domain_reward':0.6,'url_reward':0.1,'gama':0.9,
                    'base_idx':5,'prb':0.15,'base_factor':0.72
                }
                n = compute_total_reward(gl.test_sample_url_domain,'data\\result_dict2',gl.baseline_sample_url_domain,p_d)
            # print(dict)
                if n > maxN:
                    maxN = n
                    a,b,c = p_d['u_weight'],p_d['q_weight'],p_d['d_weight']
    print(maxN)
    print(a,b,c)

def tune_parameter3():
    drs = [i/10 for i in range(1,11)]
    urs = [i/10 for i in range(1,11)]
    gamas = [i/10 for i in range(1,11)]
    maxN = 0
    a,b,c =0,0,0
    for dr in drs:
        for ur in urs:
            for g in gamas:
                p_d = {
                    'bs':0.7,'s_rel':0.8,'s_irel':0.3,
                    'u_weight':0.6,'q_weight':0.5,'d_weight':0.4,
                    'domain_reward':dr,'url_reward':ur,'gama':g,
                    'base_idx':5,'prb':0.15,'base_factor':0.72
                }
                n = compute_total_reward(gl.test_sample_url_domain,'data\\result_dict2',gl.baseline_sample_url_domain,p_d)
            # print(dict)
                if n > maxN:
                    maxN = n
                    a,b,c = p_d['domain_reward'],p_d['url_reward'],p_d['gama']
    print(maxN)
    print(a,b,c)

def tune_parameter4():
    bis = [5]
    # ps = [i/100 for i in range(1,101)]
    ps = [i/100 for i in range(1,101)]
    bfs = [0.71]
    maxN = 0
    a,b,c =0,0,0
    for bi in bis:
        for p in ps:
            for bf in bfs:
                p_d = {
                    'bs':0.7,'s_rel':0.8,'s_irel':0.3,
                    'u_weight':0.6,'q_weight':0.5,'d_weight':0.4,
                    'domain_reward':0.6,'url_reward':0.1,'gama':0.9,
                    'base_idx':bi,'prb':p,'base_factor':bf
                }
                n = compute_total_reward(gl.test_sample_url_domain,'data\\result_dict2',gl.baseline_sample_url_domain,p_d)
            # print(dict)
                if n > maxN:
                    maxN = n
                    a,b,c = p_d['base_idx'],p_d['prb'],p_d['base_factor']
    print(maxN)
    print(a,b,c)

# tune_parameter4()
dict = parameter_dict(0.7,0.8,0.3,0.6,0.5,0.4,0.6,0.1,0.9,5,0.11,0.71)
ndcg = compute_total_reward\
    (gl.test_sample_url_domain,'data\\result_dict2',gl.baseline_sample_url_domain,dict)
print(ndcg)