__author__ = 'jingda'

import  json,collections,math,NDCG
import  Global_Variable as gl

def parse_test_session(test_path):
    with open(test_path,'r',newline='') as test:
        for line in test:
            session = json.loads(line)
            yield  session

def make_dict(url_list):
    dict = collections.defaultdict(int)
    for url in url_list:
        dict[url[1]] = url[0]
    return  dict

def test_local_method_proportion(test_path,base_path):
    s = parse_test_session(test_path)
    a,b = 0,0
    b_ndcg = 0
    a_ndcg = 0
    total = 0
    with open(base_path,'r',newline='') as base:
        for line in base:
            total+=1
            baseline = json.loads(line)
            base_url_list = baseline['url_list']
            base_serp = baseline['serp']
            session = s.__next__()
            test_dict = collections.defaultdict(int)
            if session['session_id'] == baseline['session_id']:
                for query in session['query_list']:
                    q_url_list = query['url_list']
                    serp_now = query['serp']
                    if serp_now != base_serp:
                        for i in range(len(q_url_list)):
                            if q_url_list[i][0] > 1:
                                test_dict[q_url_list[i][1]]=q_url_list[i][0]
                for url in base_url_list:
                    if test_dict[url[1]] > 1 and url[0] == 1:
                        a+=1
                        a_ndcg+=NDCG.computeNDCG(base_url_list)
                        print(session)
                        # break
                    elif url[0]>1 and test_dict[url[1]] > 1:
                        b+=1
                        b_ndcg+=NDCG.computeNDCG(base_url_list)
                        # break
            else:
                print('error,not found corresponding base or session')
                break
    print('a',a,a_ndcg/a,'b',b,b_ndcg/b,'total',total)

def sample(test_path,base_path):
    s = parse_test_session(test_path)
    a,b=0,0
    with open(base_path,'r',newline='') as base:
        for line in base:
            baseline = json.loads(line)
            base_url_list = baseline['url_list']
            base_serp = baseline['serp']
            base_dict = make_dict(base_url_list)
            session = s.__next__()
            temp_url_list = []
            if session['session_id'] == baseline['session_id']:
                dict = collections.defaultdict(int)
                for query in session['query_list']:
                    q_url_list = query['url_list']
                    serp_now = query['serp']
                    if serp_now != base_serp:
                        for i in range(len(q_url_list)):
                            if q_url_list[i][0] > 1:
                                score = ((q_url_list[i][0])-1)/math.log(i+2,2)
                                score = (score*math.pow(0.8,(base_serp-serp_now)))/2
                                if score > dict[q_url_list[i][1]]:
                                    dict[q_url_list[i][1]] = score
                for idx in range(len(base_url_list)):
                    base = 1/math.log(idx+2,2)
                    temp_url_list.append( [round(base+dict[base_url_list[idx][1]],2),base_url_list[idx][1]] )
                temp_url_list.sort(reverse=True)
                for url in temp_url_list:
                    url[0] = base_dict[url[1]]
                new_ndcg = NDCG.computeNDCG(temp_url_list)
                old_ndcg = NDCG.computeNDCG(base_url_list)
                if new_ndcg > old_ndcg:
                    # print(session)
                    # print('new:',new_ndcg,temp_url_list)
                    # print('old:',old_ndcg,base_url_list)
                    a+=1
                elif new_ndcg < old_ndcg:
                    print(session)
                    print(temp_url_list)
                    b+=1
            else:
                print('error,not found corresponding base or session')
                break
    print('a',a,'b',b)

def compute_reward(rel,idx,parameter,serp):
    score = (rel-1)/math.log(idx+2,2)
    score = score*math.pow(parameter,serp)
    return  score

def computeNDCG_LR(test_path,base_path):
    s = parse_test_session(test_path)
    ndcg = 0
    num = 0
    with open(base_path,'r',newline='') as base:
        for line in base:
            baseline = json.loads(line)
            base_url_list = baseline['url_list']
            base_serp = baseline['serp']
            base_dict = make_dict(base_url_list)
            session = s.__next__()
            temp_url_list = []
            if session['session_id'] == baseline['session_id']:
                url_dict = collections.defaultdict(int)
                domain_dict = collections.defaultdict(int)
                for query in session['query_list']:
                    q_url_list = query['url_list']
                    serp_now = query['serp']
                    if serp_now != base_serp:
                        for i in range(len(q_url_list)):
                            rel,url,domain = q_url_list[i][0],q_url_list[i][1],q_url_list[i][2]
                            if rel > 1:
                                url_score = compute_reward(rel,i,0.6,base_serp-serp_now)
                                if url_score > url_dict[url]:
                                    url_dict[url] = url_score
                                domain_score = compute_reward(rel,i,0.6,base_serp-serp_now)
                                if domain_score > domain_dict[domain]:
                                    domain_dict[domain] = domain_score
                for idx in range(len(base_url_list)):
                    base = 1/math.log(idx+2,2)
                    if idx>=5:
                        base = base/(idx)
                    # base = base/(idx+1)
                    d_reward = domain_dict[base_url_list[idx][2]]
                    u_reward = url_dict[base_url_list[idx][1]]
                    if d_reward > 0 and u_reward == 0:
                        reward = d_reward*0.6
                    elif u_reward > 0:
                        reward = u_reward*0.1
                    else:
                        reward = 0
                    temp_url_list.append( [base+reward/2,base_url_list[idx][1]] )

                temp_url_list.sort(reverse=True)

                for url in temp_url_list:
                    url[0] = base_dict[url[1]]
                # if int(session['session_id']) == 27558:
                #     print(session)
                #     print('new:',temp_url_list)
                #     print('old:',base_url_list)
                ndcg+=NDCG.computeNDCG(temp_url_list)
                num+=1
            else:
                print('error,not found corresponding base or session')
                break
    print('Local Reward:',ndcg/num)
    return  ndcg/num



# computeNDCG_LR(gl.test_sample_url_domain,gl.baseline_sample_url_domain)
# test_local_method_proportion(gl.test_clean_sample,gl.baseline_clean_sample)