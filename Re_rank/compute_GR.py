__author__ = 'jingda'

import time,math,json,functools
import  NDCG,Global_Variable as gl

def run_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        t0 = time.time()
        result =  func(*args, **kw)
        t1 = time.time()-t0
        print('function %s() ,running time:%s'%(func.__name__,t1))
        return result
    return wrapper

def parse_baseline(path):
    lists = []
    with open(path,'r',newline='') as baseline:
        for line in baseline:
            session = json.loads(line)
            url_list = session['url_list']
            session_id = session['session_id']
            lists.append({
                'url_list':url_list,
                'session_id':session_id
            })
        return lists

def session_GR(r_path):
    s = None
    with open(r_path,'r',newline='') as result:
        for line in result:
            if s is not None:
                yield s
            session = json.loads(line)
            session_id,total_dict = session['session_id'],session['dict']
            url_list =[]
            lists = [session_id]
            for idx in range(10):
                base = 1/math.log(idx+2,2)
                if idx>=5:
                    base = base/idx
                q,d,sat,click = total_dict[idx]['q'],total_dict[idx]['d'],total_dict[idx]['sat'],total_dict[idx]['click']
                uc,ud = total_dict[idx]['uc'],total_dict[idx]['ud']
                #--------------------------------------
                if uc != 0 and ud != 0:
                    score = (uc/ud)*0.6+base*0.4
                elif q != 0 and sat != 0:
                    score = (sat/q)*0.5+base*0.5
                elif d !=0 and click !=0 :
                    score = (click/d)*0.4+base*0.6
                else:
                    score = base*0.71
                #---------------------------------------
                # s_u,s_p,s_d = 0,0,0
                # score = 0
                # if uc != 0 and ud != 0:
                #     s_u = (uc/ud)
                # elif q != 0 and sat != 0:
                #     s_p = (sat/q)
                # elif d !=0 and click !=0 :
                #     s_d = (click/d)
                # if s_u != 0 or s_p!=0 or s_d!=0:
                #     score = s_u*pd['u_w']+s_p*pd['p_w']+s_d*pd['d_w']
                # score = score*0.8+base*0.2

                #---------------------------------------
                #     if idx>=6:
                #         score = 0.4/math.log(idx+2,2)
                #     else:
                #         score = 0.8/math.log(idx+2,2)
                url_list.append( [score,str(total_dict[idx]['url'])] )
            url_list.sort(reverse = True)
            lists.append(url_list)
            s = lists

def computeNDCG_GR(base_path,result_path):
    total = 0
    num = 0
    idx = 0
    low = 0
    base_list = parse_baseline(base_path)
    session_list = session_GR(result_path)
    for session in session_list:
        if base_list[idx]['session_id'] == session[0]:
            dict = {}
            lists = []
            for url1 in base_list[idx]['url_list']:
                dict[int(url1[1])] = url1[0]
            for k in range(len(session[1])):
                score = dict[int(session[1][k][1])]
                # if score == 0:
                #     score =0.4/math.log(k+2,2)
                lists.append( (score,session[1][k][1]) )
            # a,b = computeNDCG(lists),computeNDCG(base_list[idx]['url_list'])
            if NDCG.computeNDCG(base_list[idx]['url_list']) > NDCG.computeNDCG(lists):
                # if (computeNDCG(base_list[idx]['url_list']) - computeNDCG(lists))>0.4:
                    # print('new: ',computeNDCG(lists),session[1])
                    # print('old: ',computeNDCG(base_list[idx]['url_list']),base_list[idx]['url_list'])
                low+=1
            total += NDCG.computeNDCG(lists)
            num+=1
        else:
            print('not find corresponding session')
            break
        idx+=1
    print ('Global Reward:',total/num)
    return total/num

def tune_parameter2():
    us = [i for i in range(1,11)]
    qs = [i/10 for i in range(1,11)]
    ds = [i/10 for i in range(1,11)]
    maxN = 0
    a,b,c =0,0,0
    for u in us:
        for q in qs:
        # for q in [qi for qi in range(1,11-u)]:
            for d in ds:
            # for d in [di for di in range(1,11-u-q)]:
                p_d = {
                    'u_w':u,'p_w':q,'d_w':d,
                }
                n = computeNDCG_GR(gl.baseline_sample_url_domain,'data\\result_dict2',p_d)
            # print(dict)
                if n > maxN:
                    maxN = n
                    a,b,c = p_d['u_w'],p_d['p_w'],p_d['d_w']
    print(maxN)
    print(a,b,c)


# tune_parameter2()