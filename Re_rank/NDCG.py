__author__ = 'jingda'

import  json,math,Global_Variable as gl,random


def computeNDCG(url_list):
    dcg = 0
    idcg = 0

    for g in range(len(url_list)):
        rel = url_list[g][0]-1
        dcg += (math.pow(2,rel)-1)/math.log(g+2,2)

    url_list_copy = url_list[0:]
    url_list_copy.sort(reverse=True)

    for k in range(len(url_list_copy)):
        rel = url_list_copy[k][0]-1
        idcg += (math.pow(2,rel)-1)/math.log(k+2,2)

    return  dcg/idcg

def NDCG_baseline(path):
    with open(path,'r') as baseline:
        total = 0
        num = 0
        for line in baseline:
            url_list = json.loads(line)['url_list']
            # random.shuffle(url_list)
            ndcg = computeNDCG(url_list)
            # if ndcg < 1:
            total+=ndcg
            num+=1

        print('Baseline:',total/num)
        return total/num

def NDCG_ran_baseline(path):
    with open(path,'r') as baseline:
        total = 0
        num = 0
        for line in baseline:
            url_list = json.loads(line)['url_list']
            random.shuffle(url_list)
            ndcg = computeNDCG(url_list)
            # if ndcg < 1:
            total+=ndcg
            num+=1

        print('Random:',total/num)
        return total/num


#----------------------------------------------------------------------------
# NDCG_baseline(gl.baseline_sample_url_domain)

# parse_result('result_dict','result_parsed')
# r = computeNDCG_test(gl.baseline_clean_sample,gl.result_dict)
# r = computeNDCG_test('data_run\\random_sample_after_clean','new_dict')
# print(r)
