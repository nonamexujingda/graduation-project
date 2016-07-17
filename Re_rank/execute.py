__author__ = 'jingda'

import Global_Variable as gl
#     ,make_result_dict2 as md2\
#     ,total_reward as tr
import NDCG,compute_GR as cgr,compute_LR as clr,total_reward as ctr
from  data_utils import clean_data as cl \
    # ,generate_data as gd\
from  data_utils import sample_data as sd
import  result_pic as rp


# gd.generate(gl.original_train_path,gl.train_set,gl.test_set,gl.baseline)
# cl.delete_trainSession(gl.train_set,gl.train_set_clean)
# cl.delete_testSession(gl.train_set_clean,gl.test_set,gl.test_set_clean)
# gd.generate_baseline(gl.test_set_clean,gl.baseline_clean)

# gd.generate_test_set_url_domain(gl.original_train_path,gl.test_set_url_domain)
# sd.extract_test_url_domain(gl.test_set_url_domain,gl.baseline_clean_sample,gl.test_sample_url_domain)
# gd.generate_baseline(gl.test_sample_url_domain,gl.baseline_sample_url_domain)

# gd.generate_test_url_set(gl.test_set_clean)
# gd.generate_baseline(gl.test_set_clean,gl.baseline_clean)
# sd.extract_baseline(gl.baseline_clean,gl.baseline_clean_sample)

# session_list = md2.parse(gl.baseline_clean_sample)
# md2.generate_dict(session_list,gl.train_set_clean,'data\\result_dict2')

# sd.sample_test_set(gl.test_set_clean,gl.baseline_clean_sample,gl.test_clean_sample)
# sd.extract_baseline(gl.baseline_clean,gl.baseline_clean_sample)

# NDCG.NDCG_baseline(gl.baseline_clean_sample)
# cgr.computeNDCG_GR(gl.baseline_clean_sample,gl.result_dict)
# clr.computeNDCG_LR(gl.test_clean_sample,gl.baseline_clean_sample)
# tr.compute_total_reward(gl.test_clean_sample,gl.result_dict,gl.baseline_clean_sample)

# sd.sample_data(gl.train_set_clean)
# ---------------------------------------------------------------------------------
base = NDCG.NDCG_baseline(gl.baseline_sample_url_domain)
ran = NDCG.NDCG_ran_baseline(gl.baseline_sample_url_domain)
LR = clr.computeNDCG_LR(gl.test_sample_url_domain,gl.baseline_sample_url_domain)
p_d = {
    'u_w':1,'p_w':0.2,'d_w':0.1,
    }
GR = cgr.computeNDCG_GR(gl.baseline_sample_url_domain,'data\\result_dict2')
dict = ctr.parameter_dict(0.7,0.8,0.3,0.6,0.5,0.4,0.6,0.1,0.9,5,0.11,0.71)
TR = ctr.compute_total_reward\
    (gl.test_sample_url_domain,'data\\result_dict2',gl.baseline_sample_url_domain,dict)

p_list = [TR,GR,LR,base,ran]
rp.show(p_list)

