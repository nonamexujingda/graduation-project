__author__ = 'jingda'


import numpy as np
import matplotlib.pyplot as plt

def show(r_list):
    # x = ('random','baseline','local','global','total')
    x= ('total','global','local','baseline','random')
    y_pos = np.arange(len(x))
    plt.barh(y_pos, r_list, align='center', alpha=0.4)
    plt.yticks(y_pos, x)
    plt.xlabel('improve')
    plt.xlim((0.7000, 0.7400))
    plt.show()

# ran = 0.006
# base = 0.81
# lr = 1.33
# gr = 1.69
# tr = 1.99
# x = ('LambdaMART','RankNet','POMDP3','POMDP2','POMDP1')
# y = [tr,gr,lr,base,ran]
#
# y_pos = np.arange(len(x))
#
# plt.barh(y_pos, y, align='center', alpha=0.4)
# plt.yticks(y_pos, x)
# plt.xlabel('improve')
# plt.show()