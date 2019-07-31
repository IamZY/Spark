# print("hello world")
import pandas as pd
import math


def getUserItem():
    userItem = {}
    # file = open(r"E:\train_10.dat")
    file = open(r"E:\rec\ratings.dat")
    lines = file.readlines()
    for line in lines:
        arr = line.split("::")
        if int(arr[0]) not in userItem.keys():
            userItem[int(arr[0])] = []
        userItem[int(arr[0])].append(int(arr[1]))
    return userItem


"""
计算jaccard相似度
"""
def sim_jaccard(a, b):
    seta = set(a)
    setb = set(b)
    top = seta.intersection(setb).__len__()
    bot = seta.union(setb).__len__()
    return float(top) / bot


"""
余弦相似度
"""
def sim_cos(a, b):
    seta = set(a)
    setb = set(b)
    top = seta.intersection(setb).__len__()
    bot = math.sqrt(seta.__len__() * setb.__len__())
    return float(top) / bot


"""
计算用户相似度矩阵
"""
def userSimMatrix(userItems):
    # matrix矩阵字典
    matrix = {}


if __name__ == "__main__":
    # getRandomData()
    userItems = getUserItem()
    print(userItems)
    item1 = userItems[1]
    item2 = userItems[2]
    jac = sim_jaccard(item1, item2)
    print(jac)
    cos = sim_cos(item1, item2)
    print(cos)
    # pd.read_
