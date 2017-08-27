'''决策树ID3算法环境'''
from numpy import *
import operator
from math import log   '''引用对数函数'''

''' 用于创建用于分析的数据集'''
def createDataSet()
    group = arrary([1.0,1.1],[1.0,1.0],[0,0],[0,0.1])
    label = ["A","A","B","B"] 
    return group, labels

'''计算给定数据集的香农熵：即数据集数据不一致性的程度'''
def calcShannonEnt(dataSet):
    numEntries = len(dataSet)
    labelCounts = {}
    '''为所有可能分类创建字典'''
    for featVec in dataSet:
        currentLabel = featCec[-1]
        if currentLabel not in labelCounts.keys():
           labelCounts[curreLabel] = 0
        labelCounts[currentLabel] += 1
    '''参照信息熵公式，以2为底求对数'''
    shannonEnt = 0.0
    for key in labelCounts:
        prob = float(labelCounts[key])/numEntries
        shannonEnt -=prob * log(prob,2)
    return ShannonEnt 

'''按照给定特征划分数据集''' 
def splitDataSet(dataSet, axis, value):
    retDataSet = []  '''创建新的list对象'''
    for featVec in dataSet:
        '''抽取符合标准的值'''
        if featVec[axis]  == value:
            reduceFeatVec = featVec[:axis]
            reduceFeatVec.extned(featVec[axis+1:])
            retDataSet.append(reduceFeatVec)
    return retDataSet

'''选择最好的数据集划分方式'''
def chooseBestFeatureTo
            
