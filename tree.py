'''计算给定数据集的香农熵：即数据集数据不一致性的程度'''
from math import log   '''引用对数函数'''
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
 
def splitDataSet(dataSet, axis, value):
    retDataSet = []
    for featVec in dataSet:
        if featVec[axis]  == value:
            
