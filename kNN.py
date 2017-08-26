"" 用于创建用于分析的数据集''
from numpy import *
import operator
def createDataSet()
    group = arrary([1.0,1.1],[1.0,1.0],[0,0],[0,0.1])
    label = ["A","A","B","B"] 
    return group, labels
  
‘以下为引用该函数的举例’
import kNN
group,lables=kNN.createDataSet()
  
""对未知类别属性的数据集中的每个点依次执行以下操作""
""1、计算已知类别数据集中点于当前点之间的距离；
""2、按照距离依次递增次序排列；
""3、选取与当前点距离最小的K个点；
""4、确定前k个点所在类别的出现频率：
""5、返回前K个点出现频率最高的类别作为当前点的预测分类。

def classify0(inX, dataSet, labels, k)
    dataSetSize = dataSet.shape[0]
    diffMat = tile(intX, (dataSetSize,1))-dataSet
    sqDiffMat = diffMat**2
    sqDistances = sqDiffMat.sum(axis=1)
    distances = sqDistances**0.5 -- "两点之间距离计算" --
    sortedDistIndicies = distances.argsort()
    classCount = {}
    for i in range(K):
        voteIlabel = labels[sortedDistIndicies[i]]
    

  
