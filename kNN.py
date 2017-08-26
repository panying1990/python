''' 用于创建用于分析的数据集'''
from numpy import *
import operator
def createDataSet()
    group = arrary([1.0,1.1],[1.0,1.0],[0,0],[0,0.1])
    label = ["A","A","B","B"] 
    return group, labels
  
'''以下为引用该函数的举例'''
import kNN
group,lables=kNN.createDataSet()
  
'''
对未知类别属性的数据集中的每个点依次执行以下操作
1、计算已知类别数据集中点于当前点之间的距离；
2、按照距离依次递增次序排列；
3、选取与当前点距离最小的K个点；
4、确定前k个点所在类别的出现频率：
5、返回前K个点出现频率最高的类别作为当前点的预测分类。
'''
def classify0(inX, dataSet, labels, k)
    dataSetSize = dataSet.shape[0]
    diffMat = tile(intX, (dataSetSize,1))-dataSet
    sqDiffMat = diffMat**2
    sqDistances = sqDiffMat.sum(axis=1)
    distances = sqDistances**0.5    '''两点之间距离计算'''
    sortedDistIndicies = distances.argsort()
    classCount = {}
    for i in range(K):  '''获得排名前k的距离点的类型'''
        voteIlabel = labels[sortedDistIndicies[i]
        classCount[voteIlabel] = classCount.get(voteIlabel,0)+1
    sortedClassCount = sorted(classCount.iteritems(), key=operator.itemgetter(1), reverse=True )
    return sortedClassCount[0][0]

  
'''
在约会网站上使用K-近邻算法算法
1、收集数据：提供文本文件
2、准备数据：使用python解析文本文件；
3、分析数据：使用Matpoltlib画二维扩散图；
4、训练算法：此步骤不适用于K-邻近算法：
5、测试算法：使用初始提供的部分数据作为测试样本。测试样本与非测试样本的区别在于：测试样本是已经完成分类的数据，如果预测分类与实际类别不同，则标记为一个错误
6、使用算法：产生简单的命令行程序，然后输入一些特征数据以判断对方是否为符合类别                     
'''
def file2matrix(fliename):
    fr = open(filename)
    arraryOLines = fr.readlines()   '''得到文件行数'''
    numberOfLines = len(arraryOLines)                        
    returnMat = zeros((numberOfLines,3)) '''创建返回的Numpy矩阵'''
    classLabelVector = []
    indes = 0
    for line in arrayOLines:
        line = line.strip()
        listFromLine = line.split('\t')
        returnMat[index, : ] = listFromLine[0:3]
        classLabelVector.append(int(listFromLine[-1]))
        index += 1
        return returnMat,classLabelVector           
                      
     
                            
                         
                          
