# -*- coding；utf-8 -*-
"""
 该部分代码用于将碧生源官方旗舰店—生意参谋-自助取数模块数据：
 格式转化：将从WEB的EXCEL2003格式数据存储到MySql中；
 存储对象：1、店铺；2、商品两个对象
 数据模块：流量=f、交易=t、服务=s、其他=o四个模块；
 最近编辑时间：2018-01-10
"""
__author__ = 'Pan Ying'
__email__ = 'panying1298247841@163.com'
__version__ = '0.1'

# 环境配置
import os
import xlrd
import pandas as pd

# 获得符合条件的自助取数对象（file_name =[碧生源官方旗舰店,碧生源药品旗舰店,[1-9]]）
def Get_shopfile(SourceDir,file_name):
    SourceDirlist = os.listdir (SourceDir)
    os.chdir (SourceDir)
    try:
        temp_path = os.getcwd ()
        if temp_path == SourceDir:
            print ("The work file path was change!")
    except:
        print ("The work file path havn't change!")
    # 获得目标文件夹中的目标文件
    filelist = []
    for filename in SourceDirlist:
        if file_name in filename:
            filelist.append (filename)
        else:
            continue
    return filelist

# 对目标文件进处理并合并(流量、交易、服务、其他)
def Merge_file(filelist):
    file_num = len (filelist)
    matrix = [None] * file_num
    matrix_name = [None] * file_num
    for i in range (len (filelist)):
        filename = filelist[i]
        table = xlrd.open_workbook (filename)
        sheet = table.sheet_by_name (u'自助取数')
        colname = sheet.row_values (3)
        # 根据文件名称对字段进行描述
        matrix_name[i] = []
        matrix_name[i].append (colname[0])
        if "交易" in filename:
            for c in range (1, len (colname)):
                matrix_name[i].append ("t_" + colname[c])
        elif "流量" in filename:
            for c in range (1, len (colname)):
                matrix_name[i].append ("f_" + colname[c])
        elif "服务" in filename:
            for c in range (1, len (colname)):
                matrix_name[i].append ("s_" + colname[c])
        else:
            for c in range (1, len (colname)):
                matrix_name[i].append ("o_" + colname[c])
        nrows = sheet.nrows - 4
        matrix[i] = [0] * (nrows)
        ncols = sheet.ncols
        for m in range (nrows):
            matrix[i][m] = ["0"] * ncols
        for j in range (nrows):
            for k in range (ncols):
                matrix[i][j][k] = sheet.cell (j + 4, k).value
    data1 = pd.DataFrame (data=matrix[0], columns=matrix_name[0])
    data2 = pd.DataFrame (data=matrix[1], columns=matrix_name[1])
    data3 = pd.DataFrame (data=matrix[2], columns=matrix_name[2])
    data4 = pd.DataFrame (data=matrix[3], columns=matrix_name[3])
    data5 = pd.merge (data1, data2, on='统计日期')
    data6 = pd.merge (data3, data4, on='统计日期')
    dataset = pd.merge (data5, data6, on='统计日期')
    return dataset

