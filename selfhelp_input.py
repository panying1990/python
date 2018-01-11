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
import datetime
import pymysql.cursors
# 时间设置
def import_date():
    dt = datetime.datetime.now ()
    return dt.strftime("%Y-%m-%d")

# 获得符合条件的自助取数对象（file_name =[碧生源官方旗舰店,碧生源药品旗舰店,[1-9]]）
def get_shopfile(SourceDir,file_name):
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
    return filelist,file_name

# 对目标文件进处理并合并(流量、交易、服务、其他)
def Merge_file(filelist,file_name,dt):
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
    dataset["店铺名称"] = file_name
    dataset["添加时间"] = dt
    return dataset

# 将目标数据集插入到Mysql目标数据集合
def exportmysql(dataset):
    # 连接上数据库
    connection = pymysql.connect (host='192.168.111.251',
                                  user='root',
                                  password='P#y20bsy17',
                                  db='master_data',
                                  charset='utf8',
                                  cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor () as cursor:
                sql_rd = 'INSERT INTO `selfhelp_shop_data`(`统计日期`,`t_客单价`,`t_PC端客单价`,`t_无线端客单价`,' \
                         '`t_老买家数`,`t_新买家数`,`t_下单买家数`,`t_下单商品件数`,`t_支付买家数`,`t_支付商品件数`,' \
                         '`t_下单且支付金额`,`o_店铺收藏次数`,`o_店铺收藏人数`,`o_商品收藏次数`,`o_商品收藏人数`,`s_售中申请退款金额`,' \
                         '`s_售中申请退款买家数`,`f_访客数`,`f_老访客数`,`f_PC端访客数`,`f_无线端访客数`,`店铺名称`,`添加时间`) ' \
                         'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                for i in range (len (dataset)):
                    valuetuple = tuple (list (dataset.values[i, ]))
                    cursor.execute (sql_rd, valuetuple)
        connection.commit()

        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT max(`统计日期`) as 最晚时间 FROM `selfhelp_shop_data`"
            cursor.execute (sql)
            result = cursor.fetchone ()
            print (result)
    finally:
        connection.close ()
    return print("成功插入")

def getshopname():
    return ["碧生源官方旗舰店","碧生源药品旗舰店"]


# 主函数
if __name__ == '__main__':
    SourceDir = "V:\\更新数据\\碧生源官方旗舰店\\自助取数\\取数-12.10"
    file_name = getshopname()
    dt = import_date()
    for name in file_name:
        filelist,filename = get_shopfile(SourceDir,name)
        dataset = Merge_file (filelist,name,dt)
        print(dataset.axes)
        exportmysql (dataset)
