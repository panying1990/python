# !/usr/bin/python
# -*- coding；utf-8 -*-
"""
 该部分代码用于将碧生源官方旗舰店—生意参谋-店铺流量来源模块数据：
 格式转化：将从WEB的EXCEL2003格式数据添加标注信息，合并处理且校验后，存储到MySql-两茶过程数据中；
 存储对象：从店铺角度观察的流量来源明细
 数据模块：PC端=PC，无线端=WAP 两个模块；
 最近编辑时间：2018-02-08
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
    now_date = datetime.datetime.now()
    return now_date.strftime("%Y-%m-%d")


# 获得符合条件的店铺流量来源文件对象（flow_name =[PC流量来源,无线流量来源]）
def get_flow_file(source_dir, flow_name):
    temp_dir = source_dir+flow_name
    source_dir_list = os.listdir(temp_dir)
    os.chdir(temp_dir)
    try:
        temp_path = os.getcwd()
        if temp_path == temp_dir:
            print("The work file path was change!")
    except:
        print("The work file path haven't change!")
    # 获得目标文件夹中的目标文件
    file_list = []
    for list_name in source_dir_list:
        if flow_name[0:1] in list_name:
            file_list.append(list_name)
        else:
            continue
    return file_list


# 将得到的店铺流量来源文件按照要求进行转换并进行输出
def tranfer_file(file_list, flow_name):
    file_num = len (file_list)
    matrix = [None] * file_num
    matrix_name = []
    for i in range (file_num):
        flow_file_name = file_list[i]
        if "PC" in flow_name:
            flow_date = flow_file_name[17:27]
        elif "无线" in flow_name:
            flow_date = flow_file_name[17:27]
        table = xlrd.open_workbook (flow_file_name)
        sheet = table.sheet_by_name (flow_name)
        col_name = sheet.row_values (5)
        # 将每列的字段名赋值给matrix_name序列值
        if len (matrix_name) < 1:
            for col in col_name:
                matrix_name.append (col)
        rows_num = sheet.nrows - 6
        matrix[i] = [0] * rows_num
        cols_num = sheet.ncols
        for m in range (rows_num):
            matrix[i][m] = ["0"] * cols_num
        for j in range (rows_num):
            for k in range (cols_num):
                matrix[i][j][k] = sheet.cell (j + 6, k).value
        temp_dataframe = pd.DataFrame (data=matrix[i], columns=matrix_name)
        temp_dataframe["流量来源"] = flow_name
        temp_dataframe["流量日期"] = flow_date
        temp_dataframe["添加日期"] = datetime.datetime.now ().strftime ("%Y-%m-%d")
        if "PC" in flow_name:
            target_dir_file = "V:\\更新数据\\碧生源官方旗舰店\\店铺流量来源\\PC流量来源_转化\\" + flow_file_name[:-4] + ".xls"
            temp_dataframe.to_excel (target_dir_file, index=False)
        elif "无线" in flow_name:
            target_dir_file = "V:\\更新数据\\碧生源官方旗舰店\\店铺流量来源\\无线流量来源_转化\\" + flow_file_name[:-4] + ".xls"
            temp_dataframe.to_excel (target_dir_file, index=False)
    return True


# 将店铺目标数据集插入到Mysql目标数据集合
def export_mysql(dataset):
    # 连接上数据库
    connection = pymysql.connect (host='192.168.111.251',
                                  user='root',
                                  password='P#y20bsy17',
                                  db='master_data',
                                  charset='utf8',
                                  cursorclass=pymysql.cursors.DictCursor)
    if list(dataset.shape)[1] == 20:
        try:
            with connection.cursor () as cursor:
                sql_rd = 'INSERT INTO `selfhelp_product_data`(`统计日期`, `t_PC端支付金额`, `t_PC端支付买家数`, `t_无线端支付金额`,'\
                        '`t_无线端支付买家数`,`t_下单转化率`, `t_支付转化率`, `t_支付金额`, `商品名称`, `o_加购人数`, `o_加购商品件数`,'\
                        '`o_PC端加购商品件数`, `o_商品收藏人数`, `o_无线端加购商品件数`, `f_访客数`, `f_浏览量`, `f_PC端访客数`,'\
                        '`f_无线端访客数`, `商品ID`, `添加时间`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                          %s, %s, %s, %s, %s)'
                for i in range (len (dataset)):
                    valuetuple = tuple (list (dataset.values[i,]))
                    cursor.execute (sql_rd, valuetuple)
            connection.commit ()

            with connection.cursor () as cursor:
                # Read a single record
                sql = "SELECT max(`统计日期`) as 最晚时间 FROM `selfhelp_product_data`"
                cursor.execute (sql)
                result = cursor.fetchone ()
                print (result)
        finally:
            connection.close ()
    else:
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


# 获得转化后数据
def get_std_data(target_dir,flow_name):
    temp_dir = target_dir + flow_name+"_转化"
    target_dir_list = os.listdir (temp_dir)
    os.chdir (temp_dir)
    try:
        temp_path = os.getcwd ()
        if temp_path == temp_dir:
            print ("The work file path was change!")
    except:
        print ("The work file path haven't change!")
    # 获得目标文件夹中的目标文件
    for target_name in target_dir_list:
         table = xlrd.open_workbook(target_name)
         sheet = table.sheet_by_index(0)

         export_mysql(dateset)
    return print("数据导入完毕")


# 删除目标文件中的全部文档
def delete_all(sourcedir, filename):
    file_dir_s = sourcedir + filename+"_转化"
    try:
        os.path.isfile (file_dir_s)
    except:
        print ("该路径下没有文件夹！！！")
    os.chdir (file_dir_s)
    file_listdir = os.listdir (os.getcwd ())
    for filename in file_listdir:
        file_name = filename
        os.remove (file_name)
    return "任务完成"


# 数据初始格式校验

# 主函数
if __name__ == '__main__':
    sourcedir = "V:\\更新数据\\碧生源官方旗舰店\\店铺流量来源\\"
    date_value = import_date()
    file_name = ["PC流量来源", "无线流量来源"]
    # 将流量数据来源进行转化
    for filename in file_name:
        file_list = get_flow_file(sourcedir, filename)
        tranfer_file(file_list, filename)
    # # 删除转化后标准流量来源数据
    # for filename in file_name:
    #     delete_all (sourcedir, filename)

