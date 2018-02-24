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
import string


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


# 将得到的店铺流量来源文件按照要求进行转换,清洗后,并将数据导入数据库
def inport_data(file_list, flow_name):
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
        std_data = get_std_data(temp_dataframe)
        export_mysql(std_data)
        if "PC" in flow_name:
            target_dir_file = "V:\\更新数据\\碧生源官方旗舰店\\店铺流量来源\\PC流量来源_转化\\" + flow_file_name[:-4] + ".xls"
            temp_dataframe.to_excel (target_dir_file, index=False)
        elif "无线" in flow_name:
            target_dir_file = "V:\\更新数据\\碧生源官方旗舰店\\店铺流量来源\\无线流量来源_转化\\" + flow_file_name[:-4] + ".xls"
            temp_dataframe.to_excel (target_dir_file, index=False)
    return True


# 将店铺目标数据集插入到Mysql目标数据集合
def export_mysql(data_set):
    # 连接上数据库
    connection = pymysql.connect (host='192.168.111.251',
                                  user='root',
                                  password='P#y20bsy17',
                                  db='master_data',
                                  charset='utf8',
                                  cursorclass=pymysql.cursors.DictCursor)
    if list(data_set.shape)[1] == 35:
        try:
            print("PC流量导入")
            with connection.cursor () as cursor:
                sql_rd = 'INSERT INTO `traffic_shop_data`(来源, 来源明细, `访客数`, `访客数变化`,' \
                         '`新访客`,`新访客变化`, `浏览量`, `浏览量变化`, `人均浏览量`, `人均浏览量变化`, `收藏人数`,' \
                         '`收藏人数变化`, `加购人数`, `加购人数变化`, `跳失率`, `跳失率变化`, `下单金额`,' \
                         '`下单金额变化`, `下单买家数`, `下单买家数变化`, `下单转化率`, `下单转化率变化`,' \
                         '`支付金额`, `支付金额变化`, `支付买家数`, `支付买家数变化`, `支付转化率`, `支付转化率变化`,' \
                         '`客单价`, `客单价变化`,`UV价值`,`uv价值变化`,`流量来源`,`流量日期`,`添加日期`) ' \
                         'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,' \
                         '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                for i in range(len(data_set)):
                    value_tuple = tuple(list(data_set.values[i,]))
                    cursor.execute(sql_rd, value_tuple)
            connection.commit()

            with connection.cursor () as cursor:
                # 统计倒入数据情况
                sql = "SELECT max(`流量日期`) as 最晚时间 FROM `traffic_shop_data`"
                cursor.execute(sql)
                result = cursor.fetchone()
                print(result)
        finally:
            connection.close()
    else:
        try:
            print ("无线流量导入")
            with connection.cursor () as cursor:
                    sql_rd = 'INSERT INTO `traffic_wap_data`(`来源`, `来源明细`,`访客数`,`访客数变化`,`下单金额`,' \
                             '`下单金额变化`,`下单买家数`,`下单买家数变化`, `下单转化率`,`下单转化率变化`,`支付金额`,' \
                             '`支付金额变化`,`支付买家数`,`支付买家数变化`,`支付转化率`,`支付转化率变化`,`客单价`,`客单价变化`,' \
                             '`UV价值`,`uv价值变化`,`流量来源`,`流量日期`,`添加日期`)VALUES(%s, %s, %s, %s, %s, %s, ' \
                             '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    for i in range (len (data_set)):
                        valuetuple = tuple (list (data_set.values[i, ]))
                        cursor.execute (sql_rd, valuetuple)
            connection.commit()

            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT max(`流量日期`) as 最晚时间 FROM `traffic_wap_data`"
                cursor.execute (sql)
                result = cursor.fetchone ()
                print (result)
        finally:
            connection.close ()
    return print("成功插入")


# 获得格式转化后数据，并将原始数据中的千分位数据进行替换
def get_std_data(date_set):
    data_temp = pd.DataFrame(date_set)
    for col_name in data_temp:
        data_temp[col_name] = data_temp[col_name].str.replace (',', '')
    return data_temp


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
        inport_data(file_list, filename)
    # # 删除转化后标准流量来源数据
    # for filename in file_name:
    #     delete_all (sourcedir, filename)

