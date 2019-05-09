#-*-coding:utf-8-*-
"""
@author:panying
@time:2019/5/5 19:36
@email:panying1298247841@163.com
"""



# 布置实验环境
import pymysql
from sqlalchemy import create_engine
import os
import pandas as pd
import xlrd
import glob
from numpy import *
import datetime


# 设置服务器
def make_engine(database):
    engine_word = 'mysql+pymysql://root:P#y20bsy17@192.168.109.202:3306/'+database
    engine = create_engine(engine_word)
    return engine


# 设置数据处理时间戳
def date_tostring(date_time):
    return date_time.strftime("%Y%m%d")


# 获得目标文件
def get_file(file_dir, file_form, file_name):
    try:
        os.path.isfile(file_dir)
    except:
        print("该路径下没有文件夹！！！")
    filearray = []
    for filename in glob.glob(file_dir + "\\*." + file_form):
        if file_name in filename:
            filearray.append(filename)
        else:
            filearray.append('')
    for i in range(len(filearray)):
        if len(filearray[i]) > 4:
            target_name = filearray[i]
            return target_name

# 处理特殊表格函数
def title_process(target_name):
    xlsx_book = xlrd.open_workbook(target_name)
    table = xlsx_book.sheet_by_name('sheet1')
    # column_temp = table.col_slice()
    a = table.row_values(0)
    b = table.row_values(1)
    c =[]
    for i in range(len(a)):
        if len(a[i])>0:
            c.append(a[i])
        else:
            c.append(b[i])
    return c


# 将非加工数据导入数据库中
def input_data(target_name, engine, sql_name, title=[]):
    xlsx_book = xlrd.open_workbook(target_name)
    sheet_name = xlsx_book.sheet_names()
    if '南战区' in target_name:
        if len(sheet_name) > 1:
            with pd.ExcelFile(target_name) as xls:
                df1 = pd.read_excel(xls, 'sheet1')
                df2 = pd.read_excel(xls, 'sheet2')
                df = pd.concat([df1, df2])
            df.columns = title
            df['update_time'] = date_tostring(datetime.datetime.now())
            df.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
        else:
            with pd.ExcelFile(target_name) as xls:
                df = pd.read_excel(xls, 'sheet1')
            df.columns = title
            df['update_time'] = date_tostring(datetime.datetime.now())
            df.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
    else:
        if len(sheet_name) > 1:
            with pd.ExcelFile(target_name) as xls:
                df1 = pd.read_excel(xls, 'sheet1')
                df2 = pd.read_excel(xls, 'sheet2')
                df = pd.concat([df1, df2])
            df['update_time'] = date_tostring(datetime.datetime.now())
            df.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
        else:
            with pd.ExcelFile(target_name) as xls:
                df = pd.read_excel(xls, 'sheet1')
            df['update_time'] = date_tostring(datetime.datetime.now())
            df.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
    return True


# 删除目标文件夹中非当天生成数据
def delete_document(file_dir,date_time):
    try:
        os.path.isfile(file_dir)
    except:
        print("该路径下没有文件夹！！！")
    os.chdir(file_dir)
    print("文件目录为: %s" %os.listdir(os.getcwd()))
    file_listdir=os.listdir(os.getcwd())
    for filename in file_listdir:
         file_name = filename
         if date_time not in file_name:
             os.remove(file_name)
    return print("任务完成")


# 测试主程序，获得不同数据原始数据
if __name__ == "__main__":
    date_time = date_tostring(datetime.datetime.now())
    file_dir = "E:\\report\\waiqin_data"
    delete_document(file_dir, date_time)
    file_form = 'xls'
    engine = make_engine('waiqin_south')
    file_temp = get_file(file_dir, file_form, '客户信息')
    input_data(file_temp, engine, 'client_data_template')
    file_temp2 = get_file(file_dir, file_form, '拜访记录')
    input_data(file_temp2, engine, 'visit_data_template')
    file_temp3 = get_file(file_dir, file_form, '库存明细')
    input_data(file_temp3, engine, 'store_data_template')
    file_temp4 = get_file(file_dir, file_form, '南战区')
    file_title = title_process(file_temp4)
    input_data(file_temp4, engine, 'termtask_data_template', file_title)
    file_temp5 = get_file(file_dir, file_form, '员工')
    input_data(file_temp5, engine, 'staff_data_template')
    print('数据成功导入')
    exit()