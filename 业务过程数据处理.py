# -*- coding: utf-8 -*-
# -*- author: panying -*-
# -*- 用于两茶市场推广数据ETL -*-

# 环境设置
import os
import pandas as pd
import xlrd
import glob
from numpy import *
import datetime

# 设置数据处理时间戳
def datetime_toString(dt):
    return dt.strftime("%m.%d")
now = datetime_toString(datetime.datetime.now())

# 将符合条件的表格筛选淘宝客出来，并将该数据直接导入到本地代码中
def open_tbk_work(file_dir, fileform):
    filearray = []
    for filename in glob.glob(file_dir + "\\*." + fileform):
        if '淘宝客订单明细' in filename:
            filearray.append(filename)
        else:
            filearray.append('')
    for i in range(len(filearray)):
        if len(filearray[i]) > 4:
            tbk_file_name = filearray[i]
            return tbk_file_name


# 讲符合条件的表格筛选出来，并将该数据直接导入到本地代码中
def open_zz_work(file_dir, fileform):
    filearray = []
    for filename in glob.glob(file_dir + "\\*." + fileform):
        if '钻展' in filename:
            filearray.append(filename)
        else:
            filearray.append('')
    for i in range(len(filearray)):
        if len(filearray[i]) > 4:
            zz_file_name = filearray[i]
            return zz_file_name


# 讲符合条件的表格筛选出来，并将该数据直接导入到本地代码中
def open_ztc_work(file_dir, fileform):
    filearray = []
    for filename in glob.glob(file_dir + "\\*." + fileform):
        if '直通车' in filename:
            filearray.append(filename)
        else:
            filearray.append('')
    for i in range(len(filearray)):
        if len(filearray[i]) > 4:
            ztc_file_name = filearray[i]
            return ztc_file_name


 # 将符合条件的表格筛选出来，并将该数据直接导入到本地代码中
def open_pz_work(file_dir , fileform):
    filearray = []
    for filename in glob.glob(file_dir + "\\*." + fileform):
        if '品牌' in filename:
            filearray.append(filename)
        else:
            filearray.append('')
    for i in range(len(filearray)):
        if len(filearray[i]) > 4:
            pz_file_name = filearray[i]
            return pz_file_name

# 将淘宝客数据导入到目标文件夹中
def process_tbk_data(file_name, file_dir_s,dt):
    xlsx_book = xlrd.open_workbook(file_name)
    xlsx_sheetname = "Page1"
    try:
        xlsx_sheet_data = xlsx_book.sheet_by_name(xlsx_sheetname)
    except:
        return False
    nrows = xlsx_sheet_data.nrows
    matrix = [0] * (nrows)
    ncols = xlsx_sheet_data.ncols
    for m in range(nrows):
        matrix[m] = ["0"] * ncols
    for i in range(int(nrows)):
        for j in range(0, int(ncols)):
            matrix[i][j] = xlsx_sheet_data.cell(i, j).value
    transfer_file = "\\mkt_tbk_data"
    transfer_columns = matrix[0]
    transfer_data = matrix[1:nrows]
    file_data = pd.DataFrame(columns=transfer_columns, data=transfer_data)
    file_path = file_dir_s + transfer_file + dt +".csv"
    file_data.to_csv(file_path, index=False)
    return True


# 将淘宝客数据导入到目标文件夹中
def process_zz_data(file_name, file_dir_s,dt):
    xlsx_book = xlrd.open_workbook(file_name)
    xlsx_sheetname = "计划日报表-15天效果转化周期"
    try:
        xlsx_sheet_data = xlsx_book.sheet_by_name(xlsx_sheetname)
    except:
        return False
    nrows = xlsx_sheet_data.nrows
    matrix = [0] * (nrows)
    ncols = xlsx_sheet_data.ncols
    for m in range(nrows):
        matrix[m] = ["0"] * ncols
    for i in range(int(nrows)):
        for j in range(0, int(ncols)):
            matrix[i][j] = xlsx_sheet_data.cell(i, j).value
    transfer_file = "\\mkt_zz_data"
    transfer_columns = matrix[0]
    transfer_data = matrix[1:nrows]
    file_data = pd.DataFrame(columns=transfer_columns, data=transfer_data)
    file_path = file_dir_s + transfer_file + dt +".csv"
    file_data.to_csv(file_path, index=False)
    return True

def process_pz_data(file_name, file_dir_s,dt):
    xlsx_book = xlrd.open_workbook(file_name)
    pzbrand_sheetname = "品牌流量包"
    pzperson_sheetname = "定向人群"
    try:
        xlsx_sheet_data1 = xlsx_book.sheet_by_name(pzbrand_sheetname)
        xlsx_sheet_data2 = xlsx_book.sheet_by_name(pzperson_sheetname)
    except:
        return False
    nrows1 = xlsx_sheet_data1.nrows
    nrows2 = xlsx_sheet_data2.nrows
    matrix1 = [0] * (nrows1)
    matrix2 = [0] * (nrows2)
    ncols1 = xlsx_sheet_data1.ncols
    ncols2 = xlsx_sheet_data2.ncols
    for m in range(nrows1):
        matrix1[m] = ["0"] * ncols1
    for i in range(int(nrows1)):
        for j in range(0, int(ncols1)):
            matrix1[i][j] = xlsx_sheet_data1.cell(i, j).value
    for m in range(nrows2):
        matrix2[m] = ["0"] * ncols2
    for i in range(int(nrows2)):
        for j in range(0, int(ncols2)):
            matrix2[i][j] = xlsx_sheet_data2.cell(i, j).value
    transfer_file1 = "\\mkt_pzbrand_data"
    transfer_file2 = "\\mkt_pzperson_data"
    transfer_columns1 = matrix1[0]
    transfer_columns2 = matrix2[0]
    transfer_data1 = matrix1[1:nrows1]
    transfer_data2 = matrix2[1:nrows2]
    file_data1 = pd.DataFrame(columns=transfer_columns1, data=transfer_data1)
    file_data2 = pd.DataFrame(columns=transfer_columns2, data=transfer_data2)
    file_path1 = file_dir_s + transfer_file1 + dt +".csv"
    file_path2 = file_dir_s + transfer_file2 + dt +".csv"
    file_data1.to_csv(file_path1, index=False)
    file_data2.to_csv(file_path2, index=False)
    return True


# 删除目标文件夹中非当天生成数据
def delete_document(file_dir_s,dt):
    try:
        os.path.isfile(file_dir_s)
    except:
        print("该路径下没有文件夹！！！")
    os.chdir(file_dir_s)
    print("目录为: %s"%os.listdir(os.getcwd()))
    file_listdir=os.listdir(os.getcwd())
    for filename in file_listdir:
         file_name = filename
         if dt in file_name:
             os.remove(file_name)
    return "任务完成"




# 测试主程序，获得不同数据原始数据
file_dir = "V:\\更新数据\\11.27"
file_form = "xlsx"
file_dir_s = "V:\\更新数据\\数据转化中转文件夹"
dt = datetime_toString(datetime.datetime.now())
#delete_document(file_dir_s,dt)
#file_name1 = open_tbk_work(file_dir, file_form)
#print(file_name1)
#process_tbk_data(file_name1, file_dir_s,dt)
file_name2 = open_ztc_work(file_dir, file_form)
print(file_name2)
