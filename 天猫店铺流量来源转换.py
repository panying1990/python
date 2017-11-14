# -*- coding: utf-8 -*-
# -*- author: panying -*-
# -*- 用于使用天猫店铺-生意参谋-流量-店铺来源数据聚合转化 -*-

# 环境设置
import numpy as np
import sys
import os
import pandas as pd
from pandas import DataFrame
import xlrd

# 设置工作路径
# 获取当前工作目录
file_dir=os.getcwd()
os.chdir(file_dir)
# 获取工作目录下的目标文件
year_month_date = "2017-11-12"
channel_name = "无线"
file_name="【生意参谋平台】"+channel_name+"店铺流量来源-"+year_month_date+'_'+year_month_date
xls_file_name = file_name+'.xls'
xls_book = xlrd.open_workbook(xls_file_name)
#获得第一个sheet表的名称
xls_sheetname = xls_book.sheet_names()[0]
#获得指定索引的sheet名字
xls_sheet1=xls_book.sheet_by_name(xls_sheetname) #通过sheet名字来获取，当然如果你知道sheet名字了可以直接指定
nrows = int(xls_sheet1.nrows)
ncols = int(xls_sheet1.ncols)
# 从生意参谋导出的数据格式从第5行开始为正式值
list_columns = xls_sheet1.row_values(5)
list_rows = np.arange(0, nrows - 5, 1)
length_temp = len(list_columns)
rowgth_temp = len(list_rows)
seed_num = length_temp*rowgth_temp
# 构建新的数据框架
new_xls_pc = DataFrame(index=list_rows, columns=list_columns)
# 将xlsx中的数据填充到新的数据框架中
for i in range(rowgth_temp):
    for j in range(length_temp):
        new_xls_pc.iloc[i, [j]] = xls_sheet1.cell_value(5+i, j)
new_xls_pc["渠道来源"] = xls_sheetname
new_xls_pc["年月日"] = year_month_date

# 修改工作目录
out_file_dir="V:\\更新数据\\碧生源奥利司他\\流量监控数据\\店铺来源_修改"
os.chdir(file_dir)
out_file_name = file_name+'.xlsx'
new_xls_pc.to_excel(out_file_name)
