# -*- coding: utf-8 -*-
# -*- author: panying -*-
# -*- 用于使用天猫店铺-生意参谋-流量-店铺来源数据聚合转化 -*-

# 环境设置
import numpy as np
import os
from pandas import DataFrame
import xlrd
import xlwt

# 设置工作路径
# 获取当前工作目录
file_dir="V:\\电商数据需求\\双11流量分析\\PC店铺来源"
# 保存文件地址
file_dir_s = "V:\\电商数据需求\\双11流量分析\\PC店铺来源_转化"
os.chdir(file_dir)
file_name_list = os.listdir(file_dir)
gematrix = len(file_name_list)
matrix = [None] * gematrix
for i in range(gematrix):
    os.chdir(file_dir)
    name = file_name_list[i]
    year_month_date = name[17:27]
    name_list = name[0:38]
    xls_book = xlrd.open_workbook(name)
    xls_sheetname = xls_book.sheet_names()[0]
    xls_sheet1= xls_book.sheet_by_name(xls_sheetname) # 通过sheet名字来获取，当然如果你知道sheet名字了可以直接指定
    nrows = int(xls_sheet1.nrows) # 获得数据表中的数据列
    ncols = int(xls_sheet1.ncols) # 获得数据表中的数据行
    list_columns = xls_sheet1.row_values(5) # 构建新表中的数据列，用户承接转移过来的数据值
    # 构建新表中的数据行，用户承接转移过来的数据值
    list_rows = np.arange(0, nrows - 6, 1)
    length_temp = len(list_columns)
    rowgth_temp = len(list_rows)
    seed_num = length_temp*rowgth_temp
    new_xls_pc = DataFrame(index=list_rows, columns=list_columns)
    # 将xlsx中的数据填充到新的数据框架中
    for i in range(rowgth_temp):
        for j in range(length_temp):
            new_xls_pc.iloc[i, [j]] = xls_sheet1.cell_value(6+i, j)
    new_xls_pc["渠道来源"] = xls_sheetname
    new_xls_pc["年月日"] = year_month_date
    os.chdir(file_dir_s)
    out_file_name = name_list+".xlsx"
    new_xls_pc.to_excel(out_file_name,index = False)

print("任务完成")
# 修改工作目录
# out_file_dir="V:\\更新数据\\碧生源奥利司他\\流量监控数据\\店铺来源_修改"
# os.chdir(file_dir)
# out_file_name = file_name+'.xlsx'
# new_xls_pc.to_excel(out_file_name)

## 将数据全部数据进行合并，后修改成为数据分析需要的程度

# -*- coding: utf-8 -*-
# -*- author: panying -*-
# -*- 用于使用天猫店铺-生意参谋-流量-店铺来源数据进行转化，并对转化后的数据进行合并 -*-

# 环境设置
import numpy as np
import os
from pandas import DataFrame
import xlrd
import xlwt
import glob
from numpy import *

# 下面这些变量根据合成数据具体情况选择,如来源无线端-店铺流量数据使用 wap_header
pc_header = ['流量来源', '来源明细', '访客数', '访客数变化', '支付转化率', '支付转化率变化',	'支付金额', '支付金额变化', '客单价', '客单价变化', '新访客', '新访客变化', '浏览量', '浏览量变化', '人均浏览量', '人均浏览量变化', '收藏人数', '收藏人数变化', '加购人数', '加购人数变化', '跳失率', '跳失率变化', '下单金额', '下单金额变化', '下单买家数', '下单买家数变化', '下单转化率', '下单转化率变化', '支付买家数', '支付买家数变化', 'UV价值', 'uv价值变化','渠道来源', '年月日']
wap_header = ['流量来源', '来源明细', '访客数', '访客数变化', '支付转化率', '支付转化率变化',	'支付金额', '支付金额变化', '客单价', '客单价变化', '下单金额', '下单金额变化', '下单买家数', '下单买家数变化', '下单转化率', '下单转化率变化', '支付买家数', '支付买家数变化', 'UV价值', 'uv价值变化', '渠道来源', '年月日']
# 在哪里搜索多个表格
filelocation = "V:\\电商数据需求\\双11流量分析\\PC店铺来源_转化\\"
# 当前文件夹下搜索的文件名后缀
fileform = "xlsx"
# 将合并后的表格存放到的位置
filedestination = "V:\\电商数据需求\\双11流量分析\\PC店铺来源_转化\\"
# 合并后的表格命名为file
file = "PC店铺来源_转化"

# 首先查找默认文件夹下有多少文档需要整合
filearray = []
for filename in glob.glob(filelocation + "*." + fileform):
    filearray.append(filename)
# 以上是从pythonscripts文件夹下读取所有excel表格，并将所有的名字存储到列表filearray
print("在默认文件夹下有%d个文档哦" % len(filearray))
file_matrix = len(filearray)
matrix = [None] * file_matrix
# 实现读写数据

# 下面是将所有文件读数据到三维列表cell[][][]中（不包含表头）

for i in range(file_matrix):
    fname = filearray[i]
    year_month_date = fname[17:27]
    name_list = fname[0:38]
    work_book = xlrd.open_workbook(fname)
    xls_sheetname = work_book.sheet_names()[0]
    try:
        sheet_data = work_book.sheet_by_name(xls_sheetname)
    except:
        print("在文件%s中没有找到%d，读取文件数据失败,要不你换换表格的名字？" % (fname,xls_sheetname))
    nrows = sheet_data.nrows
    matrix[i] = [0] * (nrows - 5)

    ncols = sheet_data.ncols
    for m in range(nrows - 5):
        matrix[i][m] = ["0"] * ncols

    for j in range(6, nrows):
        for k in range(0, int(ncols)):
            matrix[i][j - 6][k] = sheet_data.cell(j, k).value
            # 下面是写数据到新的表格test.xls中哦

import xlwt
filename = xlwt.Workbook()
sheet = filename.add_sheet(file)
# 下面是把表头写上
for i in range(0, len(pc_header)):
    sheet.write(0, i, pc_header[i])
# 求和前面的文件一共写了多少行
zh = 1
for i in range(file_matrix):
    for j in range(len(matrix[i])):
        for k in range(len(matrix[i][j])):
            sheet.write(zh, k, matrix[i][j][k])
        zh = zh + 1
print("我已经将%d个文件合并成1个文件，并命名为%s.xls.快打开看看正确不？" % (file_matrix, file))
filename.save(filedestination + file + ".xls")
