import requests
import datetime
import hashlib
import json
from sqlalchemy import create_engine
import random
import pandas as pd
import numpy as np


# 设置服务器
def make_engine(database):
    engine_word = 'mysql+pymysql://root:P#y20bsy17@192.168.109.202:3306/'+database
    engine = create_engine(engine_word)
    return engine


def timestamp_tostring(date_time):
    return date_time.strftime("%Y%m%d%H%M%S")


def md5_msg(data, app_key, timestamp):
    msg = json.dumps(data)
    digest_temp = msg+"|"+app_key+"|"+timestamp
    digest = digest_temp.encode('utf-8')
    md5 = hashlib.md5()  # 获取MD5对象
    md5.update(digest)    # digest为加密内容
    md5_value = md5.hexdigest()  # md5.hexdigest()为加密结果
    return md5_value


def merge_url(url, open_id, timestamp, md5_msg):
    # 使用post 方式获得数据
    msg_id = "Y0000"+str(round(random.random()*1000))
    url_temp = url + "/" + open_id + "/" + timestamp + "/" + md5_msg+"/"+msg_id
    return url_temp


def url_post(merge_url, data):
    headers = {"Content-Type": "application/json;charset=utf-8"}
    r_json = requests.post(merge_url, data=json.dumps(data), headers=headers)
    if r_json.status_code == 200:
        content = r_json.content
        ret_con = json.loads(content.decode("utf-8"))
        return_code = ret_con.get("return_code")
        print("ReturnCode:\t%s" % (return_code))
        if return_code == '0':
            response_data = ret_con.get("response_data")
            return response_data
        else:
            return_msg = ret_con.get("return_msg")
            print("ReturnMessage:\t%s" % (return_msg))
    else:
        print(r_json.status_code)
        print(r_json.content)
    return True


def get_em(json_texts, engine, sql_name):
    emp_name = []
    emp_org_code = []
    emp_org_name = []
    for json_text in json_texts:
        emp_name.append(json_text['emp_name'])
    for a in json_texts:
        emp_org_code.append(a['waiqin365_org_id'])
    for b in json_texts:
        emp_org_name.append(b['emp_org_name'])
    df1 = pd.DataFrame(emp_name)
    df2 = pd.DataFrame(emp_org_code)
    df3 = pd.DataFrame(emp_org_name)
    cust_info = pd.concat([df1, df2, df3], axis=1, ignore_index=True)
    cust_info.columns = ["emp_name", "waiqin365_org_id", "emp_org_name"]
    cust_info['update_time'] = timestamp_tostring(datetime.datetime.now())
    cust_info.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
    return True


def get_dept(json_texts, engine, sql_name):
    dept_id = []
    dept_allname = []
    for json_text in json_texts:
        dept_id.append(json_text['id'])
    for a in json_texts:
        dept_allname.append(a['full_names'])
    df1 = pd.DataFrame(dept_id)
    df2 = pd.DataFrame(dept_allname)
    dept_info = pd.concat([df1, df2], axis=1, ignore_index=True)
    dept_info.columns = ["dept_id", "dept_allname"]
    dept_info['update_time'] = timestamp_tostring(datetime.datetime.now())
    dept_info.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
    return True


def get_cm(json_texts, engine, sql_name):
    cm_id = []
    cm_name = []
    cm_dept_id = []
    cm_manager_id = []
    create_time = []
    for json_text in json_texts:
        cm_id.append(json_text['cm_code'])
    for a in json_texts:
        cm_name.append(a['cm_name'])
    for b in json_texts:
        cm_dept_id.append(b['cm_dept_waiqin365_id'])
    for c in json_texts:
        cm_manager_id.append(c['cm_manager_waiqin365_id'])
    for d in json_texts:
        create_time.append(d['create_time'])
    df1 = pd.DataFrame(cm_id)
    df2 = pd.DataFrame(cm_name)
    df3 = pd.DataFrame(cm_dept_id)
    df4 = pd.DataFrame(cm_manager_id)
    df5 = pd.DataFrame(create_time)
    cm_info = pd.concat([df1, df2, df3, df4,df5], axis=1, ignore_index=True)
    cm_info.columns = ["cm_id", "cm_name", "cm_dept_id", "cm_manager_id","create_time"]
    cm_info['update_time'] = timestamp_tostring(datetime.datetime.now())
    cm_info.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
    return True


def get_bas(json_texts):
    gungxuan = []
    chen_lie = []
    create_time = []
    for json_text in json_texts:
        chen_lie.append(json_text['slfdf_1805150003'])
    for a in json_texts:
        chenlie.append(a['cm_name'])
    # for b in json_texts:
    #     cm_dept_id.append(b['cm_dept_waiqin365_id'])
    # for c in json_texts:
    #     cm_manager_id.append(c['cm_manager_waiqin365_id'])
    for d in json_texts:
        create_time.append(d['create_time'])
    df1 = pd.DataFrame(cm_id)
    df2 = pd.DataFrame(cm_name)
    df3 = pd.DataFrame(cm_dept_id)
    df4 = pd.DataFrame(cm_manager_id)
    df5 = pd.DataFrame(create_time)
    cm_info = pd.concat([df1, df2, df3, df4,df5], axis=1, ignore_index=True)
    cm_info.columns = ["cm_id", "cm_name", "cm_dept_id", "cm_manager_id","create_time"]
    cm_info['update_time'] = timestamp_tostring(datetime.datetime.now())
    cm_info.to_sql(name=sql_name, con=engine, if_exists='replace', index=False)
    return True


def main():
    # 接口数据
    engine = make_engine('xinchao_temp')
    timestamp = timestamp_tostring(datetime.datetime.now())
    open_id = '7862165852723018803'
    app_key = "u081CY95vvTDtXsVCd"
    # 员工信息表
    em_url = "https://openapi.waiqin365.com/api/employee/v2/queryEmployee"
    em_data = {"emp_status": "1"}
    em_md5_msg = md5_msg(em_data, app_key, timestamp)
    em_url_api = merge_url(em_url, open_id, timestamp, em_md5_msg)
    get_em(json.loads(url_post(em_url_api, em_data)), engine, "em_info_template")
    # 部门信息表
    dept_url = "https://openapi.waiqin365.com/api/organization/v1/queryOrganization"
    dept_data = {"org_status": "1"}
    dept_md5_msg = md5_msg(dept_data, app_key, timestamp)
    dept_url_api = merge_url(dept_url, open_id, timestamp, dept_md5_msg)
    get_dept(json.loads(url_post(dept_url_api, dept_data)), engine, "dept_info_template")
    # 客户信息表
    cm_url = "https://openapi.waiqin365.com/api/customer/v1/queryCustomer"
    cm_data = {"age_number": 3, "status": "1", "after_create_date": "2019-02-18 23:59:59"}
    cm_md5_msg = md5_msg(cm_data, app_key, timestamp)
    cm_url_api = merge_url(cm_url, open_id, timestamp, cm_md5_msg)
    get_cm(json.loads(url_post(cm_url_api, cm_data)), engine, "cm_info_template")
    # 专门拜访数据信息
    bas_url = "https://openapi.waiqin365.com/api/cusVisit/v1/queryCusVisitDetail"
    bas_data = {"function_id": "5943722112321855914", "date_start": "2019-07-24", "date_end": "2019-07-25", "page": "4",
                "rows": "1000"}
    bas_md5_msg = md5_msg(bas_data, app_key, timestamp)
    bas_url_api = merge_url(bas_url, open_id, timestamp, bas_md5_msg)
    get_bas(json.loads(url_post(bas_url_api, bas_data)))
    return  True
    

if __name__ == "__main__":
    
