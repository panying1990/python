# 本脚本用于手机系统日常数据工作日更新


import requests
import datetime
import hashlib
import json
from sqlalchemy import create_engine
import random
import pandas as pd


# 设置服务器
def make_engine(database):
    engine_word = 'mysql+pymysql://root:8@192.18.202:3306/'+database
    engine = create_engine(engine_word)
    return engine


def timestamp_tostring(date_time):
    return date_time.strftime("%Y%m%d%H%M%S")


def get_Yesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today-oneday
    return yesterday


# 创建DBSession类型获得数据:
def drop_all_table(engine):
    # 单线程操作线程池
    conn = engine.raw_connection()  # 从连接池中获取1个连接,开始连接
    cursor = conn.cursor()
    delete_em = "drop table if exists em_info_template"
    cursor.execute(delete_em)
    delete_dept = "drop table if exists dept_info_template"
    cursor.execute(delete_dept)
    delete_dis = "drop table if exists dis_info_template"
    cursor.execute(delete_dis)
    delete_prod = "drop table if exists prod_info_template"
    cursor.execute(delete_prod)
    delete_east = "drop table if exists userDefined_east"
    cursor.execute(delete_east)
    delete_terminal = "drop table if exists terminal_east"
    cursor.execute(delete_terminal)
    delete_custVisit = "drop table if exists custVisit_template"
    cursor.execute(delete_custVisit)
    cursor.close()
    conn.close()
    return True


def url_post(url, data, timestamp):
    open_id = '7862188803'
    app_key = "u0818sVCd"
    digest_temp = '%s|%s|%s' % (json.dumps(data), app_key, timestamp)
    digest = digest_temp.encode('utf-8')
    md5 = hashlib.md5()  # 获取MD5对象
    md5.update(digest)  # digest为加密内容
    md5_msg = md5.hexdigest()  # md5.hexdigest()为加密结果
    msg_id = "Y0000" + str(round(random.random() * 1000))
    merge_url = '%s/%s/%s/%s/%s' % (url, open_id, timestamp, md5_msg, msg_id)
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


def get_em(response, engine, sql_name):
    for each in response:
        if each.get("parent_code") != None:
            del each['parent_code']
        if each.get("exts") != None:
            del each['exts']
        if each.get("emp_job") != None:
            del each['emp_job']
        if each.get("emp_position") != None:
            del each['emp_position']
        temp = pd.DataFrame.from_dict(each, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def get_dept(response, engine, sql_name):
    for each in response:
        temp = pd.DataFrame.from_dict(each, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def get_dis(response, engine, sql_name):
    for each in response:
        temp = pd.DataFrame.from_dict(each, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def get_prod(response, engine, sql_name):
    for each in response:
        if each.get("prd_units") != None:
            del each['prd_units']
        if each.get("prd_exts") != None:
            del each['prd_exts']
        temp = pd.DataFrame.from_dict(each, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def get_terminal(response, engine, sql_name):
    for each in response:
        del each["sts"]
        temp = pd.DataFrame.from_dict(each, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def get_east_self_rp(response, engine, sql_name):
    for each in response:
        b = each['pt']
        del b["slfdf_1907160015"]
        del b["slfdf_1907160017"]
        del b["source_code"]
        del b["id"]
        temp = pd.DataFrame.from_dict(b, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def get_custVisit(response, engine, sql_name):
    for each in response:
        temp = pd.DataFrame.from_dict(each, orient='index').T
        temp.to_sql(name=sql_name, con=engine, if_exists='append', index=False)
    return True


def main(timestamp):
    # 员工信息表
    em_url = "https://openapi.waiqin365.com/api/employee/v2/queryEmployee"
    em_data = {"emp_status":"1"}
    get_em(json.loads(url_post(em_url, em_data,timestamp)), engine, "em_info_template")
    # # 部门信息表
    dept_url = "https://openapi.waiqin365.com/api/organization/v1/queryOrganization"
    dept_data = {"org_status": "1"}
    get_dept(json.loads(url_post(dept_url, dept_data,timestamp)), engine, "dept_info_template")
    # 销售区域信息表
    dis_url = "https://openapi.waiqin365.com/api/district/v1/queryDistrict"
    dis_data = {"dis_status": "1"}
    get_dis(json.loads(url_post(dis_url, dis_data, timestamp)), engine, "dis_info_template")
    # 商品信息表
    prod_url = "https://openapi.waiqin365.com/api/product/v1/queryProduct"
    prod_data = {"status":"1"}
    get_prod(json.loads(url_post(prod_url, prod_data, timestamp)), engine, "prod_info_template")
    # 超级表单自定义表
    userDefined_url = "https://openapi.waiqin365.com/api/userDefined/v1/queryUserDefined"
    userDefined_data = {"form_id": "8086498891910548585", "date_start": "2019-07-10", "date_end": "2019-07-26","page": "1", "rows": "1000"}
    userDefined_data["date_start"] = get_Yesterday().strftime("%Y-%m-%d")
    userDefined_data["date_end"] = get_Yesterday().strftime("%Y-%m-%d")
    get_east_self_rp(json.loads(url_post(userDefined_url, userDefined_data, timestamp)), engine, "userDefined_east")
    # 拜访记录表
    custVisit_url = "https://openapi.waiqin365.com/api/cusVisit/v1/queryCusVisitRecord"
    custVisit_data = {"date_start": "2017-01-10","date_end": "2017-01-20","page":"1","rows":"1000"}
    custVisit_data["date_start"] = get_Yesterday().strftime("%Y-%m-%d")
    custVisit_data["date_end"] = get_Yesterday().strftime("%Y-%m-%d")
    for a in range(1, 6):
        custVisit_data["page"] = str(a)
        get_custVisit(json.loads(url_post(custVisit_url, custVisit_data, timestamp)), engine, "custVisit_template")
    # 通用终端拜访记录表
    terminal_url = "https://openapi.waiqin365.com/api/cusVisit/v1/queryCusVisitDetail"
    terminal_data = {"function_id": "5943722112321855914", "date_start": "", "date_end": "","page": "1", "rows": "1000"}
    terminal_data["date_start"] = get_Yesterday().strftime("%Y-%m-%d")
    terminal_data["date_end"] = get_Yesterday().strftime("%Y-%m-%d")
    for a in range(1,5):
        terminal_data["page"] = str(a)
        get_terminal(json.loads(url_post(terminal_url, terminal_data, timestamp)), engine, "terminal_east")
    return True


if __name__ == "__main__":
    # 接口数据
    engine = make_engine('xinchao_temp')
    timestamp = timestamp_tostring(datetime.datetime.now())
    drop_all_table(engine)
    print(datetime.datetime.now())
    main(timestamp)
    print(datetime.datetime.now())
