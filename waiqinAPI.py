import requests
import datetime
import hashlib
import json


def timestamp_tostring(date_time):
    return date_time.strftime("%Y%m%d%H%M%S")


def msg_josn():
    json_temp = {"org_id":"ORG926","org_name":"北京澳特舒尔","org_parent_id":"","org_sequence":926}
    json_a = json.dumps(json_temp)
    return json_a


def mdfive(msg, appkey, timestamp):
    digest_temp=msg+"|"+appkey+"|"+timestamp
    digest = digest_temp.encode('utf-8')
    md5 = hashlib.md5()  # 获取MD5对象
    md5.update(digest)    # digest为加密内容
    md5_value = md5.hexdigest()  # md5.hexdigest()为加密结果
    return md5_value


def get_json():
    # 执行API调用并储存响应
    url='https://openapi.waiqin365.com/api/cusVisit/v1/queryCusVisitRecord/{ openid }/{ timestamp }/{ digest }/{ msg_id } '
    r=requests.get(url)
    print()
    #将API响应储存在一个变量中
    response_dict =r.json()
    #处理结果
    print(response_dict.keys())
    return True


if __name__ == "__main__":
    a = timestamp_tostring(datetime.datetime.now())
    data = msg_josn()
    appkey = "88888888"
    print(data)
    b = mdfive(data, appkey, a)
    print(b)
