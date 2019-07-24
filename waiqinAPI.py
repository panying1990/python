import requests
import datetime
import hashlib
import json
import bs4
import re
import random
import pandas as pd
import numpy as np


def timestamp_tostring(date_time):
    return date_time.strftime("%Y%m%d%H%M%S")


def msg_json():   # 拜访数据请求消息体
    json_temp = [{}]
    json_a = json.dumps(json_temp)
    return json_a


def md5_msg(msg, app_key, timestamp):
    digest_temp=msg+"|"+app_key+"|"+timestamp
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


def url_post(merge_url, msg_json):
    headers = {"Content-Type": "application/json;charset=utf-8"}
    r_json = requests.post(merge_url, data=msg_json, headers=headers)
    if r_json.status_code == 200:
        content = r_json.content
        ret = json.loads(content.decode("utf-8"))
        return_code = ret.get("return_code")
        print("ReturnCode:\t%s" % (return_code))
        if return_code == 0:
            response_data = ret.get("response_data")
            print("ResponseData:\t%s" % (response_data))
        else:
            return_msg = ret.get("return_msg")
            print("ReturnMessage:\t%s" % (return_msg))
    else:
        print(r_json.status_code)
        print(r_json.content)
    return True



def find_movies(res):
    soup = bs4.BeautifulSoup(res.text, 'html.parser')

    # 电影名
    movies = []
    targets = soup.find_all("div", class_="hd")
    for each in targets:
        movies.append(each.a.span.text)

    # 评分
    ranks = []
    targets = soup.find_all("span", class_="rating_num")
    for each in targets:
        ranks.append(' 评分：%s ' % each.text)

    # 资料
    messages = []
    targets = soup.find_all("div", class_="bd")
    for each in targets:
        try:
            messages.append(each.p.text.split('\n')[1].strip() + each.p.text.split('\n')[2].strip())
        except:
            continue

    result = []
    length = len(movies)
    for i in range(length):
        result.append(movies[i] + ranks[i] + messages[i] + '\n')

    return result


# 找出一共有多少个页面
def find_depth(res):
    soup = bs4.BeautifulSoup(res.text, 'html.parser')
    depth = soup.find('span', class_='next').previous_sibling.previous_sibling.text

    return int(depth)


def main():
    host = "https://movie.douban.com/top250"
    res = open_url(host)
    depth = find_depth(res)

    result = []
    for i in range(depth):
        url = host + '/?start=' + str(25 * i)
        res = open_url(url)
        result.extend(find_movies(res))

    with open("豆瓣TOP250电影.txt", "w", encoding="utf-8") as f:
        for each in result:
            f.write(each)


if __name__ == "__main__":
    timestamp = timestamp_tostring(datetime.datetime.now())
    data = msg_json()
    url = "https://openapi.waiqin365.com/api/customer/v1/queryCustomer"
    open_id = '7862165852723018803'
    app_key = "u081CY95vvTDtXsVCd"
    print(data)
    md5_msg = md5_msg(data, app_key, timestamp)
    print(md5_msg)
    url_api = merge_url(url, open_id, timestamp, md5_msg)
    print(url_api)
    url_post(url_api, data)






# Python代码示例

# !/usr/bin/python
import urllib3
import json
import hashlib
import uuid
import time
import certifi

openid = '7862165852723018803'
appkey = 'u081CY95vvTDtXsVCd'

host = 'https://openapi.waiqin365.com/api'
method = 'employee/v2/queryEmployee'
timstamp = time.strftime("%Y%m%d%H%M%S")
msgid = uuid.uuid1()

body_value = '{}'

digistSrc = '%s|%s|%s' % (body_value, appkey, timstamp)
m5 = hashlib.md5()
m5.update(digistSrc.encode('utf-8'))
digest = m5.hexdigest()

url = '%s/%s/%s/%s/%s/%s' % (host, method, openid, timstamp, digest, msgid)
print(url)

http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
r = http.request('POST', url, headers={"Content-Type": "application/json", 'Accept-Encoding': 'gzip, deflate'},
                 body=body_value)

if r.status == 200:
    data = r.data
    ret = json.loads(data.decode("utf-8"))
    return_code = ret.get("return_code")
    print("ReturnCode:\t%s" % (return_code))
    if return_code == 0:
        response_data = ret.get("response_data")
        print("ResponseData:\t%s" % (response_data))
    else:
        return_msg = ret.get("return_msg")
        print("ReturnMessage:\t%s" % (return_msg))
else:
    print(r.status)
    print(r.data)
