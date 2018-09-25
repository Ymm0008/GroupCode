# -*- coding: utf-8 -*-

import requests
import time
import random
import sys
import json
import codecs
from elasticsearch import Elasticsearch

reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append("../")
from global_utils import es_for_uid, index_name_for_uid


UA_list = [
'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1']

# 这里填写无忧代理IP提供的API订单号（请到用户中心获取）
order = "fa73de33603d659302819dba613d5f1f"
# 获取IP的API接口
apiUrl = "http://api.ip.data5u.com/dynamic/get.html?order=" + order


def get_proxy_ip():
    while 1:
        try:
            res = requests.get(apiUrl,timeout=10)
        except:
            time.sleep(5)
            continue
        else:
            break
    # 按照\n分割获取到的IP
    ips = res.text.strip("\n").split("\n")
    proxy = {"http": ips[0]}
    return proxy

def get_name_type_by_uid(uid):
    '''
    input:uid
    output: user_name,verified_type

    verified_type说明
    普通用户：-1
    名人：0
    政府：1
    企业：2
    媒体：3
    校园：4
    网站：5
    应用：6
    团体：7
    待审企业：8
    初级达人：200
    中高级达人：220
    已故V用户：400
    '''
    headers = {'User-Agent': random.choice(UA_list),}
    url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value=%s' % uid
    # print url
    resp = requests.get(url = url, headers = headers, timeout = 10, proxies = get_proxy_ip())
    time.sleep(1)
    data = resp.json()
    if data['ok']:
        user = data['data'].get('userInfo','')
        if user and str(user['id']) == str(uid):
            return user['screen_name'], user['verified_type']
        else:
            return None, None
    else:
        return None, None


def get_all_uid():
    uid_list = []
    event_name_index = 'flow_text_data'
    event_name_type = ['gangdu','maoyi','zhongxing','didi','yimiao','jiaquan']

    for i in range(len(event_name_type)):
        query_body = {
            "size": 0,
            "query": {
                "match_all": {}
            },
            "aggs": {
                "get_uid": {
                    "terms": {
                        "field": "uid",
                        "size": 100000
                    }
                }
            }
        }
        response = es_for_uid.search(index = event_name_index, doc_type = event_name_type[i], body = query_body)
        buckets = response["aggregations"]["get_uid"]["buckets"]

        for j in range(len(buckets)):
            uid_list.append(buckets[j]["key"])

    # uid list包含不同事件的所有uid，去重
    uid_list_no_duplication = []
    for uid in uid_list:
        if uid not in uid_list_no_duplication:
            uid_list_no_duplication.append(uid)

    return uid_list_no_duplication


def store_name_and_type_to_ES():

    create_index_for_uid()
    uid_list = get_all_uid()

    bulk_action = []
    index_count = 0
    counter = 0

    for uid in uid_list:
        if check_if_duplicate(uid):
            continue
        else:
            screen_name, verified_type = get_name_type_by_uid(uid)

            d = {}
            d["uid"] = uid
            d["name"] = screen_name
            d["type"] = verified_type

            bulk_action.extend([{"index": {"_id": counter}}, d])
            index_count += 1

            if index_count != 0 and index_count % 100 == 0:
                es_for_uid.bulk(bulk_action, index = index_name_for_uid, doc_type = "text")
                bulk_action = []
                print "finish index: ", index_count
            counter +=1

    if bulk_action:
        es_for_uid.bulk(bulk_action, index = index_name_for_uid, doc_type = "text")
    print "total index: ", index_count, bulk_action[0]['index']
    print "index uid successfully"


def create_index_for_uid():

    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        }
    }
    es_for_uid.indices.create(index = index_name_for_uid, body = create_body, ignore = 400)


def check_if_duplicate(uid):
    
    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "uid": uid
                    }
                }
            }
        }
    }
    response = es_for_uid.search(index = index_name_for_uid, doc_type = "text", body = query_body)

    if response["hits"]["hits"] == []:
        return 0
    else:
        return 1


if __name__ == '__main__':

    st = time.time()
    store_name_and_type_to_ES()
    et = time.time()
    print "total time: ", et - st
