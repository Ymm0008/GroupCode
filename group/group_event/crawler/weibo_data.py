# -*- coding:utf-8 -*-

import sys
import time
import json
import numpy as np
import datetime

reload(sys)
sys.path.append("../")
from global_utils import es_flow_text, es_data_text


from flow_text_preprocess.sensitive import cal_sensitive
from flow_text_preprocess.sentiment_classifier import triple_classifier
from ip.geo_ip import ip_parse


def dateRange(start_time, end_time):
    dates = []
    dt = datetime.datetime.strptime(start_time, "%Y-%m-%d")
    date = start_time[:]
    while date <= end_time:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    dates = ['flow_text_'+x  for x in dates]
    return dates


def data2es(items, index_name, type_name):
    bulk_action = []
    index_count = 0

    for item in items:
        try:
            uid = item["_source"]["uid"]
            # root_uid  = item["root_uid"] 
            text = item["_source"]["text"]
            mid = item["_source"]["mid"]
            # root_mid = item["root_mid"]            
            timestamp = item["_source"]["timestamp"]
            message_type = item["_source"]["message_type"]
            ip = item["_source"]["ip"]
        except:
            continue

        # 建索引的代码从这里开始写
        index_dict = dict()
        index_dict["uid"] = uid
        # index_dict["root_uid"] = root_uid
        index_dict["text"] = text
        index_dict["mid"] = mid
        # index_dict["root_mid"] = root_mid        
        index_dict["timestamp"] = timestamp
        index_dict["message_type"] = message_type

        sentiment, keywords_list = triple_classifier(text)
        sensitive, sensitive_words_dict = cal_sensitive(text)
        geo = ip_parse(ip)

        index_dict["geo"] = geo
        index_dict["sentiment"] = sentiment
        index_dict["sensitive"] = sensitive
        index_dict["keywords_list"] = keywords_list
        index_dict["sensitive_words_dict"] = sensitive_words_dict

        bulk_action.extend([{"index":{"_id":mid}}, index_dict])
        index_count += 1

        if index_count !=0 and index_count % 100 == 0:
            es_data_text.bulk(bulk_action, index=index_name, doc_type=type_name)
            bulk_action = []
            print "finish index: ", index_count

    if bulk_action:
        es_data_text.bulk(bulk_action, index=index_name, doc_type=type_name)
    print "total index: ", index_count


def get_weibo_data(keywords, start_time, end_time, index_name, type_name):
    keyword_query_list = []
    for keyword in keywords.split():
        keyword_query_list.append({'wildcard':{'text':'*'+keyword+'*'}})
        
    query_body = {
        'query':{
            'bool':{
                'should':keyword_query_list,
                'minimum_should_match':2
                }
            },
        "size": 100000000
    }

    index_list = dateRange(start_time, end_time)
    results = es_flow_text.search(index=index_list, doc_type='text', body=query_body)["hits"]["hits"]

    print len(results)
    data2es(results, index_name, type_name)


def get_weibo_num(keywords, start_time, end_time):
    keyword_query_list = []
    for keyword in keywords.split():
        keyword_query_list.append({'wildcard':{'text':'*'+keyword+'*'}})
        
    query_body = {
        'query':{
            'bool':{
                'should':keyword_query_list,
                'minimum_should_match':2
                }
            }
    }

    index_list = dateRange(start_time, end_time)
    count = es_flow_text.count(index=index_list, doc_type='text', body=query_body)["count"]

    return count



if __name__ == '__main__':
    keywords = '滴滴 打车 乘客 司机'
    start_time = '2016-11-24'
    end_time = '2016-11-26'
    index_name = 'weibo_data'
    type_name = 'didi_test'

    get_weibo_data(keywords, start_time, end_time, index_name, type_name)
    print 'get_data successful'

    count = get_weibo_num(keywords, start_time, end_time)
    print count


