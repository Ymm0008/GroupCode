# -*- coding:utf-8 -*-
# version 3

import sys
import time
import json
import math
import numpy as np
from collections import defaultdict
import datetime

from text_classify.test_topic import topic_classfiy
from text_classify.config import eng2chi_dict
from duplicate import duplicate
from textrank import text_rank

reload(sys)
sys.path.append("../")
from global_utils import es_flow_text, index_content_sensing
from global_utils import R_SOCIAL_SENSING as r
from global_utils import es_sensor
from global_utils import type_flow_text_index, type_content_sensing
from time_utils import ts2date
from global_config import start_time, end_time

def dateRange(start_time, end_time):
    dates = []
    dt = datetime.datetime.strptime(start_time, "%Y-%m-%d")
    date = start_time[:]
    while date <= end_time:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    dates = ['flow_text_'+ x  for x in dates]
    return dates


time_interval = 3600
forward_time_range = 24*2*3600
index_list = dateRange(start_time, end_time)


def get_weibo(item):
    keys = ["text","sensitive_words_string","sensitive", "uid", "user_fansnum", "mid", "keywords_string", "geo", "ip", "timestamp", "message_type"]
    results = dict()
    for key in keys:
        results[key] = item[key]
    return results


# 给定社会传感器，查找原创微博列表
def query_mid_list(ts, social_sensors, time_segment, message_type=1):
    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must":[
                            {"range": {
                                "timestamp": {
                                    "gte": ts - time_segment,
                                    "lt": ts
                                }
                            }},
                            {"terms":{"uid": social_sensors}},
                            {"term":{"message_type": message_type}}
                        ]
                    }
                }
            }
        },
        "sort": {"sentiment": {"order": "desc"}},
        "size": 10000
    }
    search_results = es_flow_text.search(index=index_list, doc_type=type_flow_text_index, body=query_body)["hits"]["hits"]

    mid_dict = dict()
    origin_mid_list = set()
    if search_results:
        for item in search_results:
            if message_type == 1:
                origin_mid_list.add(item["_id"])
            else:
                origin_mid_list.add(item['_source']['root_mid'])
                mid_dict[item['_source']['root_mid']] = item["_id"]    # 源头微博和当前转发微博的mid

    if message_type != 1:
    # 保证获取的源头微博能在index_list索引中内找到，否则丢弃，即：如果是很早之前发的帖子就丢弃，保证时效性
        filter_list = []
        filter_mid_dict = dict()
        for iter_index in index_list:
            if origin_mid_list:
                exist_es = es_flow_text.mget(index=iter_index, doc_type=type_flow_text_index, body={"ids":list(origin_mid_list)})["docs"]
                for item in exist_es:
                    try:
                        if item["found"]:
                            filter_list.append(item["_id"])
                            filter_mid_dict[item["_id"]] = mid_dict[item["_id"]]
                    except:
                        continue
        origin_mid_list = filter_list
        mid_dict = filter_mid_dict

    return list(origin_mid_list), mid_dict


# 过滤：已经感知过的事情不会再出出现在列表里
def filter_mid(mid_list):
    llen = len(mid_list)
    l_1000 = llen/1000

    result = []

    for i in range(l_1000+1):
        tmp = mid_list[i*1000:(i+1)*1000]
        if tmp:
            es_results = es_sensor.mget(index=index_content_sensing, doc_type=type_content_sensing, body={"ids":tmp}, _source=False)["docs"]
            for item in es_results:
                try:
                    if not item["found"]:
                        result.append(item["_id"])
                except:
                    continue

    return result


def social_sensing(task_detail):
    # 任务名, 传感器, 任务创建时间（感知时间的起点）
    
    task_name = task_detail[0]
    social_sensors = task_detail[1]
    ts = float(task_detail[2])

    print 'sensing_start_time:',ts2date(ts)
 

    # 前两天之内的原创、转发微博  list/retweeted （不包含当前一个小时）
    forward_origin_weibo_list, forward_1 = query_mid_list(ts-time_interval, social_sensors, forward_time_range)
    forward_retweeted_weibo_list, forward_3 = query_mid_list(ts-time_interval, social_sensors, forward_time_range, 3)
    
    # 前一个小时内原创、转发微博  list/retweeted
    current_origin_weibo_list, current_1 = query_mid_list(ts, social_sensors, time_interval)
    current_retweeted_weibo_list, current_3 = query_mid_list(ts, social_sensors, time_interval, 3)

    all_mid_list = []
    all_mid_list.extend(current_origin_weibo_list)
    all_mid_list.extend(current_retweeted_weibo_list)
    all_mid_list.extend(forward_origin_weibo_list)
    all_mid_list.extend(forward_retweeted_weibo_list)

    all_origin_list = []
    all_origin_list.extend(current_origin_weibo_list)
    all_origin_list.extend(forward_origin_weibo_list)
    all_origin_list = list(set(all_origin_list))

    all_retweeted_list = []
    all_retweeted_list.extend(current_retweeted_weibo_list)
    all_retweeted_list.extend(forward_retweeted_weibo_list)   #被转发微博的mid/root_mid
    all_retweeted_list = list(set(all_retweeted_list))


    all_mid_list = filter_mid(all_mid_list)
    all_origin_list = filter_mid(all_origin_list)
    all_retweeted_list = filter_mid(all_retweeted_list)

    print "all mid list: ", len(all_mid_list)
    print "all_origin_list", len(all_origin_list)
    print "all_retweeted_list", len(all_retweeted_list)


    # 查询微博在当前时间内的转发和评论数, 聚合按照message_type
    if all_origin_list:
        origin_weibo_detail = dict()
        for mid in all_origin_list:
            retweet_count = es_flow_text.count(index=index_list, doc_type="text", body={"query":{"bool":{"must":[{"term":{"root_mid": mid}}, {"term":{"message_type":3}}]}}})["count"]
            comment_count = es_flow_text.count(index=index_list, doc_type="text", body={"query":{"bool":{"must":[{"term":{"root_mid": mid}}, {"term":{"message_type":2}}]}}})["count"]
            tmp = dict()
            tmp["retweeted_stat"] = retweet_count
            tmp["comment_stat"] = comment_count
            origin_weibo_detail[mid] = tmp
    else:
        origin_weibo_detail = {}
    print "len(origin_weibo_detail): ", len(origin_weibo_detail)

    if all_retweeted_list:
        retweeted_weibo_detail = dict()
        for mid in all_retweeted_list:
            retweet_count = es_flow_text.count(index=index_list, doc_type="text", body={"query":{"bool":{"must":[{"term":{"root_mid": mid}}, {"term":{"message_type":3}}]}}})["count"]
            comment_count = es_flow_text.count(index=index_list, doc_type="text", body={"query":{"bool":{"must":[{"term":{"root_mid": mid}}, {"term":{"message_type":2}}]}}})["count"]
            tmp = dict()
            tmp["retweeted_stat"] = retweet_count
            tmp["comment_stat"] = comment_count
            retweeted_weibo_detail[mid] = tmp
    else:
        retweeted_weibo_detail = {}
    print "len(retweeted_weibo_detail): ", len(retweeted_weibo_detail)


    # 有事件发生时开始，查询所有的 all_mid_list, 一小时+两天
    if index_list and all_mid_list:
        query_body = {
            "query":{
                "filtered":{
                    "filter":{
                        "terms":{"mid": all_mid_list}
                    }
                }
            },
            "size": 5000
        }
        search_results = es_flow_text.search(index=index_list, doc_type="text", body=query_body)['hits']['hits']
        print "search mid len: ", len(search_results)


        all_text_dict = dict()          # 感知到的事, all_mid_list
        mid_value = dict()              # 文本赋值
        duplicate_dict = dict()         # 重合字典
        classify_text_dict = dict()     # 分类文本
        sensitive_words_dict = dict()    

        duplicate_text_list = []
        classify_mid_list = []
        
        if search_results:
            for item in search_results:
                iter_mid = item['_source']['mid']
                iter_text = item['_source']['text'].encode('utf-8', 'ignore')
                iter_sensitive = item['_source'].get('sensitive', 0)
                tmp_text = get_weibo(item['_source'])

                all_text_dict[iter_mid] = tmp_text
                duplicate_text_list.append({"_id":iter_mid, "title": "", "content":iter_text.decode("utf-8",'ignore')})

                if iter_sensitive:
                    sensitive_words_dict[iter_mid] = iter_sensitive

                keywords_dict = json.loads(item['_source']['keywords_dict'])
                personal_keywords_dict = dict()
                for k, v in keywords_dict.iteritems():
                    k = k.encode('utf-8', 'ignore')
                    personal_keywords_dict[k] = v

                classify_text_dict[iter_mid] = personal_keywords_dict
                classify_mid_list.append(iter_mid)

            # 去重
            print "start duplicate:",'----'
            if duplicate_text_list:
                dup_results = duplicate(duplicate_text_list)
                for item in dup_results:
                    if item['duplicate']:
                        duplicate_dict[item['_id']] = item['same_from']
            print '----', "duplicate finished:"

            # 分类
            print "start classify:",'----'
            mid_value = dict()
            if classify_text_dict:
                classify_results = topic_classfiy(classify_mid_list, classify_text_dict)

                for k,v in classify_results.iteritems(): # mid:value
                    mid_value[k]=v[0]
            print '----', "classify finished:"
                            

        mid_list = all_text_dict.keys()
        mid_duplicate_list = set(duplicate_dict.keys())|set(duplicate_dict.values())
        intersection_list = set(mid_list)-(set(duplicate_dict.keys())|set(duplicate_dict.values()))
        print "final mid:", len(mid_list)
        print "duplicate mid:", len(mid_duplicate_list)
        print "duplicate:", len(set(duplicate_dict.values()))
        print "single: ", len(intersection_list)

        # 将字典键值对倒过来
        reverse_duplicate_dict = defaultdict(list)
        for k,v in duplicate_dict.iteritems():
            reverse_duplicate_dict[v].append(k)

        for term in intersection_list:
            reverse_duplicate_dict[term] = [term]

        bulk_action = []
        count = 0
        for id in reverse_duplicate_dict.keys():    
            iter_dict = dict()

            inter_mid_list = []
            inter_mid_list.append(id)
            inter_mid_list.extend(reverse_duplicate_dict[id])


            # 计算发起者
            timestamp_list = []
            for mid in inter_mid_list:
                timestamp_list.append(all_text_dict[mid]['timestamp'])

            mid_initial = inter_mid_list[timestamp_list.index(min(timestamp_list))]


            # 计算推动者
            push_list = []
            for mid in inter_mid_list:
                if origin_weibo_detail.has_key(mid):
                    retweeted_stat = origin_weibo_detail[mid]['retweeted_stat']
                elif retweeted_weibo_detail.has_key(mid):
                    retweeted_stat = retweeted_weibo_detail[mid]
                else:
                    retweeted_stat = 0
                push_list.append(retweeted_stat)

            mid_push = inter_mid_list[push_list.index(max(push_list))]
            mid = mid_push

            if origin_weibo_detail.has_key(mid):
                iter_dict.update(origin_weibo_detail[mid])   #  update  函数把字典dict2的键/值对更新到dict里
                iter_dict["type"] = 1
            elif retweeted_weibo_detail.has_key(mid):
                iter_dict.update(retweeted_weibo_detail[mid])
                iter_dict["type"] = 0
            else:
                iter_dict["retweeted_stat"] = 0
                iter_dict["comment_stat"] = 0
                iter_dict["type"] = -1


            iter_dict["heat"] = iter_dict["retweeted_stat"] + iter_dict["comment_stat"]     
            iter_dict["status"] = 0      # 是否加入监测
            iter_dict["delete"] = 0      # 是否删除
            iter_dict["topic_field"] = eng2chi_dict[mid_value[mid]]    # 分类标签
            iter_dict["detect_ts"] = ts       # 感知开始时间
            iter_dict["initiator"] = all_text_dict[mid_initial]['uid']       # 发起者
            iter_dict["push"] = all_text_dict[mid_push]['uid']       # 推动者

            iter_dict.update(all_text_dict[mid])
            iter_dict["keywords"] = text_rank(iter_dict['text'], 5)
            count += 1

            _id = mid
            bulk_action.extend([{"index":{"_id": _id}}, iter_dict])
            if count % 500 == 0:
                es_sensor.bulk(bulk_action, index=index_content_sensing, doc_type=type_content_sensing, timeout=600)
                bulk_action = []

        if bulk_action:
            es_sensor.bulk(bulk_action, index=index_content_sensing, doc_type=type_content_sensing)

    return "1"

