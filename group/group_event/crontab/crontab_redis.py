# -*- coding:utf-8 -*-

# 本程序定时启动，是监控任务的入口，从manag es中读取尚未完成的任务，送入redis的任务队列中，
# 由子程序从队列中pop出任务信息进行监控计算

import sys
import time
import json

reload(sys)
sys.path.append("../")
from global_utils import es_data_text, es_incident
from global_utils import R_SOCIAL_SENSING as r
from global_utils import index_monitor_task, type_monitor_task, index_incident_task, type_incident_task
from global_utils import index_weixin_name, type_weixin_name, index_weibo_name

from crewler.get_user_name_type_by_uid import store_name_and_type_to_ES
from crawler.crawler_stat import get_data2es
from sensing.task_list import create_task_list, social_sensing_task

from geo_analysis.geo_function import geo_analysis_run
from network_analysis.run_network import network_analysis
from evolution_analysis.risk_evolution_storage_module import store_result_to_ES

# 爬取人物昵称及类型任务
def crawler_name_type_task_start():
    store_name_and_type_to_ES()


# 事件感知任务
def sensing_task():
    create_task_list()
    social_sensing_task()


# 事件爬取数据任务
def push_crawler_task_redis():
    query_body = {
        "query":{
            "match_all": {}
        }
    }

    search_results = es.search(index=index_monitor_task, doc_type=type_monitor_task, body=query_body)['hits']['hits']

    count = 0
    if search_results:
        for iter_item in search_results:
            item = iter_item['_source']

            task = []
            task.append(item['task_name']) # task_name
            task.append(item['keywords'])  # keywords
            
            r.lpush(item['task_name'], json.dumps(task))
            count += 1

    print 'task_count_sum:' , count


def pop_crawler_task_redis():
    count = 0
    while 1:
        temp = r.rpop("task_name")
        if temp:
            print "current_task:", json.loads(temp)[0]

        if not temp:
            print 'the last task NO:', count
            now_date = ts2date(time.time())
            print 'All tasks Finished:',now_date
            break  
            
        task_detail = json.loads(temp)
        count += 1
        crawler_task_start(task_detail)
        print json.loads(temp)[0],':Finished'


def crawler_task_start(task_detail):
    task_name = task_detail[0]
    keywords = task_detail[1]
    end_time = ts2date(time.time())
    start_time = ts2date(time.time()-3600*24)

    type_weibo_name = task_name
    index_baidunews_name = task_name
    type_baidunews_name = task_name
    get_data2es(keywords, start_time, end_time, index_weixin_name, type_weixin_name, index_weibo_name, type_weibo_name, index_baidunews_name, type_baidunews_name)
    print 'get_data successful'


# 事件定时计算任务
def push_calculate_task_redis():
    query_body = {
        "query":{
            "match_all": {}
        }
    }

    search_results = es.search(index=index_monitor_task, doc_type=type_monitor_task, body=query_body)['hits']['hits']

    count = 0
    if search_results:
        for iter_item in search_results:
            item = iter_item['_source']

            task = []
            task.append(item['task_name']) 
            task.append(item['event_category']) 
            task.append(item['create_at']) 
            task.append(item['processing_status']) 

            
            r.lpush(item['task_name'], json.dumps(task))
            count += 1

    print 'task_count_sum:' , count


def pop_calculate_task_redis():
    count = 0
    while 1:
        temp = r.rpop("task_name")
        if temp:
            print "current_task:", json.loads(temp)[0]

        if not temp:
            print 'the last task NO:', count
            now_date = ts2date(time.time())
            print 'All tasks Finished:',now_date
            break  
            
        task_detail = json.loads(temp)
        count += 1
        calculate_task_start(task_detail)
        print json.loads(temp)[0],':Finished'


def calculate_task_start(task_detail):
    event_name = task_detail[0]
    event_category = task_detail[1]
    create_at = task_detail[2]
    processing_status = task_detail[3]

    item = dict()
    item['event_name'] = event_name
    item['event_category'] = event_category
    item['create_at'] = create_at
    item['processing_status'] = processing_status


    geo_analysis_run(index_weibo_name, event_name, es_data_text)
    network_analysis(index_weibo_name, event_name, es_data_text)
    store_result_to_ES(index_weibo_name, event_name, es_data_text)

    id = event_name + '_' + ts2datetime_full(item['create_at'])
    es_incident.index(index=index_incident_task, doc_type=type_incident_task, id=id, body=item)

    
    print 'event calculate_task successful'

if __name__ == "__main__":
    store_name_and_type_to_ES()
    push_crawler_task_redis()
    pop_crawler_task_redis()
    sensing_task()
