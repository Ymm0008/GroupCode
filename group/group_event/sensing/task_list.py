# -*- coding:utf-8 -*-

# 本程序定时启动，是监控任务的入口，从manag es中读取尚未完成的任务，送入redis的任务队列中，
# 由子程序从队列中pop出任务信息进行监控计算

import sys
import time
import json
import copy
import os

from manual_create_task import create_sensor_task
from function_sensing_v import social_sensing


reload(sys)
sys.path.append("../")
from global_utils import es_sensor as es
from global_utils import R_SOCIAL_SENSING as r
from global_utils import index_manage_sensing, type_manage_sensing
from global_config import S_TYPE,S_DATE
from time_utils import ts2datetime, datetime2ts, ts2date, ts2datehour, datehour2ts



def create_task_list():
    # 1. search from manage_sensing_task
    # 2. push to redis list-----task_work
    
    create_sensor_task()

    if S_TYPE == 'test':
        now_ts = datetime2ts(S_DATE)
    else:
        now_ts = datehour2ts(ts2datehour(time.time()-3600))
        
    query_body = {
        "query":{
            "match_all": {}
        }
    }

    search_results = es.search(index=index_manage_sensing, doc_type=type_manage_sensing, body=query_body)['hits']['hits']

    count = 0
    if search_results:
        for iter_item in search_results:
            _id = iter_item['_id']
            item = iter_item['_source']

            task = []
            task.append(item['task_name']) # task_name
            try:
                task.append(json.loads(item['social_sensors'])) # social sensors
            except:
                task.append(item['social_sensors'])  # social sensors
            task.append(now_ts)
            
            r.lpush("task_name", json.dumps(task))
            count += 1

    print 'task_count_sum:' , count


def social_sensing_task():
    while 1:
        temp = r.rpop("task_name")

        if not temp:
            now_date = ts2date(time.time())
            print 'All tasks Finished:',now_date
            break  
            
        task_detail = json.loads(temp)
        social_sensing(task_detail)
        print json.loads(temp)[0],':Finished'


if __name__ == "__main__":
    create_task_list()
    social_sensing_task()
