# -*- coding:utf-8 -*-

import os
import sys
import time
import json
from function_sensing import social_sensing
reload(sys)
sys.path.append("../")

from global_utils import es_sensor as es
from global_utils import R_SOCIAL_SENSING as r
from time_utils import ts2date

def social_sensing_task():

    count = 0
    now_ts = ts2date(time.time())

    while 1:
        temp = r.rpop("task_name")
        if temp:
            print "current_task:", json.loads(temp)[0]

        if not temp:
            print 'the last task:', count
            now_date = ts2date(time.time())
            print 'All tasks Finished:',now_date
            break  
            
        task_detail = json.loads(temp)
        count += 1
        social_sensing(task_detail)
        print json.loads(temp)[0],':Finished'

if __name__ == "__main__":
    social_sensing_task()
