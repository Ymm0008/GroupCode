# -*- coding: utf-8 -*-
'''
use to save table info in database
'''
import redis
from elasticsearch import Elasticsearch

from global_config import ES_SENSOR_HOST ,ES_FLOW_TEXT_HOST, ES_DATA_HOST, ES_FOR_UID_HOST, ES_INCIDENT_HOST, ES_MONITOR_HOST
from global_config import REDIS_HOST, REDIS_PORT


es_sensor = Elasticsearch(ES_SENSOR_HOST, timeout=600)
es_monitor = Elasticsearch(ES_MONITOR_HOST, timeout=600)
es_incident = Elasticsearch(ES_INCIDENT_HOST, timeout=600)
es_flow_text = Elasticsearch(ES_FLOW_TEXT_HOST, timeout=600)
es_data_text = Elasticsearch(ES_DATA_HOST, timeout=600)
es_for_uid = Elasticsearch(ES_FOR_UID_HOST, timeout=600)


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port)

# social sensing redis
R_SOCIAL_SENSING = _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=3)


# social sensing
index_manage_sensing = "manage_sensing_task"
type_manage_sensing = "task"

# event_sensing_text
index_content_sensing = "event_sensing_text"
type_content_sensing = "text"

# event_sensing_task
id_sensing_task = "event_sensing_task"

# flow_text
type_flow_text_index = "text"

# monitor_task
index_monitor_task = "monitor_task"
type_monitor_task = "task"

# monitor_task
index_incident_task = "incident_task"
type_incident_task = "text"

# crawler: uid -> screen_name,type
index_name_for_uid = "uid_mapping_table"

# data for analysis
index_weixin_name = "weixin_data_text"
index_weibo_name = "weibo_data_text"
index_baidunews_name = "baidunews_data_text"
