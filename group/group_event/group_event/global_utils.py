# -*- coding: utf-8 -*-
'''
use to save table info in database
'''
import redis
from elasticsearch import Elasticsearch

from global_config import ES_SENSOR_HOST ,ES_FLOW_TEXT_HOST
from global_config import REDIS_HOST, REDIS_PORT


es_sensor = Elasticsearch(ES_SENSOR_HOST, timeout=600)
es_flow_text = Elasticsearch(ES_FLOW_TEXT_HOST, timeout=600)


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port)


# social sensing redis
R_SOCIAL_SENSING = _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=3)


# social sensing
index_manage_sensing = "manage_sensing_task"
type_manage_sensing = "task"

index_content_sensing = "event_sensing_text"
type_content_sensing = "text"

id_sensing_task = "event_sensing_task"

type_flow_text_index = "text"

index_monitor_task = "monitor_task"
type_monitor_task = "task"