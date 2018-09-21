#-*- coding:utf-8 -*-

import os
import sys
import json
import time
from collections import Counter
from flask import Blueprint, render_template, request

from elasticsearch import Elasticsearch
from group_event.global_utils import es_sensor
from group_event.global_utils import index_content_sensing, type_content_sensing, index_monitor_task,type_monitor_task
from group_event.time_utils import ts2datetime, datetime2ts, ts2date, ts2datehour, datehour2ts, ts2datetime_full

reload(sys)
sys.setdefaultencoding("utf-8")


AB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../public/')
sys.path.append(AB_PATH)

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('sensing', __name__, url_prefix='/event')   # url_prefix = '/test'  增加相对路径的前缀
 
@mod.route('/sensing')
def index():
    """返回页面
    """
    return render_template('index_query.html')


# 按照地点、关键词查询，按照热度、时间排序
def query_sensing(keywords, place, sort, from_ts, to_ts):
	keyword_query_list = []
	if keywords:
		for keyword in keywords.split(','):
			keyword_query_list.append({'wildcard':{'text':'*'+keyword+'*'}})

	query_body = {
	    "query":{
	        "bool":{
	        	"must": [
	        		{"wildcard":{ "geo": '*'+place+'*'}},
	        		{'range':{'timestamp':{'gte':from_ts, 'lte':to_ts}}}
	        	],
	        	"should":keyword_query_list,
	            "minimum_should_match": 1
	        }
	    },
	    "size": 20000,
	    "sort":  {sort:{"order": "desc"}},
	}

	results = es_sensor.search(index=index_content_sensing, doc_type=type_content_sensing, body=query_body)["hits"]["hits"]   

	count_mid = len(results)

	country_list = []
	province_list = []
	city_list = []
	country_dict = dict()
	province_dict = dict()
	city_dict = dict()
	for item in results:
		term = item['_source']['geo'].split('&')
		try:
			if term[0]:
				country = term[0]
			else:
				country = ''
		except:
			country = ''
		try:
			if term[1]:
				province = term[1]
			else:
				province = ''
		except:
			province = ''
		try:
			if term[2]:
				city = term[2]
			else:
				city = ''
		except:
			city = ''

		country_list.append(country)
		province_list.append(province)
		city_list.append(city)

	country_dict = Counter(country_list)
	province_dict = Counter(province_list)
	city_dict = Counter(city_list)


	return results, count_mid, province_dict, city_dict

# 按照地点、关键词查询，按照热度、时间排序
def query_monitor(keywords, user, from_ts, to_ts):
	keyword_query_list = []
	if keywords:
		for keyword in keywords.split(','):
			keyword_query_list.append({'wildcard':{'task_name':'*'+keyword+'*'}})

	query_body = {
	    "query":{
	        "bool":{
	        	"must": [
	        		{"term":{ "create_user": user}},
	        		{'range':{'create_at':{'gte':from_ts, 'lte':to_ts}}}
	        	],
	        	"should":keyword_query_list,
	            "minimum_should_match": 1
	        }
	    },
	    "size": 20000
	}

	results = es_sensor.search(index=index_monitor_task, doc_type=type_monitor_task, body=query_body)["hits"]["hits"]   


	return results

# 查询感知
# http://127.0.0.1:9001/event/sensing
@mod.route('/query', methods=['POST']) 
def sensing_calculate():

    term = request.get_json()

    if term.has_key('keywords'):
    	keywords = term['keywords']
    else:
    	keywords = []
    if term.has_key('sort'):
    	sort = term['sort']
    else:
    	sort = 'heat'
    if term.has_key('place'):
    	place = term['place']
    else:
    	place = '*'
    if term.has_key('to_ts'):
    	to_ts = datehour2ts(term['to_ts'])
    else:
    	to_ts = time.time()
    if term.has_key('from_ts'):
    	from_ts = datehour2ts(term['from_ts'])
    else:
	    if term.has_key('to_num'):
	    	to_num = term['to_num']
	    else:
	    	to_num = 180
	    from_ts = to_ts-to_num*3600*24
    if term.has_key('page'):
		page = term['page']
    else:
    	page = 1

    results, count_mid, province_dict, city_dict = query_sensing(keywords=keywords, place=place, sort=sort, from_ts=from_ts, to_ts=to_ts)


    content = []
    for item in results:
    	item_dict = {}
    	item_dict['name'] = item["_source"]['uid']
        item_dict['date'] = ts2datetime_full(item["_source"]['timestamp'])
        item_dict['initiator'] = item["_source"]['initiator']
        item_dict['heat'] = item["_source"]['heat']
        item_dict['geo'] = item["_source"]['geo']
        item_dict['keywords_string'] = item["_source"]['keywords_string']
        item_dict['status'] = item["_source"]['status']
        content.append(item_dict)


    return_results = dict()
    return_results['event_count'] = count_mid
    return_results['place_count'] = len(province_dict)
    return_results['color'] = province_dict
    return_results['list'] = content[5*(page-1):5*page]

    print count_mid, province_dict, city_dict

    return json.dumps(return_results)



# 地图交互
# http://127.0.0.1:9001/event/sensing
@mod.route('/map', methods=['POST']) 
def map_calculate():

    term = request.get_json()

    if term.has_key('keywords'):
    	keywords = term['keywords']
    else:
    	keywords = []
    if term.has_key('sort'):
    	sort = term['sort']
    else:
    	sort = 'heat'
    if term.has_key('place'):
    	place = term['place']
    else:
    	place = '*'
    if term.has_key('from_ts'):
    	from_ts = datehour2ts(term['from_ts'])
    else:
    	from_ts = time.time()
    if term.has_key('to_ts'):
    	to_ts = datehour2ts(term['to_ts'])
    else:
	    if term.has_key('to_num'):
	    	to_num = term['to_num']
	    else:
	    	to_num = 180
	    to_ts = from_ts-to_num*3600*24
    if term.has_key('page'):
		page = term['page']
    else:
    	page = 1

    results, count_mid, province_dict, city_dict = query_sensing(keywords=keywords, place=place, sort=sort, from_ts=from_ts, to_ts=to_ts)


    content = []
    for item in results:
    	item_dict = {}
    	item_dict['name'] = item["_source"]['uid']
        item_dict['date'] = ts2datetime_full(item["_source"]['timestamp'])
        item_dict['initiator'] = item["_source"]['initiator']
        item_dict['heat'] = item["_source"]['heat']
        item_dict['geo'] = item["_source"]['geo']
        item_dict['keywords_string'] = item["_source"]['keywords_string']
        item_dict['status'] = item["_source"]['status']
        content.append(item_dict)


    return_results = dict()
    return_results['event_count'] = count_mid
    return_results['place_count'] = len(province_dict)
    return_results['color'] = city_dict
    return_results['list'] = content[5*(page-1):5*page]

    print count_mid, province_dict, city_dict

    return json.dumps(return_results)



# 添加监测任务表
# http://127.0.0.1:9001/event/sensing
@mod.route('/monitor_insert', methods=['POST']) 
def monitor_insert():

    term = request.get_json()

    item = dict()
    item['task_name'] = term['name']
    item['event_category'] = term['category']
    item['keywords'] = term['keywords']
    item['from_date'] = term['from_date']
    item['to_date'] = term['to_date']

    item['delete'] = 'no'
    item['create_at'] = time.time()
    item['create_user'] = 'root'
    item['processing_status'] = 'no'
    item['event_detail'] = 'yes'

    id = item['task_name'] + '_' + ts2datetime_full(item['create_at'])

    es_sensor.index(index=index_monitor_task, doc_type=type_monitor_task, id=id, body=item)

    return json.dumps('1')


# 查询监测任务
# http://127.0.0.1:9001/event/sensing
@mod.route('/monitor_query', methods=['POST']) 
def monitor_query():

    term = request.get_json()

    if term.has_key('keywords'):
    	keywords = term['keywords']
    else:
    	keywords = []
    if term.has_key('user'):
    	user = term['user']
    else:
    	user = 'root'
    if term.has_key('from_ts'):
    	from_ts = datehour2ts(term['from_ts'])
    else:
    	from_ts = time.time()
    if term.has_key('to_ts'):
    	to_ts = datehour2ts(term['to_ts'])
    else:
	    if term.has_key('to_num'):
	    	to_num = term['to_num']
	    else:
	    	to_num = 6
	    to_ts = from_ts-to_num*3600
    if term.has_key('page'):
		page = term['page']
    else:
    	page = 1

    results = query_monitor(keywords=keywords, user=user, from_ts=from_ts, to_ts=to_ts)

    content = []
    for item in results:
    	item_dict = {}
    	item_dict['task_name'] = item["_source"]['task_name']
        item_dict['start_time'] = ts2datetime_full(float(item["_source"]['from_date']))
        item_dict['end_time'] = ts2datetime_full(float(item["_source"]['to_date']))
        item_dict['create_user'] = item["_source"]['create_user']
        item_dict['event_detail'] = item["_source"]['event_detail']
        item_dict['create_at'] = ts2datetime_full(float(item["_source"]['create_at']))
        item_dict['delete'] = item["_source"]['delete']
        item_dict['processing_status'] = item["_source"]['processing_status']
        content.append(item_dict)

    return_results = dict()
    return_results['page_count'] = len(results)
    return_results['list'] = content[5*(page-1):5*page]
    return json.dumps(return_results)



# 删除监测任务表
# http://127.0.0.1:9001/event/sensing
@mod.route('/monitor_delete', methods=['POST']) 
def monitor_delete():

    content = request.get_json()
    id = content['id']

    es_sensor.delete(index=index_monitor_task, doc_type=type_monitor_task, id=id)

    return json.dumps('1')

# 
