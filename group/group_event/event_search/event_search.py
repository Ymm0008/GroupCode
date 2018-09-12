#-*- coding:utf-8 -*-


import sys
import json
import time
from flask import Blueprint, render_template, Flask,request

from elasticsearch import Elasticsearch
es = Elasticsearch("219.224.134.226:9209",timeout=600)

reload(sys)
sys.setdefaultencoding("utf-8")
  
# mod = Flask(__name__)

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('event_search', __name__, url_prefix='/eventSearch')   # url_prefix = '/test'  增加相对路径的前缀
# Standardize the output format according to the requirements of the web frontend
# 对输出格式进行标准化输出
def standard_output(es_result):
    result = list()

    for t, _ in enumerate(es_result):
        dic = dict()
        timeArray = time.localtime(float(_["_source"]["timestamp"]))
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        _["_source"]["timestamp"] = otherStyleTime
        dic["id"] = _["_id"]
        dic = dict(dic , **_["_source"]) #字典的拼接
        result.append(dic)

    return result

# Query events based on keyword for field("event_title")
def data_read(keyword,page_number, page_size):
    start_from = (page_number - 1) * page_size
    query_body = {
        "query": {
            "bool": {
                "should":[{
                    "term":{
                        "event_title": keyword
                    }
                },
                {
                    "term":{
                        "keywords_string": keyword
                    }

                }]
            }
        },
        "from": start_from,
        "size": page_size
    }
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"]
    result = standard_output(es_result)

    return result


def data_reads(keyword,page_number, page_size):
    start_from = (page_number - 1) * page_size
    query_body = {
        "query": {
            "bool": {
                "must":{
                    "term": {
                        "event_title": keyword
                    }
                }
            }
        },
        "from": start_from,
        "size": page_size
    }
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"]


    result = standard_output(es_result)

    return result

# Query all event information
def data_eventlist(page_number, page_size):
    start_from = (int(page_number) - 1) * int(page_size)
    query_body = {
        "query": {
            "bool": {
                "must": {
                    "match_all": {}  #match_all默认返回10个
                }
            }
        },
        "from": start_from,
        "size": page_size
    }
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    result = standard_output(es_result)

    return result

def data_eventlist_sort(sort_field,sort_order):
    query_body = {
        "query": {
            "bool": {
                "must": {
                    "match_all": {}  #match_all默认返回10个
                }
            }
        },
        "size" : 50,
         "sort" :{
            sort_field : {"order": sort_order}
        }
    }
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    result = standard_output(es_result)

    return result

# Query events based on field("tags_string")
def data_event_category(key_event,page_number, page_size):
    start_from = (page_number - 1) * page_size
    query_body = {
        "query": {
            "bool": {
                "must":{
                    "term": {
                        "tags_string": key_event
                    }
                }
            }
        },
        "from": start_from,
        "size": page_size
    }
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    result = standard_output(es_result)

    return result

# Query all event tags and tag weight  
def data_browse_by_category():
    query_body = {
        "query": {
            "bool": {
                "must": {
                    "match_all": {}  #match_all默认返回10个
                }
            }
        },
        "size" : 50
    }
    result = es.search(index="group_incidents", doc_type="text", body=query_body,_source="tags_string")["hits"]["hits"]
    tags = list()
    last_tags = []
    for t in range(len(result)):
        tags.append(result[t]["_source"]["tags_string"])#获取每一件事件的标签
        last_tags = last_tags + tags[t].encode("utf-8").split("&")# 将有多个标签的拆分
    last_tags = list(set(last_tags))   #给标签去重

    final_tags = list()
    for t, _ in enumerate(last_tags):
        dic = dict()
        # number = len(data_event_browse(_))
        dic[_] = len(data_event_category(_))
        final_tags.append(dic)

    return final_tags


def data_advanced_query_in_eventlist(keyword,from_time,to_time,key_geo,page_number, page_size):
    if keyword != "":
        result1 = data_read(keyword,page_number, page_size) #返回结果为列表
    else:
        result1 = data_eventlist(page_number, page_size)
    result = list()
    for t, _ in enumerate(result1):
        timearray = time.strptime(_["timestamp"], "%Y-%m-%d %H:%M:%S")
        timestamp = float(time.mktime(timearray))
        if float(from_time) <= timestamp and timestamp <= float(to_time) and  _["geo"].encode("utf-8") == key_geo:
            result.append(_)

    return result

def data_advanced_query_in_category(tags_string,from_time,to_time,key_geo,page_number, page_size):
    if tags_string != "":
        result1 = data_event_category(tags_string,page_number, page_size) #返回结果为列表
    else:
        result1 = data_eventlist(page_number, page_size)
    result = list()
    for t, _ in enumerate(result1):
        timearray = time.strptime(_["timestamp"], "%Y-%m-%d %H:%M:%S")
        timestamp = float(time.mktime(timearray))
        if float(from_time) <= timestamp and timestamp <= float(to_time) and  _["geo"].encode("utf-8") == key_geo:
            result.append(_)

    return result


# Select events to add "my attention"
def data_my_focus(focus_id,focus_type):

    es.update(index="group_incidents",doc_type="text",id = focus_id,body = {"doc":{"focus_type":focus_type}})
    dic1 =  es.get(index="group_incidents",doc_type="text",id =focus_id)
    dic2 = {}
    dic2[u"_id"] = dic1["_id"]
    result = dict(dic1["_source"], **dic2)

    return result

def data_my_collect(collect_id,collect_type):

    es.update(index="group_incidents",doc_type="text",id = collect_id,body = {"doc":{"collect_type":collect_type}})
    dic1 =  es.get(index="group_incidents",doc_type="text",id =collect_id)
    dic2 = {}
    dic2[u"_id"] = dic1["_id"]
    result = dict(dic1["_source"], **dic2)

    return result

# Initial interface
@mod.route('/')
def index():

    return render_template('index.html')

# interface for query events based on keyword for field("event_title")
# 单事件检索
@mod.route('/search', methods=['GET','POST']) 
def search_event():
    keyword = request.args.get('keyword')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    result = data_read(keyword,page_number, page_size)

    return json.dumps(result)

# interface for query all event information
# 返回全部事件列表
@mod.route('/eventlist', methods=['GET','POST']) 
def get_eventlist():
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    result = data_eventlist(page_number, page_size)

    return json.dumps(result)

# 对全部列表进行字段排序
@mod.route('/eventlist_sort', methods=['GET','POST']) 
def get_eventlist_sort():
    sort_field = request.args.get('sort_field')
    sort_order = request.args.get('sort_order')
    result = data_eventlist_sort(sort_field,sort_order)

    return json.dumps(result)

# interface for query all event tags and tag weight
# 返回事件标签及各标签权重
@mod.route('/browse_by_category', methods=['GET','POST']) 
def browse_by_category():
    result = data_browse_by_category()

    return json.dumps(result)

# interface for query events based on field("tags_string")
# 按照事件标签对是事件进行查询
@mod.route('/event_category', methods=['GET','POST']) 
def event_category():
    key_event = request.args.get('key_event')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    result = data_event_category(key_event,page_number, page_size)

    return json.dumps(result)

# interface for query event based on time
# 在事件列表下根据时间地域进行高级搜索（没有关键词的话 要返回空字符串）
@mod.route('/advanced_query_in_eventlist', methods=['GET','POST']) 
def advanced_query_in_eventlist():
    keyword = request.args.get('keyword')
    from_time = request.args.get('from_time')
    to_time = request.args.get('to_time')
    key_geo = request.args.get('key_geo')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    result = data_advanced_query_in_eventlist(keyword,from_time,to_time,key_geo,page_number,page_size)

    return json.dumps(result)
    
 # 在分类浏览下根据时间地域进行高级搜索（没有事件标签的话 要返回空字符串）
@mod.route('/advanced_query_in_category', methods=['GET','POST']) 
def advanced_query_in_category():
    tags_string = request.args.get('tags_string')
    from_time = request.args.get('from_time')
    to_time = request.args.get('to_time')
    key_geo = request.args.get('key_geo')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    result = data_advanced_query_in_category(tags_string,from_time,to_time,key_geo,page_number,page_size)

    return json.dumps(result)

# interface for Select events to add "my attention"
# 我的关注
@mod.route('/my_focus', methods=['GET','POST']) 
def my_focus():
    focus_id = request.args.get('focus_id')
    focus_type = request.args.get('focus_type')
    result = data_my_focus(focus_id,focus_type)

    return json.dumps(result)

# 我的收藏 
@mod.route('/my_collect', methods=['GET','POST']) 
def my_collect():
    collect_id = request.args.get('collect_id')
    collect_type = request.args.get('collect_type')
    result = data_my_collect(collect_id,collect_type)

    return json.dumps(result)
