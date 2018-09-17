#-*- coding:utf-8 -*-


import sys
import json
import time
from flask import Blueprint, render_template, Flask,request
#from group_event.global_utils import es_sensor
#from group_event.global_utils import index_content_sensing, type_content_sensing, index_monitor_task,type_monitor_task
#from group_event.time_utils import ts2datetime, datetime2ts, ts2date, ts2datehour, datehour2ts, ts2datetime_full

from elasticsearch import Elasticsearch
es = Elasticsearch("219.224.134.226:9207",timeout=600)

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

def standard_search(count,data_result):
    result = dict()
    result["total_num"] = count
    result["data_result"] = data_result

    return result

def data_event_category_count(key_event):
    query_body = {
        "query": {
            "bool": {
                "must":{
                    "wildcard": {
                        "tags_string": "*" + key_event + "*"
                    }
                }
            }
        }
    }

    result = es.count(index="group_incidents", doc_type="text", body=query_body)["count"]

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
        "size" : 10000
    }
    result = es.search(index="group_incidents", doc_type="text", body=query_body,_source="tags_string")["hits"]["hits"]
    tags = list()
    last_tags = []
    for t in range(len(result)):
        tags.append(result[t]["_source"]["tags_string"])#获取每一件事件的标签
        last_tags = last_tags + tags[t].encode("utf-8").split("&")# 将有多个标签的拆分
    last_tags = list(set(last_tags))   #给标签去重

    final_tags = dict()
    for t, _ in enumerate(last_tags):
        final_tags[_]=data_event_category_count(_)
        # dic = dict()
        # number = len(data_event_browse(_))
        # dic[_] = data_event_category_count(_)
        # final_tags.append(dic)

    return final_tags

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

def event_search_keyword(keyword,from_time,to_time,key_geo,page_size,page_number,sort_field,sort_order):
    
    if keyword:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must":[],
                            "should":[{
                                "wildcard":{
                                    "event_title": "*" +keyword + "*"
                                }
                            },
                            {
                                "wildcard":{
                                    "keywords_string": "*" +keyword + "*"
                                }

                            }]
                        }
                    }
                }
            }
        }
    else:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must": []
                        }
                    }
                }
            }
        }

    if from_time :
        es_time = {
                "range": {
                    "timestamp": {
                        "gte": from_time,
                        "lte": to_time
                    }
                }
            }
        query_body["query"]["filtered"]["filter"]["bool"]["must"].append(es_time)
    if key_geo:
        es_geo = {
            "wildcard": {
                "geo": "*" + key_geo + "*"
            }
        }
        query_body["query"]["filtered"]["filter"]["bool"]["must"].append(es_geo)
    if page_number:
        start_from = (int(page_number) - 1) * int(page_size)
        page_print ={
                 "from": start_from,
                 "size": page_size
        }
        query_body = dict(query_body,**page_print)
    if sort_field:
        sort = {
            "sort" :{
                sort_field : {"order": sort_order}
            }
        }
        query_body = dict(query_body,**sort)
    count_result = es.count(index="group_incidents", doc_type="text", body=query_body)["count"]
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    data_result = standard_output(es_result)
    result = standard_search(count_result,data_result)
    final_result["result"] = result

    return final_result

def event_search_category(tags_string,from_time,to_time,key_geo,page_size,page_number,sort_field,sort_order): 
    
    if tags_string:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must":[{
                                "wildcard" :{
                                    "tags_string" : "*" + tags_string + "*"
                                }
                            }]
                        }
                    }
                }
            }
        }
    else:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must": []
                        }
                    }
                }
            }
        }
    if from_time:
        es_time = {
                "range": {
                    "timestamp": {
                        "gte": from_time,
                        "lte": to_time
                    }
                }
            }
        query_body["query"]["filtered"]["filter"]["bool"]["must"].append(es_time)
    if key_geo:
        es_geo = {
            "wildcard": {
                "geo": "*" + key_geo + "*"
            }
        }
        query_body["query"]["filtered"]["filter"]["bool"]["must"].append(es_geo)
    if page_number:
        start_from = (int(page_number) - 1) * int(page_size)
        page_print ={
                 "from": start_from,
                 "size": page_size
        }
        query_body = dict(query_body,**page_print)
    if sort_field:
        sort = {
            "sort" :{
                sort_field : {"order": sort_order}
            }
        }
        query_body = dict(query_body,**sort)
    count_result = es.count(index="group_incidents", doc_type="text", body=query_body)["count"]
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    data_result = standard_output(es_result)
    result = standard_search(count_result,data_result)

    return result


@mod.route('/')
def index():

    return render_template('index.html')

@mod.route('/event_search', methods=['POST']) 
def event_search():

    term = request.get_json()

    if term.has_key('keyword'): #关键词
        keyword = term['keyword']
    else:
        keyword = ""
    if term.has_key('tags_string'): #事件标签
        tags_string = term['tags_string']
    else:
        tags_string = ""
    if term.has_key('from_time'): #开始时间 时间戳形式
        from_time = term['from_time']  #int类型
    else:
        from_time = "" #不查则为空
    if term.has_key("to_tome"): #结束时间 时间戳形式
        to_time = term['to_time'] #int 类型
    else:
        to_time = time.time() #时间戳形式
    if term.has_key('page_size'):
        page_size = term['page_size']
    else:
        page_size = 1
    if term.has_key("page_number"):
        page_number = term["page_number"]
    else:
        page_number = 5
    if term.has_key("sort_field"):
        sort_field = term["sort_field"]
    else:
        sort_field = "safety_index"
    if term.has_key("sort_order"):
        sort_order = term["sort_order"]
    else:
        sort_order = "asc"

    if term["type"] == 1: #事件列表
        result = event_search_keyword(keyword,from_time,to_time,key_geo,page_size,page_number,sort_field,sort_order)
    elif term["type"] == 2: #分类浏览
        echarts_result = data_browse_by_category()
        data_result = event_search_category(tags_string,from_time,to_time,key_geo,page_size,page_number,sort_field,sort_order)
        result["result"] = data_result
        result["echarts"] = "echarts_result"

    return json.dumps(result)

# interface for Select events to add "my attention"
# 我的关注 my_focus?focus_id=AWV1Ta3H8PIDIgu4NCV9&focus_type=1
@mod.route('/my_focus', methods=['GET','POST']) 
def my_focus():
    focus_id = request.args.get('focus_id')
    focus_type = request.args.get('focus_type')
    result = data_my_focus(focus_id,focus_type)

    return json.dumps("1")

# 我的收藏 my_collect?collect_id=AWV1Ta3H8PIDIgu4NCV9&collect_type=1
@mod.route('/my_collect', methods=['GET','POST']) 
def my_collect():
    collect_id = request.args.get('collect_id')
    collect_type = request.args.get('collect_type')
    result = data_my_collect(collect_id,collect_type)

    return json.dumps("1")

'''
# 新建任务 加入事件定制  
@mod.route('/event_insert', methods=['POST']) 
def event_insert():

    term = request.get_json()

    item = dict()
    item['task_name'] = term['name'] #事件名称
    item['event_category'] = term['category'] #事件标签
    item['keywords'] = term['keywords'] #事件关键词
    item['from_date'] = term['from_date']# 开始时间
    item['to_date'] = term['to_date'] #结束时间

    item['delete'] = 'no'
    item['create_at'] = time.time()
    item['create_user'] = 'root'
    item['processing_status'] = 'no'
    item['event_detail'] = 'yes'

    id = item['task_name'] + '_' + ts2datetime_full(item['create_at'])

    es_sensor.index(index=index_monitor_task, doc_type=type_monitor_task, id=id, body=item)

    return json.dumps('1')
'''
'''
# 事件定制部分的查询
@mod.route('/event_query', methods=['POST']) 
def event_query():

    term = request.get_json()

    if term.has_key('keywords'): #关键词
        keywords = term['keywords']
    else:
        keywords = []
    if term.has_key('user'): #提交用户
        user = term['user']
    else:
        user = 'root'
    if term.has_key('from_ts'): #开始时间
        from_ts = datehour2ts(term['from_ts'])
    else:
        from_ts = time.time()
    if term.has_key('to_ts'): #结束时间 
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

'''
# 定制时间删除
@mod.route('/event_delete', methods=['POST']) 
def event_delete():

    content = request.get_json()
    id = content['id']

    es_sensor.delete(index=index_monitor_task, doc_type=type_monitor_task, id=id)

    return json.dumps('1')