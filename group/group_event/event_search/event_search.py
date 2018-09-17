#-*- coding:utf-8 -*-


import sys
import json
import time
from flask import Blueprint, render_template, Flask,request
from group_event.global_utils import es_sensor
from group_event.global_utils import index_content_sensing, type_content_sensing, index_monitor_task,type_monitor_task
from group_event.time_utils import ts2datetime, datetime2ts, ts2date, ts2datehour, datehour2ts, ts2datetime_full

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

# 排序
def if_sort(query_body,sort_field,sort_order):
    sort = {
            "sort" :{
                sort_field : {"order": sort_order}
            }
    }

    result = dict(query_body,**sort)
    return result
    
def page_print(query_body,start_from, page_size):
    page_print ={
                 "from": start_from,
                 "size": page_size
    }
    result = dict(query_body,**page_print)
    return result

# 高级搜索
def query_condition(result1,from_time,to_time,key_geo):
    result = list()
    for t, _ in enumerate(result1):
        timearray = time.strptime(_["timestamp"], "%Y-%m-%d %H:%M:%S")
        timestamp = float(time.mktime(timearray))
        if from_time != "" and key_geo != "":
            if float(from_time) <= timestamp and timestamp <= float(to_time) and  _["geo"].encode("utf-8") == key_geo:
                result.append(_)
        elif from_time =='' and key_geo != '':
            if _["geo"].encode("utf-8") == key_geo:
                result.append(_)
        elif from_time != '' and key_geo == '':
            if float(from_time) <= timestamp and timestamp <= float(to_time):
                result.append(_)
    return result

# Query events based on keyword for field("event_title") 关键词搜索及对其结果的分页与排序
# search?keyword=留守儿童&page_number=1&page_size=2&sort_field=&sort_order=     基本查询
# search?keyword=留守儿童&page_number=1&page_size=2&sort_field=heat_index&sort_order=asc     对结果排序
def data_read(keyword,page_number, page_size,sort_field,sort_order):
    
    query_body = {
        "query": {
            "bool": {
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
        },
        "sort":{"safety_index":{"order":"asc"}} #默认以安全指数升序排序
    }
    count_result = es.count(index="group_incidents", doc_type="text", body=query_body)["count"]
    if page_number != "all_event":
        start_from = (int(page_number) - 1) * int(page_size)
        query_body = page_print(query_body,start_from,page_size)
    else:
        size = {"size":10000}
        query_body = dict(query_body,**size)
    if sort_field  != "":
        query_body = if_sort(query_body,sort_field,sort_order)

    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"]
    data_result = standard_output(es_result)
    result = standard_search(count_result,data_result)

    return result




# Query all event information 返回全部事件列表 可作为默认显示的事件列表
# eventlist?page_number=1&page_size=5&sort_field=&sort_order=            基本查询
# eventlist?page_number=1&page_size=5&sort_field=heat_index&sort_order=asc     对结果排序
def data_eventlist(page_number, page_size,sort_field,sort_order):
    
    query_body = {
            "query": {
                "bool": {
                    "must": {
                        "match_all": {}  #match_all默认返回10个
                    }
                }
            },
            "sort":{"safety_index":{"order":"asc"}} #默认以安全指数升序排序
        }
    count_result = es.count(index="group_incidents", doc_type="text", body=query_body)["count"]
    if page_number != "all_event":
        start_from = (int(page_number) - 1) * int(page_size)
        query_body = page_print(query_body,start_from,page_size)
    else:
        size = {"size":10000}
        query_body = dict(query_body,**size)
    if sort_field  != "": #给排序就按获取到的进行排序
        query_body = if_sort(query_body,sort_field,sort_order)
    
    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    data_result = standard_output(es_result)
    result = standard_search(count_result,data_result)

    return result

# Query events based on field("tags_string")  根据事件标签进行查询 echarts部分
# 基本查询 event_category?key_event=社会民生&page_number=1&page_size=2&sort_field=&sort_order=
# 对结果进行排序 event_category?key_event=社会民生&page_number=1&page_size=2&sort_field=heat_index&sort_order=asc
def data_event_category(key_event,page_number, page_size,sort_field,sort_order):
    
    query_body = {
        "query": {
            "bool": {
                "must":{
                    "wildcard": {
                        "tags_string": "*" + key_event + "*"
                    }
                }
            }
        },
        "sort":{"safety_index":{"order":"asc"}} #默认以安全指数升序排序
    }
    count_result = es.count(index="group_incidents", doc_type="text", body=query_body)["count"]
    if page_number != "all_event":
        start_from = (int(page_number) - 1) * int(page_size)
        query_body = page_print(query_body,start_from,page_size)
    else:
        size = {"size":10000}
        query_body = dict(query_body,**size)
    if sort_field  != "":
        query_body = if_sort(query_body,sort_field,sort_order)

    es_result = es.search(index="group_incidents", doc_type="text", body=query_body)["hits"]["hits"] 
    data_result = standard_output(es_result)
    result = standard_search(count_result,data_result)

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

'''
def data_advanced_query_in_eventlist(keyword,from_time,to_time,key_geo,page_number, page_size,sort_field,sort_order):
    if keyword != "":
        result1 = data_read(keyword,page_number, page_size,sort_field,sort_order) #返回结果为列表
    else:
        result1 = data_eventlist(page_number, page_size,sort_field,sort_order)

    result = query_condition(result1,from_time,to_time,key_geo)

    return result
    '''

def data_advanced_query_in_eventlist(keyword,from_time,to_time,key_geo,page_number, page_size,sort_field,sort_order):
    if keyword != "":
        result1 = data_read(keyword,"all_event", page_size,sort_field,sort_order) #返回结果为列表
        result2 = result1["data_result"]
    else:
        result1 = data_eventlist("all_event", page_size,sort_field,sort_order)
        result2 = result1["data_result"]

    result3 = query_condition(result2,from_time,to_time,key_geo) # 符合条件的数据
    count_result = len(result3)
    start_from = (int(page_number) - 1) * int(page_size)
    stop_at = start_from + int(page_size)
    data_result = result3[start_from:stop_at]
    result = standard_search(count_result,data_result)

    return result



def data_advanced_query_in_category(tags_string,from_time,to_time,key_geo,page_number, page_size,sort_field,sort_order):
    if tags_string != "":
        result1 = data_event_category(tags_string,"all_event", page_size,sort_field,sort_order)
        result2 = result1["data_result"] #返回结果为列表
    else:
        result1 = data_eventlist("all_event", page_size,sort_field,sort_order)
        result2 = result1["data_result"]
        

    result3 = query_condition(result2,from_time,to_time,key_geo) # 符合条件的数据
    count_result = len(result3) 
    start_from = (int(page_number) - 1) * int(page_size)
    stop_at = start_from + int(page_size)
    data_result = result3[start_from:stop_at]
    result = standard_search(count_result,data_result)

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
# 关键词搜索及对其结果的分页与排序
# search?keyword=留守儿童&page_number=1&page_size=2&sort_field=&sort_order=     基本查询
# search?keyword=留守儿童&page_number=1&page_size=2&sort_field=heat_index&sort_order=asc     对结果排序
@mod.route('/search', methods=['GET','POST']) 
def search_event():
    keyword = request.args.get('keyword')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    sort_field = request.args.get('sort_field')
    sort_order = request.args.get('sort_order')
    result = data_read(keyword,page_number, page_size,sort_field,sort_order)

    return json.dumps(result)

# interface for query all event information
# 返回全部事件列 可作为默认显示的事件列表
# eventlist?page_number=1&page_size=5&sort_field=&sort_order=            基本查询
# eventlist?page_number=1&page_size=5&sort_field=heat_index&sort_order=asc     对结果排序
@mod.route('/eventlist', methods=['GET','POST']) 
def get_eventlist():
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    sort_field = request.args.get('sort_field')
    sort_order = request.args.get('sort_order')
    result = data_eventlist(page_number, page_size,sort_field,sort_order)

    return json.dumps(result)

# interface for query all event tags and tag weight
# 返回事件标签及各标签权重 获得数据做echarts
@mod.route('/browse_by_category', methods=['GET','POST']) 
def browse_by_category():
    result = data_browse_by_category()

    return json.dumps(result)

# interface for query events based on field("tags_string")
# 按照事件标签对是事件进行查询 点击echarts进行查询部分
# 基本查询 event_category?key_event=社会民生&page_number=1&page_size=2&sort_field=&sort_order=
# 对结果进行排序 event_category?key_event=社会民生&page_number=1&page_size=2&sort_field=heat_index&sort_order=asc
@mod.route('/event_category', methods=['GET','POST']) 
def event_category():
    key_event = request.args.get('key_event')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    sort_field = request.args.get('sort_field')
    sort_order = request.args.get('sort_order')
    result = data_event_category(key_event,page_number, page_size,sort_field,sort_order)

    return json.dumps(result)

# interface for query event based on time
# 在事件列表下根据时间地域进行高级搜索  有关键词 即对关键词查询结果进行高级搜索 没有关键词 为对默认事件列表进行高级搜索
# 高级搜索是进行分页 排序
# 事件列表下 有关键词 advanced_query_in_eventlist?keyword=留守儿童&from_time=&to_time=&key_geo=上海&page_number=1&page_size=2&sort_field=&sort_order=
# 事件列表下 没有关键词 advanced_query_in_eventlist?keyword=&from_time=&to_time=&key_geo=上海&page_number=1&page_size=2&sort_field=&sort_order=
@mod.route('/advanced_query_in_eventlist', methods=['GET','POST']) 
def advanced_query_in_eventlist():
    keyword = request.args.get('keyword')
    from_time = request.args.get('from_time')
    to_time = request.args.get('to_time')
    key_geo = request.args.get('key_geo')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    sort_field = request.args.get('sort_field')
    sort_order = request.args.get('sort_order')
    result = data_advanced_query_in_eventlist(keyword,from_time,to_time,key_geo,page_number,page_size,sort_field,sort_order)

    return json.dumps(result)
    
 # 在分类浏览下根据时间地域进行高级搜索（没有事件标签的话 要返回空字符串）
 # 分类浏览下 无标签高级搜索 advanced_query_in_category?tags_string=&from_time=1526018962&to_time=1526380138&key_geo=&page_number=1&page_size=4&sort_field=&sort_order=
 # 分类浏览下 有标签高级搜索 advanced_query_in_category?tags_string=社会民生&from_time=&to_time=&key_geo=北京&page_number=1&page_size=2&sort_field=&sort_order=
 # 高级搜索是进行分页 排序
@mod.route('/advanced_query_in_category', methods=['GET','POST']) 
def advanced_query_in_category():
    tags_string = request.args.get('tags_string')
    from_time = request.args.get('from_time')
    to_time = request.args.get('to_time')
    key_geo = request.args.get('key_geo')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    sort_field = request.args.get('sort_field')
    sort_order = request.args.get('sort_order')
    result = data_advanced_query_in_category(tags_string,from_time,to_time,key_geo,page_number,page_size,sort_field,sort_order)

    return json.dumps(result)

# interface for Select events to add "my attention"
# 我的关注 my_focus?focus_id=AWV1Ta3H8PIDIgu4NCV9&focus_type=1
@mod.route('/my_focus', methods=['GET','POST']) 
def my_focus():
    focus_id = request.args.get('focus_id')
    focus_type = request.args.get('focus_type')
    result = data_my_focus(focus_id,focus_type)

    return json.dumps(result)

# 我的收藏 my_collect?collect_id=AWV1Ta3H8PIDIgu4NCV9&collect_type=1
@mod.route('/my_collect', methods=['GET','POST']) 
def my_collect():
    collect_id = request.args.get('collect_id')
    collect_type = request.args.get('collect_type')
    result = data_my_collect(collect_id,collect_type)

    return json.dumps(result)

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