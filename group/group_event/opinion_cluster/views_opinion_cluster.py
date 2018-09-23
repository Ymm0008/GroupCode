#-*- coding:utf-8 -*-

import os
import sys
import json
import time
from flask import Blueprint, render_template, request

from elasticsearch import Elasticsearch
es = Elasticsearch("219.224.134.226:9209", timeout=600)

reload(sys)
sys.setdefaultencoding("utf-8")


# Blueprint 模块化管理程序路由的功能
mod = Blueprint('opinion_cluster', __name__, url_prefix='/opinion')   # url_prefix = '/test'  增加相对路径的前缀
# @mod.route('/')
# def index():
#     """返回页面
#     """
#     return render_template('index_cluster.html')
@mod.route('/set_page', methods=['POST']) 
def set_page(list1, page_number, page_size):              #分页函数 list1指等待被分页的列表
    start_from = (int(page_number) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)
    return list1[start_from:end_at]

@mod.route('/datetime', methods=['POST']) 
def ts2datetime_full(ts):                                 #时间戳的转换函数，可以把时间戳转换为时间
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))
#接口代码
#事件聚类展示接口
@mod.route('/event_cluster', methods=['POST']) 
def event_cluster():
    term = request.get_json()
    # event_name = request.args.get('event_name')
    if term.has_key('event_name'): 
        event_name = term['event_name']
    else:
        event_name = 'event1'
    if term.has_key('source'): 
        source = term['source']
    else:
        source = "weibo"
    keywords = dict()
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'cluster_keywords_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type=event_name, body=query_body)['hits']['hits']
    print result
    # for item in result:
    print result[0]['_source']
    for i in range(5):
        # print type(result[0]['_source']['cluster_keywords_dict'])
        # print eval(result[0]['_source']['cluster_keywords_dict'])
        item = eval(result[0]['_source']['cluster_keywords_dict'])
        keywords[i] = {'keyword':item[str(i)][0].split(','),'val':item[str(i)][1]}

    return json.dumps(keywords)


#聚类关键词和代表性文本联动接口
@mod.route('/rep_text', methods=['POST']) 
def rep_text():
    term = request.get_json()
    
    if term.has_key('index_all'): 
        index_all = term['index_all']
    else:
        index_all = 'flow_text_gangdu'
    if term.has_key('event_name'): 
        event_name = term['event_name']
    else:
        event_name = 'event1'
    if term.has_key('cluster'): 
        cluster = term['cluster']
    else:
        cluster = "0"
    if term.has_key('page_number'): 
        page_number = term['page_number']
    else:
        page_number = 2
    if term.has_key('page_size'): 
        page_size = term['page_size']
    else:
        page_size = 5
    
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'rep_text_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type=event_name, body=query_body)['hits']['hits']
    print type(result[0]['_source']['rep_text_dict'])
    items = eval(result[0]['_source']['rep_text_dict'])

    
    midlist = []
    for j in range(len(items[cluster])):
        midlist.append(items[cluster][j]['mid'])
    print midlist
    text_list = []
    for k in range(len(midlist)):
        query_body = {   
            'query':{
                'bool':{
                    'must':
                        {'term':{'mid': midlist[k]}}                            
                }
            },
            'size': 1        
        }          
        result = es.search(index = index_all, doc_type='text', body=query_body)['hits']['hits']
        result = result[0]['_source']
        print result
        rep_text = dict()
        print result['text'].encode('utf-8')
        rep_text['text'] = result['text']
        timestamp = ts2datetime_full(result['timestamp'])
        rep_text['timestamp'] = timestamp
        rep_text['uid'] = result['uid']
        query_body = {     
                'query':{
                    'filtered': {
                        'query':{                        
                            'term':{'message_type': 2}   

                        },
                        'filter':{     
                            
                            'term': {'root_mid': midlist[k]}
                        }
                    }
                }
            }
            # print 1
        rep_text['retweet'] = es.count(index = index_all, doc_type='text', body=query_body)['count']
        query_body = {      
                'query':{
                    'filtered': {
                        'query':{
                            'term':{'message_type': 3}   
                            

                        },
                        'filter':{     
                            'term': {'root_mid': midlist[k]}
                        }
                    }
                }
            }
        rep_text['comment'] = es.count(index = index_all, doc_type='text', body=query_body)['count']
        text_list.append(rep_text)
    print len(text_list)
    text_page = dict()
    text_page['total_num'] = len(text_list)
    text_page['data'] = set_page(text_list, page_number, page_size)
    print text_page
    return json.dumps(text_page)



#热帖排行展示接口
@mod.route('/hot_rank', methods=['POST']) 
def hot_rank():
    term = request.get_json()
    
    if term.has_key('index_all'): 
        index_all = term['index_all']
    else:
        index_all = 'flow_text_gangdu'
    if term.has_key('event_name'): 
        event_name = term['event_name']
    else:
        event_name = 'event1'
    if term.has_key('page_number'): 
        page_number = term['page_number']
    else:
        page_number = 2
    if term.has_key('page_size'): 
        page_size = term['page_size']
    else:
        page_size = 5
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'hot_rank_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type=event_name, body=query_body)['hits']['hits']
     
    items = eval(result[0]['_source']['hot_rank_dict'])
    
    hot_rank_list = []
    for i in range(len(items)):
        query_body = {   
            'query':{
                'bool':{
                    'must':
                        {'term':{'mid': items[i]['oringinal_blog']}}                            
                }
            },
            'size': 1        
        }          
        result = es.search(index = index_all, doc_type='text', body=query_body)['hits']['hits']
        
        result = result[0]['_source']
        print result
        hot_rank_text = dict()
        print result['text'].encode('utf-8')
        hot_rank_text['text'] = result['text']
        timestamp = ts2datetime_full(result['timestamp'])
        hot_rank_text['timestamp'] = timestamp
        hot_rank_text['uid'] = items[i]['uid']
        hot_rank_text['comment'] = items[i]['comment']
        hot_rank_text['retweet'] = items[i]['retweet']
        hot_rank_list.append(hot_rank_text)
    hot_page = dict()
    hot_page['total_num'] = len(hot_rank_list)
    hot_page['data'] = set_page(hot_rank_list, page_number, page_size)

    return json.dumps(hot_page)
