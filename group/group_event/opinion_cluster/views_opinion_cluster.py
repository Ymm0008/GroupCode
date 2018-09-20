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
    event_name = request.args.get('event_name')

    keywords = dict()
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'cluster_keywords_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type='event1', body=query_body)['hits']['hits']
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
    event_name = request.args.get('event_name')

    
    keywords = dict()
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'cluster_keywords_dict'           
        }          
    result = es.search(index = 'opinion_cluster', doc_type='event1', body=query_body)['hits']['hits']
    item = eval(result[0]['_source']['cluster_keywords_dict'])
    for i in range(5):
        keywords[i] = item[str(i)][0]
    print keywords
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'rep_text_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type='event1', body=query_body)['hits']['hits']
    print type(result[0]['_source']['rep_text_dict'])
    items = eval(result[0]['_source']['rep_text_dict'])

    rep_keyword_text = []
    for i in range(5):
        keyword_text = dict()
    	cluster = keywords[i]
        midlist = []
        for j in range(len(items[str(i)])):
            midlist.append(items[str(i)][j]['mid'])
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
            result = es.search(index = 'flow_text_gangdu', doc_type='text', body=query_body)['hits']['hits']
            result = result[0]['_source']
            print result
            rep_text = dict()
            print result['text'].encode('utf-8')
            rep_text['text'] = result['text']
            timestamp = ts2datetime_full(result['timestamp'])
            rep_text['timestamp'] = timestamp
            rep_text['uid'] = result['uid']
            rep_text['comment'] = result['comment']
            rep_text['retweet'] = result['retweeted']
            text_list.append(rep_text)
            keyword_text[cluster]=text_list
        rep_keyword_text.append(keyword_text)
    # print rep_keyword_text
    return json.dumps(rep_keyword_text)



#热帖排行展示接口
@mod.route('/hot_rank', methods=['POST']) 
def hot_rank():
    term = request.get_json()
    event_name = request.args.get('event_name')
    index_all = request.args.get('index_all')
    
    # if term.has_key('get_data_num'):
    # 	get_data_num = term['get_data_num']
    # else:
    # 	get_data_num = 100
    # list_of_original_blog = weibo_rank(get_data_num)
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'hot_rank_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type='event1', body=query_body)['hits']['hits']
     
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
        result = es.search(index = 'flow_text_gangdu', doc_type='text', body=query_body)['hits']['hits']
        
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
    return json.dumps(hot_rank_list)
