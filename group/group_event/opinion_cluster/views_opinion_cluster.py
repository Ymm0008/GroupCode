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
 
@mod.route('/')
def index():
    """返回页面
    """
    return render_template('index_cluster.html')



    
 #    for item in result: 
 #        item = item['_source'] 
 #        weibo = item['text'] 
 #        weibo_list.append(weibo) 
#聚类模型
def event_cluster():
    bulk_action = []
    index_count = 0
    count = 0
    keyword = []
    words = ['港独, 国歌, 台独, 侮辱, 呕吐','国歌, 亵渎, 港独, 侮辱, 呕吐','港独, 国歌, 台独, 侮辱, 呕吐','港独, 国歌, 台独, 侮辱, 呕吐']
    keyword_dict = dict()
    print type(words[0])
    for i in range(len(words)):
        print len(words)
        keyword_dict[i] = words[i].encode('utf-8')


    with codecs.open('cluster.json','r','utf-8') as f:
        
        item_text = []
        a=0
        for line in f:
            a += 1
            item = json.loads(line.strip())

            
            text = dict()    
            uid = item["uid"]
            texts = item["text"].encode('utf-8')
            mid = item["mid"]
            timestamp = item["timestamp"]
            text['uid'] = uid
            text['text'] = texts
            text['mid'] = mid
            text['timestamp'] = timestamp

            query_body = {      #对所有root_mid是用户原创微博的mid的数据进行过滤，计算得到这些微博被转发评论的次数，确定影响力
                'query':{
                    'filtered': {
                        'query':{                        
                            'term':{'message_type': 2}   #注意此用法

                        },
                        'filter':{     
                            
                            'term': {'root_mid': mid}
                        }
                    }
                }
            }
            # print 1
            text['retweet'] = es.count(index = 'flow_text_gangdu', doc_type='text', body=query_body)['count']
            # print 0
        
            query_body = {      #对所有root_mid是用户原创微博的mid的数据进行过滤，计算得到这些微博被转发评论的次数，确定影响力
                'query':{
                    'filtered': {
                        'query':{
                            'term':{'message_type': 3}   #注意此用法
                            

                        },
                        'filter':{     
                            'term': {'root_mid': mid}
                        }
                    }
                }
            }
            text['comment'] = es.count(index = 'flow_text_gangdu', doc_type='text', body=query_body)['count'] 
            print text['comment']    
            item_text.append(text)
        # print a
        print len(item_text)
    rep_text = dict()
    items = []
    for i in range(5):
        items.append(item_text)
    for j in range(5):
        #print len(items)
        rep_text[j] = item_text

    times = 123456789
    # 建索引的代码从这里开始写
    
    return keyword_dict, rep_text, times
def weibo_rank(get_data_num):
    
    query_body = {   
        'query':{
            'bool':{
                'must':
                    {'term':{'message_type': 1}}      #注意此用法                      
            }
        },
        'size': 20000         
    }          
    result = es.search(index = 'flow_text_gangdu', doc_type='text', body=query_body)['hits']['hits']
    list_of_original_blog = list() 
    # list0 = list()
    for j in range(len(result)):     
        dic0 = dict()
       
        dic0['uid'] = result[j]['_source']['uid']
        # list1.append(result[j]['_source']['mid'])
        dic0['oringinal_blog'] = result[j]['_source']['mid']
        list_of_original_blog.append(dic0)  

    print len(list_of_original_blog)
    # print list_of_original_blog
    
    for i in range(len(list_of_original_blog)):    
        query_body = {      
            'query':{
                'filtered': {
                    'query':{                        
                        'term':{'message_type': 2}   #注意此用法

                    },
                    'filter':{     
                        
                        'term': {'root_mid': list_of_original_blog[i]['oringinal_blog']}
                    }
                }
            }
        }
        # print 1
        list_of_original_blog[i]['retweet'] = es.count(index = 'flow_text_gangdu', doc_type='text', body=query_body)['count']
        # print 0
    for i in range(len(list_of_original_blog)):    
        query_body = {      #对所有root_mid是用户原创微博的mid的数据进行过滤，计算得到这些微博被转发评论的次数，确定影响力
            'query':{
                'filtered': {
                    'query':{
                        'term':{'message_type': 3}   #注意此用法
                        

                    },
                    'filter':{     
                        'term': {'root_mid': list_of_original_blog[i]['oringinal_blog']}
                    }
                }
            }
        }
        list_of_original_blog[i]['comment'] = es.count(index = 'flow_text_gangdu', doc_type='text', body=query_body)['count']       
    list_of_original_blog = sorted(list_of_original_blog, key=lambda x: x['retweet']+x['comment'], reverse=True)
#    print list_of_original_blog
    list_of_original_blog = list_of_original_blog[0:(get_data_num+1)]
    # print list_of_original_blog
    return list_of_original_blog
    
    
def create_mappings(index_name):
    index_info = {
            'settings':{
                'number_of_replicas': 0,
                'number_of_shards': 5,
                'analysis':{
                    'analyzer':{
                        'my_analyzer':{
                            'type': 'pattern',
                            'pattern': '&'
                        }
                    }
                }
            },

            'mappings':{
                'event1':{
                    'properties':{
                            
                            "timestamp": {
                            "type": "long"
                        },
                            "cluster_keywords_dict": {
                            "index": "not_analyzed",
                            "type": "string"
                        },
                         
                            "rep_text_dict": {
                            "index": "not_analyzed",
                            "type": "string"
                        },
                            
                            "hot_rank_dict": {
                            "index": "not_analyzed",
                            "type": "string"
                        }
                    }
                }
            }
        }

    exist_indice = es.indices.exists(index=index_name)
    if not exist_indice:
        es.indices.create(index=index_name, body=index_info, ignore=400)  
def save_search_data():

    keyword_dict,rep_text,times = data2es()
    list_of_original_blog = weibo_rank('flow_text_gangdu', 100)

    index_dict = dict()
    
    index_dict['cluster_keywords_dict'] = json.dumps(keyword_dict)
    # print 1
    
    index_dict['rep_text_dict'] =  json.dumps(rep_text)
    # print 1
    index_dict['timestamp'] = times
    # print 1
    index_dict['hot_rank_dict'] = json.dumps(list_of_original_blog)
    
    #print index_dict    
    es.index(index='opinion_cluster', doc_type='event1', body=index_dict)

# uid： 1381253043

# @mod.route('/get', methods=['GET']) 
# def get_calculate():
#     uid = request.args.get('uid')
#     result = data_read(uid)

#     return json.dumps(result)

#接口代码
#事件聚类展示接口
@mod.route('/event_cluster', methods=['POST']) 
def event_cluster():
    term = request.get_json()
    # keyword_dict, rep_text, times = event_cluster()
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
        print type(result[0]['_source']['cluster_keywords_dict'])
        print eval(result[0]['_source']['cluster_keywords_dict'])
        item = eval(result[0]['_source']['cluster_keywords_dict'])
        keywords[i] = item[str(i)]

    return json.dumps(keywords)



#聚类关键词和代表性文本联动接口
@mod.route('/rep_text', methods=['POST']) 
def rep_text():
    term = request.get_json()
    # keyword_dict, rep_text, times = event_cluster()
    
    keywords = dict()
    query_body = {  
            'query':{
                'match_all':{}            
                },
            '_source':'cluster_keywords_dict'
                    
        }          
    result = es.search(index = 'opinion_cluster', doc_type='event1', body=query_body)['hits']['hits']
    # print result
    # for item in result:
    # print result[0]['_source']
    # print type(result[0]['_source']['cluster_keywords_dict'])
    # print eval(result[0]['_source']['cluster_keywords_dict'])
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
    # print result
    # for item in result:
    # print result[0]['_source']
    a = 0
    print type(result[0]['_source']['rep_text_dict'])
    items = eval(result[0]['_source']['rep_text_dict'])
    # keyword_text = dict()
    rep_keyword_text = []
    for i in range(5):
        a += 1
        print a
        keyword_text = dict()
    	cluster = keywords[i]
        # print items[str(i)] 
    	keyword_text[cluster] = items[str(i)]
        rep_keyword_text.append(keyword_text)
    # print rep_keyword_text
    return json.dumps(rep_keyword_text)



#热帖排行展示接口
@mod.route('/hot_rank', methods=['POST']) 
def hot_rank():
    term = request.get_json()

    if term.has_key('get_data_num'):
    	get_data_num = term['get_data_num']
    else:
    	get_data_num = 100
    list_of_original_blog = weibo_rank(get_data_num)
    hot_rank_list = []
    for i in range(len(list_of_original_blog)):
        query_body = {   
            'query':{
                'bool':{
                    'must':
                        {'term':{'mid': list_of_original_blog[i]['oringinal_blog']}}      #注意此用法                      
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
        hot_rank_text['timestamp'] = result['timestamp']
        hot_rank_text['uid'] = list_of_original_blog[i]['uid']
        hot_rank_text['comment'] = list_of_original_blog[i]['comment']
        hot_rank_text['retweet'] = list_of_original_blog[i]['retweet']
        hot_rank_list.append(hot_rank_text)
    return json.dumps(hot_rank_list)
