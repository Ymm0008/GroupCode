#-*- coding:utf-8 -*-

from elasticsearch import Elasticsearch
import time
import copy
import sys
import json

def ts2datetime_full(ts):                                 #时间戳的转换函数，可以把时间戳转换为时间
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def calculate_influence(es, index_name, event_name, uid_input):
    '''该函数用于计算一个事件中每个用户的原创微博被转发评论的数量
       输入:事件名称、待计算影响力的uid,要求输出数据条数
       输出一个字典形如{'uid': u'6324622961', 'influence': 104}
    '''
    query_body = {   # 计算输入的uid的影响力
        'query':{
            'bool':{
                'must':[
                    {'term':{'uid': uid_input}},
                    {'term':{'message_type': 1}}      #注意此用法 
                ]                  
            }
        },
        'size': 25000          #检索出不多于25000条原创微博的mid
    }          
    result = es.search(index=index_name, doc_type=event_name, body=query_body)['hits']['hits']
    dic_of_result = dict()
    list_of_blog = list()
    dic_of_result['uid'] = uid_input    
    if result != []:
        for j in range(len(result)):   #遍历每个原创微博,把每个用户的原创微博和uid成列表元素是字典的形式             
            list_of_blog.append(result[j]['_source']['mid'])
            
        query_body = {      #对所有root_mid是用户原创微博的mid的数据进行过滤，计算得到这些微博被转发评论的次数，确定影响力
            'query':{
                'filtered': {
                    'filter':{     
                        'terms': {'root_mid': list_of_blog}   #注意此用法
                    }
                }
            }
        }
        dic_of_result['influence'] = es.count(index=index_name, doc_type=event_name,body=query_body)['count']
        return dic_of_result
    else:
        dic_of_result['influence'] = 0
        return dic_of_result

def influence_rank(es, index_name, event_name, get_data_num):
    '''该函数用于计算一个事件中每个用户对于他人微博的转发数、评论数以及原创数和活跃度
       输入:事件名称和要求输出数据条数
       输出一个列表形如[{'comment': 2, 'retweeted': 102, 'uid': u'6324622961', 'influence': 104}, 
       {'comment': 32, 'orginal': 19, 'retweeted': 37, 'uid': u'1658725993', 'influence': 88}, 
       {'orginal': 72, 'uid': u'1689257855', 'influence': 72}]，每个列表元素表示一个用户
    '''
    query_body = {   # 1. 检索原创微博的mid
        'query':{
            'bool':{
                'must':
                    {'term':{'message_type': 1}}      #注意此用法                      
            }
        },
        'size': 25000          #检索出不多于25000条原创微博的mid
    }          
    result = es.search(index=index_name, doc_type=event_name, body=query_body)['hits']['hits']
    list_of_original_blog = list() 
    list0 = list()
    for j in range(len(result)):  #遍历每个原创微博,把每个用户的原创微博和uid成列表元素是字典的形式
        dic0 = dict()
        list1 = list() 
        if result[j]['_source']['uid'] not in list0:
            list0.append(result[j]['_source']['uid'])
            dic0['uid'] = result[j]['_source']['uid']
            list1.append(result[j]['_source']['mid'])
            dic0['oringinal_blog'] = list1
            list_of_original_blog.append(dic0)  
        else:
            for k in range(len(list_of_original_blog)):
                if list_of_original_blog[k]['uid'] == result[j]['_source']['uid']:                   
                    list_of_original_blog[k]['oringinal_blog'].append(result[j]['_source']['mid'])
                    break
    
    for i in range(len(list_of_original_blog)):    
        query_body = {      #对所有root_mid是用户原创微博的mid的数据进行过滤，计算得到这些微博被转发评论的次数，确定影响力
            'query':{
                'filtered': {
                    'filter':{     
                        'terms': {'root_mid': list_of_original_blog[i]['oringinal_blog']}   #注意此用法
                    }
                }
            }
        }
        list_of_original_blog[i]['influence'] = es.count(index=index_name, doc_type=event_name, body=query_body)['count']
    list_of_original_blog = sorted(list_of_original_blog, key=lambda x: x['influence'], reverse=True)
    list_of_original_blog = list_of_original_blog[0:(get_data_num+1)]
    list_of_uid_chosen = list()           # 把所有排在前面的uid取出放在列表里面待用
    for i in range(len(list_of_original_blog)):
        list_of_uid_chosen.append(list_of_original_blog[i]['uid'])
    query_body = {                      #提取影响力较高的用户对他人微博的转发评论量
        'query':{
            'filtered':{
                'filter':{
                    'terms':{'uid': list_of_uid_chosen}
                }
            }
        },
        'size': 0,                      #查询结果不输出，只输出聚合结果
        'aggs':{
            'group_by_uid':{
                'terms':{'field': 'uid',
                        'size' : get_data_num},  #输出get_data_num条
                'aggs': {
                    'group_by_message_type':{
                        'terms': {'field': 'message_type'} 
                    } 
                }
            }
        }
    }
    result1 = es.search(index=index_name, doc_type=event_name, body=query_body)                         
    list2 = result1['aggregations']['group_by_uid']['buckets']
    list3 = list()                                
    for item in range(len(list2)):                          # 遍历每一个用户
        dic = dict()                                        # 记得一个用户做完要清零
        dic['uid'] = list2[item]['key']                     # 提取每一个用户的uid,存到字典里面
        for i in range(len(list2[item]['group_by_message_type']['buckets'])):  #统计每一个用户转发、评论和原创数量
            if list2[item]['group_by_message_type']['buckets'][i]['key'] == 3:   #借助信息类型，存入字典dic    
                dic['retweeted'] = list2[item]['group_by_message_type']['buckets'][i]['doc_count']
            elif list2[item]['group_by_message_type']['buckets'][i]['key'] == 2:
                dic['comment'] = list2[item]['group_by_message_type']['buckets'][i]['doc_count']
            elif list2[item]['group_by_message_type']['buckets'][i]['key'] == 1:
                dic['orginal'] = list2[item]['group_by_message_type']['buckets'][i]['doc_count']
            else:
                pass                 
        list3.append(dic)   # 把字典dic连成一个列表list1,有除了影响力之外的项的值                                 
    list_of_influence_rank = list()
    for item in range(len(list_of_original_blog)):
        dic1 = dict()
        dic1['uid'] = list_of_original_blog[item]['uid']
        dic1['influence'] = list_of_original_blog[item]['influence']
        for i in range(len(list3)):
            if dic1['uid'] == list3[i]['uid']:
                dictMerged = dict(dic1, **list3[i])
                break
        list_of_influence_rank.append(dictMerged)
    return list_of_influence_rank

def influence_rank_classified(es,total_influence_rank):
    '''计算每个媒体账号的影响力排行，同时加上每个uid的名字
       输入：整体排行列表
       输出：[{'orginal': 1, 'influence': 467, 'uid': u'2803301701', 
       'screen_name': u'\u4eba\u6c11\u65e5\u62a5'},...}
       计算每个人物账号的影响力排行，同时加上每个uid的名称
       输入：整体排行
       输出：[{'orginal': 4, 'influence': 71, 'uid': u'2319877413', 
       'screen_name': u'\u7b11\u8db4\u4e86'},...}
    '''
    influence_rank_of_media = list()
    influence_rank_of_men = list()
    for i in range(len(total_influence_rank)): 
        query_body = {
            'query':{
                'filtered':{
                    'filter':{
                        'term':{'uid':total_influence_rank[i]['uid']}
                    }
                }
            }
        }
        result = es.search(index = 'uid_mapping_table', doc_type = 'text', body = query_body)['hits']['hits']
        if result == [] or result[0]['_source']['name'] == None:  #有的uid的name为空
            pass
        else:
            if result[0]['_source']['type'] == 3:
                dic_of_media_rank = dict()
                dic_of_media_rank = total_influence_rank[i]
                dic_of_media_rank['nick_name'] = result[0]['_source']['name']
                influence_rank_of_media.append(dic_of_media_rank)
            elif result[0]['_source']['type'] == -1 or result[0]['_source']['type'] == 0 or\
                result[0]['_source']['type'] == 220 or result[0]['_source']['type'] == 200 or\
                result[0]['_source']['type'] == 400:
                dic_of_men_rank = dict()
                dic_of_men_rank = total_influence_rank[i]
                dic_of_men_rank['nick_name'] = result[0]['_source']['name']
                influence_rank_of_men.append(dic_of_men_rank)
            else:
                pass
    return influence_rank_of_media, influence_rank_of_men    

def get_uid_name(es,uid):
    '''
    该函数用于获取每个uid的昵称
    输入：uid
    输出：{'nick_name':   }
    '''
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'term':{'uid':uid}
                }
            }
        }
    }
    result = es.search(index = 'uid_mapping_table', doc_type = 'text', body = query_body)['hits']['hits']
    dic_of_uid_name = dict()
    if result == [] or result[0]['_source']['name'] == None:
        dic_of_uid_name['nick_name'] = 'unknow_person %s'%uid
    else:
        dic_of_uid_name['nick_name'] = result[0]['_source']['name']
    return dic_of_uid_name           
                
def representive_text(es,index_name, event_name, list_of_rank): 
    '''该函数用于返回活跃度较高的（表格中出现的）用户的转发、评论、原创微博里面热度较高的微博内容（包括微博转发量、评论量
       发博时间，对应的uid和mid(也就是代表内容)。
       输入：事件类型，影响力列表（size待定）
       输出：列表[{'comment': 1, 'uid': u'2409482243','nick_name': , 'influence': 2（可以删去）, 
       'retweeted': 1, 'mid': u'4237066155960488', 'text': u'\u200b\u200b\u200b\u200b#\u62b5\u5236\u53f0\u72ec\
       u6e2f\u72ec#\u3010\u201c\u5c31\u60f3\u5455\u9999\u6e2f\u3002\u201dhttp://t.cn/RuFzWVe \uff08 \u200b\
       u200b\u200b\u200b\u200b', 'launch_date': '2018-05-07 15:39:15'},{...}]，每个用户代表列表的一个元素。
    ''' 
    query_body = {   #  检索指定列表中所发的微博的mid,同时计算出这些微博的转发和评论次数
        'query':{         #注意此用法
            'bool':{
                'must':
                    {'terms': {'uid': list_of_rank}}     #注意此用法                      
            }
        },
        'size': 6000          #检索出不多于6000条原创微博的mid
    }          
    result2 = es.search(index=index_name, doc_type=event_name, body=query_body)['hits']['hits']    
    list_of_original_blog = list()   #存入一个uid相关的所有mid的相关信息
    for j in range(len(result2)):     #遍历每个mid微博，提取mid微博相关数据(主要是原创微博)
        dic0 = dict() 
        dic0['uid'] = result2[j]['_source']['uid']
        dic0['text'] = result2[j]['_source']['text']
        dic0['root_mid'] = result2[j]['_source']['mid']  #注意！！！这里起名是为了下面可以查询
        dic0['launch_date'] = ts2datetime_full(result2[j]['_source']['timestamp'])
        list_of_original_blog.append(dic0)

    query_body = {   #得到每个mid的转发、评论数量
        'query':{
            'filtered': {
                'filter':{     
                    'terms': {'root_mid': list_of_original_blog}   #注意此用法
                }
            }
        },
        'size': 0,          #控制是否输出原条数据全部内容(follow query)
        'aggs':{
            'group_by_root_mid':{
                'terms':{'field': 'root_mid',
                        'size' : 25000},      #控制聚合数据输出条数，得到size_of_blog条代表微博
                'aggs': {
                    'group_by_message_type':{
                        'terms': {'field': 'message_type'} 
                    } 
                }
            }
        }   
    }
    result3 = es.search(index = index_name, doc_type = event_name, body = query_body)
    list5 = result3['aggregations']['group_by_root_mid']['buckets']
    for i in range(len(list5)): #遍历每一个mid
        total = 0
        for j in range(len(list_of_original_blog)):  #在原创微博的列表list_of_original_blog中加字段
            if list5[i]['key'] == list_of_original_blog[j]['root_mid']: #这里的root_mid其实是所有uid的mid
                for k in range(len(list5[i]['group_by_message_type']['buckets'])):
                    total = total + list5[i]['group_by_message_type']['buckets'][k]['doc_count']
                    if list5[i]['group_by_message_type']['buckets'][k]['key'] == 2:
                        list_of_original_blog[j]['comment'] = list5[i]['group_by_message_type']['buckets'][k]['doc_count']
                    elif list5[i]['group_by_message_type']['buckets'][k]['key'] == 3:
                        list_of_original_blog[j]['retweeted'] = list5[i]['group_by_message_type']['buckets'][k]['doc_count']
                    else:
                        pass
                list_of_original_blog[j]['influence'] = total
                break

    list_of_blog = list()
    for i in range(len(list_of_original_blog)):    #删掉没有影响力的列表元素，否则不能排序
        if 'influence' in list_of_original_blog[i].keys():
            list_of_blog.append(list_of_original_blog[i])
    list_of_blog = sorted(list_of_blog, key=lambda x: x['influence'], reverse=True)
    for i in range(len(list_of_blog)): #给每个uid加上名字
        list_of_blog[i]['nick_name'] = get_uid_name(es, list_of_blog[i]['uid'])['nick_name']

    for i in range(len(list_of_blog)):
        del list_of_blog[i]['influence']
        del list_of_blog[i]['root_mid']   #删掉返回数据中不需要的数据
    
    list_of_final_blog = list()
    for i in range(len(list_of_blog)): #计算出每个uid的微博的相关信息
        dic = dict()
        list1 = list()
        dic['uid'] = list_of_blog[i]['uid']
        for j in range(len(list_of_blog)):
        	if list_of_blog[j]['uid'] == dic['uid']:    
        	    list1.append(list_of_blog[j])
        dic['blog'] = list1
        list_of_final_blog.append(dic)
    return list_of_final_blog

def related_men(es, index_name, event_name, list0):
    '''该函数用于搜索listA中得到的活跃度比较高的用户的关联用户（转发评论过他和被他转发评论过的），同时找出他们的活跃度
       输入：事件类型（size待定）、list0是指整体排行列表
       输出得到一个列表，类似list5=[{'influence': 88, 'uid': u'1658725993', 'related_men': 
       [{'uid': u'5259666210', 'influence': 4}, {'uid': u'5561952241', 'influence': 23}, 
        {'uid': u'6063147756', 'influence': 1}, {'uid': u'3858386276', 'influence': 1}]},{...}]
       list0传入5个为什么输出关联人物时，main_uid只有3个
    '''
    lista = list()
    for i in range(len(list0)):             #遍历每一个上面得到的排名前几的用户(要看list0传入几条)
        try:                                #查找该用户的关联用户，并输出前10条结果（可能小于10）
            query_body = {                  #数据库的数据抓取不完整，可能有的匹配不到结果，所以用这个语句
                'query': {
                    'bool': {
                        'should':[
                            {'term': {'uid': list0[i]['uid']}},
                            {'term': {'root_uid': list0[i]['uid']}}
                        ]          
                    }
                },    
                'size': 100      #每一个用户最多只输出100个关联用户
            }
            result = es.search(index = index_name, doc_type = event_name, body = query_body)
        except:
            continue
        list1 = result['hits']['hits']       
        list2 = list()
        for j in range(len(list1)):          # 遍历该用户的每一个关联用户，提取他们的uid放入列表list2
            if list1[j]['_source']['uid'] == list0[i]['uid']:    # 被该用户评论或者转发过
                if list1[j]['_source']['message_type'] == 3 or list1[j]['_source']['message_type'] == 2:
                    if list1[j]['_source']['root_uid'] not in list2:    # ！！判断该root_uid是否已经在列表中——去重
                        list2.append(list1[j]['_source']['root_uid'])
            else:                                              # 转发评论过该用户微博的用户
                if list1[j]['_source']['uid'] not in list2:
                    list2.append(list1[j]['_source']['uid'])
        if list2 != []:
            dic1 = dict()
            dic1['related_men'] = list2         
            dic1['uid'] = list0[i]['uid']
            dic1['influence'] = list0[i]['influence']
            lista.append(dic1)   #每个被该用户评论或者转发过,以及转发评论过该用户微博的用户,以列表嵌套字典形式存储
    
    list_related_men = list()
    list3 = list()                                     
    for i in range(len(lista)):                        #遍历每一个活跃度比较高的用户
        for j in range(len(lista[i]['related_men'])):   #遍历一个用户的每个关联用户，得到其影响力
            list3.append(calculate_influence(es, index_name,event_name,lista[i]['related_men'][j]))
        if list3 != []:              
            list3 = sorted(list3, key=lambda x: x['influence'], reverse=True) 
            dic2 = dict()
            if len(list3) > 30: #控制每个人物的关联人物只输出50条
                list4 = list3[0:30]
            else:
                list4 = list3
            for k in range(len(list4)):
                list4[k]['nick_name'] = get_uid_name(es, list4[k]['uid'])['nick_name']
            dic2['related_men'] = list4
            dic2['influence'] = lista[i]['influence']
            dic2['uid'] = lista[i]['uid']
            dic2['nick_name'] = get_uid_name(es, lista[i]['uid'])['nick_name']
            list_related_men.append(dic2)
    return list_related_men  # 嵌套：列表-字典-列表-字典 list_related_men里的related_men存在是自己的情况
    
def related_men_typical_text(es, index_name, event_name, listC):
    '''该函数计算某个事件，特定人物与关联任务的代表微博
    '''
    list1 = copy.deepcopy(listC)
    list2 = list()
    for i in range(len(listC)):  #遍历每一个表格中的关联用户
        list0 = list()
        dic0 = dict()
        dic0['uid'] = listC[i]['uid']
        dic0['influence'] = listC[i]['influence']
        dic0['nick_name'] = listC[i]['nick_name']
        list1[i]['related_men'].append(dic0)   #related_men的uid加上main_uid
        list0 = representive_text(es, index_name, event_name, list1[i]['related_men']) 
        if list0 != []: 
            list1[i]['related_men'] = list0
        else:
            list1[i]['related_men'] = []
    for j in range(len(list1)):    #只输出可以找到相关人物微博的关键人物和关联人物
        if list1[j]['related_men'] != []:
            list2.append(list1[j])
    return list2  

def data_for_graph(list_of_related_men):
    list0 = list()
    list1 = list()
    dic0 = dict()
    for i in range(len(list_of_related_men)):
        if list_of_related_men[i]['uid'] not in list1:   ##去重
            dic1 = dict()
            dic1['type'] = 1
            dic1['uid'] = list_of_related_men[i]['uid']
            dic1['nick_name'] = list_of_related_men[i]['nick_name']
            list0.append(dic1)
            list1.append(dic1['uid'])
            for j in range(len(list_of_related_men[i]['related_men'])):
                if list_of_related_men[i]['related_men'][j]['uid'] not in list1:  #去重
                    dic1 = dict()
                    dic1['type'] = 0
                    dic1['nick_name'] = list_of_related_men[i]['related_men'][j]['nick_name']
                    dic1['uid'] = list_of_related_men[i]['related_men'][j]['uid']
                    list0.append(dic1)
                    list1.append(list_of_related_men[i]['related_men'][j]['uid'])
    dic0['node'] = list0     #找出所有节点的名字
    list1 = list()
    for i in range(len(list_of_related_men)):
        for j in range(len(list_of_related_men[i]['related_men'])):
            dic1 = dict()
            dic1['source'] = list_of_related_men[i]['nick_name']
            dic1['target'] = list_of_related_men[i]['related_men'][j]['nick_name']
            list1.append(dic1)  #找出每条关系的起点和终点
    dic0['link'] = list1
    return dic0

def save_search_data(es, event_name,list_of_media, list_of_men, list_of_media_blog, list_of_men_blog,\
                    list_of_related_men, list_of_related_men_blog,list_of_data_for_graph):
    es.index(index='network_analysis', doc_type = event_name, id = ts2datetime(int(time.time())),
             body={'media_rank': list_of_media , 'man_rank': list_of_men, \
                'related_men': list_of_related_men,
                'representitive_blog_of_men': list_of_men_blog, \
                'representitive_blog_of_media':list_of_media_blog, \
                'representitive_blog_of_related_men': list_of_related_men_blog, \
                'content_for_graph': list_of_data_for_graph,\
                'search_date_timestamp': int(time.time())})  
    print 'ok'

def network_analysis(index_name, event_name, es):
    listA = influence_rank(es, index_name, event_name, 100)
    list_of_media, list_of_men = influence_rank_classified(es, listA) 
    list_of_media_blog = representive_text(es, index_name, event_name, list_of_media)
    list_of_men_blog = representive_text(es, index_name, event_name, list_of_men)
    list_of_related_men = related_men(es, index_name, event_name, listA)      #引入listA算法
    list_of_related_men_blog = related_men_typical_text(es, index_name, event_name, list_of_related_men) #调用listA算法     
    list_of_data_for_graph = data_for_graph(list_of_related_men)
    save_search_data(es, event_name, list_of_media, list_of_men, list_of_media_blog, list_of_men_blog,list_of_related_men, list_of_related_men_blog, list_of_data_for_graph)

if __name__ == '__main__':
    es = Elasticsearch('219.224.134.226:9207', timeout=1200)
    index_name = 'flow_text_gangdu'
    event_name = 'text'
    network_analysis(index_name, event_name, es)
