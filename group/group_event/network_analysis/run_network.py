#-*- coding:utf-8 -*-

from elasticsearch import Elasticsearch
import time
import copy

es = Elasticsearch('219.224.134.226:9209', timeout=600)
es1 = Elasticsearch('127.0.0.1:9200', timeout=600)

def influence_rank(event_type, get_data_num):
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
        'size': 5000          #检索出不多于5000条原创微博的mid
    }          
    result = es.search(index = event_type, doc_type='text', body=query_body)['hits']['hits']
    list_of_original_blog = list() 
    list0 = list()
    for j in range(len(result)):     #遍历每个原创微博,把每个用户的原创微博和uid成列表元素是字典的形式
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
        list_of_original_blog[i]['influence'] = es.count(index = event_type, doc_type='text', body=query_body)['count']
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
    result1 = es.search(index = event_type, doc_type='text', body=query_body)                         
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

def set_page(list1, page_number, page_size):              #分页函数 list1指等待被分页的列表
    start_from = (page_number - 1) * page_size
    end_at = start_from + page_size
    print list1[start_from:end_at]

def ts2datetime_full(ts):                                 #时间戳的转换函数，可以把时间戳转换为时间
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def representive_text(event_type, list_of_rank): 
    '''该函数用于返回活跃度较高的（表格中出现的）用户的转发、评论、原创微博里面热度较高的微博内容（包括微博转发量、评论量
       发博时间，对应的uid和mid(也就是代表内容)。
       输入：事件类型，活跃度列表（size待定）
       输出：列表[{'comment': 1, 'uid': u'2409482243', 'influence': 2（可以删去）, 
       'retweeted': 1, 'mid': u'4237066155960488', 'text': u'\u200b\u200b\u200b\u200b#\u62b5\u5236\u53f0\u72ec\
       u6e2f\u72ec#\u3010\u201c\u5c31\u60f3\u5455\u9999\u6e2f\u3002\u201dhttp://t.cn/RuFzWVe \uff08 \u200b\
       u200b\u200b\u200b\u200b', 'launch_date': '2018-05-07 15:39:15'},{...}]，每个用户代表列表的一个元素。
    '''
    query_body = {   #先用uid过滤出出现在表格里面的用户的相关微博，再进行聚合，可以得到每个root_mid的转发、评论数量
        'query':{
            'filtered': {
                'filter':{     
                    'terms': {'uid': list_of_rank}   #注意此用法
                }
            }
        },
        'size': 0,          #控制是否输出原条数据全部内容(follow query)
        'aggs':{
            'group_by_root_mid':{
                'terms':{'field': 'root_mid',
                        'size' : 2000},      #控制聚合数据输出条数，得到size_of_blog条代表微博
                'aggs': {
                    'group_by_message_type':{
                        'terms': {'field': 'message_type'} 
                    } 
                }
            }
        }   
    }
    result = es.search(index = event_type, doc_type='text', body=query_body) #result是个字典格式，对结果进一步处理
    dic = dict()
    dict1 = dict()
    dict2 = dict()
    list0 = result['aggregations']['group_by_root_mid']['buckets']
    list1 = result['hits']['hits']
    list2 = list()
    list3 = list()
    list4 = list()
    for item in range(len(list0)):                    #遍历每一条微博（root_mid)                      
        dic = dict()
        dic['mid'] = list0[item]['key']               #把每条root_mid存成mid
        total = 0
        for i in range(len(list0[item]['group_by_message_type']['buckets'])): #统计每一个mid被转发、评论的数量，存入字典
            if list0[item]['group_by_message_type']['buckets'][i]['key'] == 3: #借助信息类型判断
                dic['retweeted'] = list0[item]['group_by_message_type']['buckets'][i]['doc_count']
            elif list0[item]['group_by_message_type']['buckets'][i]['key'] == 2:
                dic['comment'] = list0[item]['group_by_message_type']['buckets'][i]['doc_count']
            else:
                pass
            total = total + list0[item]['group_by_message_type']['buckets'][i]['doc_count']
        list4.append(dic)   #每条微博对应列表的一个元素，这个元素是字典                 
        dic2 = copy.deepcopy(dic)
        dic2['influence'] = total
        list2.append(dic2)        # 每条微博的转发数、评论数、影响力以及微博mid 
    list2 = sorted(list2, key=lambda x: x['influence'], reverse=True)
    lista = list()
    for i in range(len(list2)):        # 找每一个mid的文本和对应uid,text,timestamp，加入list2，得到lista
        dica = dict()
        query_body = {
            'query': {
                'filtered': {
                    'filter':{       
                        'term': {'mid': list2[i]['mid']}  #最多可以过滤出10000条满足条件的数据，注意用法
                    }
                }
            },
            'size': 100  #root_mid 和mid可以匹配上的数据最多输出100条（会受到前一个size的影响,要比上一个size小）
        }
        result1 = es.search(index = event_type, doc_type='text', body=query_body)
        if result1['hits']['hits'] == []:       #有的root_mid搜不到对应的mid，所以没有文本内容等信息，需要先判断一下
            continue                            #否则会报错
        else:   
            dica = copy.deepcopy(list2[i])     
            dica['text'] = result1['hits']['hits'][0]['_source']['text']
            dica['uid'] = result1['hits']['hits'][0]['_source']['uid']
            dica['launch_date'] = ts2datetime_full(result1['hits']['hits'][0]['_source']['timestamp'])
            del dica['influence']  #删掉用来排序的无需返回的inluence项
            del dica['mid']
            lista.append(dica)
    return lista  #每个微博是列表的一个元素，该元素为一个字典（dica) 

def related_men(event_type, list0):
    '''该函数用于搜索listA中得到的活跃度比较高的用户的关联用户（转发评论过他和被他转发评论过的），同时找出他们的活跃度
       输入：事件类型（size待定）、list0是指人物排行列表
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
            result = es.search(index = event_type, doc_type='text', body=query_body)
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
    list3 = influence_rank(event_type, 20000)   #计算20000个uid的活跃度，留着做查询
    list5 = list()
    list4 = list()                                     
    for i in range(len(lista)):                        #遍历每一个活跃度比较高的用户
        for j in range(len(lista[i]['related_men'])):            #遍历一个用户的每个关联用户，得到其活跃度
            if lista[i]['related_men'][j] != lista[i]['uid']:   #可能有人评论自己的微博，所以去掉关联人物中自己的信息
                for k in range(len(list3)):
                    if list3[k]['uid'] == lista[i]['related_men'][j]:
                        dic3 = dict()
                        dic3['influence'] = list3[k]['influence']
                        dic3['uid'] = lista[i]['related_men'][j]
                        list4.append(dic3)
                        continue     #匹配到结果就结束该循环
        if list4 != []:              #可能匹配不到指定uid的活跃度，所以判断一下
            list4 = sorted(list4, key=lambda x: x['influence'], reverse=True) #获得main_uid的关联人物的活跃度，存入列表
            dic2 = dict()
            dic2['related_men'] = list4
            dic2['influence'] = lista[i]['influence']
            dic2['uid'] = lista[i]['uid']
            list5.append(dic2)  
    return list5  # 嵌套：列表-字典-列表-字典 list5里的related_men存在是自己的情况
    
def related_men_typical_text(event_type, listC):
    '''该函数计算某个事件，特定人物与关联任务的代表微博
    '''
    list1 = copy.deepcopy(listC)
    for i in range(len(listC)):  #遍历每一个表格中的关联用户
        list0 = list()
        dic0 = dict()
        dic0['uid'] = listC[i]['uid']
        dic0['inflence'] = listC[i]['influence']
        list1[i]['related_men'].append(dic0)   #related_men的uid加上main_uid
        list0 = representive_text(event_type, list1[i]['related_men']) 
        list1[i]['related_men'] = list0
    return list1  

def create_es_table():
    index_info = {
        'settings': {
            'number_of_shards': 5, #定义该索引分片数	
		    'number_of_replicas':0, # 定义该索引副本数
		    'analysis':{ # 自定义简单分析器
		    	'analyzer':{
				    'my_analyzer':{
			     		'type':'pattern',
			    		'patern':'&' #用&分隔
		  	    	}
			    }
		    },
	    },
	    'mappings':{
		    'network_analysis':{
			    'properties':{
			        '@timestamp':{
                            'type' : 'date',
	                        'index' : 'not_analyzed',
	                        'doc_values' : 'true',
	                        'format' : 'dd/MM/YYYY:HH:mm:ss Z'
                    },
			    	'media_rank':{ 							# 微博文本
					    'type':'object'
				    },								# 微博关键词
				    'man_rank':{
					    'type':'object'
		            },
                    'representitive_blog_of_men':{
                        'type':'object'
                    },
                    'representitive_blog_of_media':{
                        'type':'object'
                    },
                    'representitive_blog_of_related_men':{
                        'type':'object'
                    },
                    'related_men':{
                        'type':'object'
                    }
		        }
            }
        }
    }
    exist_indice = es.indices.exists(index='network_analysis') # 判断索引存不存在
    print exist_indice
    if not exist_indice:
        print es.indices.create(index='network_analysis', body=index_info, ignore=400)
  # print es1.indices.create(index='network_analysis', ignore=400)
  # print es1.indices.delete(index = 'network_analysis')

def save_search_data(event_type,listA, listB, listC, listD):
    es.index(index='network_analysis', doc_type = event_type, id = ts2datetime(int(time.time())),
             body={'media_rank': listA , 'man_rank': listA, 'related_men': listC,
              'representitive_blog_of_men': listB, 'representitive_blog_of_media':listB, 
              'representitive_blog_of_related_men': listD, 
              'search_date_timestamp': int(time.time())})      # input account_type as var 
    print 'ok'

if __name__ == '__main__':
    listA = influence_rank('flow_text_gangdu', 100)
    listB = representive_text('flow_text_gangdu', listA)
    listC = related_men('flow_text_gangdu', listA)      #引入listA算法
    listD = related_men_typical_text('flow_text_gangdu', listC) #调用listA算法
#    create_es_table()
    save_search_data('gangdu', listA, listB, listC, listD)
#    set_page(listD,1,5) # set page 
