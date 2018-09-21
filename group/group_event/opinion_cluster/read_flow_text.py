#-*-coding: utf-8-*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8') 

from flow_text_cluster_mappings import es
import json
import codecs



def data2es(event_type):
    bulk_action = []
    index_count = 0
    count = 0
    keyword = []
    words = ['港独, 国歌, 台独, 侮辱, 呕吐','国歌, 亵渎, 港独, 侮辱, 呕吐','港独, 国歌, 台独, 侮辱, 呕吐','港独, 国歌, 台独, 侮辱, 呕吐','港独, 国歌, 台独, 侮辱, 呕吐']
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
            text['retweet'] = es.count(index = event_type, doc_type='text', body=query_body)['count']
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
            text['comment'] = es.count(index = event_type, doc_type='text', body=query_body)['count'] 
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
def weibo_rank(event_type, get_data_num):
    '''该函数用于计算原创微博的转发数、评论数以及原创数
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
        'size': 20000         #检索出不多于5000条原创微博的mid
    }          
    result = es.search(index = event_type, doc_type='text', body=query_body)['hits']['hits']
    list_of_original_blog = list() 
    # list0 = list()
    for j in range(len(result)):     #遍历每个原创微博,把每个用户的原创微博和uid成列表元素是字典的形式
        dic0 = dict()
        # list1 = list() 
        # if result[j]['_source']['uid'] not in list0:
            # list0.append(result[j]['_source']['uid'])
#            print list0
        dic0['uid'] = result[j]['_source']['uid']
        # list1.append(result[j]['_source']['mid'])
        dic0['oringinal_blog'] = result[j]['_source']['mid']
        list_of_original_blog.append(dic0)  
#        print list_of_original_blog
        # else:
        #     for k in range(len(list_of_original_blog)):
        #         if list_of_original_blog[k]['uid'] == result[j]['_source']['uid']:                   
        #             list_of_original_blog[k]['oringinal_blog'].append(result[j]['_source']['mid'])
        #             break
    print len(list_of_original_blog)
    # print list_of_original_blog
    
    for i in range(len(list_of_original_blog)):    
        query_body = {      #对所有root_mid是用户原创微博的mid的数据进行过滤，计算得到这些微博被转发评论的次数，确定影响力
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
        list_of_original_blog[i]['retweet'] = es.count(index = event_type, doc_type='text', body=query_body)['count']
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
        list_of_original_blog[i]['comment'] = es.count(index = event_type, doc_type='text', body=query_body)['count']       
    list_of_original_blog = sorted(list_of_original_blog, key=lambda x: x['retweet']+x['comment'], reverse=True)
#    print list_of_original_blog
    list_of_original_blog = list_of_original_blog[0:(get_data_num+1)]
    # print list_of_original_blog
    return list_of_original_blog
    
    
#   
def save_search_data():

    keyword_dict,rep_text,times = data2es('flow_text_gangdu')
    list_of_original_blog = weibo_rank('flow_text_gangdu', 100)

    index_dict = dict()
    
    index_dict['cluster_keywords_dict'] = json.dumps(keyword_dict)
    print 1
    
    index_dict['rep_text_dict'] =  json.dumps(rep_text)
    print 1
    index_dict['timestamp'] = times
    print 1
    index_dict['hot_rank_dict'] = json.dumps(list_of_original_blog)
    
    #print index_dict    
    es.index(index='opinion_cluster', doc_type='event1', body=index_dict)
    

if __name__=='__main__':
	key_list = ["comment","uid","sentiment","sensitive_words_string","text","hashtag","user_fansnum","mid","keywords_string","geo","sensitive","timestamp","ip","keywords_dict","sensitive_words_dict","message_type","retweeted","directed_uname","directed_uid","root_mid","root_uid"]
	index = ['opinion_clusetr']
	file_list = [r'D:\code\group_event_es\data_gangdu.json',r'D:\code\group_event_es\data_maoyi.json', r'D:\code\group_event_es\data_zhongxing.json']

	save_search_data()

