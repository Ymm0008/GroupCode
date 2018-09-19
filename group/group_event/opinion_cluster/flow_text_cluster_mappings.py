# -*- coding: UTF-8 -*-
'''
use to save text from flow_text --for all people 7day
url: https://github.com/huxiaoqian/user_portrait_ending2/tree/f2978135ff672f58090e202e588f7321ed121477/user_portrait_0320/user_portrait
# es_flow_text = Elasticsearch(FLOW_TEXT_ES_HOST, timeout=600)
# FLOW_TEXT_ES_HOST = ['10.128.55.75', '10.128.55.76']

'''
from elasticsearch import Elasticsearch
es = Elasticsearch("219.224.134.226:9209", timeout=600)


def get_mappings(index_name):
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

if __name__=='__main__':
    index = ['opinion_cluster']
    for index_name in index:
        get_mappings(index_name)

