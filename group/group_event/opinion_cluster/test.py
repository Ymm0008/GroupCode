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

query_body = {   # 1. 检索原创微博的mid
            'query':{
                'bool':{
                    'must':
                        {'term':{'mid': 4237056027146081}}      #注意此用法                      
                }
            },
            'size': 10        #检索出不多于5000条原创微博的mid
        }          
result = es.search(index = 'flow_text_gangdu', doc_type='text', body=query_body)['hits']['hits']
print type(result)
print type(result[0])
# result = result['_source']
# print result['text']