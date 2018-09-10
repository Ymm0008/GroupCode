#-*-coding:utf-8-*-

import json
import codecs
from elasticsearch import Elasticsearch
import time

es = Elasticsearch("219.224.134.220:7200", timeout=600)

def datehour2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M')))


def query_keguan(place,keywords,from_ts=time.time(), to_ts=time.time()-6*3600):
	keyword_query_list = []
	for keyword in keywords.split('&'):
		keyword_query_list.append({'wildcard':{'text':'*'+keyword+'*'}})

	query_body = {
	    "query":{
	        "bool":{
	        	"must": [
	        		{"wildcard":{ "geo": '*'+place+'*'}},
	        		{'range':{'timestamp':{'gte':from_ts, 'lte':to_ts}}}
	        	],
	        	"should":keyword_query_list,

	            "minimum_should_match": 1
	        }
	    },
	    "size": 20000
	}

	results = es.search(index="event_sensing_text", doc_type="text", body=query_body)["hits"]["hits"]   # tpye(result) = list

	return results


if __name__ == '__main__':
	place = '北京'
	keywords = '港独&国旗&碾压&联盟'
	print type(place),type(keywords)
	print place,keywords
	results = query_keguan(place, keywords, datehour2ts('2018-04-01 14:34'), datehour2ts('2018-05-27 14:34') )

	print len(results)




