#-*-coding:utf-8-*-

import json
import codecs
import re
from elasticsearch import Elasticsearch
import sys  

reload(sys)  
sys.setdefaultencoding('utf8') 

es = Elasticsearch("219.224.134.220:8200", timeout=600)


ids = []
with codecs.open('./user_filter.txt','r','utf-8') as fp:
    for line in fp:
        ids.append(line.strip())

# 博主发帖量
for id in ids:
	count = es.count(index='post', doc_type="post", body={
		"query":{
			"bool":{
				"must":[
					{"term":{"user_id": id}}
				]
			}
		}
	})["count"]

	user_post_count = dict() 
	user_post_count[id] = count

	with open('./stat_data/user_post_count.json','a') as f:
		f.write(json.dumps(user_post_count)+'\n')

	with open('./stat_data/user_post_count.txt','a') as f:
		f.write(str(count)+'\n')


# 统计博主  转发数
for id in ids:
	count = es.search(index='post', doc_type="post", body={
		"query":{
			"bool":{
				"must":[
					{"term":{"user_id": id}}
				]
			},
		},
		"size" : 0,
		"aggs":{
			"reposts_count_sum":{
				"sum":{
					"field":"reposts_count"
				}
			}
		}
	})['aggregations']['reposts_count_sum']['value']

	user_reposts_count = dict() 
	user_reposts_count[id] = count

	with open('./stat_data/user_reposts_count.json','a') as f:
		f.write(json.dumps(user_reposts_count)+'\n')

	with open('./stat_data/user_reposts_count.txt','a') as f:
		f.write(str(count)+'\n')


# 统计博主帖子被  评论数
for id in ids:
	count = es.search(index='post', doc_type="post", body={
		"query":{
			"bool":{
				"must":[
					{"term":{"user_id": id}}
				]
			},
		},
		"size" : 0,
		"aggs":{
			"comments_count_sum":{
				"sum":{
					"field":"comments_count"
				}
			}
		}
	})['aggregations']['comments_count_sum']['value']

	user_comments_count = dict() 
	user_comments_count[id] = count

	with open('./stat_data/user_comments_count.json','a') as f:
		f.write(json.dumps(user_comments_count)+'\n')

	with open('./stat_data/user_comments_count.txt','a') as f:
		f.write(str(count)+'\n')


# 统计博主帖子被  点赞数
for id in ids:
	count = es.search(index='post', doc_type="post", body={
		"query":{
			"bool":{
				"must":[
					{"term":{"user_id": id}}
				]
			},
		},
		"size" : 0,
		"aggs":{
			"attitudes_count_sum":{
				"sum":{
					"field":"attitudes_count"
				}
			}
		}
	})['aggregations']['attitudes_count_sum']['value']

	user_attitudes_count = dict() 
	user_attitudes_count[id] = count

	with open('./stat_data/user_attitudes_count.json','a') as f:
		f.write(json.dumps(user_attitudes_count)+'\n')

	with open('./stat_data/user_attitudes_count.txt','a') as f:
		f.write(str(count)+'\n')
