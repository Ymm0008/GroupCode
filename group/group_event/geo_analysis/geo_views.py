#-*- coding: UTF-8 -*- 
from elasticsearch import Elasticsearch
import operator
import time 
import json
import os
import sys
from collections import Counter
import re
from flask import Blueprint, render_template, Flask,request

es = Elasticsearch("219.224.134.226:9209", timeout=600)

reload(sys)
sys.setdefaultencoding("utf-8")

mod = Blueprint("geo_analysis",__name__,url_prefix='/geo')

############################第一块：国内及国内词云图  国外及国外词云图
@mod.route('/province_static', methods=['POST','GET'])
def province_static():
	'''
	input in browser
	http://219.224.134.220:9283/geo/province_static?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3
	The items in [] is your input
	'''
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	domestic_anlysis_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'province_static')["_source"]["province_static"][1:]

	return json.dumps(domestic_anlysis_result[(page_number-1)*page_size:page_number*page_size])

@mod.route('/country_static', methods=['POST','GET'])
def country_static():
	# http://219.224.134.220:9283/geo/country_static?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	country_anlysis_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'country_static')["_source"]["country_static"]

	return json.dumps(country_anlysis_result[(page_number-1)*page_size:page_number*page_size])

@mod.route('/domestic_WordCloud', methods=['POST','GET'])
def domestic_WordCloud():
	# http://219.224.134.220:9283/geo/domestic_WordCloud?event_name=gangdu&date=2018-09-17
	event_name = request.args.get('event_name')
	date = request.args.get('date')

	domestic_WordCloud_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'domestic_WordCloud')["_source"]["domestic_WordCloud"]

	return json.dumps(domestic_WordCloud_result)

@mod.route('/abroad_WordCloud', methods=['POST','GET'])
def abroad_WordCloud():
	# http://219.224.134.220:9283/geo/abroad_WordCloud?event_name=gangdu&date=2018-09-17
	event_name = request.args.get('event_name')
	date = request.args.get('date')

	abroad_WordCloud_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'abroad_WordCloud')["_source"]["abroad_WordCloud"]

	return json.dumps(abroad_WordCloud_result)

###########################第二块：国内微博条数及代表微博  国外微博条数及代表微博
@mod.route('/domestic_repreblog_num', methods=['POST','GET'])
def domestic_repreblog_num():
	# http://219.224.134.220:9283/geo/domestic_repreblog_num?event_name=gangdu&date=2018-09-17
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	domestic_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'domestic_representitive_blog')["_source"]["domestic_representitive_blog"]

	return json.dumps(len(domestic_blog_result))

@mod.route('/domestic_representitive_blog', methods=['POST','GET'])
def domestic_representitive_blog():
	# http://219.224.134.220:9283/geo/domestic_representitive_blog?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	domestic_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'domestic_representitive_blog')["_source"]["domestic_representitive_blog"]

	return json.dumps(domestic_blog_result[(page_number-1)*page_size:page_number*page_size])

@mod.route('/abroad_repreblog_num', methods=['POST','GET'])
def abroad_repreblog_num():
	# http://219.224.134.220:9283/geo/abroad_repreblog_num?event_name=gangdu&date=2018-09-17
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	abroad_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'abroad_representitive_blog')["_source"]["abroad_representitive_blog"]

	return json.dumps(len(abroad_blog_result))

@mod.route('/abroad_representitive_blog', methods=['POST','GET'])
def abroad_representitive_blog():
	#http://219.224.134.220:9283/geo/abroad_representitive_blog?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	abroad_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'abroad_representitive_blog')["_source"]["abroad_representitive_blog"]

	return json.dumps(abroad_blog_result[(page_number-1)*page_size:page_number*page_size])

##########################第三块：传播迁徙的34个省份被转发数目
@mod.route('/blog_spread', methods=['POST','GET'])
def blog_spread():
	#http://219.224.134.220:9283/geo/blog_spread?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	spread_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'blog_spread')["_source"]["blog_spread"]

	return json.dumps(spread_blog_result[(page_number-1)*page_size:page_number*page_size])

####################第四块：传播迁徙的代表微博数目，及展示

'''
问题：每个地区 存入全部的数据/固定条数  本模块为全部数据
'''
@mod.route('/spread_blog_num', methods=['POST','GET'])
def spread_blog_num():
	# http://219.224.134.220:9283/geo/spread_blog_num?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3&geo_num=1&geo_name=%E5%8C%97%E4%BA%AC
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	geo_name=request.args.get('geo_name')  #与上面的地区联动,前端需返回表格的地区名称
	geo_num=request.args.get('geo_num') #与上面的地区联动,前端需返回表格的页码
	spread_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'representitive_blog_spread')\
	["_source"]["representitive_blog_spread"][int(geo_num)-1][geo_name]

	return json.dumps(len(spread_blog_result))

#######方案一
@mod.route('/representitive_blog_spread', methods=['POST','GET'])
def representitive_blog_spread():
	# http://219.224.134.220:9283/geo/representitive_blog_spread?event_name=gangdu&date=2018-09-17&page_number=1&page_size=3&geo_num=1&geo_name=%E5%8C%97%E4%BA%AC
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))
	geo_num=request.args.get('geo_num')  #与上面联动，前端需返回表格页码   
	geo_name=request.args.get('geo_name')  #与上面的地区联动,前端需返回表格的地区名称

	representitive_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'representitive_blog_spread')["_source"]["representitive_blog_spread"][int(geo_num)-1][geo_name]

	return json.dumps(representitive_blog_result[(page_number-1)*page_size:page_number*page_size])








# def get_init_mid(event_name):
# 	query_body={
# 		"query":{
# 		"bool":{
# 		"must":{
# 		"term":{
# 		"message_type":1
# 		}
# 		}
# 		}
# 		},"size":23392
# 	}
# 	ids=es.search(index=event_name,doc_type="text",body=query_body)\
# 	["hits"]["hits"]

# 	########### init_mid_lst为原创微博mid的列表
# 	init_mid_lst=[]
# 	for i in range(len(ids)):
# 		init_mid_lst.append(ids[i]["_source"]["mid"])

# 	return init_mid_lst

# def get_repost_num(init_mid_lst,event_name):
# 	########### 得到原创微博的转发量
# 	repst_dict=dict()
# 	for mid in init_mid_lst:
# 		count = es.count(index=event_name, doc_type="text", body={
# 			"query":{
# 				"bool":{
# 					"must":[
# 						{"term":{"message_type": 3}},
# 						{"term":{"root_mid": mid}}
# 					]
# 				}
# 			}
# 		})['count']
# 		repst_dict[mid]=count
# 	return repst_dict
# 	# print "repst_dict",len(repst_dict)
# def get_comment_num(init_mid_lst,event_name):

# 	########### 得到原创微博的评论量
# 	commt_dict=dict()
# 	for mid in init_mid_lst:
# 		count = es.count(index=event_name, doc_type="text", body={
# 			"query":{
# 				"bool":{
# 					"must":[
# 						{"term":{"message_type": 2}},
# 						{"term":{"root_mid": mid}}
# 					]
# 				}
# 			}
# 		})['count']
# 		commt_dict[mid]=count
# 	return commt_dict
# 	#########统计原创微博的转发数和评论数
# def repost_comment_compute(repst_dict,commt_dict):

# 	######## 定义一个新字典mid_lst,并将上面两个字典repst_dict和commt_dict聚合起来,得到每个原创帖子对应的转发数和评论数
# 	repost_comment_lst=[]
# 	for mid in repst_dict.keys():  #对于每一个mid
# 		influ_dict=dict()
# 		if commt_dict.get(mid)!=None:
# 			influ_dict["mid"]=mid
# 			influ_dict["repst"]=repst_dict[mid]
# 			influ_dict["commt"]=commt_dict[mid]
# 			repost_comment_lst.append(influ_dict)
# 	return repost_comment_lst

# 	######## 将结果中"commt"或"repst"不等于0的结果筛选出，并保存在result_lst列表中

# def get_result_lst(repost_comment_lst):
# 	result_lst=[]
# 	for i in repost_comment_lst:
# 		if i["commt"]!=0 or i["repst"]!=0:
# 			result_lst.append(i)
# 	return result_lst
# 	########## 计算影响力并排序，然后将各个结果保存到列表中
# def get_mid_influence(result_lst):

# 	mid_influence_lst = []
# 	for i in range(len(result_lst)):
# 		dict_influence = dict()
# 		a = int(result_lst[i]["repst"])
# 		b = int(result_lst[i]["commt"])
# 		total = a + b
# 		dict_influence['mid'] = result_lst[i]['mid']
# 		dict_influence['influence'] = total
# 		mid_influence_lst.append(dict_influence)

# 	mid_influence_lst = sorted(mid_influence_lst, key=operator.itemgetter('influence'), reverse=True)
# 	return mid_influence_lst

# 	########## 国内原创微博的代表微博,结果保存在final_lst中

# def rootmid_repost(event_name):
# 	query_body={
# 			"query":{
# 				"bool":{
# 					"must":{
# 						"term":{"message_type":3}
# 							}
# 						}
# 					},
# 			"size":23392,
# 			"aggs":{
# 			    "groupby_rootmid":{
# 			    "terms":{"field":"root_mid","size":6000},
# 			    "aggs":{"groupby_geo":{"terms":{"field":"geo","size":36}}}
# 			    }
# 			}
# 		}
# 	groupby_rootmid=es.search(index=event_name,doc_type="text",body=query_body)\
# 	["aggregations"]["groupby_rootmid"]["buckets"]

# 	mid_repst_dict=dict()
# 	for i in range(len(groupby_rootmid)):
# 		mid_name=groupby_rootmid[i]["key"]
# 		one_mid_list=groupby_rootmid[i]["groupby_geo"]["buckets"]
# 		dic2=dict()
# 		geo_name_lst=[u"北京",u"天津",u"上海",u"重庆",u"河北",u"山西",u"辽宁",u"吉林",
# 	u"黑龙江",u"江苏",u"浙江",u"安徽",u"福建",u"江西",u"山东",u"河南",u"湖北",
# 	u"湖南",u"广东",u"海南",u"四川",u"贵州",u"云南",u"陕西",u"甘肃",u"青海",u"台湾",
# 	u"内蒙古",u"广西",u"西藏",u"宁夏",u"新疆",u"香港",u"澳门"]

# 		for t in range(len(one_mid_list)):

# 			if one_mid_list[t]["key"] in geo_name_lst:
# 				dic2[one_mid_list[t]["key"]]=one_mid_list[t]["doc_count"]

# 		mid_repst_dict[mid_name]=dic2
# 	return mid_repst_dict
# def get_rootmid_geo(mid_lst,event_name):
# 	dic={}
# 	for i in mid_lst:
# 		query_body={
# 			"query":{
# 				"bool":{
# 					"must":{
# 						"term":{"mid":i}
# 							}
# 						}
# 					}
# 			    }
# 		mid_geo=es.search(index=event_name,doc_type="text",body=query_body)\
# 		["hits"]["hits"][0]["_source"]["geo"]

# 		geo_name_lst=[u"北京",u"天津",u"上海",u"重庆",u"河北",u"山西",u"辽宁",u"吉林",
# 	u"黑龙江",u"江苏",u"浙江",u"安徽",u"福建",u"江西",u"山东",u"河南",u"湖北",
# 	u"湖南",u"广东",u"海南",u"四川",u"贵州",u"云南",u"陕西",u"甘肃",u"青海",u"台湾",
# 	u"内蒙古",u"广西",u"西藏",u"宁夏",u"新疆",u"香港",u"澳门"]
		
# 		if re.split(r"&",mid_geo)[0]==u"中国" and (re.split(r"&",mid_geo)[1] in geo_name_lst):
# 			mid_geo=re.split(r"&",mid_geo)[1]
# 			dic.setdefault(mid_geo,[]).append(i)
# 	return dic
# def get_geo_geo_repost(dic,mid_repst_dict):
# 	result_lst=[]

# 	for item in dic.items():  #"beijing":["14525533","785112123"] type=1
# 		result_dict={}
# 		geo_dict=dict()
# 		geo=item[0]  #"beijing

# 		for mid in item[1]: #["14525533","785112123"]
# 			if mid in mid_repst_dict:  #{u'4238875130189335': {u'\u5e7f\u5dde': 18, u'\u5e7f\u4e1c': 49,
# 				result_dict=dict(Counter(result_dict)+Counter(mid_repst_dict[mid]))
# 		#排序result_dict
# 		# result_dict=sorted(result_dict, key=lambda k:result_dict.values(),reverse=True)
		
# 		geo_dict[geo]=result_dict
# 		result_lst.append(geo_dict)
# 	result_lst=sorted(result_lst, key=lambda k: sum(k.values()[0].values()),reverse=True)

# 	return json.dumps(result_lst) 

# def most_spead_weibo(mid_sorted_lst,event_name):
# 	final_lst=[]
# 	for i in range(len(mid_sorted_lst)):	
# 		dict1=dict()	
# 		query_body={"query":{
# 				"bool":{
# 					"must":
						
# 						{"term":{"mid":mid_sorted_lst[i]["mid"]}}
							
# 							}
# 						}
# 					}

# 		result=es.search(index=event_name,doc_type="text",body=query_body)["hits"]["hits"][0]["_source"]

# 		dict1["uid"]=result["uid"]
# 		dict1["text"]=result["text"]
# 		dict1["time"]=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(result["timestamp"]))
# 		dict1["keyword"]=result["keywords_string"]
# 		dict1["retweet_num"]=mid_sorted_lst[i]["repst"]
# 		dict1["comment_num"]=mid_sorted_lst[i]["commt"]
				
# 		final_lst.append(dict1)
# 	return final_lst


#########################第四块：传播迁徙的代表微博数目，及展示
#方案一
# @mod.route('/spread_blog_num', methods=['POST','GET'])
# def spread_blog_num():
# 	event_name = request.args.get('event_name')
# 	date = request.args.get('date')
# 	spread_blog_result=es.get(index = "geo_analysis", \
# 		doc_type = event_name, id = date,_source = 'representitive_blog_spread')["_source"]["representitive_blog_spread"]

# 	return json.dumps(len(spread_blog_result))


#######方案二
# @mod.route("/most_spead_weibo", methods=['POST','GET'])
# def get_repre_repost_weibo():
# 	#http://219.224.134.220:9239/geo/most_spead_weibo?geo=北京
# 	geo = request.args.get('geo')
# 	event_name = request.args.get('event_name')
# 	page_number = int(request.args.get('page_number'))
# 	page_size = int(request.args.get('page_size'))

# 	init_mid_lst=get_init_mid(event_name)
# 	mid_repst_dict=rootmid_repost(event_name)
# 	dic=get_rootmid_geo(init_mid_lst,event_name)
# 	'''
# 	此处如何与前端实现联动
# 	'''
# 	repst_dict=get_repost_num(dic[geo],event_name)
# 	sorted_mid_lst=sorted(repst_dict, key=repst_dict.__getitem__, reverse=True)  #对repst_dict的键按值进行排序，并将排序后的键以列表形式返回
	
# 	#获得代表内容
# 	repst_dict=get_repost_num(sorted_mid_lst,event_name)
# 	commt_dict=get_comment_num(sorted_mid_lst,event_name)
# 	repost_comment_lst=repost_comment_compute(repst_dict,commt_dict)

# 	#根据转发量排序
# 	repost_comment_lst=sorted(repost_comment_lst, key=lambda k:k["repst"], reverse=True)

# 	final_lst=most_spead_weibo(repost_comment_lst,event_name)
# 	return json.dumps(final_lst[(page_number-1)*page_size:page_number*page_size])

########方案三
#在es中存前【5】个地域的代表微博

