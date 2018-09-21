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
es1 = Elasticsearch("127.0.0.1:9200", timeout=600)


##############第一块，统计国内外次数、词云图
def cal_geo():
	'''
	calculate event's heat in domestic provinces,and get top[10] provinces
	''' 
	geo_list=["北京","天津","上海","重庆","河北","山西","辽宁","吉林",
	"黑龙江","江苏","浙江","安徽","福建","江西","山东","河南","湖北",
	"湖南","广东","海南","四川","贵州","云南","陕西","甘肃","青海","台湾",
	"内蒙古","广西","西藏","宁夏","新疆","香港","澳门"]

	geo_lst=[]
	query_body = {
		"query":{
			"bool":{
				"must":{
					"terms":{"geo":geo_list}
						}
					}
				},
				"size":0,
				"aggs":{"geo_aggs":{"terms":{"field":"geo","size":10}}}
			}
	
	geo_lst = es.search(index='flow_text_gangdu',doc_type='text',body=query_body)["aggregations"]["geo_aggs"]["buckets"]
	return geo_lst	
def cal_country():
	'''
	calculate event's heat abroad,and get top[10]countries
	''' 
	geo_list=["北京","天津","上海","重庆","河北","山西","辽宁","吉林",\
	"黑龙江","江苏","浙江","安徽","福建","江西","山东","河南","湖北",\
	"湖南","广东","海南","四川","贵州","云南","陕西","甘肃","青海","台湾",\
	"内蒙古","广西","西藏","宁夏","新疆","香港","澳门"]
	geo_list=geo_list+["中国","局域网"]

	query_body = {
	"query":{
		"bool":{
			"must_not":{
				"terms":{
					"geo":geo_list
					}
				}
			}
		},
	"size":0,
	"aggs":{
		"group_by_country":{
			"terms":{"field":"geo","size":10}
				}
			}
	}

	country_lst = es.search(index='flow_text_gangdu',doc_type='text',\
				body=query_body)["aggregations"]["group_by_country"]["buckets"]
	return country_lst	

def get_domestic_WordCloud():   
	'''
	get top[100] most mentioned words
	'''
	
	geo_list=["北京","天津","上海","重庆","河北","山西","辽宁","吉林",\
	"黑龙江","江苏","浙江","安徽","福建","江西","山东","河南","湖北",\
	"湖南","广东","海南","四川","贵州","云南","陕西","甘肃","青海","台湾",\
	"内蒙古","广西","西藏","宁夏","新疆","香港","澳门"]
	
	query_body = {
	"query":{
		"bool":{
			"must":{
				"terms":{
					"geo":geo_list
					}
				}
			}
		},
	"size":0,
	"aggs":{
		"group_by_word":{
			"terms":{"field":"keywords_string","size":100}
				}
			}
		}
	domestic_WordCloud = es.search(index='flow_text_gangdu',doc_type='text',body=query_body)["aggregations"]["group_by_word"]["buckets"]
	return domestic_WordCloud
def get_abroad_WordCloud():
	'''
	get top[100] most mentioned words
	'''
	
	geo_list=["北京","天津","上海","重庆","河北","山西","辽宁","吉林",\
	"黑龙江","江苏","浙江","安徽","福建","江西","山东","河南","湖北",\
	"湖南","广东","海南","四川","贵州","云南","陕西","甘肃","青海","台湾",\
	"内蒙古","广西","西藏","宁夏","新疆","香港","澳门","中国","局域网"]
	
	query_body = {
	"query":{
		"bool":{
			"must_not":{
				"terms":{
					"geo":geo_list
					}
				}
			}
		},
	"size":0,
	"aggs":{
		"group_by_word":{
			"terms":{"field":"keywords_string","size":100}
				}
			}
	}

	abroad_WordCloud = es.search(index='flow_text_gangdu',doc_type='text',body=query_body)["aggregations"]["group_by_word"]["buckets"]
	return abroad_WordCloud


##############第二块,得到国内外的代表微博 
def get_init_mid():
	'''
	get given[size] init_mid
	'''
	query_body={
		"query":{
		"bool":{
		"must":{
		"term":{
		"message_type":1
		}
		}
		}
		},"size":23392
	}
	ids=es.search(index="flow_text_gangdu",doc_type="text",body=query_body)\
	["hits"]["hits"]

	########### init_mid_lst为原创微博mid的列表
	init_mid_lst=[]
	for i in range(len(ids)):
		init_mid_lst.append(ids[i]["_source"]["mid"])

	return init_mid_lst

def get_repost_num(init_mid_lst):
	########### 得到原创微博的转发量
	repst_dict=dict()
	for mid in init_mid_lst:
		count = es.count(index='flow_text_gangdu', doc_type="text", body={
			"query":{
				"bool":{
					"must":[
						{"term":{"message_type": 3}},
						{"term":{"root_mid": mid}}
					]
				}
			}
		})['count']
		repst_dict[mid]=count
	return repst_dict
	# print "repst_dict",len(repst_dict)
def get_comment_num(init_mid_lst):

	########### 得到原创微博的评论量
	commt_dict=dict()
	for mid in init_mid_lst:
		count = es.count(index='flow_text_gangdu', doc_type="text", body={
			"query":{
				"bool":{
					"must":[
						{"term":{"message_type": 2}},
						{"term":{"root_mid": mid}}
					]
				}
			}
		})['count']
		commt_dict[mid]=count
	return commt_dict
	#########统计原创微博的转发数和评论数
def repost_comment_compute(repst_dict,commt_dict):

	######## 定义一个新字典mid_lst,并将上面两个字典repst_dict和commt_dict聚合起来,得到每个原创帖子对应的转发数和评论数
	repost_comment_lst=[]
	for mid in repst_dict.keys():  #对于每一个mid
		influ_dict=dict()
		if commt_dict.get(mid)!=None:
			influ_dict["mid"]=mid
			influ_dict["repst"]=repst_dict[mid]
			influ_dict["commt"]=commt_dict[mid]
			repost_comment_lst.append(influ_dict)
	return repost_comment_lst

	######## 将结果中"commt"或"repst"不等于0的结果筛选出，并保存在result_lst列表中
def get_result_lst(repost_comment_lst):
	result_lst=[]
	for i in repost_comment_lst:
		if i["commt"]!=0 or i["repst"]!=0:
			result_lst.append(i)
	return result_lst
	########## 计算影响力并排序，然后将各个结果保存到列表中
def get_mid_influence(result_lst):

	mid_influence_lst = []
	for i in range(len(result_lst)):
		dict_influence = dict()
		a = int(result_lst[i]["repst"])
		b = int(result_lst[i]["commt"])
		total = a + b
		dict_influence['mid'] = result_lst[i]['mid']
		dict_influence['influence'] = total
		mid_influence_lst.append(dict_influence)

	mid_influence_lst = sorted(mid_influence_lst, key=operator.itemgetter('influence'), reverse=True)
	return mid_influence_lst

	########## 国内原创微博的代表微博,结果保存在final_lst中

'''
指定size 和 page_num , 返回result_lst 的长度给前端
'''
def domestic_repre_weibo(mid_influence_lst,result_lst):  #p.s. 若要规定返回的条数，可以增加参数sizes

	final_lst=[]
	for i in range(len(mid_influence_lst)):
		dict1=dict()
		########### 1.得到国内原创帖子	
		query_body={"query":{
				"bool":{
					"must":[
						{"term":{"geo":u"中国"}},
						{"term":{"mid":mid_influence_lst[i]["mid"]}}
							]
							}
						}
					}

		if es.search(index="flow_text_gangdu",doc_type="text",body=query_body)["hits"]["hits"]==[]:
			pass
		else:
			result=es.search(index="flow_text_gangdu",doc_type="text",body=query_body)["hits"]["hits"][0]["_source"]

			dict1["uid"]=result["uid"]
			dict1["text"]=result["text"]
			# dict1["time"]=result["timestamp"]
			dict1["time"]=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(result["timestamp"]))
			dict1["keyword"]=result["keywords_string"]

			for item in range(len(result_lst)):
				if result_lst[item]["mid"]==mid_influence_lst[i]["mid"]:
					dict1["retweet_num"]=result_lst[item]["repst"]
					dict1["comment_num"]=result_lst[item]["commt"]
					break
			final_lst.append(dict1)
	return final_lst
	########## 国外原创微博的代表微博,结果保存在final_lst中
def abroad_repre_weibo(mid_influence_lst,result_lst):

	final_lst=[]
	for i in range(len(mid_influence_lst)):	
		dict1=dict()
		############ 2.得到国外原创帖子
		query_body={"query":{
				"bool":{
					"must_not":{"terms":{"geo":[u"中国",u"局域网"]}},
						
					"must":	{"term":{"mid":mid_influence_lst[i]["mid"]}}
					
							
							}
						}
					}

		if es.search(index="flow_text_gangdu",doc_type="text",body=query_body)["hits"]["hits"]==[]:
			pass
		else:
			result=es.search(index="flow_text_gangdu",doc_type="text",body=query_body)["hits"]["hits"][0]["_source"]

			dict1["uid"]=result["uid"]
			dict1["text"]=result["text"]
			# dict1["time"]=result["timestamp"]
			dict1["time"]=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(result["timestamp"]))
			dict1["keyword"]=result["keywords_string"]

			for item in range(len(result_lst)):
				if result_lst[item]["mid"]==mid_influence_lst[i]["mid"]:
					dict1["retweet_num"]=result_lst[item]["repst"]
					dict1["comment_num"]=result_lst[item]["commt"]
					break
			final_lst.append(dict1)
	return final_lst


#############第三块,得到原创帖子被转发地域、原创帖子地域、地域之间的转发关系
def rootmid_repost():
	query_body={
			"query":{
				"bool":{
					"must":{
						"term":{"message_type":3}
							}
						}
					},
			"size":0,
			"aggs":{
			    "groupby_rootmid":{
			    "terms":{"field":"root_mid","size":6000},   #指定原创帖子数量
			    "aggs":{"groupby_geo":{"terms":{"field":"geo","size":36}}}
			    }
			}
		}
	groupby_rootmid=es.search(index="flow_text_gangdu",doc_type="text",body=query_body)\
	["aggregations"]["groupby_rootmid"]["buckets"]

	mid_repst_dict=dict()
	for i in range(len(groupby_rootmid)):
		mid_name=groupby_rootmid[i]["key"]
		one_mid_list=groupby_rootmid[i]["groupby_geo"]["buckets"]
		dic2=dict()
		geo_name_lst=[u"北京",u"天津",u"上海",u"重庆",u"河北",u"山西",u"辽宁",u"吉林",
	u"黑龙江",u"江苏",u"浙江",u"安徽",u"福建",u"江西",u"山东",u"河南",u"湖北",
	u"湖南",u"广东",u"海南",u"四川",u"贵州",u"云南",u"陕西",u"甘肃",u"青海",u"台湾",
	u"内蒙古",u"广西",u"西藏",u"宁夏",u"新疆",u"香港",u"澳门"]

		for t in range(len(one_mid_list)):

			if one_mid_list[t]["key"] in geo_name_lst:
				dic2[one_mid_list[t]["key"]]=one_mid_list[t]["doc_count"]

		mid_repst_dict[mid_name]=dic2
	return mid_repst_dict

def get_rootmid_geo(mid_lst):
	dic={}
	for i in mid_lst:
		query_body={
			"query":{
				"bool":{
					"must":{
						"term":{"mid":i}
							}
						}
					}
			    }
		mid_geo=es.search(index="flow_text_gangdu",doc_type="text",body=query_body)\
		["hits"]["hits"][0]["_source"]["geo"]

		geo_name_lst=[u"北京",u"天津",u"上海",u"重庆",u"河北",u"山西",u"辽宁",u"吉林",
	u"黑龙江",u"江苏",u"浙江",u"安徽",u"福建",u"江西",u"山东",u"河南",u"湖北",
	u"湖南",u"广东",u"海南",u"四川",u"贵州",u"云南",u"陕西",u"甘肃",u"青海",u"台湾",
	u"内蒙古",u"广西",u"西藏",u"宁夏",u"新疆",u"香港",u"澳门"]
		
		if re.split(r"&",mid_geo)[0]==u"中国" and (re.split(r"&",mid_geo)[1] in geo_name_lst):
			mid_geo=re.split(r"&",mid_geo)[1]
			dic.setdefault(mid_geo,[]).append(i)
	return dic

'''
返回result_lst的总长度给前端，并默认返回前5条数据
'''
def get_geo_geo_repost(dic,mid_repst_dict):
	result_lst=[]

	for item in dic.items():  #"beijing":["14525533","785112123"] type=1
		result_dict={}
		geo_dict=dict()
		geo=item[0]  #"beijing

		for mid in item[1]: #["14525533","785112123"]
			if mid in mid_repst_dict:  #{u'4238875130189335': {u'\u5e7f\u5dde': 18, u'\u5e7f\u4e1c': 49,
				result_dict=dict(Counter(result_dict)+Counter(mid_repst_dict[mid]))
		#排序result_dict
		# result_dict=sorted(result_dict, key=lambda k:result_dict.values(),reverse=True)
		
		geo_dict[geo]=result_dict
		result_lst.append(geo_dict)
	result_lst=sorted(result_lst, key=lambda k: sum(k.values()[0].values()),reverse=True)

	return result_lst

'''
返回总条数，默认返回五条代表微博
'''

#############第四块,得到转发量最多的代表微博
def repre_weibo_content(mid_sorted_lst):
	final_lst=[]
	for i in range(len(mid_sorted_lst)):	
		dict1=dict()	
		query_body={"query":{
				"bool":{
					"must":
						
						{"term":{"mid":mid_sorted_lst[i]["mid"]}}
							
							}
						}
					}

		result=es.search(index="flow_text_gangdu",doc_type="text",body=query_body)["hits"]["hits"][0]["_source"]

		dict1["uid"]=result["uid"]
		dict1["text"]=result["text"]
		dict1["time"]=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(result["timestamp"]))
		dict1["keyword"]=result["keywords_string"]
		dict1["retweet_num"]=mid_sorted_lst[i]["repst"]
		dict1["comment_num"]=mid_sorted_lst[i]["commt"]
				
		final_lst.append(dict1)
	return final_lst

def get_repre_spread_weibo(result_lst,dic):

	global weibo_all
	weibo_all=[]

	for i in range(len(result_lst)):
		if i<=10:   #指定地区size
			weibo_geo_dict=dict()

			geo=result_lst[i].keys()[0]

			#排序
			repst_dict=get_repost_num(dic[geo])
			sorted_mid_lst=sorted(repst_dict, key=repst_dict.__getitem__, reverse=True)  #对repst_dict的键按值进行排序，并将排序后的键以列表形式返回

			#获得代表内容
			repst_dict=get_repost_num(sorted_mid_lst)
			commt_dict=get_comment_num(sorted_mid_lst)
			repost_comment_lst=repost_comment_compute(repst_dict,commt_dict)

			#根据转发量排序
			repost_comment_lst=sorted(repost_comment_lst, key=lambda k:k["repst"], reverse=True)
			
			repre_weibo_content1=repre_weibo_content(repost_comment_lst[0:10])       #指定size，每个地区代表微博数目
			
			weibo_geo_dict[geo]=repre_weibo_content1
			weibo_all.append(weibo_geo_dict)
		else:
			break
	return weibo_all

#############第五块，将地域分析的数据结果存到es中
def save_geo_analyze(province_static,
	country_static,
	domestic_WordCloud,
	abroad_WordCloud,\
	domestic_repre_weibo,abroad_repre_weibo,\
	result_lst,weibo_all
	):   
	index_info = {
		"settings": {
			"number_of_shards": 5, 	
			"number_of_replicas":0, 
			"analysis":{ 
				"analyzer":{
					"my_analyzer":{
						"type":"pattern",
						"patern":"&"
					}
				}
			}
		},
		"mappings":{
			"geo_analysis":{
				"properties":{
				    "@timestamp":{
	                        "type" : "date",
		                    "index" : "not_analyzed",
		                    "doc_values" : "true",
		                    "format" : "dd/MM/YYYY:HH:mm:ss Z"
	                },
	                
					"province_static":{ 							
						"type":"object"
						
					},								
					"country_static":{
						"type":"object"
						
					},
					"domestic_WordCloud":{
					    "type":"object"
					},
					"abroad_WordCloud":{
					    "type":"object"
					},
					"domestic_representitive_blog":{
					    "type":"object"
					},
					"abroad_representitive_blog":{
					    "type":"object"
					},
					"blog_spread":{
					    "type":"object"
					}
					,"representitive_blog_spread":{
					"type":"object"
					}
				}
			}
		}
	}

	# 判断索引存不存在
	if not es.indices.exists(index='geo_analysis'): 
		es.indices.create(index='geo_analysis', body=index_info, ignore=400)
	es.index(index='geo_analysis',doc_type='gangdu',id=time.strftime("%Y-%m-%d",time.localtime()),
		body={
		"save_timestamp":int(time.time()),
		"province_static":province_static,
		"country_static":country_static,
		"domestic_WordCloud":domestic_WordCloud,
		"abroad_WordCloud":abroad_WordCloud,
		"domestic_representitive_blog":domestic_repre_weibo,
		"abroad_representitive_blog":abroad_repre_weibo,
		"blog_spread":result_lst,
		"representitive_blog_spread":weibo_all
				})


if __name__=="__main__":
	es = Elasticsearch("219.224.134.226:9209", timeout=600)
	es1 = Elasticsearch("127.0.0.1:9200", timeout=600)

	##########part_1
	province_static=cal_geo()
	country_static=cal_country()
	domestic_WordCloud=get_domestic_WordCloud()
	abroad_WordCloud=get_abroad_WordCloud()	


	#########part_2
	init_mid_lst=get_init_mid()
	repst_dict=get_repost_num(init_mid_lst)
	commt_dict=get_comment_num(init_mid_lst)
	repost_comment_lst=repost_comment_compute(repst_dict,commt_dict)

	result_lst=get_result_lst(repost_comment_lst)
	mid_influence_lst=get_mid_influence(result_lst)

	domestic_repre_weibo=domestic_repre_weibo(mid_influence_lst,result_lst)
	abroad_repre_weibo=abroad_repre_weibo(mid_influence_lst,result_lst)


	#########part_3
	mid_repst_dict=rootmid_repost()
	
	dic=get_rootmid_geo(init_mid_lst)

	result_lst=get_geo_geo_repost(dic,mid_repst_dict)

	########part_4

	'''
	方案二 指定地区size，存储size个地区的代表微博
	'''
	weibo_all=get_repre_spread_weibo(result_lst,dic)
	
	########part_5 
	save_geo_analyze(province_static,country_static,domestic_WordCloud,
		abroad_WordCloud,domestic_repre_weibo,abroad_repre_weibo,\
		result_lst,weibo_all) 

	'''
	方案一 存某一地区的代表微博
	'''

	# #排序
	# repst_dict=get_repost_num(dic[u"北京"])
	# sorted_mid_lst=sorted(repst_dict, key=repst_dict.__getitem__, reverse=True)  #对repst_dict的键按值进行排序，并将排序后的键以列表形式返回

	# #获得代表内容
	# repst_dict=get_repost_num(sorted_mid_lst)
	# commt_dict=get_comment_num(sorted_mid_lst)
	# repost_comment_lst=repost_comment_compute(repst_dict,commt_dict)

	# #根据转发量排序
	# repost_comment_lst=sorted(repost_comment_lst, key=lambda k:k["repst"], reverse=True)

	# repre_weibo_content=repre_weibo_content(repost_comment_lst)
	# # print repre_weibo_content