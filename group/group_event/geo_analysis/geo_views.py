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
	# http://219.224.134.220:9283/geo/province_static?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	domestic_anlysis_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'province_static')["_source"]["province_static"][1:]

	return json.dumps(domestic_anlysis_result[(page_number-1)*page_size:page_number*page_size])

@mod.route('/country_static', methods=['POST','GET'])
def country_static():
	# http://219.224.134.220:9283/geo/country_static?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	country_anlysis_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'country_static')["_source"]["country_static"]

	return json.dumps(country_anlysis_result[(page_number-1)*page_size:page_number*page_size])

@mod.route('/domestic_WordCloud', methods=['POST','GET'])
def domestic_WordCloud():
	# http://219.224.134.220:9283/geo/domestic_WordCloud?event_name=gangdu&date=2018-09-18
	event_name = request.args.get('event_name')
	date = request.args.get('date')

	domestic_WordCloud_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'domestic_WordCloud')["_source"]["domestic_WordCloud"]

	return json.dumps(domestic_WordCloud_result)

@mod.route('/abroad_WordCloud', methods=['POST','GET'])
def abroad_WordCloud():
	# http://219.224.134.220:9283/geo/abroad_WordCloud?event_name=gangdu&date=2018-09-18
	event_name = request.args.get('event_name')
	date = request.args.get('date')

	abroad_WordCloud_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'abroad_WordCloud')["_source"]["abroad_WordCloud"]

	return json.dumps(abroad_WordCloud_result)

###########################第二块：国内微博条数及代表微博  国外微博条数及代表微博

@mod.route('/domestic_representitive_blog', methods=['POST','GET'])
def domestic_representitive_blog():
	domestic_blog_dict=dict()
	# http://219.224.134.220:9283/geo/domestic_representitive_blog?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	domestic_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'domestic_representitive_blog')["_source"]["domestic_representitive_blog"]
	domestic_blog_len=len(domestic_blog_result)

	domestic_blog_dict["weibo_length"]=domestic_blog_len
	domestic_blog_dict["weibo_content"]=domestic_blog_result[(page_number-1)*page_size:page_number*page_size]
	return json.dumps(domestic_blog_dict)

@mod.route('/abroad_representitive_blog', methods=['POST','GET'])
def abroad_representitive_blog():
	#http://219.224.134.220:9283/geo/abroad_representitive_blog?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	abroad_blog_dict=dict()
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	abroad_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'abroad_representitive_blog')["_source"]["abroad_representitive_blog"]
	
	abroad_blog_len=len(abroad_blog_result)

	abroad_blog_dict["weibo_length"]=abroad_blog_len
	abroad_blog_dict["weibo_content"]=abroad_blog_result[(page_number-1)*page_size:page_number*page_size]
	return json.dumps(abroad_blog_dict)

##########################第三块：传播迁徙的34个省份被转发数目
@mod.route('/geo_spread', methods=['POST','GET'])
def geo_spread():
	#http://219.224.134.220:9283/geo/geo_spread?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	spread_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'blog_spread')["_source"]["blog_spread"]

	return json.dumps(spread_blog_result[(page_number-1)*page_size:page_number*page_size])

####################第四块：传播迁徙的代表微博数目，及展示

@mod.route('/representitive_blog_spread', methods=['POST','GET'])
def representitive_blog_spread():
	# http://219.224.134.220:9283/geo/representitive_blog_spread?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3&geo_name=%E5%8C%97%E4%BA%AC
	representitive_blog_dict=dict()

	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size')) 
	geo_name=request.args.get('geo_name')  #与上面的地区联动,前端需返回表格的地区名称

	representitive_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'representitive_blog_spread')["_source"]["representitive_blog_spread"][geo_name]
	representitive_blog_dict["geo_blog_len"]=len(representitive_blog_result)
	representitive_blog_dict["geo_blog_content"]=representitive_blog_result[(page_number-1)*page_size:page_number*page_size]

	return json.dumps(representitive_blog_dict)
