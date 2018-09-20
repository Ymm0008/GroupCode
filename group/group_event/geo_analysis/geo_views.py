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
@mod.route('/geo_static', methods=['POST','GET'])
def province_static():
	# 国内：http://219.224.134.220:9283/geo/geo_static?geo_static=province_static&event_name=gangdu&date=2018-09-18&page_number=1&page_size=5
	# 国内：http://219.224.134.220:9283/geo/geo_static?geo_static=country_static&event_name=gangdu&date=2018-09-18&page_number=1&page_size=5
	geo=request.args.get('geo_static')
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))
	if geo=="province_static":
		geo_static_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'province_static')["_source"][geo][1:]
	elif geo=="country_static":
		geo_static_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,_source = 'country_static')["_source"][geo]
	return json.dumps(geo_static_result[(page_number-1)*page_size:page_number*page_size])

@mod.route('/WordCloud', methods=['POST','GET'])
def WordCloud():
	# http://219.224.134.220:9283/geo/WordCloud?geo_WordCloud=domestic_WordCloud&event_name=gangdu&date=2018-09-18
	# http://219.224.134.220:9283/geo/WordCloud?geo_WordCloud=abroad_WordCloud&event_name=gangdu&date=2018-09-18
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	geo_WordCloud=request.args.get('geo_WordCloud')
	if geo_WordCloud=='domestic_WordCloud':
		WordCloud_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date)["_source"][geo_WordCloud]
	elif geo_WordCloud=='abroad_WordCloud':
		WordCloud_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date)["_source"][geo_WordCloud]
	return json.dumps(WordCloud_result)


###########################第二块：国内微博条数及代表微博  国外微博条数及代表微博
@mod.route('/representitive_blog1', methods=['POST','GET'])
def representitive_blog1():
	# http://219.224.134.220:9283/geo/representitive_blog1?geo_representitive_blog=domestic_representitive_blog&event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	# http://219.224.134.220:9283/geo/representitive_blog1?geo_representitive_blog=abroad_representitive_blog&event_name=gangdu&date=2018-09-18&page_number=1&page_size=3
	blog_dict=dict()

	geo_representitive_blog=request.args.get('geo_representitive_blog')
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size'))

	if geo_representitive_blog=="domestic_representitive_blog":
		blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date,)["_source"][geo_representitive_blog]
	elif geo_representitive_blog=="abroad_representitive_blog":
		blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date)["_source"][geo_representitive_blog]

	blog_len=len(blog_result)

	blog_dict["weibo_length"]=blog_len
	blog_dict["weibo_content"]=blog_result[(page_number-1)*page_size:page_number*page_size]
	return json.dumps(blog_dict)


##########################第三块：传播迁徙的34个省份被转发数目
@mod.route('/geo_spread', methods=['POST','GET'])
def geo_spread():
	#http://219.224.134.220:9283/geo/geo_spread?geo_name=北京&event_name=gangdu&date=2018-09-18
	event_name = request.args.get('event_name')
	date = request.args.get('date')
	geo_name=request.args.get('geo_name')

	spread_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date)["_source"]["blog_spread"][0][geo_name]
	return json.dumps(spread_blog_result)


####################第四块：传播迁徙的代表微博数目，及展示
@mod.route('/representitive_blog2', methods=['POST','GET'])
def representitive_blog_spread():
	# http://219.224.134.220:9283/geo/representitive_blog2?event_name=gangdu&date=2018-09-18&page_number=1&page_size=3&geo_name=%E5%8C%97%E4%BA%AC
	representitive_blog_dict=dict()

	event_name = request.args.get('event_name')
	date = request.args.get('date')
	geo_name=request.args.get('geo_name')  #与上面的地区联动,前端需返回表格的地区名称
	page_number = int(request.args.get('page_number'))
	page_size = int(request.args.get('page_size')) 

	representitive_blog_result=es.get(index = "geo_analysis", \
		doc_type = event_name, id = date)["_source"]["representitive_blog_spread"][geo_name]
	
	representitive_blog_dict["geo_blog_len"]=len(representitive_blog_result)
	representitive_blog_dict["geo_blog_content"]=representitive_blog_result[(page_number-1)*page_size:page_number*page_size]
	return json.dumps(representitive_blog_dict)