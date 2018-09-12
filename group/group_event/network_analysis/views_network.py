#-*- coding:utf-8 -*-

import os
import sys
import json
import time
from flask import Blueprint, render_template, request

from elasticsearch import Elasticsearch
es = Elasticsearch('219.224.134.226:9209', timeout=600)

reload(sys)
sys.setdefaultencoding("utf-8")

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('network_analysis', __name__, url_prefix='/network')   # url_prefix = '/test'  增加相对路径的前缀

@mod.route('/set_page', methods=['POST','GET']) 
def set_page(list1, page_number, page_size):              #分页函数 list1指等待被分页的列表
    start_from = (int(page_number) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)
    return list1[start_from:end_at]

@mod.route('/influence_rank_of_men', methods=['POST','GET'])
def influence_rank_of_men():
    event_name = request.args.get('event_name')
    date = request.args.get('date')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    list_of_men = es.get(index = 'network_analysis', doc_type = event_name, id = date,\
                        _source = 'man_rank')['_source']['man_rank']
    list_of_men_11 = set_page(list_of_men, page_number, page_size)
    return json.dumps(list_of_men_11)

@mod.route('/influence_rank_of_media', methods=['POST','GET'])
def influence_rank_of_media():
    event_name = request.args.get('event_name')
    date = request.args.get('date')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    list_of_media = es.get(index = 'network_analysis', doc_type = event_name, \
                            id = date, _source = 'media_rank')['_source']['media_rank']
    media_list = set_page(list_of_media, page_number, page_size) 
    return json.dumps(media_list)

@mod.route('/representitive_blog_of_men', methods=['POST','GET']) 
def representitive_blog_of_men():
    event_name = request.args.get('event_name')
    date = request.args.get('date')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    representitive_blog_of_men = es.get(index = 'network_analysis', doc_type = event_name,\
                                        id = date, _source = 'representitive_blog_of_men')\
                                        ['_source']['representitive_blog_of_men']
    representitive_blog_of_men_1 = set_page(representitive_blog_of_men, page_number, page_size)
    return json.dumps(representitive_blog_of_men_1)

@mod.route('/representitive_blog_of_meida', methods=['POST','GET'])
def representitive_blog_of_media():
    event_name = request.args.get('event_name')
    date = request.args.get('date')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')
    representitive_blog_of_media = es.get(index = 'network_analysis', doc_type = event_name,\
                                        id = date, _source = 'representitive_blog_of_media')\
                                        ['_source']['representitive_blog_of_media']
    representitive_blog_of_media_1 = set_page(representitive_blog_of_media, page_number, page_size)
    return json.dumps(representitive_blog_of_media_1)

@mod.route('/related_men', methods=['POST','GET']) 
def related_men(): 
    event_name = request.args.get('event_name')
    date = request.args.get('date')
    page_number = request.args.get('page_number')
    related_men = es.get(index = 'network_analysis', doc_type = event_name, id = date,\
                        _source = 'related_men')['_source']['related_men']
    related_men_1 = set_page(related_men, page_number, 1)
    return json.dumps(related_men_1)

@mod.route('/representitive_blog_of_related_men', methods=['POST','GET']) 
def representitive_blog_of_related_men():
    event_name = request.args.get('event_name')
    date = request.args.get('date')
    page_number = request.args.get('page_number')
    representitive_blog_of_related_men = es.get(index = 'network_analysis', doc_type = event_name,\
                                                id = date, _source = 'representitive_blog_of_related_men')\
                                                ['_source']['representitive_blog_of_related_men']
    representitive_blog_of_related_men_1 = set_page(representitive_blog_of_related_men, page_number, 1)
    representitive_blog_of_related_men_2 = representitive_blog_of_related_men_1[0]['related_men']
    return json.dumps(representitive_blog_of_related_men_2)