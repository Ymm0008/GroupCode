#-*- coding:utf-8 -*-

import os
import sys
import json
import time
from flask import Blueprint, render_template, request

from elasticsearch import Elasticsearch
es = Elasticsearch("219.224.134.220:7200", timeout=600)

reload(sys)
sys.setdefaultencoding("utf-8")

AB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../public/')
sys.path.append(AB_PATH)

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('plot', __name__, url_prefix='/test')   # url_prefix = '/test'  增加相对路径的前缀
 
@mod.route('/')
def index():
    """返回页面
    """
    return render_template('index.html')


def data_read(uid):
	query_body = {
	    "query":{
	    	"bool":{
	    		"must":[
	    			{"term":{"uid":uid}}
	    		]
	    	}
	    },
	    "size": 25000
	}
	result = es.search(index="social_sensing_text", doc_type="text", body=query_body)["hits"]["hits"]   

	return result

# uid： 2286908003

@mod.route('/get', methods=['GET']) 
def get_calculate():
    uid = request.args.get('uid')
    result = data_read(uid)

    return json.dumps(result)


@mod.route('/post', methods=['POST']) 
def post_calculate():
    list = request.get_json()
    for term in list:
    	print term

    return json.dumps({'status':'success'})
