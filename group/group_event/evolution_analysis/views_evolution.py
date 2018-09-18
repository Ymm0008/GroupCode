#-*- coding:utf-8 -*-

import os
import sys
import json
import time
from flask import Blueprint, render_template, request
from risk_evolution_output_module import curve_output_for_frontend
from risk_evolution_output_module import content_output_for_frontend

from elasticsearch import Elasticsearch
es = Elasticsearch('219.224.134.226:9209', timeout = 600)

reload(sys)
sys.setdefaultencoding("utf-8")

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('evolution_analysis', __name__, url_prefix='/evolution')   # url_prefix = '/test'  增加相对路径的前缀


@mod.route('/curve_output', methods=['POST','GET'])
def curve_output():

    event_name = request.args.get('event_name')
    evolution_and_key_user, heat_and_emotion = curve_output_for_frontend(event_name)

    return json.dumps(evolution_and_key_user), json.dumps(heat_and_emotion)


@mod.route('/risk_details_output', methods=['POST','GET'])
def risk_details_output():

    event_name = request.args.get('event_name')
    timestamp = request.args.get('timestamp')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')

    content_result = content_output_for_frontend(event_name, timestamp，page_number, page_size)  
    
    return json.dumps(content_result)
