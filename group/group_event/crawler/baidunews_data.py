# -*- coding: utf-8 -*-

import re
import time
import datetime
import json
import chardet
import sys
import os
import requests
from lxml import etree
import urllib
from elasticsearch import Elasticsearch
from readability.readability import Document

reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append("../")
from global_utils import es_data_text


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36',
}

def get_baidunews_num(keywords, start_time, end_time):
    url = get_url(keywords, start_time, end_time)
    response = requests.get(url=url, headers=headers, timeout=10)
    response.encoding = 'utf-8'
    html = response.text
    tree = etree.HTML(html)
    a = tree.xpath('string(//div[@id="header_top_bar"]//span[@class="nums"])')
    total_num = re.findall(r'(\d*,?\d+)', a)[0]
    return int(str(total_num).replace(',', ''))

def get_baidunews_data(keywords, start_time, end_time, index_name, type_name):

    pn = 0
    start_url = get_url(keywords, start_time, end_time)
    while 1:
        url = start_url + '&pn=%s' % pn
        try:
            response = requests.get(url=url, headers=headers, timeout=10)
        except:
            pn += 50
            time.sleep(2)
            continue
        response.encoding = 'utf-8'
        html = response.text
        tree = etree.HTML(html)

        result = tree.xpath('//*[@class="result"]')
        for div in result:
            item = {}
            item['key_word'] = keywords.decode('utf-8')
            item['news_title'] = div.xpath('string(./*[@class="c-title"]/a)').replace(' ', '').replace('\n', '')
            news_url = div.xpath('./*[@class="c-title"]/a/@href')[0]
            author = div.xpath(
                'string(.//*[@class="c-author"])').split(u'\xa0\xa0')
            # print author[0].decode('utf-8')
            if len(author) == 2:
                item['news_source'] = author[0].replace(' ', '')

                item['news_date'] = trans_time(
                    author[1].replace('\t', '').replace('\n', ''))
            else:
                item['news_source'] = 'null'
                item['news_date'] = trans_time(
                    author[0].replace('\t', '').replace('\n', ''))
            item['abstract'] = ''.join(div.xpath(
                './/*[@class="c-author"]/../text() | .//*[@class="c-author"]/../em/text()')).replace(' ', '').replace('\r', '').replace('\n', '')

            es_data_text.index(index=index_name, doc_type=type_name, id=news_url, body=item)
            print 'baidunews_data successful insert es'
        try:
            next_page = tree.xpath('//p[@id="page"]/a[last()]/text()')[0]
            if next_page != '下一页>':
                print('no next page')
                break
        except:
            print('no next page')
            break
        time.sleep(2)
        pn += 50


def Caltime(date1, date2):
    # 计算两个日期相差天数，自定义函数名，和两个日期的变量名。
    date1 = time.strptime(date1, "%Y-%m-%d")
    date2 = time.strptime(date2, "%Y-%m-%d")
    date1 = datetime.datetime(date1[0], date1[1], date1[2])
    date2 = datetime.datetime(date2[0], date2[1], date2[2])
    # 返回两个变量相差的值，就是相差天数
    # 当date2日期晚于date1日期,结果为正值
    # 反之,返回结果为负值
    return (date2 - date1).days


def trans_time(date):
    date = date.replace('年', '-').replace('月', '-').replace('日', '')
    if '分钟前' in date:
        now_ts = time.time()
        minute = int(date.replace(u'分钟前',''))
        ts = now_ts - minute * 60
    elif '小时前' in date:
        now_ts = time.time()
        hour = int(date.replace(u'小时前',''))
        ts = now_ts - hour * 3600
    elif '天前' in date:
        now_ts = time.time()
        day = int(date.replace(u'天前',''))
        ts = now_ts - day * 86400
    else:
        datetime_re = re.findall(
            r'\d+-\d+-\d+', date)[0]
        timeArray = time.strptime(datetime_re, "%Y-%m-%d")
        ts = time.mktime(timeArray)
    return time.strftime('%Y-%m-%d', time.localtime(int(ts)))


def get_url(keywords, start_time, end_time):
    y0 = start_time.split('-')[0]
    y1 = end_time.split('-')[0]
    cha1 = Caltime('1970-1-1', start_time)
    cha2 = Caltime('1970-1-1', end_time)
    bt = cha1 * 86400 - 28800
    et = cha2 * 86400 - 28800 + 86399
    url = 'http://news.baidu.com/ns?word={keywords}&cl=2&ct=0&tn=newsdy&rn=50&ie=utf-8&bt={bt}&et={et}'.format(
        keywords=urllib.quote(keywords), bt=bt, et=et)
    return url


if __name__ == '__main__':
    start_time = '2018-08-01'
    end_time = '2018-08-10'
    keywords = '滴滴 遇害 温州 司机'
    index_name = 'baidunews_data'
    type_name = '滴滴'

    get_baidunews_data(keywords, start_time, end_time, index_name, type_name)
    count = get_baidunews_num(keywords, start_time, end_time)
    print count