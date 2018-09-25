# coding=utf-8

import re
import sys
import time
import datetime
import json
import traceback
from lxml import etree
import urllib
import requests
from smtp import send_mail

reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append("../")
from global_utils import es_data_text

session = requests.Session()
cookies = [{'domain': '.weixin.sogou.com', 'expiry': 2168136424.861524, 'httpOnly': False, 'name': 'SUID', 'path': '/', 'secure': False, 'value': '332A276A232C940A000000005BA31CE8'}, {'domain': 'weixin.sogou.com', 'expiry': 1540008424.861412, 'httpOnly': False, 'name': 'ABTEST', 'path': '/', 'secure': False, 'value': '2|1537416424|v1'}, {'domain': '.sogou.com', 'expiry': 1538626031.605724, 'httpOnly': False, 'name': 'sgid', 'path': '/', 'secure': False, 'value': '19-35979717-AVujHOic7oK84gwdfwS5AkFY'}, {'domain': '.sogou.com', 'expiry': 1568952424.861478, 'httpOnly': False, 'name': 'IPLOC', 'path': '/', 'secure': False, 'value': 'CN1100'}, {'domain': '.sogou.com', 'expiry': 1538626031.605669, 'httpOnly': False, 'name': 'ppinf', 'path': '/', 'secure': False, 'value': '5|1537416431|1538626031|dHJ1c3Q6MToxfGNsaWVudGlkOjQ6MjAxN3x1bmlxbmFtZTo5OiVFNyU5RiVCM3xjcnQ6MTA6MTUzNzQxNjQzMXxyZWZuaWNrOjk6JUU3JTlGJUIzfHVzZXJpZDo0NDpvOXQybHVLX2w0bk1PVkxuWnlGVnkzaTdQbkFVQHdlaXhpbi5zb2h1LmNvbXw'},
           {'domain': '.sogou.com', 'expiry': 2168136424.963582, 'httpOnly': False, 'name': 'SUID', 'path': '/', 'secure': False, 'value': '332A276A2213940A000000005BA31CE8'}, {'domain': 'weixin.sogou.com', 'expiry': 1546056425, 'httpOnly': False, 'name': 'weixinIndexVisited', 'path': '/', 'secure': False, 'value': '1'}, {'domain': '.sogou.com', 'expiry': 1852776425.893456, 'httpOnly': False, 'name': 'SUV', 'path': '/', 'secure': False, 'value': '00881DEF6A272A335BA31CE980682533'}, {'domain': '.sogou.com', 'expiry': 1538626031.6057, 'httpOnly': False, 'name': 'pprdig', 'path': '/', 'secure': False, 'value': 'IvF9OXl5HnGUC-oJSBMtQnLLapumaQbOmlcym1YpOCXmOEwoxQmWywW7v8z73soAEronLhJP9XpJHjr8V7XHP4Imis6eAnVAKLnNsRzPjZP7iLL7I_MNOFGKiKDprCmra1mWfOO5-9NttrUqB_rXU14S2XMFNWxnWviZUL5fH9U'}, {'domain': 'weixin.sogou.com', 'httpOnly': True, 'name': 'ppmdig', 'path': '/', 'secure': False, 'value': '1537416431000000968dbc6cb7c2a02b309769ec86026974'}]

for cookie in cookies:
    session.cookies.set(cookie['name'], cookie['value'])


def get_weixin_data(key_word, start_date, end_date, index_name, type_name):
    kw = urllib.quote(key_word)
    headers = {
        'Referer': 'http://weixin.sogou.com/weixin?type=2&query={kw}&ie=utf8&s_from=input&_sug_=n&_sug_type_=1&w=01015002&oq=&ri=4&sourceid=sugg&sut=0&sst0=1537420253087&lkt=0%2C0%2C0&p=40040108'.format(kw=kw),
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
    }
    page = 1

    while 1:
        url = 'http://weixin.sogou.com/weixin?usip=&query={kw}&ft={start_date}&tsn=5&et={end_date}&interation=&type=2&wxid=&page={page}&ie=utf8'.format(
            kw=kw, page=page, start_date=start_date, end_date=end_date)
        r = session.get(url=url, headers=headers,timeout=5,allow_redirects=False)
        # if len(r.cookies)!=2:
        #     raise ValueError('cookies is timeout')

        time.sleep(2)
        r.encoding = 'utf-8'
        html = r.text
        tree = etree.HTML(html)
        boxs = tree.xpath('//div[@class="news-box"]//li/div[@class="txt-box"]')
        for box in boxs:
            item = {}
            item['url'] = box.xpath('./h3/a/@href')[0]
            item['title'] = box.xpath('string(./h3)').encode('utf-8').strip()
            item['date'] = ts2date(
                float(box.xpath('./div[@class="s-p"]/@t')[0]))
            item['content'] = get_content(url, item['url'])

            es_data_text.index(index=index_name, doc_type=type_name, id=item['url'], body=item)
            print 'weixin_data successfully insert es'

        if tree.xpath('//a[@id="sogou_next"]'):
            page += 1
        else:
            break


def get_weixin_num(key_word, start_date, end_date):
    kw = urllib.quote(key_word)
    headers = {
        'Referer': 'http://weixin.sogou.com/weixin?type=2&query={kw}&ie=utf8&s_from=input&_sug_=n&_sug_type_=1&w=01015002&oq=&ri=4&sourceid=sugg&sut=0&sst0=1537420253087&lkt=0%2C0%2C0&p=40040108'.format(kw=kw),
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
    }
    url = 'http://weixin.sogou.com/weixin?usip=&query={kw}&ft={start_date}&tsn=5&et={end_date}&interation=&type=2&wxid=&ie=utf8'.format(
        kw=kw, start_date=start_date, end_date=end_date)
    r = session.get(url=url, headers=headers)
    time.sleep(2)
    r.encoding = 'utf-8'
    html = r.text
    tree = etree.HTML(html)
    try:
        total_num = re.findall(r'<!--resultbarnum:(\d*,?\d*)-->', html)[0]
    except:
        boxs = tree.xpath('//div[@class="news-box"]//li/div[@class="txt-box"]')
        return len(boxs)
    return int(str(total_num).replace(',', ''))


def get_content(referer, url):
    headers = {
        'Referer': referer,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
    }
    r = session.get(url=url, headers=headers)
    time.sleep(2)
    r.encoding = 'utf-8'
    html = r.text
    tree = etree.HTML(html)
    content = tree.xpath('string(.//div[@id="js_content"])')
    return content.replace('\r', '').replace('\n', '').replace(' ', '')


def ts2date(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))


if __name__ == '__main__':
    start_time = '2018-09-01'
    end_time = '2018-09-02'
    keywords = '滴滴 遇害 温州 司机'
    index_name = 'weixin_data'
    type_name = 'test'
    try:
        get_weixin_data(keywords, start_time, end_time, index_name, type_name)
    except:
        error_msg = traceback.format_exc()
        # send_mail(msg=error_msg, mode=False)
        print error_msg

    print get_weixin_num(keywords, start_time, end_time)

