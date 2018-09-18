# -*- coding=utf-8 -*-
import requests
import time
import sys
import json
reload(sys)
sys.setdefaultencoding('utf-8')


def get_name_type_by_uid(uid):
    '''
    input:uid
    output: user_name,verified_type

    verified_type说明
    普通用户：-1
    政府：1
    企业：2
    媒体：3
    校园：4
    网站：5
    应用：6
    团体：7
    待审企业：8
    初级达人：200
    中高级达人：220
    已故V用户：400

    '''
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',}
    url = 'https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D3%26q%3D{}%26t%3D0&page_type=searchall'.format(uid)
    resp = requests.get(url=url, headers=headers,timeout=10)
    time.sleep(2)
    data = resp.json()
    if data['ok']:
        for card in data['data']['cards']:
            cg = card.get('card_group', '')
            if cg:
                for c in cg:
                    user = c.get('user', '')
                    if user and str(user['id']) == str(uid):
                        return user['screen_name'],user['verified_type']
    else:
        return None,None


def get_name_and_type(uid):

    screen_name, verified_type = get_name_type_by_uid(uid)

    return screen_name, verified_type


if __name__ == '__main__':

    get_name_and_type(2596119483)