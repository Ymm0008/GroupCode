# -*- coding:utf-8 -*-

from baidunews_data import get_baidunews_num, get_baidunews_data
from weixin_data import get_weixin_num, get_weixin_data
from weibo_data import get_weibo_num, get_weibo_data

def channel_count(keywords, start_time, end_time):
    weixin_count = get_weixin_num(keywords, start_time, end_time)
    baidunews_count = get_baidunews_num(keywords, start_time, end_time)
    weibo_count = get_weibo_num(keywords, start_time, end_time)

    return weixin_count, baidunews_count, weibo_count


def get_data2es(keywords, start_time, end_time, index_weixin_name, type_weixin_name, index_weibo_name, type_weibo_name, index_baidunews_name, type_baidunews_name):
    get_weixin_data(keywords, start_time, end_time, index_weixin_name, type_weixin_name)
    get_weibo_data(keywords, start_time, end_time, index_weibo_name, type_weibo_name)
    get_baidunews_data(keywords, start_time, end_time, index_baidunews_name, type_baidunews_name)



if __name__ == "__main__":
    keywords = '滴滴 打车 乘客 司机'
    start_time = '2016-11-24'
    end_time = '2016-11-26'

    index_weixin_name = "weixin_data_text"
    index_weibo_name = "weibo_data_text"
    index_baidunews_name = "baidunews_data_text"

    type_weixin_name = 'didi'
    type_weibo_name = 'didi'
    type_baidunews_name = 'didi'

    weixin_count, baidunews_count, weibo_count = channel_count(keywords, start_time, end_time)
    print weixin_count, baidunews_count, weibo_count

    get_data2es(keywords, start_time, end_time, index_weixin_name, type_weixin_name, index_weibo_name, type_weibo_name, index_baidunews_name, type_baidunews_name)
    print 'get_data successful'





