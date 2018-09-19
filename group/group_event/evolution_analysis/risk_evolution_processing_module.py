# -*- coding: utf-8 -*-

import time
import json
import codecs

from collections import defaultdict
from collections import OrderedDict
from elasticsearch import Elasticsearch

from get_user_name_type_by_uid import get_name_and_type


# global variables
time_slice = 21600  # 21600s = 6h, interval length for X axis   86400 = 1 day
es = Elasticsearch(['219.224.134.226:9207'])
parameter_for_heat_index = [0.2, 0.4, 0.4]   # paras can be changed
parameter_for_risk_index = [0.2, 0.4, 0.4]


def initialization(event_name):
    '''
    get start timestamp and end timestamp of the whole event
    used as global vars
    :param event_name: event name
    :return: none
    '''

    global start_timestamp
    global end_timestamp

    query_body = {
        "size": 0,
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "message_type": 1
                    }
                }
            }
        },
        "aggregations": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,  # global var
                    "min_doc_count": 0

                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body)
    start_timestamp = response["aggregations"]["time_slice"]["buckets"][0]["key"]
    end_timestamp = response["aggregations"]["time_slice"]["buckets"][-1]["key"]


def heat_curve(event_name):
    '''
    calculate values for heat curve
    include datetime(whole X axis), origin, comment, forward and heat index(each interval)

    :param event_name: name of event

    :return: heat_index_list: a list containing heat index for each interval
             datetime_list: a list containing datetime for each interval
             result_list_for_frontend: a list containing origin, comment, forward and heat index for each interval
    '''

    # local vars
    field_name = "message_type"

    # get num of origin, comment & forward in each interval
    origin_response = query(event_name, field_name, 1)
    comment_response = query(event_name, field_name, 2)
    forward_response = query(event_name, field_name, 3)
    origin_count_list = count_in_each_interval(origin_response)
    comment_count_list = count_in_each_interval(comment_response)
    forward_count_list = count_in_each_interval(forward_response)

    # get elements for X axis
    datetime_list = construct_X_axis(origin_response)

    # get heat indices
    heat_index_list = calculate_heat_index(origin_count_list, comment_count_list,
                                           forward_count_list, parameter_for_heat_index)

    # generate heat curve result, including heat index, origin, comment and forward
    result_list_for_frontend = generate_heat_result_list(heat_index_list, origin_count_list,
                                                    comment_count_list, forward_count_list)

    return heat_index_list, datetime_list, result_list_for_frontend


def emotion_curve(event_name):
    '''
    calculate values for emotion curve
    include positive percent, negative percent and proportion: negative/positive

    :param event_name: name of event

    :return: negative_percentage: a list containing negative percent value for each interval
             result_list_for_frontend: a list containing negative percent, positive percent and proportion
    '''

    # local vars
    field_name = "sentiment"

    # get num of positive and negative posts in each interval
    positive_response = query(event_name, field_name, 1)
    negative_response = query(event_name, field_name, 3)
    positive_count_list = count_in_each_interval(positive_response)
    negative_count_list = count_in_each_interval(negative_response)

    # calculate positive percent, negative percent and proportion
    # negative percent = negative / (negative + positive)
    positive_percentage, negative_percentage, vertical_axis = \
        calculate_percentage(positive_count_list, negative_count_list)

    result_list_for_frontend = generate_emotion_result_list(positive_percentage,
                                                            negative_percentage, vertical_axis)

    return negative_percentage, result_list_for_frontend


def risk_evolution_curve(event_name, heat_index_list, negative_percentage):
    '''
    calculate values for risk evolution curve
    include risk index, heat risk, emotion risk and sensitive risk

    :param event_name: name of event
    :param heat_index_list: a list containing heat index for each interval
    :param negative_percentage: a list containing negative percent for each interval

    :return: result_list_for_frontend: a list containing risk index, heat risk,
             emotion risk and sensitive risk for each interval
    '''

    # get avg sensitive value in each interval
    query_body = {
        "size": 0,
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "sensitive": {
                        "avg": {
                            "field": "sensitive"
                        }
                     }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body)

    sensitive_value_list = get_sensitive_value(response)

    risk_index_list = calculate_risk_index(heat_index_list, negative_percentage,
                    sensitive_value_list, parameter_for_risk_index)

    # heat risk = heat index, emotion risk = negative percent
    result_list_for_frontend = generate_risk_result_list(risk_index_list, heat_index_list,
                                                         negative_percentage, sensitive_value_list)

    return result_list_for_frontend


def key_user_identification(event_name):
    '''
    等待uid映射表

    :param event_name: name of event

    :return:
    '''

    query_body = {
        "size": 0,
        "query": {
            "filtered": {
                "filter": {
                    "terms": {
                        "message_type": [2,3]
                    }
                }
            }
        },
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "key_users": {
                        "terms": {
                            "field": "root_uid",
                            "size": 3
                        }
                    }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body)

    vertical_axis_list, key_users_list = get_vertical_axis_and_key_users(response)

    result_list_for_frontend = generate_key_users_result_list(vertical_axis_list, key_users_list)

    return result_list_for_frontend


def risk_details(event_name):

    query_body = {   # 返回6小时内全部mid
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "key_users": {
                        "terms": {
                            "field": "mid",
                            "size": 30   # 返回的30各帖子不一定是原创 取决于ES查询的返回机制
                        }
                    }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body)

    hot_post_list = get_hot_posts(event_name, response)

    # for i in range(len(hot_post_list)):
    #     print hot_post_list[i]

    return hot_post_list


def query(event_name, field_name, value):

    query_body = {
        "size": 0,
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        field_name: value
                    }
                }
            }
        },
        "aggregations": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,  # global var
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body)

    return response


def construct_X_axis(origin_response):
    # comment and forward must happen after origin
    x_axis = []
    buckets = origin_response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        x_axis.append(timestamp_to_date(buckets[i]["key"]))

    return x_axis


def timestamp_to_date(unix_time):
    format = '%m/%d %H:%M'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def count_in_each_interval(response):
    # get doc count
    counts = []
    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        counts.append(buckets[i]["doc_count"])

    return counts


def calculate_heat_index(origin_list, comment_list, forward_list, parameter_list):

    heat_index = []
    temp = []

    for i in range(len(origin_list)):
       index = round(parameter_list[0] * origin_list[i] + parameter_list[1] * comment_list[i] \
                    + parameter_list[2] * forward_list[i])
       temp.append(index)

    denominator = max(temp)

    for i in range(len(origin_list)):
        heat_index.append(int(round((temp[i] / denominator) * 100)))

    return heat_index


def generate_heat_result_list(heat_list, origin_list, comment_list, forward_list):
    result_list = []

    for i in range(len(heat_list)):
        d = {'heat': heat_list[i], 'origin': origin_list[i],
             'comment': comment_list[i], 'forward': forward_list[i]}
        result_list.append(d)

    return result_list


def calculate_percentage(positive_list, negative_list):
    vertical_axis = []
    total = []
    negative_percentage = []
    positive_percentage = []

    for i in range(len(positive_list)):
        total.append(positive_list[i] + negative_list[i])

    for i in range(len(positive_list)):
        if negative_list[i] == 0 and positive_list[i] == 0:
            vertical_axis.append(0)
            positive_percentage.append(0)
            negative_percentage.append(0)

        elif negative_list[i] == 0 and positive_list[i] != 0:
            vertical_axis.append(0)
            positive_percentage.append(1)
            negative_percentage.append(0)

        elif negative_list[i] != 0 and positive_list[i] == 0:
            vertical_axis.append(1)
            positive_percentage.append(0)
            negative_percentage.append(1)

        else:
            negative_percentage.append(round((float(negative_list[i]) / total[i]), 2))
            positive_percentage.append(round((1 - negative_percentage[i]), 2))
            vertical_axis.append(round((negative_percentage[i] / positive_percentage[i]), 2))

    return positive_percentage, negative_percentage, vertical_axis


def generate_emotion_result_list(positive_percentage, negative_percentage, vertical_axis):
    result_list = []

    for i in range(len(positive_percentage)):
        negative_percentage[i] = int(negative_percentage[i] * 100)

    for i in range(len(positive_percentage)):
        positive_percentage[i] = int(positive_percentage[i] * 100)   # float to int

    for i in range(len(positive_percentage)):
        d = {'positive': positive_percentage[i], 'negative': negative_percentage[i],
             'vertical_axis': vertical_axis[i]}
        result_list.append(d)

    return result_list


def get_sensitive_value(sensitive_response):
    sensitive_value_list = []
    buckets = sensitive_response["aggregations"]["time_slice"]["buckets"]

    # 均值很小 0.1 四舍五入后都是0和1
    for i in range(len(buckets)):
        if buckets[i]["sensitive"]["value"] != None:
            sensitive_value_list.append(int(round(buckets[i]["sensitive"]["value"])))
        else:
            sensitive_value_list.append(0)

    # map to 0-100
    denominator = float(max(sensitive_value_list))
    for i in range(len(buckets)):
        sensitive_value_list[i] = int(round((sensitive_value_list[i] / denominator) * 100))

    return sensitive_value_list


def calculate_risk_index(heat_list, emotion_list, sensitive_list, parameter_list):
    '''
    calculates risk index

    :param heat_list: heat index, range 0-100
    :param emotion_list: negative percent
    :param sensitive_list: sensitive value, range 0-100

    :return: risk index list, range 0-100
    '''
    temp = []
    risk_index_list = []

    for i in range(len(heat_list)):
        temp.append(parameter_list[0] * heat_list[i] + parameter_list[1] * emotion_list[i]
                    + parameter_list[2] * sensitive_list[i])

    denominator = max(temp)

    for i in range(len(heat_list)):
        risk_index_list.append(int(round((temp[i] / denominator) * 100)))   # map to 0-100

    return risk_index_list


def generate_risk_result_list(risk, heat, emotion, sensitive):
    result_list = []

    for i in range(len(risk)):
        d = {'risk_index': risk[i], 'heat_risk': heat[i],
             'emotion_risk': emotion[i], 'sensitive_risk': sensitive[i]}
        result_list.append(d)

    return result_list


def get_vertical_axis_and_key_users(response):
    # 竖轴值表示某一时间段内 转发评论数的最大值 该值对应的用户是key_users列表中的第一个值
    # 需要改成 按评论转发量排序 返回100个uid，从这100个uid里找出第一个出现的媒体，大V和普通用户
    key_users = []
    vertical_axis = []
    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        if buckets[i]["key_users"]["buckets"] == []:
            vertical_axis.append(0)
            key_users.append({"key_users": None})
        else:
            vertical_axis.append(buckets[i]["key_users"]["buckets"][0]["doc_count"])
            d = {}
            for j in range(len(buckets[i]["key_users"]["buckets"])):
                d.setdefault("key_users", [])
                d["key_users"].append(buckets[i]["key_users"]["buckets"][j]["key"])
            key_users.append(d)

    return vertical_axis, key_users


def generate_key_users_result_list(vertical_axis_list, key_users_list):
    result_list = []

    for i in range(len(vertical_axis_list)):
        d = {'max_num_of_comment_and_forward': vertical_axis_list[i],
             'key_users_list': key_users_list[i]}
        result_list.append(d)

    return result_list


def get_hot_posts(event_name, response):

    result_list = []
    buckets = response["aggregations"]["time_slice"]["buckets"]   # len = 137

    for i in range(len(buckets)):
        temp = []
        for j in range(len(buckets[i]["key_users"]["buckets"])):
            d = {}
            num_of_comment = query_for_hot_posts(event_name, 2, buckets[i]["key_users"] \
                                                ["buckets"][j]["key"], buckets[i]["key"])
            num_of_forward = query_for_hot_posts(event_name, 3, buckets[i]["key_users"] \
                                                ["buckets"][j]["key"], buckets[i]["key"])
            total = num_of_comment + num_of_forward
            mid = buckets[i]["key_users"]["buckets"][j]["key"]
            d["mid"] = mid
            d["total"] = total
            d["comment"] = num_of_comment
            d["forward"] = num_of_forward
            d["timestamp"] = buckets[i]["key"]
            d["type"] = event_name
            temp.append(d)
        result_list.append(temp)

    return result_list


def query_for_hot_posts(event_name, message_type, mid, start_timestamp):

    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "message_type": message_type
                                }
                            },
                            {
                                "term": {
                                    "root_mid": mid
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": start_timestamp,
                                        "lte": start_timestamp + time_slice
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body)

    return response["hits"]["total"]


def generate_table_for_curve(event_name, datetime, heat_result, emotion_result, risk_result, key_users):

    table_for_curve = []

    for i in range(len(datetime)):
        d = OrderedDict()
        d["type"] = event_name
        d["datetime"] = datetime[i]
        d["heat_index"] = heat_result[i]["heat"]
        d["origin"] = heat_result[i]["origin"]
        d["comment"] = heat_result[i]["comment"]
        d["forward"] = heat_result[i]["forward"]
        d["proportion"] = emotion_result[i]["vertical_axis"]
        d["negative"] = emotion_result[i]["negative"]
        d["positive"] = emotion_result[i]["positive"]
        d["risk_index"] = risk_result[i]["risk_index"]
        d["heat_risk"] = risk_result[i]["heat_risk"]
        d["emotion_risk"] = risk_result[i]["emotion_risk"]
        d["sensitive_risk"] = risk_result[i]["sensitive_risk"]
        d["max_num_of_comment_and_forward"] = key_users[i]["max_num_of_comment_and_forward"]
        d["key_users"] = key_users[i]["key_users_list"]["key_users"]
        table_for_curve.append(d)

    return table_for_curve


def processing_flow(event_name):   # main function that invokes other functions

    initialization(event_name)

    heat_index_list, datetime_list, heat_result = heat_curve(event_name)
    negative_percentage, emotion_result = emotion_curve(event_name)
    risk_result = risk_evolution_curve(event_name, heat_index_list, negative_percentage)
    key_user_result = key_user_identification(event_name)   # uid-身份映射表先放一放 此模块待更新

    table_for_curve = generate_table_for_curve(event_name, datetime_list, heat_result,
                                                                  emotion_result, risk_result, key_user_result)

    hot_post_list = risk_details(event_name)

    return table_for_curve, hot_post_list


# if __name__ == '__main__':

#     processing_flow("flow_text_maoyi")