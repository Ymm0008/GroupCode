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
es = Elasticsearch(['219.224.134.226:9209'])
parameter_for_heat_index = [0.2, 0.4, 0.4]   # paras can be changed
parameter_for_risk_index = [0.2, 0.4, 0.4]


def initialization(event_name):
    '''
    get start timestamp and end timestamp
    used as global vars
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
        body = query_body,
        timeout = 60)
    start_timestamp = response["aggregations"]["time_slice"]["buckets"][0]["key"]
    end_timestamp = response["aggregations"]["time_slice"]["buckets"][-1]["key"]


def heat_curve(event_name):

    # local vars
    field_name = "message_type"

    # get query results
    origin_response = query(event_name, field_name, 1)
    comment_response = query(event_name, field_name, 2)
    forward_response = query(event_name, field_name, 3)

    # get elements for X axis (in list)
    datetime_list = construct_X_axis(origin_response)

    # get statistical data for origin, comment & forward (in list)
    origin_count_list = count_in_each_interval(origin_response)
    comment_count_list = count_in_each_interval(comment_response)
    forward_count_list = count_in_each_interval(forward_response)

    # get heat index (in list)
    heat_index_list = calculate_heat_index(origin_count_list, comment_count_list,
                                           forward_count_list, parameter_for_heat_index)

    result_list_for_frontend = generate_heat_result_list(heat_index_list, origin_count_list,
                                                    comment_count_list, forward_count_list)

    return heat_index_list, datetime_list, result_list_for_frontend


def emotion_curve(event_name):

    # local vars
    field_name = "sentiment"

    positive_response = query(event_name, field_name, 1)
    negative_response = query(event_name, field_name, 3)

    positive_count_list = count_in_each_interval(positive_response)
    negative_count_list = count_in_each_interval(negative_response)

    positive_percentage, negative_percentage, vertical_axis = \
        calculate_percentage(positive_count_list, negative_count_list)

    result_list_for_frontend = generate_emotion_result_list(positive_percentage,
                                                            negative_percentage, vertical_axis)

    return negative_percentage, result_list_for_frontend


def risk_evolution_curve(event_name, heat_index_list, negative_percentage):

    # get max sensitive value in each interval
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
                        "max": {
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
        body = query_body,
        timeout = 60)

    sensitive_value_list = get_sensitive_value(response)

    risk_index_list = calculate_risk_index(heat_index_list, negative_percentage,
                                           sensitive_value_list, parameter_for_risk_index)

    result_list_for_frontend = generate_risk_result_list(risk_index_list, heat_index_list,
                                                         negative_percentage, sensitive_value_list)

    return result_list_for_frontend


def key_user_identification(event_name):

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
                            "size": 10
                        }
                    }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body,
        timeout = 60)

    vertical_axis_list, key_users_list = get_vertical_axis_and_key_users(response)

    result_list_for_frontend = generate_key_users_result_list(vertical_axis_list, key_users_list)

    return result_list_for_frontend


def risk_details(event_name):

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
                    "hot_posts": {
                        "terms": {
                            "field": "root_mid",
                            "size": 6   # top 6 hot posts (at most)
                        }
                    }
                }
            }
        }
    }
    response = es.search(
        index = event_name,
        doc_type = "text",
        body = query_body,
        timeout = 60)

    timestamp_list, hot_posts_list = extract_useful_posts(response)

    message_type = 2
    num_of_comment_list = get_num_of_comment_and_forward(timestamp_list, hot_posts_list,
                                                    event_name, message_type)
    message_type = 3
    num_of_forward_list = get_num_of_comment_and_forward(timestamp_list, hot_posts_list,
                                                    event_name, message_type)

    return timestamp_list, hot_posts_list, num_of_comment_list, num_of_forward_list


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
        body = query_body,
        timeout = 60)

    return response


def construct_X_axis(origin_response):
    # 评论和转发一定发生在原创之后
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

    for i in range(len(buckets)):
        if buckets[i]["sensitive"]["value"] != None:
            sensitive_value_list.append(int(buckets[i]["sensitive"]["value"]))
        else:
            sensitive_value_list.append(0)

    denominator = float(max(sensitive_value_list))

    for i in range(len(buckets)):
        sensitive_value_list[i] = int(round((sensitive_value_list[i] / denominator) * 100))

    return sensitive_value_list


def calculate_risk_index(heat_list, emotion_list, sensitive_list, parameter_list):
    '''
    This function calculates risk index
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


def extract_useful_posts(response):
    timestamp_list = []
    hot_posts_list = []
    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        timestamp_list.append(buckets[i]["key"])
        temp = []
        for j in range(len(buckets[i]["hot_posts"]["buckets"])):
            if buckets[i]["hot_posts"]["buckets"][j]["key"] == "0":
                continue
            else:
                temp.append(buckets[i]["hot_posts"]["buckets"][j]["key"])
        hot_posts_list.append(temp)

    return timestamp_list, hot_posts_list


def get_num_of_comment_and_forward(timestamp_list, hot_posts_list, event_name, message_type):

    result_list = []

    for i in range(len(timestamp_list)):
        start_timestamp = timestamp_list[i]
        end_timestamp = start_timestamp + time_slice
        temp_list = []
        for j in range(len(hot_posts_list[i])):
            root_mid = hot_posts_list[i][j]
            query_body = {
                "query": {
                    "filtered": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"term": {"root_mid": root_mid}},
                                    {"term": {"message_type": message_type}},
                                    {"range": {"timestamp": {
                                                   "gte": start_timestamp,
                                                   "lte": end_timestamp }
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
                body = query_body,
                timeout = 60)
            temp_list.append(response["hits"]["total"])
        result_list.append(temp_list)

    return result_list


def convert_to_table(event_name, datetime, heat_result, emotion_result, risk_result, key_users,
                    timestamp, hot_posts, num_of_comment, num_of_forward):

    table_for_curve = []
    table_for_content = []

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

    # table4curve_in_json = json.dumps(table_for_curve)

    for j in range(len(timestamp)):
        for k in range(len(hot_posts[j])):
            d = OrderedDict()
            d["type"] = event_name
            d["timestamp"] = timestamp[j]
            d["mid"] = hot_posts[j][k]
            d["num_of_comment"] = num_of_comment[j][k]
            d["num_of_forward"] = num_of_forward[j][k]
            table_for_content.append(d)

    # table4content_in_json = json.dumps(table_for_content)

    return table_for_curve, table_for_content


def processing_flow(event_name):   # main function that invokes other functions

    initialization(event_name)

    heat_index_list, datetime_list, heat_result = heat_curve(event_name)
    negative_percentage, emotion_result = emotion_curve(event_name)
    risk_result = risk_evolution_curve(event_name, heat_index_list, negative_percentage)
    key_user_result = key_user_identification(event_name)
    timestamp, hot_posts, num_of_comment, num_of_forward = risk_details(event_name)

    table_for_curve, table_for_content = \
        convert_to_table(event_name, datetime_list, heat_result, emotion_result, risk_result,
                        key_user_result, timestamp, hot_posts, num_of_comment, num_of_forward)

    return table_for_curve, table_for_content


# if __name__ == '__main__':
#
#      name, type = get_name_and_type(2596119483)