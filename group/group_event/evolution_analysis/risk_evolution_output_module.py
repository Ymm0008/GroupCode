# -*- coding: utf-8 -*-

import json
from elasticsearch import Elasticsearch

# global vars
index_name_for_curve = "risk_evolution_curve_result"
index_name_for_content = "risk_evolution_content_result"
es_for_output = Elasticsearch(['219.224.134.226:9207'])


def extract_curve_result(index_name, event_name):

    evolution_and_key_user_result_list = []
    heat_and_emotion_result_list = []

    length = get_length(index_name, event_name)

    for i in range(length):
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            "_id": i
                        }
                    }
                }
            }
        }
        response = es_for_output.search(
            index = index_name,
            doc_type = event_name,
            body = query_body)
        d1 = {}
        d1["datetime"] = response["hits"]["hits"][0]["_source"]["datetime"]

        d1["heat_index"] = response["hits"]["hits"][0]["_source"]["heat_index"]
        d1["total_origin"] = response["hits"]["hits"][0]["_source"]["total_origin"]
        d1["total_comment"] = response["hits"]["hits"][0]["_source"]["total_comment"]
        d1["total_forward"] = response["hits"]["hits"][0]["_source"]["total_forward"]

        d1["proportion"] = response["hits"]["hits"][0]["_source"]["proportion"]
        d1["negative_percent"] = response["hits"]["hits"][0]["_source"]["negative_percent"]
        d1["positive_percent"] = response["hits"]["hits"][0]["_source"]["positive_percent"]

        d2 = {}
        d2["datetime"] = response["hits"]["hits"][0]["_source"]["datetime"]

        d2["risk_index"] = response["hits"]["hits"][0]["_source"]["risk_index"]
        d2["heat_risk"] = response["hits"]["hits"][0]["_source"]["heat_risk"]
        d2["emotion_risk"] = response["hits"]["hits"][0]["_source"]["emotion_risk"]
        d2["sensitive_risk"] = response["hits"]["hits"][0]["_source"]["sensitive_risk"]

        d2["max_num_of_comment_and_forward"] = response["hits"]["hits"][0] \
                                                      ["_source"]["max_num_of_comment_and_forward"]
        d2["key_users"] = response["hits"]["hits"][0]["_source"]["key_users"]

        heat_and_emotion_result_list.append(d1)
        evolution_and_key_user_result_list.append(d2)

    return evolution_and_key_user_result_list, heat_and_emotion_result_list


def extract_content_result(index_name, event_name, timestamp):

    result_list = []

    query_body = {
        "size": 30,   # at most 30, depending on size in risk_details()
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "timestamp": timestamp
                    }
                }
            }
        }
    }
    response = es_for_output.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)

    length = response["hits"]["total"]

    for i in range(length):
        d = {}
        d["datetime"] = response["hits"]["hits"][i]["_source"]["datetime"]
        d["uid"] = response["hits"]["hits"][i]["_source"]["uid"]
        d["key"] = response["hits"]["hits"][i]["_source"]["key"]
        d["text"] = response["hits"]["hits"][i]["_source"]["text"]
        d["num_of_comment"] = response["hits"]["hits"][i]["_source"]["num_of_comment"]
        d["num_of_forward"] = response["hits"]["hits"][i]["_source"]["num_of_forward"]

        d["mid"] = response["hits"]["hits"][i]["_source"]["mid"]
        d["timestamp"] = response["hits"]["hits"][i]["_source"]["timestamp"]
        d["total"] = response["hits"]["hits"][i]["_source"]["total_num_of_comment_and_forward"]
        result_list.append(d)

    length = len(result_list)
    while length > 0:
        for j in range(len(result_list) - 1):
            if result_list[j]["total"] < result_list[j + 1]["total"]:
                temp = result_list[j]
                result_list[j] = result_list[j + 1]
                result_list[j + 1] = temp
        length -= 1

    # 如果存储时某个时间戳是空列表，则取出时亦为空列表
    return result_list


def get_length(index_name, event_name):
    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        }
    }
    response = es_for_output.search(
        index=index_name,
        doc_type=event_name,
        body=query_body)

    return response["hits"]["total"]


def curve_output_for_frontend(event_name):   # 只调用一次

    # 返回整个时间轴的数据
    evolution_and_key_user, heat_and_emotion = extract_curve_result(index_name_for_curve, event_name)

    return evolution_and_key_user, heat_and_emotion


def content_output_for_frontend(event_name, timestamp, page_num, page_size):   # 每次请求现调用

    # 返回该时间戳对应的所有帖子
    content_result_list = extract_content_result(index_name_for_content, event_name, timestamp)

    # print len(content_result_list)

    start_from = (int(page_num) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)

    temp_list = content_result_list[start_from:end_at]

    result_list = []
    d = {}
    d["total"] = len(temp_list)
    d["data"] = temp_list
    result_list.append(d)

    return result_list


# if __name__ == '__main__':

#     page_num = 3
#     page_size = 5

#     a, b = curve_output_for_frontend("flow_text_gangdu")
#     c = content_output_for_frontend("flow_text_gangdu", 1513966400, page_num, page_size)
    # print a
    # print b
    # print c
