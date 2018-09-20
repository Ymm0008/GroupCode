# -*- coding: utf-8 -*-

import time
import json
from elasticsearch import RequestsHttpConnection, Elasticsearch
from risk_evolution_processing_module import processing_flow

# global vars
index_name_for_curve = "risk_evolution_curve_result"
index_name_for_content = "risk_evolution_content_result"
es_for_storage = Elasticsearch(['219.224.134.226:9209'])
timeout = 15


def create_index_for_curve(index_name):

    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            "table_for_curve": {
                "properties": {
                    "datetime": {
                        "type": "date",
                        "format": "%m/%d %H:%M",
                        "index": "not_analyzed"
                    },
                    "heat_index": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "total_origin": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "total_comment": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "total_forward": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "proportion": {
                        "type": "float",
                        "index": "not_analyzed"
                    },
                    "negative_percent": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "positive_percent": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "risk_index": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "heat_risk": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "emotion_risk": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "sensitive_risk": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "max_num_of_comment_and_forward": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "key_users": {
                        "type": "text",
                        "index": "not_analyzed"
                    }
                }
            }
        }
    }
    es_for_storage.indices.create(index = index_name, body = create_body, ignore = 400)


def create_index_for_content(index_name):

    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            "table_for_content": {
                "properties": {
                    "timestamp": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "mid": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "num_of_comment": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "num_of_forward": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "uid": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "datetime": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "key": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "text": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "total_num_of_comment_and_forward": {
                        "type": "integer",
                        "index": "not_analyzed"
                    }
                }
            }
        }
    }
    es_for_storage.indices.create(index = index_name, body = create_body, ignore = 400)


def index_data_for_curve(index_name, event_name, curve_result):

    for i in range(len(curve_result)):
        es_for_storage.index(index = index_name, doc_type = event_name, id = i, body = {
            "datetime": curve_result[i]["datetime"],
            "heat_index": curve_result[i]["heat_index"],
            "total_origin": curve_result[i]["origin"],
            "total_comment": curve_result[i]["comment"],
            "total_forward": curve_result[i]["forward"],
            "proportion": curve_result[i]["proportion"],
            "negative_percent": curve_result[i]["negative"],
            "positive_percent": curve_result[i]["positive"],
            "risk_index": curve_result[i]["risk_index"],
            "heat_risk": curve_result[i]["heat_risk"],
            "emotion_risk": curve_result[i]["emotion_risk"],
            "sensitive_risk": curve_result[i]["sensitive_risk"],
            "max_num_of_comment_and_forward": curve_result[i]["max_num_of_comment_and_forward"],
            "key_users": json.dumps(curve_result[i]["key_users"])
        })

    print "index curve data successfully"


def index_data_for_content(index_name, event_name, content_result):

    counter = 0

    for i in range(len(content_result)):
        for j in range(len(content_result[i])):
            es_for_storage.index(index = index_name, doc_type = event_name, id = counter, body = {
                "timestamp": content_result[i][j]["timestamp"],
                "mid": str(content_result[i][j]["mid"]),   # unicode to str
                "num_of_comment": content_result[i][j]["comment"],
                "num_of_forward": content_result[i][j]["forward"],
                "uid": content_result[i][j]["uid"],
                "total_num_of_comment_and_forward": content_result[i][j]["total"],
                "datetime": content_result[i][j]["datetime"],
                "key": content_result[i][j]["key"],
                "text": content_result[i][j]["text"]
            })
            counter = counter + 1

    print "index content data successfully"


def construct_risk_details(event_name, content_result):

    for i in range(len(content_result)):
        if content_result[i] == []:
            continue
        else:
            for j in range(len(content_result[i])):
                query_body = {
                    "query": {
                        "filtered": {
                            "filter": {
                                "term": {
                                    "mid": content_result[i][j]["mid"]
                                }
                            }
                        }
                    }
                }
                response = es_for_storage.search(
                    index = event_name,
                    doc_type = "text",
                    body = query_body,
                    timeout = timeout)

                time.sleep(0.01)  # 避免频繁查询导致出错  可以考虑用try catch
                # print i, j

                content_result[i][j]["uid"] = response["hits"]["hits"][0]["_source"]["uid"]
                content_result[i][j]["key"] = response["hits"]["hits"][0]["_source"]["keywords_string"]
                content_result[i][j]["text"] = response["hits"]["hits"][0]["_source"]["text"]
                content_result[i][j]["datetime"] = timestamp_to_date(response["hits"]["hits"] \
                                                                     [0]["_source"]["timestamp"])
    return content_result


def timestamp_to_date(unix_time):
    format = '%y-%m-%d %H:%M'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def store_result_to_ES(event_name):

    table_for_curve, hot_post_list = processing_flow(event_name)

    table_for_complete_content = construct_risk_details(event_name, hot_post_list)

    # store processing results into ES
    create_index_for_curve(index_name_for_curve)
    index_data_for_curve(index_name_for_curve, event_name, table_for_curve)

    create_index_for_content(index_name_for_content)
    index_data_for_content(index_name_for_content, event_name, table_for_complete_content)


if __name__ == '__main__':

    store_result_to_ES("flow_text_gangdu")