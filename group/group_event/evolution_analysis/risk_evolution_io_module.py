# -*- coding: utf-8 -*-

import time
import json
from elasticsearch import RequestsHttpConnection, Elasticsearch
from risk_evolution_processing_module import processing_flow

# global vars
event_name = "flow_text_gangdu"
index_name_for_curve = "result_for_curves"
index_name_for_content = "result_for_content"
local_es = Elasticsearch(['127.0.0.1:9200'])
es = Elasticsearch(['219.224.134.226:9209'])

def create_index_for_curve(index_name):

    create_body = {
        "settings": {
            "number_of_shards": 4,
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
    local_es.indices.create(index = index_name, body = create_body, ignore = 400)


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
                    }
                }
            }
        }
    }
    local_es.indices.create(index = index_name, body = create_body, ignore = 400)


def index_data_for_curve(index_name, event_name, curve_result):

    for i in range(len(curve_result)):
        local_es.index(index = index_name, doc_type = event_name, id = i, body = {
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

    for i in range(len(content_result)):
        local_es.index(index = index_name, doc_type = event_name, id = i, body = {
            "timestamp": content_result[i]["timestamp"],
            "mid": json.dumps(content_result[i]["mid"]),   # unicode to str
            "num_of_comment": content_result[i]["num_of_comment"],
            "num_of_forward": content_result[i]["num_of_forward"],
            "uid": content_result[i]["uid"],
            "datetime": content_result[i]["datetime"],
            "key": content_result[i]["key"],
            "text": content_result[i]["text"]
        })

    print "index content data successfully"


def construct_risk_details(content_result):

    for i in range(len(content_result)):
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            "mid": content_result[i]["mid"]
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
        if response["hits"]["hits"] == []:
            content_result[i]["uid"] = json.dumps(None)
            content_result[i]["datetime"] = json.dumps(None)
            content_result[i]["key"] = json.dumps(None)
            content_result[i]["text"] = json.dumps(None)
        else:
            content_result[i]["uid"] = json.dumps(response["hits"]["hits"][0]["_source"]["uid"])
            date = timestamp_to_date(response["hits"]["hits"][0]["_source"]["timestamp"])
            content_result[i]["datetime"] = json.dumps(date)
            content_result[i]["key"] = json.dumps(response["hits"]["hits"][0]["_source"]["keywords_string"])
            content_result[i]["text"] = json.dumps(response["hits"]["hits"][0]["_source"]["text"])

    return content_result


def timestamp_to_date(unix_time):
    format = '%y-%m-%d %H:%M'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def extract_curve_result(index_name, event_name):

    result_list = []

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
        response = local_es.search(
            index = index_name,
            doc_type = event_name,
            body = query_body)
        d = {}
        d["datetime"] = response["hits"]["hits"][0]["_source"]["datetime"]

        d["heat_index"] = response["hits"]["hits"][0]["_source"]["heat_index"]
        d["total_origin"] = response["hits"]["hits"][0]["_source"]["total_origin"]
        d["total_comment"] = response["hits"]["hits"][0]["_source"]["total_comment"]
        d["total_forward"] = response["hits"]["hits"][0]["_source"]["total_forward"]

        d["proportion"] = response["hits"]["hits"][0]["_source"]["proportion"]
        d["negative_percent"] = response["hits"]["hits"][0]["_source"]["negative_percent"]
        d["positive_percent"] = response["hits"]["hits"][0]["_source"]["positive_percent"]

        d["risk_index"] = response["hits"]["hits"][0]["_source"]["risk_index"]
        d["heat_risk"] = response["hits"]["hits"][0]["_source"]["heat_risk"]
        d["emotion_risk"] = response["hits"]["hits"][0]["_source"]["emotion_risk"]
        d["sensitive_risk"] = response["hits"]["hits"][0]["_source"]["sensitive_risk"]

        d["max_num_of_comment_and_forward"] = response["hits"]["hits"][0] \
                                                      ["_source"]["max_num_of_comment_and_forward"]
        d["key_users"] = response["hits"]["hits"][0]["_source"]["key_users"]

        # print d
        result_list.append(d)

    return result_list


def extract_content_result(index_name, event_name):

    result_list = []

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
        response = local_es.search(
            index = index_name,
            doc_type = event_name,
            body = query_body)
        d = {}
        d["datetime"] = response["hits"]["hits"][0]["_source"]["datetime"]

        d["uid"] = response["hits"]["hits"][0]["_source"]["uid"]
        d["key"] = response["hits"]["hits"][0]["_source"]["key"]
        d["text"] = response["hits"]["hits"][0]["_source"]["text"]
        d["num_of_comment"] = response["hits"]["hits"][0]["_source"]["num_of_comment"]
        d["num_of_forward"] = response["hits"]["hits"][0]["_source"]["num_of_forward"]

        d["mid"] = response["hits"]["hits"][0]["_source"]["mid"]

        result_list.append(d)

    return result_list


def get_length(index_name, event_name):

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        }
    }
    response = local_es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)

    return response["hits"]["total"]


def io_flow():

    table_for_curve, table_for_content = processing_flow(event_name)   # table in json

    table_for_complete_content = construct_risk_details(table_for_content)

    # store processing results into ES
    create_index_for_curve(index_name_for_curve)
    index_data_for_curve(index_name_for_curve, event_name, table_for_curve)

    create_index_for_content(index_name_for_content)
    index_data_for_content(index_name_for_content, event_name, table_for_complete_content)

    # extract result from ES for transmission
    curve_result_list = extract_curve_result(index_name_for_curve, event_name)
    content_result_list = extract_content_result(index_name_for_content, event_name)

    curve_result_json = json.dumps(curve_result_list)
    content_result_json = json.dumps(content_result_list)

    print curve_result_json
    print content_result_json
    print "OK"


if __name__ == '__main__':

    io_flow()