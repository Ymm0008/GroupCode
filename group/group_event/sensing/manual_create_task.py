# -*-coding:utf-8-*-

import json
import sys
reload(sys)
sys.path.append('../')

from global_utils import es_sensor as es
from global_utils import index_manage_sensing, type_manage_sensing, id_sensing_task


social_sensors = ["1784473157", "2286908003", "1314608344", "1644114654",\
        "1686546714", "1656737654", "2028810631", "1677991972", "3881380517", "1847582585", \
       "2615417307", "1191965271", "1643971635", \
        "1778758223", "1977460817",  \
         "1656831930", "1699432410", "1722628512", "1267454277",\
        "2443459455", "3921730119", "1867571077", "1718493627", "1653460650", "1737737970",\
        "3271121353", "1326410461", "1645705403", \
        "1653944045", "5977555696", "1992613670", \
        "1724367710", "1974808274", "3164957712", "3266943013",\
        "2127460165", "2083844833", "5305757517", "2803301701", "2656274875", "1618051664", "1974576991", \
        "1642512402", "1402977920", \
        "1893801487", "2810373291", "1749990115", \
         "1652484947", "1265998927", "1698857957", \
         "1698233740",  "5044281310"]


def create_sensor_task():
    task_detail = dict()
    task_detail["task_name"] = id_sensing_task
    task_detail["remark"] = "感知热门事件"
    task_detail["social_sensors"] = json.dumps(list(social_sensors))
    task_detail["history_status"] = json.dumps([])
    es.index(index=index_manage_sensing, doc_type=type_manage_sensing, id=id_sensing_task, body=task_detail)



if __name__ == "__main__":
    create_sensor_task()





