# -*- coding:utf-8 -*-

import sys
import time
import json
import numpy as np
import datetime

reload(sys)
sys.path.append("../")


from ip.geo_ip import ip_parse


aa = ip_parse('73.231.33.243')
print aa

