# -*- coding: utf-8 -*-
"""
Created on Sat Feb 06 09:02:38 2016

@author: Platina
"""

import simplejson as json
from pyspark import SparkContext
sc = SparkContext(appName="AvgStars")

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset.json")

def nei_star(data):
    nei_star_list = []
    stars = data.get('stars', None)
    neighborhoods = data.get('neighborhoods', None)
    if neighborhoods:
        for n in neighborhoods:
            if stars != None:
                nei_star_list.append((n, stars))
    else:
        nei_star_list.append(('Unknown', stars))
    return nei_star_list
    
def nei_re(data):
    nei_re_list = []
    re_count = data.get('review_count', None)
    neighborhoods = data.get('neighborhoods', None)
    if neighborhoods:
        for n in neighborhoods:
            if re_count != None:
                nei_re_list.append((n, re_count))
    else:
        nei_re_list.append(('Unknown', re_count))
    return nei_re_list

nei_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(nei_star) \
                      .mapValues(lambda x: (x, 1)) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                      .map(lambda x: (x[0], x[1][0]/x[1][1]))

nei_re = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(nei_re) \
                      .reduceByKey(lambda x, y: x + y)
    