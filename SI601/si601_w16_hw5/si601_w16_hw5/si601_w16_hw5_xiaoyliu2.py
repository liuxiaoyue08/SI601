# -*- coding: utf-8 -*-
"""
Created on Sat Feb 06 09:30:39 2016

@author: Platina
"""

import simplejson as json
from pyspark import SparkContext
sc = SparkContext(appName="AvgStars")

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset.json")

def city_nei_re_star(data):
    city_nei_re_star_list = []
    obtype = data.get('type', None)
    if obtype == 'business':
        cities = data.get('city', None)
        neighborhoods = data.get('neighborhoods', None)
        re_count = data.get('review_count', None)
        stars = data.get('stars', None)
        if neighborhoods:
            for n in neighborhoods:
                if stars != None:
                    if re_count != None:
                        city_nei_re_star_list.append((cities, n, re_count, stars))
        else:
            city_nei_re_star_list.append((cities, 'Unknown', re_count, stars))
    return city_nei_re_star_list
    
nei = input_file.map(lambda line: json.loads(line)) \
                .flatMap(city_nei_re_star) \
                .map(lambda x: ((x[0], x[1]), (1, x[2], x[3]))) \
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
                .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2] / x[1][0]))

nei_sorted = nei.sortBy(lambda x: (x[0][0],x[0][1]), ascending = True)                
nei_sorted.map(lambda t : str(t[0][0]) + '\t' + str(t[0][1]) + '\t' + str(t[1]) + '\t' + str(t[2]) +'\t' + str(t[3])).saveAsCsvFile('si601_w16_hw5_xiaoyliu2')
                
                

    




