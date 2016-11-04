# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 22:07:01 2016

@author: Platina
"""

import simplejson as json
from pyspark import SparkContext
sc = SparkContext(appName="AvgStars")

business_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_business.json")
review_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_review.json")

def business_city(data):
    business_city_list =[]
    obtype = data.get('type', None)
    if obtype == 'business':
        city = data.get('city', None)
        business_id = data.get('business_id', None)
        if city:
            business_city_list.append((business_id,city))
    return business_city_list
    
def business_user(data):
    business_user_list = []
    obtype = data.get('type', None)
    if obtype == 'review':
        user_id = data.get('user_id', None)
        business_id = data.get('business_id', None)
        if user_id:
            business_user_list.append((business_id,user_id))
    return business_user_list

business_cities = business_file.map(lambda line: json.loads(line))\
                        .flatMap(business_city)#.map(lambda x:(x[0],x[1]))

business_users = review_file.map(lambda line: json.loads(line))\
                        .flatMap(business_user)#.map(lambda x:(x[0],x[1]))
#Got(business_id,city,usertuples
user_city = sc.parallelize(business_users.join(business_cities)\
                          .map(lambda x : (x[1][0],x[1][1])).distinct()\
                          .countByKey().items()).map(lambda x: x[1]).histogram(range(1,31))
user_hist = [(user_city[0][i],user_city[1][i]) for i in range(0,len(user_city[0])-1)]
user_city_hist= sc.parallelize([('cities','yelp users')]+user_hist)
user_city_hist.map(lambda t : str(t[0]) + ',' + str(t[1])).saveAsTextFile('si601w16hw6_output_xiaoyliu1')