# Spark solution to Homework 5 Extra Credit problem
# Written by Dr. Yuhang Wang
'''
To run on Fladoop cluster

spark-submit --master yarn-client --queue si601w15 --num-executors 12 --executor-memory 4g --executor-cores 1 spark_si601_w15_hw5ec.py
'''

import simplejson as json
import math
from pyspark import SparkContext

sc = SparkContext(appName="PythonPosNegWords")

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_review.json")

step_1 = input_file.map(lambda line: json.loads(line)) \
                  .filter(lambda x : x.get('stars') and x.get('text')) \
                  .flatMap(lambda x : [(x['stars'], w) for w in x['text'].split()]) \
                  .cache()

step_2a = step_1.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
step_2b = step_1.filter(lambda x: x[0] >= 5).map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
step_2c = step_1.filter(lambda x: x[0] <= 2).map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

freq_words = step_2a.filter(lambda x : x[1] > 1000).cache()

step_3a = freq_words.join(step_2b)
step_3b = freq_words.join(step_2c)

all_review_word_count = step_2a.map(lambda x: x[1]).sum()
pos_review_word_count = step_2b.map(lambda x: x[1]).sum()
neg_review_word_count = step_2c.map(lambda x: x[1]).sum()

positive_words = step_3a.map(lambda x: (x[0], math.log(float(x[1][1])/pos_review_word_count) - math.log(float(x[1][0])/all_review_word_count))) \
                        .sortBy(lambda x: x[1], ascending = False)

negative_words = step_3b.map(lambda x: (x[0], math.log(float(x[1][1])/neg_review_word_count) - math.log(float(x[1][0])/all_review_word_count))) \
                        .sortBy(lambda x: x[1], ascending = False)

positive_words.saveAsTextFile("hdfs:///user/yuhangw/spark_positive_words_output")
negative_words.saveAsTextFile("hdfs:///user/yuhangw/spark_negative_words_output")
