#!/usr/bin/python

'''
SI 601 Winter 2016 Lab 5
Written by Dr. Yuhang Wang

Fill in code where specified to compute bigram counts for the input text file.

To run on Fladoop cluster:

spark-submit --master yarn-client --queue si601w16 --num-executors 2 --executor-memory 1g --executor-cores 2 si601_w16_lab5_spark_youruniquename.py hdfs:///user/yuhangw/si601w16lab5_ebooks si601w16lab5_output_spark
'''

import sys, re
from pyspark import SparkContext

if len(sys.argv) < 3:
  print "need to provide input and output dir"
else:
  inputdir = sys.argv[1]
  outputdir = sys.argv[2]

  sc = SparkContext(appName="PythonBigram")
  WORD_RE = re.compile(r"\b[\w']+\b")
  input_file = sc.textFile(inputdir)
  
  bigram_counts = input_file.map(+++your code here to find words and convert to lower case+++) \
                      .flatMap(+++your code here to find bigrams and emit (bigram, count) pairs in a list+++) \
                      .reduceByKey(lambda a, b: a + b) \
                      .sortBy(+++your code here to sort the bigrams and counts+++)
  
  bigram_counts.map(lambda t : t[0] + '\t' + str(t[1])).repartition(1).saveAsTextFile(outputdir)
