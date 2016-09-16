# Word count example in Pyspark
# Written by Dr. Yuhang Wang for SI601
# To run on Fladoop cluster:
# First ssh into a login node, then launch pyspark shell by typing 'pyspark --queue si601w15'
# Copy & paste code below into the pyspark shell

import re

WORD_RE = re.compile(r"\b[\w']+\b")
input_file = sc.textFile("hdfs:///user/yuhangw/pg1268.txt")

word_counts = input_file.flatMap(lambda line: WORD_RE.findall(line)) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

word_counts_sorted = word_counts.sortBy(lambda x: x[1], ascending = False)
word_counts_sorted.take(5)
word_counts_sorted.saveAsTextFile("hdfs:///user/yuhangw/spark_wordcount_output")
word_counts_sorted.map(lambda t : t[0] + '\t' + str(t[1])).saveAsTextFile('spark_wordcount_output2')

