# Sums up the number of local business in each city in Yelp's Academic Dataset
# Written by Dr. Yuhang Wang for SI601
# To run on Fladoop cluster:
# First ssh into a login node, then launch pyspark shell by typing 'pyspark --queue si601w15'
# Copy & paste code below into the pyspark shell

import simplejson as json

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_business.json")

city_businesses = input_file.map(lambda line: json.loads(line)) \
                            .map(lambda x : (x['city'], 1)) \
                            .reduceByKey(lambda a, b: a + b)

city_businesses.collect()
