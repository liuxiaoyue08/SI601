# Filter data in yelp_academic_dataset_business.json to get only local businesses in Pittsburgh
# Written by Dr. Yuhang Wang for SI601
# To run on Fladoop cluster:
# First ssh into a login node, then launch pyspark shell by typing 'pyspark --queue si601w15'
# Copy & paste code below into the pyspark shell

import simplejson as json

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_business.json")

pitts_businesses = input_file.map(lambda line: json.loads(line)) \
                             .filter(lambda x : x.get('city', '') == 'Pittsburgh')

pitts_businesses.take(5)