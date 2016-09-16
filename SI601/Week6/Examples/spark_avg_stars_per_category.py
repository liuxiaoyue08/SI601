# Calculate the average stars for each business category
# Written by Dr. Yuhang Wang for SI601
# To run on Fladoop cluster:
# First ssh into a login node, then launch pyspark shell by typing 'pyspark --queue si601w15'
# Copy & paste code below into the pyspark shell

import simplejson as json

input_file = sc.textFile("hdfs:///user/yuhangw/yelp_academic_dataset_business.json")

def cat_star(data):
  cat_star_list = []
  categories = data.get('categories', None)
  if categories:
    for c in categories:
      stars = data.get('stars', None)
      if stars != None:
        cat_star_list.append((c, stars))
  return cat_star_list

cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_star) \
                      .mapValues(lambda x: (x, 1)) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                      .map(lambda x: (x[0], x[1][0]/x[1][1]))

cat_stars.collect()
