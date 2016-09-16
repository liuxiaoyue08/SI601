# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import re, json

response = urllib2.urlopen('http://www.indexmundi.com/g/r.aspx?c=xx&v=30')
html_doc = response.read()
soup = BeautifulSoup(html_doc)

outfile = open('indexmundi.html', 'w')
outfile.write(soup.prettify().encode('utf-8'))
outfile.close()

data_table = soup.find_all('table')[2]

lol = []

for row in data_table.find_all('tr'):
  l = []
  for s in row.strings:
    l.append(unicode(s))    
  
  lol.append(l)
      
outfile = open('indexmundi.txt', 'w')
for l in lol:
  line = '\t'.join(l) + '\n'
  outfile.write(unicode(line).encode('utf8'))
outfile.close()

del lol[0]
# print json.dumps(lol)


data_dict = {}

for l in lol:
  data_dict[l[1]] = [l[0], l[2]]
  
json_str = json.dumps(data_dict)

new_data = json.loads(json_str)

print new_data

