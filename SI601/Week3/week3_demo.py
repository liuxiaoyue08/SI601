#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TO DO:
# 1. Grab the page at http://www.themoviedb.org/movie
# 2. Parse the HTML document to get all movie titles on that page
# 3. Save the results in a JSON string and write it to a file

import urllib2
from bs4 import BeautifulSoup
import json

response = urllib2.urlopen('http://www.themoviedb.org/movie')
html = response.read()

soup = BeautifulSoup(html)
movielist = []
movielist.append(soup.find(class_ ="first").a['title'])

for m in soup.find('ul', class_ ="media_items").find_all(class_ ="info"):
  movielist.append(m.a.string)

json_str = json.dumps(movielist)

f = open('movies.json', 'w')

f.write(json_str)
f.close()

infile = open('movies.json', 'rU')
mystr = infile.read()
my_movies = json.loads(mystr)
infile.close()

print my_movies

