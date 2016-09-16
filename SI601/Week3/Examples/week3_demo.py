#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TO DO:
# 1. Grab the page at http://www.themoviedb.org/movie
# 2. Parse the HTML document to get all movie titles on that page
# 3. Save the results in a JSON string and write it to a file

import urllib2
from bs4 import BeautifulSoup
import json

