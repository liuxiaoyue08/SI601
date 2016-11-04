# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 13:46:40 2016

@author: Heathtasia
"""

import pandas as pd
import re
import urlparse
#Read in data
access = pd.read_csv('access_log.txt', sep = ' ', header = None)

#Regular expressions for fields we want
status = '^[235][0-9][0-9]$'
start = '^[Hh][Tt][Tt][Pp](://|[Ss]://)'
ip = '^([\d]{1,3})\.([\d]{1,3})\.([\d]{1,3})\.([\d]{1,3})$'
host = '^[^/]'

#Get the http verb
def gethttp(s):
    return s.split()[0]
    
#Get the url query string    
def geturl(s):
    return s.split()[1]

#Check the length of values of the url query string
def urlqs(s):
    flag = True
    qss = urlparse.parse_qsl(s)
    for qs in qss:
        if len(qs[1]) > 80:
            flag = False
    return flag
    
#Check the validity
def is_valid(s):
    valid = False
    if len(s[5].split())>1:
        httpverb = gethttp(s[5])
        urlrs = geturl(s[5])
        if httpverb == 'GET' or 'POST' or 'HEAD':
            if re.search(status,str(s[6])) != None:
                if re.search(start, urlrs) != None:
                    if urlqs(urlrs) == True:
                        valid = True
        if httpverb == 'CONNECT':
            if (re.search(ip, urlrs) != None)|(re.search(host,urlrs) != None):
                if re.search(status, str(s[6])) != None:
                    valid = True
    return valid
    
#Initiating the validity column
valids=[]
for i in range(0, 200):
    valids.append(is_valid(access.iloc[i]))
    
#Add the validity column
access[10]=valids

    
