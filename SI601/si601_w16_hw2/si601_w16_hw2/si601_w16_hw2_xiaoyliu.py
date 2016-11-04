# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 13:46:40 2016

@author: Heathtasia
"""

import re
import urlparse
#Regular expressions for fields we want
status = '^[235][0-9][0-9]$'
start = '^[Hh][Tt][Tt][Pp](://|[Ss]://)'
ip = '^([\d]{1,3})\.([\d]{1,3})\.([\d]{1,3})\.([\d]{1,3})$'
host = '^[^/]'


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
    s = s.split(' ')
    valid = False
    httpverb = s[5]
    urlrs = s[6]
    if httpverb == '"GET' or '"POST' or '"HEAD':
        if re.search(status,s[8]) != None:
            if re.search(start, urlrs) != None:
                if urlqs(urlrs) == True:
                    valid = True
        if httpverb == '"CONNECT':
            if (re.search(ip, urlrs) != None) or (re.search(host,urlrs) != None):
                if re.search(status, str(s[8])) != None:
                    valid = True
    return valid
#Read in each line    
input = open('access_log.txt', 'rU')
valid = open('valid_access_log_xiaoyliu.txt', 'w')
invalid = open('invalid_access_log_xiaoyliu.txt', 'w')
#For each line check the validity and output to valid/invalid files
for line in input:
    if is_valid(line):
        valid.write(line)
    else:
        invalid.write(line)

input.close()
valid.close()
invalid.close()

# extract_ip
def extract_ip(line):
    return line.split(' ')[0]
    
input2 = open('invalid_access_log_xiaoyliu.txt', 'rU')
#Count the attempts for each invalid ip address
susp = {}
for line in input2:
    ip = extract_ip(line)
    if ip not in susp:
        susp[ip] = 1
    else:
        susp[ip] += 1
input2.close()
#Sort the attempts
susp_sorted = sorted(susp.items(), key = lambda x : x[1], reverse = True)
#Output to csv file
output = open('suspicious_ip_summary_xiaoyliu.csv', 'w')
firstline = 'IP Address'+','+'Attempts'+'\n'
for susip in susp_sorted:
    line = susip[0]+','+str(susip[1])
    output.write(line+'\n')
output.close()





    
