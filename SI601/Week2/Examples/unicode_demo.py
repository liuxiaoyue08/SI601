#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs

str1 = '中国'
unicode_str1 = u'中国'

print len(str1)
print len(unicode_str1)

str_list = ['In Chinese, China is', u'中国.']

#output_file = open('unicode_demo.txt', 'w')
#
#for line in str_list:
#  output_file.write(line + '\n')
#
#output_file.close()

output_file = codecs.open('unicode_demo.txt', encoding='utf-8', mode = 'w')
for line in str_list:
  output_file.write(line + '\n')

output_file.close()
