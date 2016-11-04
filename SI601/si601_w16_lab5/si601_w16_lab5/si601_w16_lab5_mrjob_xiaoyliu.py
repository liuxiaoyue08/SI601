#!/usr/bin/python

'''
SI 601 Winter 2016 Lab 5
Written by Dr. Yuhang Wang

To run ion Fladoop cluster:
First following instruction at http://caen.github.io/hadoop/user-hadoop.html#mrjob to create your
.mrjob.conf file, use 'si601w16' as queuename.
Then run commmands:
module load python-hadoop/2.7
python2.7 si601_w16_lab5_mrjob_youruniquename.py -r hadoop --no-output hdfs:///user/yuhangw/si601w16lab5_ebooks -o si601w16lab5_output_mrjob
'''

import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b[\w']+\b")

class BigramCount(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    
    def mapper(self, _, line):  
        words = WORD_RE.findall(line)
        for i in range(0,len(words)-1):
            bigram = words[i] + ' ' + words[i+1]
            yield (bigram.lower(), 1)

    def combiner(self, bigram, counts):
        yield (bigram, sum(counts))
        
    def reducer(self, bigram, counts):
        yield (bigram, str(sum(counts)))

if __name__ == '__main__':
    BigramCount.run()