#!/usr/bin/env python

# Written by Dr. Yuhang Wang for Lab 1 of UMSI SI 601 Fall 2015 class
# A few lines of code are taken from Google's Python Class
# http://code.google.com/edu/languages/google-python-class/

# Fill in the code for the functions below. main() is already set up
# to call the functions with a few different inputs,
# printing 'OK' when each function is correct.
# The starter code for each function includes a 'return'
# which is just a placeholder for your code.
# You need to fill in code for the function and let it actually return
# the stuff specified

# A. middle_part
# Given a string s, first remove any white spaces at the beginning or at the end,
# and then return a string made of middle part of the string excluding the first 2
# and the last 2 chars of the original string,
# so ' Michigan   ' yields 'chig'. However, if the string length
# is less than or equal to 4 after removing white spaces at the beginning or at the end,
# return the empty string.
# HINT:
# Look up these Python functions:
# str.strip()
# len()

def middle_part(s):
  s = s.strip()
  if len(s) <= 4:
      return ''
  else:
      return s[2:-2]

# B. donuts
# Given an integer count of the number of donuts, return a string
# of the form 'donut 1, donut 2, ..., donut <count>.', where <count> is the number
# passed in. There are two special cases:
# If the passed count is 0, return 'No donuts'.
# If the count is 5 or more, then use the string 'and <num> more donuts.' as the last item
# instead of the actual list of donuts, where <num> is the number of remaining donuts
# So the function call donuts(0) returns 'No donuts.'
# donuts(3) returns 'donut 1, donut 2, donut 3.'
# donuts(4) returns 'donut 1, donut 2, donut 3, donut 4.'
# donuts(10) returns 'donut 1, donut 2, donut 3, donut 4, and 6 more donuts!'
# donuts(20) returns 'donut 1, donut 2, donut 3, donut 4, and 16 more donuts!'
# HINT:
# Look up these Python functions:
# range()
# str()

def donuts(count):
  if count <= 4 and count > 1:
     str1 = 'donut'+ ' ' + str(1)
     for i in range(2,count+1):
         str1 += ', ' + 'donut'+ ' ' + str(i)
     str1 += '.'
  elif count > 4:
     str1 = 'donut 1, donut 2, donut 3, donut 4, and ' + str(count - 4) +' more donuts!'
  elif count == 1:
     str1 = 'donut 1.'
  else:
     str1 = 'No donuts.'
  return str1

# C. match_ends
# Given a list of strings, return the number of UNIQUE strings in the list
# where the string length is 9 or more and the string starts with 'a' and
# ends with 'ology'
# So match_ends(['aerobiology', 'neurology', 'aerology', 'anthropology', 'aerobiology', 'neurology', 'aerology', 'anthropology'])
# should return 2
# HINT:
# Look up these Python functions:
# set()
# set.add()
# str.startswith()
# str.endswith()

def match_ends(words):
  uni_words = set()
  for word in words:
     uni_words.add(word)
     
  count = 0
  for word in uni_words:
     if len(word) < 9:
       continue
     elif word.startswith('a', 0, 1) and word.endswith('ology'):
       count += 1
     else:
       continue
  return count
  

# D. unique_counts
# Given a list of strings, return a list of tuples containing the counts of each of the
# UNIQUE strings. The returned results should be ordered by the counts
# in decreasing order. In case of ties of counts, break the tie by string value in increasing order
# Note: python does not have a ++ operator, but += works.
# So unique_counts(['Aurora', 'Jasmine', 'Jasmine', 'Jasmine', 'Belle', 'Belle'])
# should return [('Jasmine', 3), ('Belle', 2), ('Aurora', 1)]
# and unique_counts(['Belle', 'Adella', 'Aurora', 'Belle', 'Irene', 'Jasmine', 'Belle', 'Aurora'])
# should return [('Belle', 3), ('Aurora', 2), ('Adella', 1), ('Irene', 1), ('Jasmine', 1)])
# HINT:
# Look up these Python functions:
# dict.items()
# sorted()
# You will need to write a helper function to use with sorted()
# You can either write a named function or an anonymous lambda function

def unique_counts(words):
  words_dic = {}
  words_list = []
  for word in words:
     if word in words_dic:
         words_dic[word] += 1
     else:
         words_dic[word] = 1
         
  words_list = words_dic.items()
  words_list = sorted(words_list, key = lambda x: x[0])
  words_list = sorted(words_list, key = lambda x: x[1], reverse=True)
  return words_list
 

#######################################################################
# DO NOT MODIFY ANY CODE BELOW
#######################################################################

# Provided simple test() function used in main() to print
# what each function returns vs. what it's supposed to return.
def test(got, expected):
  if got == expected:
    prefix = ' OK '
  else:
    prefix = '  X '
  print '%s got: %s expected: %s' % (prefix, repr(got), repr(expected))


# Provided main() calls the above functions with interesting inputs,
# using test() to check if each result is correct or not.
def main():
  print
  print 'middle_part'
  """ If you get the output below, you are good. Each OK is worth one point.
  OK  got: 'chig' expected: 'chig'
  OK  got: 'ten Ta' expected: 'ten Ta'
  OK  got: '' expected: ''
  OK  got: '' expected: ''
  OK  got: '' expected: ''
  """
  test(middle_part(' Michigan   '), 'chig')
  test(middle_part('Guten Tag!'), 'ten Ta')
  test(middle_part('cool'), '')
  test(middle_part('abc '), '')
  test(middle_part(''), '')


  print 'donuts'
  # Each line calls donuts, compares its result to the expected for that call.
  """ If you get the output below, you are good. Each OK is worth one point.
  OK  got: 'No donuts.' expected: 'No donuts.'
  OK  got: 'donut 1.' expected: 'donut 1.'
  OK  got: 'donut 1, donut 2, donut 3.' expected: 'donut 1, donut 2, donut 3.'
  OK  got: 'donut 1, donut 2, donut 3, donut 4.' expected: 'donut 1, donut 2, donut 3, donut 4.'
  OK  got: 'donut 1, donut 2, donut 3, donut 4, and 6 more donuts!' expected: 'donut 1, donut 2, donut 3, donut 4, and 6 more donuts!'
  """
  test(donuts(0), 'No donuts.')
  test(donuts(1), 'donut 1.')
  test(donuts(3), 'donut 1, donut 2, donut 3.')
  test(donuts(4), 'donut 1, donut 2, donut 3, donut 4.')
  test(donuts(10), 'donut 1, donut 2, donut 3, donut 4, and 6 more donuts!')


  print 'match_ends'
  """ If you get the output below, you are good. Each OK is worth one point.
  OK  got: 2 expected: 2
  OK  got: 2 expected: 2
  OK  got: 2 expected: 2
  OK  got: 3 expected: 3
  OK  got: 0 expected: 0
  """
  test(match_ends(['aerobiology', 'neurology', 'aerology', 'anthropology']), 2)
  test(match_ends(['aerobiology', 'neurology', 'Battlestar Galactica', 'aerology', 'anthropology']), 2)
  test(match_ends(['aerobiology', 'neurology', 'aerology', 'anthropology', 'aerobiology', 'neurology', 'aerology', 'anthropology']), 2)
  test(match_ends(['anthropology', 'anthropology', 'aerobiology', 'neurology', 'Battlestar', 'Galactica', 'aerology', 'anthropology', 'antitechnology']), 3)
  test(match_ends([]), 0)

  print 'unique_counts'
  """ If you get the output below, you are good. Each OK is worth one point.
  OK  got: [] expected: []
  OK  got: [('apple', 1)] expected: [('apple', 1)]
  OK  got: [('pear', 2)] expected: [('pear', 2)]
  OK  got: [('apple', 3), ('pear', 2), ('banana', 1)] expected: [('apple', 3), ('pear', 2), ('banana', 1)]
  OK  got: [('apple', 3), ('grape', 2), ('ginseng fruit', 1), ('orange', 1), ('star fruit', 1)] expected: [('apple', 3), ('grape', 2), ('ginseng fruit', 1), ('orange', 1), ('star fruit', 1)]
  """
  test(unique_counts([]), [])
  test(unique_counts(['apple']), [('apple', 1)])
  test(unique_counts(['pear', 'pear']), [('pear', 2)])
  test(unique_counts(['banana', 'apple', 'apple', 'apple', 'pear', 'pear']), [('apple', 3), ('pear', 2), ('banana', 1)])
  test(unique_counts(['apple', 'orange', 'grape', 'apple', 'star fruit', 'ginseng fruit', 'apple', 'grape']), [('apple', 3), ('grape', 2), ('ginseng fruit', 1), ('orange', 1), ('star fruit', 1)])
  
  
# Standard boilerplate to call the main() function.
if __name__ == '__main__':
  main()