#!/usr/bin/env python
# open the file containing data for each world_bank indicators
# step 1
#################################################################################################################################################
input1 = open('world_bank_indicators.tsv', 'rU')

names = set()
total_pop = {}
ub_pop = {}
life_exp = {}

# discard the first line
next(input1)
# loop through the file
import string
from string import punctuation
for line in input1:
     # split the contents of each line on the tab character
      col = line.split('\t')
     # transform the country names to standard string format 
      name_str_list = col[0].split()
      for i in range(0, len(name_str_list)):
            name_str_list[i] = filter(lambda x:x.isalpha(), name_str_list[i])
            
      name_str = ' '.join(name_str_list)
      
     # keep a set of distinct country names that occur in the dataset
      names.add(name_str)
     # create a tuple that holds the country name, to be used as key
      key = name_str
     # keep a dictionary of total_pop data for each country
      if '' != col[9]:
         total_pop.setdefault(key,[]).append(string.atof(filter(lambda x:x.isdigit(),col[9])))
      else:
         total_pop.setdefault(key,[]).append('')
      # keep a dictionary of ub_pop data for each country  
      if '' != col[10]:
         ub_pop.setdefault(key,[]).append(string.atof(filter(lambda x:x.isdigit(),col[10])))
      else:
         ub_pop.setdefault(key,[]).append('')
      # keep a dictionary of life_exp data for each country  
      if '' != col[14]:
         life_exp.setdefault(key,[]).append(string.atof(filter(lambda x:x.isdigit(),col[14])))
      else:
         life_exp.setdefault(key,[]).append('')
 
sum_total_pop = {}
sum_ub_pop = {}
ave_ub_rt = {}
ave_life_exp = {}
country1 = set()
country2 = set()
# calulate the sum of total population, sum of urban population and average urban populaton ratio for countries with no missing data in urban population
for name in names:
      if '' in ub_pop[name]:
        continue
      else:
        country1.add(name)
        sum_total_pop[name] = sum(total_pop[name])
        sum_ub_pop[name] = sum(ub_pop[name])
      
      ave_ub_rt[name] = sum_ub_pop[name]/sum_total_pop[name]
      
# define a function for caculating mean value
def averge(s):
    m = 0
    cnt = 0
    for i in s:
      if i != '':
         cnt += 1
         m = m + i
      else:
         continue
  
    if cnt != 0:
      m = m/cnt
      return m
    else:
      return ''

# caculate the average life expectation 
for name in names:
     
       if averge(life_exp[name]) != '':
         country2.add(name)
         ave_life_exp[name] = averge(life_exp[name])
       else:
         continue
# use a new set to save interception of the two country sets      
country_set = country1 & country2
country_list = map(lambda x: x,country_set)
country_list = sorted(country_list, key = str.lower)

#output to the CSV file
import csv
writer = csv.writer(open('si601_w16_hw1_step1_huiwenc.csv','wb'),dialect = 'excel')
# the first line should be the header
writer.writerow(['country name','average urban population ratio','average life expectancy','sum of total population in all years','sum of urban population in all years'])
# write the data line by line under sorted country name
for country_name in country_list:
      writer.writerow([country_name, ave_ub_rt[country_name],ave_life_exp[country_name], int(sum_total_pop[country_name]), int(sum_ub_pop[country_name])])
      
# step 2
#########################################################################################################################################################################
input2 = open('world_bank_regions.tsv', 'rU')
regions = set()
country_reg = {}
region_cty = {}

for line in input2:
     # split the contents of each line on the tab character
      [region, sub_reg, country] = line.split('\t')
      
      # transform the country names to standard string format 
      name_str_list = country.split()
      for i in range(0, len(name_str_list)):
            name_str_list[i] = filter(lambda x:x.isalpha(), name_str_list[i])
            
      country = ' '.join(name_str_list)

      if country in country_list:
            country_reg[country] = region
            regions.add(region)
      else:
            continue

sum_re_ub = {}
sum_re_total = {}
ave_re_ub_rt = {}
ave_re_life_exp = {}
for reg in regions:
     for country in country_reg.keys():
        if country_reg[country] == reg:
            region_cty.setdefault(reg,[]).append(country)
        else:
            continue
     
     sum_re_ub[reg] = 0
     sum_re_total[reg] = 0
     ave_re_life_exp[reg] = 0
     for cty in region_cty[reg]:
              sum_re_ub[reg] += sum_ub_pop[cty]
              sum_re_total[reg] += sum_total_pop[cty]
              ave_re_life_exp[reg] += ave_life_exp[cty]
       
          
     ave_re_ub_rt[reg] = sum_re_ub[reg]/sum_re_total[reg]
     ave_re_life_exp[reg] = ave_re_life_exp[reg]/len(region_cty[reg])
     

#output to the CSV file
import csv
writer2 = csv.writer(open('si601_w16_hw1_step2_huiwenc.csv','wb'),dialect = 'excel')
# the first line should be the header
writer2.writerow(['region','average urban population ratio','average life expectancy'])
# write the data line by line 
for reg in regions:
      writer2.writerow([reg, ave_re_ub_rt[reg],ave_re_life_exp[reg]])



