# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 09:59:19 2016

@author: Heathtasia
"""
import numpy as np
import pandas as pd
from pandas import DataFrame

#Step1

input_data = open ('world_bank_indicators.tsv','rU')
line_no = 0
data_dict = {}
Country = []
year = []
Tpop = []
Upop = []
HleT = []
for line in input_data:
    line_no += 1
    if line_no != 1:
        (country, date, tR, tP, bM, bI, hM, hHp, hHt, popT, popU, popB, hLeF, hLeM, hLeT, pop14, pop64, pop65, fG, fGp) = line.split('\t')
        Country.append(country.strip('/"'))
        year.append(date)
        Tpop.append(filter(str.isdigit, popT))
        Upop.append(filter(str.isdigit, popU))
        HleT.append(filter(str.isdigit, hLeT))
input_data.close()
data_dict = {'country' : Country, 'year': year, 'popT': Tpop, 'popU': Upop, 'hLeT': HleT}
df = DataFrame(data_dict, columns=['country','year', 'popT','popU','hLeT'])

for x in range(0,2354):
    if df.ix[x][2] != '':
        df.ix[x][2]=float(df.ix[x][2])
    else:
        df.ix[x][2]=float(np.nan)
    if df.ix[x][3] != '':
        df.ix[x][3]=float(df.ix[x][3])
    else:
        df.ix[x][3]=float(np.nan)
    if df.ix[x][4] != '':
        df.ix[x][4]=float(df.ix[x][4])
    else:
        df.ix[x][4]=float(np.nan)

df[['popT','popU','hLeT']]=df[['popT','popU','hLeT']].astype(float)
df_sum = df.groupby(df['country']).sum()
df_mean = df.groupby(df['country']).mean()
df_ratio = df['popU'].groupby(df['country']).sum() / df['popT'].groupby(df['country']).sum()

df_o = DataFrame({'country name': list(df_mean.index), 'average urban population ratio': list(df_ratio), 'average life expectancy': list(df_mean['hLeT']), 'sum of total population in all years': list(df_sum['popT']), 'sum of urban population in all years': list(df_sum['popU'])})
#Drop rows with missing valuse
df_do = df_o.dropna(axis = 0)

df_do.to_csv('si601_w16_hw1_step1_xiaoyliu.csv',encoding='utf-8', index=False, columns=['country name','average urban population ratio','average life expectancy','sum of total population in all years','sum of urban population in all years'])

#Step2
#Region = []
#Subregion = []
#Country2 = []
#region_input = open('world_bank_regions.tsv','rU')
#for line in region_input:
#    (region,subregion,country2) = line.strip().split('\t')
#    region = region.strip()
#    Region.append(region)
#    Subregion.append(subregion)
#    country2 = country2.strip('/"')
#    country2 = country2.strip('\n')
#    Country2.append(country2)
#region_input.close()
region_data = pd.read_csv('world_bank_regions.tsv',sep='\t',header=None)
region_data.columns = ['region','subregion','country']

df_do2 = df_do
df_merged = pd.merge(df_do2,region_data,left_on='country name',right_on='country')

df_merged_ratio = df_merged['sum of urban population in all years'].groupby(df_merged['region']).sum()/df_merged['sum of total population in all years'].groupby(df_merged['region']).sum()
df_merged_mean =df_merged['average life expectancy'].groupby(df_merged['region']).mean()
df_o2 = DataFrame({'region': list(df_merged_mean.index),'average urban population ratio':list(df_merged_ratio),'average life expectancy':list(df_merged_mean)})
df_o2 = df_o2.sort(columns = 'average life expectancy', ascending = False)
df_o2.to_csv('si601_w16_hw1_step2_xiaoyliu.csv',encoding='utf-8', index=False, columns=['region','average urban population ratio','average life expectancy'])


#df = DataFrame({'country':[country], 'popT':[popT[1:]], 'popU': [popU[1:]], 'hLeT': [hLeT[1:]]})

#print df,'\n'
