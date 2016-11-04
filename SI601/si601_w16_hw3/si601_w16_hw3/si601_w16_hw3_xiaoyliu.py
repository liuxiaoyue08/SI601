# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 22:04:09 2016

@author: Heathtasia
"""

import urllib2
from bs4 import BeautifulSoup
import json
import time
import re

##Step1
##From charset of the pages we know that the encoding is already utf-8 
for i in range(1,152,50):
    page  = urllib2.urlopen("http://www.imdb.com/search/title?at=0&genres=sci_fi&sort=user_rating&start="+str(i)+"&title_type=feature")
    html_doc = page.read()
    f = open("step1_top_scifi_movies_"+str(i)+"_to_"+str(i+50-1)+".html", "w")
    f.write(html_doc)
    f.close()

#Step2
#We find that the first table contains the information of the movies
#table = soup1.find_all('table')[0]

#Function to get the information of the movies
def scan_movies(soup1):
    #Get all the ranks
    ranks = []
    for rank in soup1.find_all("td", class_="number"):
        rank = rank.string
        rank = re.findall("[\d]+", rank)[0]
        ranks.append(rank)
    #Get all the IMDB ID
    imdbids = []
    for imdbid in soup1.find_all('td',class_="title"):
        imdbid = imdbid.a
        imdbid = re.findall("tt[\d]+",imdbid["href"])[0]
        imdbids.append(imdbid)
    #Get all the titles
    titles=[]
    for title in soup1.find_all('td',class_="title"):
        title = title.a
        titles.append(unicode(title.string))
    #Get all the years
    years = []
    for year in soup1.find_all("span", class_="year_type"):
        year = year.string
        year = re.findall("[\d]+", year)[0]
        years.append(year)
    #Get all the ratings
    ratings = [] 
    for rating in soup1.find_all("span", class_="value"):
        if rating.string == "":
            ratings.append("NA")
        else:
            ratings.append(rating.string)
    movies = {"Rank":ranks,"IMDB ID":imdbids,"Title":titles,"Year":years,"Rating":ratings}
    return movies

#Initiating for output lists 
ranks = []
imdbids = []
titles = []
years = []
ratings = []       
        
#Read movie information from each html file and combine into output lists        
for i in range(1,152,50):
    page= open("step1_top_scifi_movies_"+str(i)+"_to_"+str(i+50-1)+".html", "rU")
    html_doc = page.read()
    soup = BeautifulSoup(html_doc)
    movies = scan_movies(soup)
    ranks.extend(movies["Rank"])
    imdbids.extend(movies["IMDB ID"])
    titles.extend(movies["Title"])
    years.extend(movies["Year"])
    ratings.extend(movies["Rating"])
    page.close()

#At first I got some extra 1 IMDB ID for each page. I found it is because some movies in the outline
#ids=[]
#f2 = open ("step3_desired_output.txt","rU")
#for line in f2:    
#    (id2,content) = line.split('\t') 
#    ids.append(id2)
#    
#f2.close()
#
#strange = [val for val in imdbids if val not in ids]

titlesj = json.loads(json.dumps(titles))
    
output = open("step2_top_200_scifi_movies.tsv", "w")
firstline = "Rank\tIMDB ID\tTitle\tYear\tRating\n"
output.write(firstline)
for i in range(0,len(ranks)):
    line = ranks[i]+'\t'+imdbids[i]+'\t'+titlesj[i]+'\t'+years[i]+'\t'+ratings[i]+'\n'
    line8 = line.encode('utf-8')
    output.write(line8)
output.close()
    
##Step3
output2 = open("step3.txt","w")

for imdbid in imdbids:
    response = urllib2.urlopen("https://api.themoviedb.org/3/find/"+imdbid+"?api_key=35e5a77e113d2e11721db6b318577065&external_source=imdb_id") 
    content = response.read()
    output2.write(imdbid+"\t"+content+"\n")
    time.sleep(5)
output2.close()

#Step4
imdbids2 = []
themoviedbs = []
f = open("step3.txt","rU")
output3 = open("step4.csv","w")
output3.write("IMDB ID,Title,Year,IMDB Rating,Themoviedb Rating\n")
for line in f:
    (imdbid, themoviedb) = line.split('\t')
    imdbids2.append(imdbid)
    try:
        themoviedb = json.loads(themoviedb)[u'movie_results'][0][u'vote_average']
    except:
        themoviedb = 0.0
    themoviedbs.append(themoviedb)
for i in range(0,len(imdbids2)):
    if ratings[i]!="NA" and themoviedbs[i]!=0.0:
        line = imdbids2[i]+','+titlesj[i]+','+years[i]+','+ratings[i]+','+unicode(themoviedbs[i])+'\n'
        line8 = line.encode('utf-8')
        output3.write(line8)
f.close()
output3.close()





    
#rating = content[u'movie_results'][0][u'vote_average']    



#    
#table=soup1.table.find_all(has_href_no_title)
#
##Get all the titles
#titles=[]
#for title in soup1.table.find_all(has_href_no_title,href=re.compile("tt[\d]+")):
#    titles.append(title.string)
#
##Get all the ranks
#ranks = []
#for rank in soup1.find_all("td", class_="number"):
#    ranks.append(rank.string)
#    
##Get all the ratings.
#ratings = [] 
#for rating in soup1.find_all("span", class_="value"):
#    if rating.string == "":
#        ratings.append("NA")
#    else:
#        ratings.append(rating.string)
#    
##Get all the years
#years = []
#for year in soup1.find_all("span", class_="year_type"):
#    year = year.string
#    year = re.findall("[\d]+", year)[0]
#    years.append(year)
#    
##Get all the IMDB ID
#imdbids = []
#for imdbid in soup1.table.find_all(has_href_no_title,href=re.compile("tt[\d]+")):
#    imdbid = re.findall("tt[\d]+",imdbid["href"])[0]
#    imdbids.append(imdbid)
#    