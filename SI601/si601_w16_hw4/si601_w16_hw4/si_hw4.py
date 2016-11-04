# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 21:26:35 2016

@author: Heathtasia
"""

import sqlite3 as sqlite
import json

f = open('movie_actors_data.txt','rU')
data = {}
movie_genre = []
movies = []
movie_actor = []
for line in f:
    data = json.loads(line)
    for x in data['genres']:
        movie_genre.append((data['imdb_id'], x))
    movies.append((data['imdb_id'],data['title'],data['year'],data['rating']))    
    for actor in data['actors']:
        movie_actor.append((data['imdb_id'], actor))
    
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS movie_genre")
    cur.execute("CREATE TABLE movie_genre(imdb_id TEXT, genre TEXT)")
    cur.executemany("INSERT INTO movie_genre VALUES(?,?)", movie_genre)
    con.commit()
    
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS movies")
    cur.execute("CREATE TABLE movies(imdb_id TEXT, title TEXT, year INT, rating REAL)")
    cur.executemany("INSERT INTO movies VALUES(?,?,?,?)", movies)
    con.commit()
    
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS movie_actor")
    cur.execute("CREATE TABLE movie_actor(imdb_id TEXT, actor TEXT)")
    cur.executemany("INSERT INTO movie_actor VALUES(?,?)", movie_actor)
    con.commit()

with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS movie_actor2")
    cur.execute("CREATE TABLE movie_actor2(imdb_id TEXT, actor TEXT)")
    cur.executemany("INSERT INTO movie_actor2 VALUES(?,?)", movie_actor)
    con.commit()


with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("SELECT genre, count(*) as N_COUNT FROM movie_genre GROUP BY genre ORDER BY N_COUNT DESC LIMIT 10")
    rows = cur.fetchall()
    print "Top 10 genres:"
    print "Genre, Movies"
    for row in rows:
        print "%s, %s" % (row[0],row[1])
        
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("SELECT count(*) as N_COUNT, year FROM movies GROUP BY year ORDER BY year")
    rows = cur.fetchall()
    print "Movies broken down by year:"
    print "Year, Movies"
    for row in rows:
        print "%s, %s" % (row[0],row[1])
        
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("SELECT title, year, rating FROM movies JOIN movie_genre on (movies.imdb_id=movie_genre.imdb_id) WHERE genre = 'Sci-Fi' ORDER BY rating DESC, year DESC")
    rows = cur.fetchall()
    print "Sci-fi Movies:"
    print "Title, Year, Rating"
    for row in rows:
        print "%s, %s, %s" % (row[0],row[1],row[2])
        
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("SELECT actor, count(*) as N_COUNT FROM movie_actor JOIN movies on (movie_actor.imdb_id=movies.imdb_id) WHERE year>=2000 GROUP BY actor ORDER BY N_COUNT DESC, actor LIMIT 10")
    rows = cur.fetchall()
    print "In and after year 2000, top 10 actors who played in most movies:"
    print "Actor, Movies"
    for row in rows:
        print "%s, %s" % (row[0],row[1])

        
with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("SELECT movie_actor.actor, movie_actor2.actor, count(*) as N_COUNT FROM movie_actor LEFT OUTER JOIN movie_actor2 ON (movie_actor.imdb_id=movie_actor2.imdb_id) \
    WHERE movie_actor.actor<>movie_actor2.actor GROUP BY movie_actor.actor, movie_actor2.actor HAVING N_COUNT>=3 ORDER BY N_COUNT DESC")
    rows = cur.fetchall()
    print "Pairs of actors who co-stared in 3 or more movies:"
    print "Actor A, Actor B, Co-stared Movies"
    for row in rows:
        print "%s, %s, %s" % (row[0],row[1],row[2])