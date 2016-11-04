# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 18:15:54 2016

@author: Heathtasia
"""
import sqlite3 as sqlite
import sys

def main():
  
  if len(sys.argv[1:]) != 2:
    print 'Usage:\n commandline_args_example1.py genre k'
    sys.exit(1)
  
  
  genre = sys.argv[1]
  k = sys.argv[2]
  with sqlite.connect('si601_w16_hw4.db') as con:
    cur = con.cursor()
    cur.execute("SELECT actor, count(*) as N_COUNT FROM movie_actor JOIN movie_genre on (movie_actor.imdb_id=movie_genre.imdb_id) WHERE movie_genre.genre='%s' GROUP BY movie_actor.actor ORDER BY N_COUNT DESC, actor LIMIT '%s'" %(genre,k))
    rows = cur.fetchall()
    print "Top %s actors who played in most %s movies:" % (k,genre)
    print "Actor, %s Movies Played in" % genre
    for row in rows:
        print "%s, %s" % (row[0],row[1])


if __name__ == "__main__":
    main()

con = sqlite.connect('si601_w16_hw4.db')

