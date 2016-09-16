#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Written by Dr. Yuhang Wang for SI 601
# Based on http://zetcode.com/db/sqlitepythontutorial/

"""
http://docs.python.org/2/library/sqlite3.html
SQLite is a C library that provides a lightweight disk-based database that doesn誸
require a separate server process and allows accessing the database using a
nonstandard variant of the SQL query language.
Some applications can use SQLite for internal data storage. It誷 also possible
to prototype an application using SQLite and then port the code to a larger
database such as MySQL or Oracle.

See also
http://code.google.com/p/pysqlite/
The pysqlite web page � sqlite3 is developed externally under the name 襭ysqlite�.
http://www.sqlite.org
The SQLite web page; the documentation describes the syntax and the available data types for the supported SQL dialect.
http://www.w3schools.com/sql/
Tutorial, reference and examples for learning SQL syntax.

"""

import sqlite3 as sqlite
import sys

con = sqlite.connect('test.db')

"""
To create a table, we give a name to a table and to its columns. Each column can have one of these data types:

NULL - The value is a NULL value
INTEGER - a signed integer
REAL - a floating point value
TEXT - a text string
BLOB - a blob of data
"""

cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS Cars")
cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
cur.execute("INSERT INTO Cars VALUES(2,'Mercedes',57127)")
cur.execute("INSERT INTO Cars VALUES(3,'Skoda',9000)")
cur.execute("INSERT INTO Cars VALUES(4,'Volvo',29000)")
cur.execute("INSERT INTO Cars VALUES(5,'Bentley',350000)")
cur.execute("INSERT INTO Cars VALUES(6,'Citroen',21000)")
cur.execute("INSERT INTO Cars VALUES(7,'Hummer',41400)")
cur.execute("INSERT INTO Cars VALUES(8,'Volkswagen',21600)")

con.commit()
con.close()
"""
Now type
sqlite3 test.db
in a terminal. 

We can run these commands in sqlite3 shell:

sqlite> .tables
Cars
sqlite> .headers on
sqlite> .mode column
sqlite> select * from cars;
Id          Name        Price     
----------  ----------  ----------
1           Audi        52642     
2           Mercedes    57127     
3           Skoda       9000      
4           Volvo       29000     
5           Bentley     350000    
6           Citroen     21000     
7           Hummer      41400     
8           Volkswagen  21600     
"""

with sqlite.connect('test.db') as con: 
  cur = con.cursor()
  cur.execute("DROP TABLE IF EXISTS Cars")
  cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
  cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
  cur.execute("INSERT INTO Cars VALUES(2,'Mercedes',57127)")
  cur.execute("INSERT INTO Cars VALUES(3,'Skoda',9000)")
  cur.execute("INSERT INTO Cars VALUES(4,'Volvo',29000)")
  cur.execute("INSERT INTO Cars VALUES(5,'Bentley',350000)")
  cur.execute("INSERT INTO Cars VALUES(6,'Citroen',21000)")
  cur.execute("INSERT INTO Cars VALUES(7,'Hummer',41400)")
  cur.execute("INSERT INTO Cars VALUES(8,'Volkswagen',21600)")
    
# Using the with keyword, the changes are automatically committed.

# Demo how to populate table using executemany() method.
cars = [
    (1, 'Audi', 52642),
    (2, 'Mercedes', 57127),
    (3, 'Skoda', 9000),
    (4, 'Volvo', 29000),
    (5, 'Bentley', 350000),
    (6, 'Hummer', 41400),
    (7, 'Volkswagen', 21600)
]

with sqlite.connect('test.db') as con: 
  cur = con.cursor()    
  cur.execute("DROP TABLE IF EXISTS Cars")
  cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
  cur.executemany("INSERT INTO Cars VALUES(?, ?, ?)", cars)

# Demo how to use executescript()
with sqlite.connect('test.db') as con: 
  cur = con.cursor()  
  cur.executescript("""
      DROP TABLE IF EXISTS Cars;
      CREATE TABLE Cars(Id INT, Name TEXT, Price INT);
      INSERT INTO Cars VALUES(1,'Audi',52642);
      INSERT INTO Cars VALUES(2,'Mercedes',57127);
      INSERT INTO Cars VALUES(3,'Skoda',9000);
      INSERT INTO Cars VALUES(4,'Volvo',29000);
      INSERT INTO Cars VALUES(5,'Bentley',350000);
      INSERT INTO Cars VALUES(6,'Citroen',21000);
      INSERT INTO Cars VALUES(7,'Hummer',41400);
      INSERT INTO Cars VALUES(8,'Volkswagen',21600);
      """)

# use computer memory instead of file on hard drive to store tables
with sqlite.connect(':memory:') as con:  
  cur = con.cursor()
  # Id column is auto incremented, and it is the primary key of the Friends table
  cur.execute("CREATE TABLE Friends(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Tom');")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Rebecca');")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Jim');")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Robert');")
      
  lid = cur.lastrowid
  print "The last Id of the inserted row is %d" % lid


# Demo how to retrieve all data using fetchall()
with sqlite.connect('test.db') as con:    
  cur = con.cursor()    
  cur.execute("SELECT * FROM Cars")
  # fetchall() method gets all records. It returns a list of tuples.
  # Each of the tuples represent a row in the table.
  rows = cur.fetchall()

  print rows
  
  for row in rows:
      print row


# Returning all data at a time may not be feasible. We can fetch rows one by one.
with sqlite.connect('test.db') as con:   
  cur = con.cursor()    
  cur.execute("SELECT * FROM Cars")

  while True:
    row = cur.fetchone()
    
    if row == None:
        break
        
    print row[0], row[1], row[2]

# Demo use of the dictionary cursor
with sqlite.connect('test.db') as con:
  # select a dictionary cursor.
  # sqlite3.Row provides both index-based and case-insensitive name-based access
  # to columns with almost no memory overhead. 
  con.row_factory = sqlite.Row
     
  cur = con.cursor() 
  cur.execute("SELECT * FROM Cars")

  rows = cur.fetchall()
  print rows
  print type(rows[0])

  for row in rows:
    # name-based access
    print "%s %s %s" % (row["Id"], row["Name"], row["Price"])
    # index-based access
    print "%s %s %s" % (row[0], row[1], row[2])
    
    
# Demo parameterized queries
newId = 1
newPrice = 62300 

with sqlite.connect('test.db') as con:
  cur = con.cursor()
  
  # The question marks (?) are placeholders for values. The values are added to the placeholders.
  cur.execute("UPDATE Cars SET Price=? WHERE Id=?", (newPrice, newId))        
  con.commit()
  
  # The rowcount property returns the number of updated rows. 
  print "Number of rows updated: %d" % cur.rowcount
  
  cur.execute("SELECT * FROM Cars")

  rows = cur.fetchall()
  for row in rows:
    print row[0], row[1], row[2]
    
  uId = 4
  # select a name and a price of a car using named placeholders.
  # The named placeholders start with a colon character.
  cur.execute("SELECT Name, Price FROM Cars WHERE Id=:Id", {"Id": uId})        

  row = cur.fetchone()
  print row[0], row[1]


# Demo the autocommit mode where an SQL statement is executed immediately.
try:
  # We have an autocommit mode, when we set the isolation_level to None.
  con = sqlite.connect('test.db', isolation_level=None)
  cur = con.cursor()    
  cur.execute("DROP TABLE IF EXISTS Friends")
  cur.execute("CREATE TABLE Friends(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Tom')")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Rebecca')")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Jim')")
  cur.execute("INSERT INTO Friends(Name) VALUES ('Robert')")
  
except sqlite.Error, e:    
  print "Error %s:" % e.args[0]
  sys.exit(1)
finally:
  if con:
    con.close()
  
# Create and populate Orders table
with sqlite.connect('test.db') as con: 
  cur = con.cursor()  
  cur.executescript("""
      DROP TABLE IF EXISTS Orders;
      CREATE TABLE Orders(Id integer PRIMARY KEY AUTOINCREMENT, OrderPrice integer, Customer text);
      INSERT INTO Orders(OrderPrice, Customer) VALUES(1200, "Williamson");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(200, "Robertson");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(40, "Robertson");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(1640, "Smith");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(100, "Robertson");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(50, "Williamson");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(150, "Smith");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(250, "Smith");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(840, "Brown");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(440, "Black");
      INSERT INTO Orders(OrderPrice, Customer) VALUES(20, "Brown");
      """)    

    
# Demo more select statements
with sqlite.connect('test.db') as con:    
  cur = con.cursor()    
  cur.execute("SELECT * FROM Cars WHERE price > 30000")
  rows = cur.fetchall()
  for row in rows:
      print row
  
  print
  
  cur.execute("SELECT * FROM Cars ORDER BY price")
  rows = cur.fetchall()
  for row in rows:
      print row
  
  print

  cur.execute("SELECT * FROM Cars ORDER BY price DESC LIMIT 3")
  rows = cur.fetchall()
  for row in rows:
      print row
  
  print

  cur.execute("SELECT sum(price) FROM Cars")
  print cur.fetchone()[0]

  print
  
  cur.execute("SELECT DISTINCT Customer FROM Orders WHERE Customer LIKE 'B%'")
  rows = cur.fetchall()
  for row in rows:
      print row[0]
  
  print

  cur.execute("SELECT Customer, sum(OrderPrice) AS Total FROM Orders GROUP BY Customer ORDER BY Total DESC")
  rows = cur.fetchall()
  for row in rows:
      print row[0]
  
  print
  
  cur.execute("SELECT Customer, sum(OrderPrice) AS Total FROM Orders\
              GROUP BY Customer HAVING sum(OrderPrice)>1000")
  rows = cur.fetchall()
  for row in rows:
      print row

print


# Demo inner join, first create two tables
with sqlite.connect('test.db') as con: 
  cur = con.cursor()  
  cur.executescript("""
    DROP TABLE IF EXISTS Customers;
    CREATE TABLE Customers(CustomerId integer PRIMARY KEY, Name text);
    INSERT INTO Customers(Name) VALUES('Paul Novak');
    INSERT INTO Customers(Name) VALUES('Terry Neils');
    INSERT INTO Customers(Name) VALUES('Jack Fonda');
    INSERT INTO Customers(Name) VALUES('Tom Willis');
    
    DROP TABLE IF EXISTS Reservations;
    CREATE TABLE Reservations(Id integer PRIMARY KEY, CustomerId integer, Day text);
    INSERT INTO Reservations(CustomerId, Day) VALUES(1, '2009-22-11');
    INSERT INTO Reservations(CustomerId, Day) VALUES(2, '2009-28-11');
    INSERT INTO Reservations(CustomerId, Day) VALUES(2, '2009-29-11');
    INSERT INTO Reservations(CustomerId, Day) VALUES(1, '2009-29-11');
    INSERT INTO Reservations(CustomerId, Day) VALUES(3, '2009-02-12');
      """)
  
with sqlite.connect('test.db') as con:    
  cur = con.cursor()
  # INNER JOIN
  cur.execute("SELECT Name, Day FROM Customers AS C JOIN Reservations AS R ON (C.CustomerId=R.CustomerId)")
  rows = cur.fetchall()
  for row in rows:
      print row
  
  print
  # LEFT OUTER JOIN. SQLite only supports left outer joins.
  # The LEFT OUTER JOIN returns all values from the left table, even if there is no match with the right table. It such rows, there will be NULL values.
  cur.execute("SELECT Name, Day FROM Customers LEFT OUTER JOIN Reservations ON Customers.CustomerId = Reservations.CustomerId;")
  rows = cur.fetchall()
  for row in rows:
      print row
  
  print
  