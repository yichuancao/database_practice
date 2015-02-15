#!/usr/bin/python
# -*- coding: utf-8 -*-

# This code demos retrieving data from World Bank Developer API and interfacing with PostgreSQL DB hosted on AWS
import wbpy
import psycopg2
import sys
from pprint import pprint

api = wbpy.IndicatorAPI()

iso_country_codes = ["GB", "FR", "JP", "CN"]
total_population = "SP.POP.TOTL"

dataset = api.get_dataset(total_population, iso_country_codes, date="2010:2012")

print "This is our toy dataset from World Bank"
for country in iso_country_codes:
    for year in range(2010, 2013):
        print country, year, dataset.as_dict().get(country).get(str(year))

print

con = None
try:
    print "Connecting to DB..."
    con = psycopg2.connect("dbname='' port='' user='' host='' password=''")
    cur = con.cursor()
    # get DB version
    cur.execute('SELECT version()')
    ver = cur.fetchone()
    print "Database version"
    print ver
    print
    # check if a table exists in current DB
    cur.execute("select * from information_schema.tables where table_name=%s", ('products',))
    print "Table products exists? ", bool(cur.rowcount)
    cur.execute("select * from information_schema.tables where table_name=%s", ('cars',))
    print "Table cars exists? ", bool(cur.rowcount)
    # Delete a table
    print
    cur.execute("DROP TABLE IF EXISTS population")
    print "Deleted table population"
    print
    cur.execute("select * from information_schema.tables where table_name=%s", ('population',))
    print "Table population exists?", bool(cur.rowcount)
    print
    if bool(cur.rowcount) is False:
        print "We should create a table called population"
        # create DB table
        cur.execute("CREATE TABLE population(country_id text, year text, population integer)")
        command = ""
        for country in iso_country_codes:
            for year in range(2010, 2013):
                command = "INSERT INTO population VALUES(" + "'" + country + "','" + str(year) + "'," + str(dataset.as_dict().get(country).get(str(year))) + ")"
                print command
                # insert data
                cur.execute(command)
        con.commit()
        print "Sucessfully finished inserting data into table population"
        print
    else:
        print "The table already exists"
        print
    # Query the DB for a table
    print "Querying data from table population"
    cur.execute("SELECT * FROM population")
    rows = cur.fetchall()
    for row in rows:
        print row
except psycopg2.DatabaseError, e:
    if con:
        con.rollback() 
    print 'Error %s' % e
    sys.exit(1)
finally:
    if con:
        con.close()