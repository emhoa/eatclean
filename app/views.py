#!/usr/bin/python
#
# Script to load restaurant violation and grade info into Postgre
#
import os
import psycopg2
import urlparse
import requests
from csv import reader
import datetime
from app import app
from flask import render_template
from flask import request

@app.route('/')
@app.route('/restaurants')
def index():
	return render_template("bulkInsertGrades.html")

@app.route('/test')
def mytest():
	return render_template("test.html")

def get_timestamp(): 
	return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@app.route('/restaurants', methods=['POST'])
def bulkInsert():
	print "in bulk insert"
	RESTAURANT_SOURCE_FILE="https://nycopendata.socrata.com/api/views/xx67-kt59/rows.csv?accessType=DOWNLOAD"
	RESTAURANT_DEST_FILE="/tmp/" + "restaurantgrades" + datetime.datetime.now().strftime("%Y-%m-%d") + ".csv"
	DOWNLOAD_DATA_TABLE="input_data"


	urlparse.uses_netloc.append("postgres")
	url = urlparse.urlparse(os.environ["DATABASE_URL"])

	try:
#	conn = psycopg2.connect("dbname='restaurantgrades' user='postgres' host='localhost' password='postgres'")
		conn = psycopg2.connect(database=url.path[1:], user=url.username, password=url.password, host=url.hostname, port=url.port)
	except:
		print "Unable to connect to the database"
		return render_template("bulkInsertGrades.html")


	print get_timestamp() + ": Starting download of source data file"

	try:
		download_file = open(RESTAURANT_DEST_FILE, "wb")
	except:
		print get_timestamp() + ": Unable to open " + RESTAURANT_DEST_FILE + " for writing."
		return render_template("bulkInsertGrades.html")

	try:
		r = requests.get(RESTAURANT_SOURCE_FILE)
	except:
		print get_timestamp() + ": Unable to download file " + RESTAURANT_SOURCE_FILE
		return render_template("bulkInsertGrades.html")

	print get_timestamp() + ": Downloaded " + RESTAURANT_SOURCE_FILE
	download_file.write(r.content)
	download_file.close()

	print get_timestamp() + ": Opening file to find column names"
	restaurant_file = open(RESTAURANT_DEST_FILE, "rb")

	columns = restaurant_file.readline().split(",")
	restaurant_file.close()
	drop_table_query = """DROP table if exists """ + DOWNLOAD_DATA_TABLE + ";"""
	create_table_query = """CREATE table """ + DOWNLOAD_DATA_TABLE + """(id serial, """

	i=0
	collen = len(columns)
	insert_stmt = """\copy """ + DOWNLOAD_DATA_TABLE + " ("

	#Add an _ to column names with spaces in them to make them sql-safe
	#concatenate columns to build sql statement to create table and bulk copy
	for c in columns:
        	colname = ""
	        for ch in c:
        	        if ch == ' ':
	                        colname += '_'
	       	        else:
        	                colname += ch
	        columns[i] = colname
        	create_table_query += colname + " text"
	        insert_stmt += colname
        	if i<collen-1:
                	create_table_query += ", "
	                insert_stmt += ", "

        	i=i+1

	create_table_query += """);"""

	# complete insert_stmt so that it can read from the csv file
	#insert_stmt += ") FROM '" + RESTAURANT_DEST_FILE +"""' with (FORMAT CSV, HEADER, QUOTE '"');"""
	insert_stmt += ") FROM " + RESTAURANT_DEST_FILE +""" with CSV QUOTE '"'"""
	print "Copy string is: " + insert_stmt + "|"

	add_pkey = "ALTER TABLE " + DOWNLOAD_DATA_TABLE + " ADD PRIMARY KEY (id);"
	analyze_query = "ANALYZE " + DOWNLOAD_DATA_TABLE + ";"

	print get_timestamp() + ": Now preparing to insert rows into " + DOWNLOAD_DATA_TABLE + "\n"
	cur = conn.cursor()
	try:
		cur.execute(drop_table_query)
	except:
		print "Couldn't even drop table"

	try:
		cur.execute(create_table_query)
	except:
		print "Couldn't even create table"
	try:
		conn.commit()
	except:
		print "Can't commit dropping table and recreating it"

	insert_cur = conn.cursor()

	try:
		#insert_cur.execute(insert_stmt + add_pkey + analyze_query)
		insert_cur.execute(insert_stmt)
	except Exception as e:
		#print get_timestamp() + ": Unable to create table and bulk upload"
		print get_timestamp() + ": Unable to bulk upload with error: " + str(e)

	try:
		insert_cur.execute(add_pkey)
	except:
		print get_timestamp() + ": Unable to add primary key"

	try:
		insert_cur.execute(analyze_query)
	except:
		print get_timestamp() + ": Unable to analyze table"

	return render_template("bulkInsertGrades.html")


	insert_cur.close()
	print get_timestamp() + ": Finished with inserting into " + DOWNLOAD_DATA_TABLE + "\n"

	#Now create restaurant table
	rest_cur = conn.cursor()
	create_rest_query = "drop table if exists restaurant; create table restaurant as (select distinct camis, dba, boro, building, street, zipcode, phone, cuisine_description from " + DOWNLOAD_DATA_TABLE + "); alter table restaurant add primary key (camis);"
	try:
		rest_cur.execute(create_rest_query)
	except:
		print get_timestamp() + ": Could not create restaurant table"

	print get_timestamp() + ": Created restaurant table"
	rest_cur.close()

	#Create restaurant grades table
	restgrades_cur = conn.cursor()
	drop_restgrades_query = "drop table if exists restaurant_grades;"
	create_restgrades_query = """create table restaurant_grades as select distinct camis, to_date(inspection_date, 'MM/DD/YYY') as inspection_date, inspection_type, action, score, grade, to_date(grade_date, 'MM/DD/YYYY') as grade_date, to_date(record_date, 'MM/DD/YYYY') as record_date from """ + DOWNLOAD_DATA_TABLE + """ where grade != '';"""
	add_restgrades_pkey_query = "alter table restaurant_grades add primary key (camis, inspection_date, inspection_type, grade);"
	try:
		restgrades_cur.execute(drop_restgrades_query + create_restgrades_query + add_restgrades_pkey_query)
	except:
		print get_timestamp() + ": Could not create restaurant_grades table"

	print get_timestamp() + ": Created restaurant grades table"

	restgrades_cur.close()
	conn.commit()
	conn.close()


