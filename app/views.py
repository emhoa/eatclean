#!/usr/bin/python
#
# Script to load restaurant violation and grade info into Postgre
#
import os
import psycopg2
import urlparse
import requests
import datetime
from app import app
from flask import render_template
from flask import request
from flask import Markup


@app.route('/')
@app.route('/restaurants')
def index():
	return render_template("bulkInsertGrades.html")

def get_timestamp(): 
	return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@app.route('/restaurants', methods=['POST'])
def bulkInsert():
	select_query = """select distinct dba, building, street, boro, phone, score::int, grade, grade_date from restaurant_grades rg, restaurant r where r.camis = rg.camis and r.cuisine_description = 'Thai' and r.camis not in (select camis from restaurant_grades where grade not in ('A', 'B')) order by grade_date desc, score::int asc limit 10;"""

 	#Connect to postgres database

       	urlparse.uses_netloc.append("postgres")
        url = urlparse.urlparse(os.environ["DATABASE_URL"])

       	try:
               	conn = psycopg2.connect(database=url.path[1:], user=url.username, password=url.password, host=url.hostname, port=url.port)
        except Exception as e:
       	        print "Unable to connect to the database: " + str(e)
	cur = conn.cursor()

	try:
		cur.execute("select yes from restaurant_data_available;")
	except Exception as e:
		errormsg = "Latest restaurant data is still in the process of being loaded"
		print errormsg
		return render_template("bulkInsertGradesResults.html", outcome=errormsg)
	yes = cur.fetchall()
	if len(yes) != 1:
		return render_template("bulkInsertGradesResults.html", outcome="Data on restaurant grades is still in the process of being uploaded. Please check back later.")
	
	if yes[0] == 0:
		return render_template("bulkInsertGradesResults.html", outcome="Data on restaurant grades is still in the process of being uploaded. Please check back later.")

	cur.close()
	select_cur = conn.cursor()

	try:
		select_cur.execute(select_query)
	except Exception as e:
		print
		return render_template("bulkInsertGradesResults.html", outcome="Error in executing (" + select_query + "): " + str(e))


	mexican_eateries = "Top 10 Thai restaurants with most recent cleanest inspection scores<br>"
	for (dba, building, street, boro, phone, score, grade, grade_date) in select_cur:
		mexican_eateries += "* {}, {} {} in {} (tel: {}) (Grade: {} Score: {})<br>".format(dba, building, street, boro, phone, grade, score) 
	return render_template("bulkInsertGradesResults.html", outcome=Markup(mexican_eateries))

