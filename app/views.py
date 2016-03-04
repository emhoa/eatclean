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
        url = urlparse.urlparse(os.environ["DATABASE_U
RL"])

       	try:
               	conn = psycopg2.connect(database=url.p
ath[1:], user=url.username, password=url.password, hos
t=url.hostname, port=url.port)
        except Exception as e:
       	        print "Unable to connect to the databa
se"
	cur = conn.cursor()
	cur.execute(select_query)

	mexican_eateries = ""
	for (dba, building, street, boro, phone, score, grade, grade_date) in cursor:
		mexican_eateris += "{}, {} {} in {} (tel: {}) (Grade: {} on {%b/%d/%Y} Score: {})\n".format(dba, building, street, boro, phone, grade, grade_date, score) 
	return render_template("bulkInsertGradesResults.html", outcome=Markup(mexican_eateries))

