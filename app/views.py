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

	return render_template("bulkInsertGrades.html", outcome="")

