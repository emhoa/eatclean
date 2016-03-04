import os
import tornado.httpserver
import tornado.ioloop
from tornado.web import FallbackHandler, RequestHandler, Application 
from tornado.wsgi import WSGIContainer
from app import app
import psycopg2
import urlparse
import requests
from csv import reader
import datetime
from threading import Thread
 
def get_timestamp():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def bulkInsert():

        #Set up some constants
       	RESTAURANT_SOURCE_FILE="https://nycopendata.socrata.com/api/views/xx67-kt59/rows.csv?accessType=DOWNLOAD"
        RESTAURANT_DEST_FILE="/tmp/" + "restaurantgrades" + datetime.datetime.now().strftime("%Y-%m-%d") + ".csv"
        DOWNLOAD_DATA_TABLE="input_data"
       
       	#Download the restaurant data file from NYC site
        print get_timestamp() + ": Starting download of source data file"
       	try:
                r = requests.get(RESTAURANT_SOURCE_FILE)
       	except Exception as e:
                print get_timestamp() + ": Unable to download file " + RESTAURANT_SOURCE_FILE

       	print get_timestamp() + ": Downloaded " + RESTAURANT_SOURCE_FILE
	        #Open local destination data file for writing
       	try:
                download_file = open(RESTAURANT_DEST_FILE, "wb")
       	except Exception as e:
               	print get_timestamp() + ": Unable to open " + RESTAURANT_DEST_FILE + " for writing."
               
        download_file.write(r.content)
       	download_file.close()
        #Connect to postgres database

       	urlparse.uses_netloc.append("postgres")
        url = urlparse.urlparse(os.environ["DATABASE_URL"])

       	try:
               	conn = psycopg2.connect(database=url.path[1:], user=url.username, password=url.password, host=url.hostname, port=url.port)
        except Exception as e:
       	        print "Unable to connect to the database"
		exit()
        
	create_lock_table_query = "create table if not exists restaurant_data_available (yes int); insert restaurant_data_available (yes) values (0);"
	lock_cur = conn.cursor()

	try:
		lock_cur.execute(create_lock_table_query)
	except Exception as e:
		print "Unable to update lock table"
		exit()
	conn.commit()

        #Open restaurant data file and peak at first line to find column names
       	print get_timestamp() + ": Opening file to find column names"
        restaurant_file = open(RESTAURANT_DEST_FILE, "rb")

       	columns = restaurant_file.readline().split(",")
        #Prepare three queries that will specify column names: drop, create and bulk copy
       	drop_table_query = """DROP table if exists """ + DOWNLOAD_DATA_TABLE + ";"""
        create_table_query = """CREATE table """ + DOWNLOAD_DATA_TABLE + """(id serial, """

       	insert_stmt = "insert into " + DOWNLOAD_DATA_TABLE + " ("

        #Add an _ to column names with spaces in them to make them sql-safe
        #concatenate columns to build sql statement to create table and bulk copy
       	i=0
        collen = len(columns)
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

        #Now perform queries to drop table, recreate, bulk copy, add primary key and run an analyze
       	insert_cur = conn.cursor()

        try:
       	       insert_cur.execute(drop_table_query)
        except Exception as e:
       	        print "Couldn't drop table"
                
        try:
       	        insert_cur.execute(create_table_query)
        except Exception as e:
       	        print "Couldn't create table"
               

        # complete insert_stmt so that it can read from the csv file
       	insert_stmt += ") values ("
        for count in range(0, collen-1):
       	        insert_stmt += "%s, "
        insert_stmt += "%s);"

       	print get_timestamp() + ": Now preparing to insert rows into " + DOWNLOAD_DATA_TABLE + "\n"
        
       	records = reader(restaurant_file, delimiter=',', quotechar='"')

        for line in records:
       	        record_tuple = tuple(line)

               	try:
                       	insert_cur.execute(insert_stmt, record_tuple)
                except Exception as e:
       	                errormsg = get_timestamp() + ": Failed to insert record " + ",".join(line) + ") with error: " + str(e) + "\n"
               	        print errormsg
	restaurant_file.close()

       	#Also prepare quriers to add primary key and run an analyze after bulk upload of data
        add_pkey = "ALTER TABLE " + DOWNLOAD_DATA_TABLE + " ADD PRIMARY KEY (id);"
#       	analyze_query = "ANALYZE " + DOWNLOAD_DATA_TABLE + ";"

        try:
       	        insert_cur.execute(add_pkey)
        except Exception as e:
       	        print get_timestamp() + ": Unable to add primary key"
                
#       try:
#      	        insert_cur.execute(analyze_query)
#        except Exception as e:
#       	        print get_timestamp() + ": Unable to analyze table"
                
        insert_cur.close()
       	print get_timestamp() + ": Finished with inserting into " + DOWNLOAD_DATA_TABLE + "\n"

        #Now create restaurant table
       	rest_cur = conn.cursor()
        create_rest_query = "drop table if exists restaurant; create table restaurant as (select distinct camis, dba, boro, building, street, zipcode, phone, cuisine_description from " + DOWNLOAD_DATA_TABLE + "); alter table restaurant add primary key (camis);"
       	try:
               	rest_cur.execute(create_rest_query)
        except Exception as e:
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
        except Exception as e:
       	        print get_timestamp() + ": Could not create restaurant_grades table"

        print get_timestamp() + ": Created restaurant grades table"

       	restgrades_cur.close()

	lift_lock_cur = conn.cursor()

	try:
		lift_lock_cur.execute("update restaurant_data_available set yes = 1;")
	except Exception as e:
		print get_timestamp() + ": Could not release lock on restaurant_data_available"

	lift_lock_cur.close()
        conn.commit()
       	conn.close()


class MainHandler(tornado.web.RequestHandler):
	def get(self):
		self.write("This message comes from Tornado ^_^")

	 
def main():

	tr = WSGIContainer(app)

	application = tornado.web.Application([
        	(r"/", MainHandler),
		(r".*", FallbackHandler, dict(fallback=tr)),
	    ])
	http_server = tornado.httpserver.HTTPServer(application)
	port = int(os.environ.get("PORT", 5000))
	http_server.listen(port)
	t = Thread(target=bulkInsert)
	t.daemon = True
	t.start()
	tornado.ioloop.IOLoop.instance().start()

 
if __name__ == "__main__":
    main()
