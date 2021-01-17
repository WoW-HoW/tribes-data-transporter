#imports
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

from gremlin_python.driver import client, serializer
import sys
import traceback

from datetime import datetime
import time

import os
from dotenv import load_dotenv, find_dotenv

from insert import Insert # function for inserting nodes and relationships 

#initializing .env, flask app and azure client
load_dotenv(find_dotenv())
app 			= Flask(__name__)
azure_client 	= client.Client(str(os.environ.get("ENDPOINT")), 'g',
                           username=str(os.environ.get("USERN")),
                           password=str(os.environ.get("PASSWORD")),
                           message_serializer=serializer.GraphSONSerializersV2d0()
                           )
time_now = datetime.now()


@app.route('/')
def home():
	return 'Please go to /fullsync for Full Sync else /partialsync for non-scheduled Partial Sync'

@app.route('/fullsync')
def fullsync():
	#cleaning up the graph
	callback = azure_client.submitAsync("g.V().drop()")
	if callback.result() is not None:
		print("Cleaned up the graph!")

	#Running full sync
	Insert(azure_client)

	#saving time of full sync to compare to bucket data  
	global time_now
	time_now = datetime.now()
	return "Full Sync Done and a Partial Sync will occur in 24 hours"

@app.route('/partialsync')
def partialsync():
	
	global time_now
	
	#Running Partial Sync
	Insert(azure_client,time_now)

	#updating time of sync to compare to bucket data
	time_now = datetime.now()
	return 'Partial Sync Done'

#Scheduler for running partial sync every 24 hours 
sched = BackgroundScheduler(daemon=True)
sched.add_job(partialsync,'interval',minutes=1440)
sched.start()

if __name__ == "__main__":
	app.run()