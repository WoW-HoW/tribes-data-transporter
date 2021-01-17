#imports
from google.cloud import storage
import json
import pytz

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

#By default, the datetime object is naive in Python, so you need to make both of them either naive or aware datetime objects
utc=pytz.UTC

#main code
def returndata(*args):
	#creating storage for nodes and relationships
	nodes	 		= []
	relationships 	= []
	#initializing google client and accessing database
	google_client 	= storage.Client()
	blobs 			= google_client.list_blobs(str(os.environ.get("BUCKET")))

	partial_sync = False
	if args[0] != ():
		partial_sync = True

	for blob in blobs:
		if((blob.content_type == 'application/json') and (partial_sync == False or (partial_sync == True and args[0][0].replace(tzinfo=utc)<blob.time_created.replace(tzinfo=utc)))):

			#downloading bucket data as text and converting it into a dictionary
			bucket_content = blob.download_as_text()
			jsondata = json.loads(bucket_content)

			for data in jsondata:
				
				#checking if the data is shows a node or a relationship
				if data['Kind'] == "node":
					
					#checking if Name property exists because some data did not have this property
					if 'Name' in data['Property']:
						name = data['Property']['Name'] 
					else:
						name = data['Property']['NameLower']

					#appending gremlin queries
					nodes.append(f"g.addV('Entity').property('id', '{data['Property']['IdMaster']}').property('Name', '{name}')")
				
				else:
					rel = f"g.V('{data['FromIdObject']}').addE('{data['Type']}').to(g.V('{data['ToIdObject']}'))"
					#checking DeDuplication 
					if data['DeDuplication'] == "FALSE":
						relationships.append(rel)
					else:
						#if true, checking if relationship already in database
						if rel not in relationships:
							relationships.append(rel)
	return nodes, relationships
		