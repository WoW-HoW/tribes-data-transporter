from gremlin_python.driver import client, serializer
import sys
import traceback
from cleanup import cleanup_graph
from insert import Insert
from datetime import datetime

class Initial:
	def __init__(self):
		self.azure_client = client.Client('wss://intership-assignments-2020.gremlin.cosmos.azure.com/', 'g',
                           username="/dbs/graph-data-aditya-chopra/colls/graph-data-aditya-chopra",
                           password="azure 9bpwqQIoaLegfAtUtTJIN8F4bsSPA1bcpqxBVBl0S7eOvRXAp5IdMIPWITj3qGhGVJsLzwk1s4hu3annNsWSag==",
                           message_serializer=serializer.GraphSONSerializersV2d0()
                           )
		
	def full_sync(self):
		cleanup_graph(self.azure_client)
		Insert(self.azure_client)
		self.time = datetime.now()
	def partial_sync(self):
		Insert(self.azure_client,self.time)
		self.time = datetime.now()
