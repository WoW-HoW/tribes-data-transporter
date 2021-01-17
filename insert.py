from data import returndata
import time
class Insert:

	def __init__(self,client,*args):
		#taking data from google bucket
		self.nodes, self.relationships = returndata(args)

		#assigning azure client
		self.client = client
	
		if self.nodes !=[]:
			self.insertnodes()
		if self.relationships !=[]:
			self.insertrelations()
		print("DONE")
	def insertnodes(self):
		for query in self.nodes:
			callback = self.client.submitAsync(query)
			if callback.result() is None:
				print("Something went wrong with this query: {0}".format(query))
			else:
				print(query+' node Inserted')

	def insertrelations(self):
		print('dne')
		for query in self.relationships:
			callback = self.client.submitAsync(query)

			if callback.result() is None:
				print("Something went wrong with this query:\n\t{0}".format(query))
			else:
				print(query+' relationship assigned')