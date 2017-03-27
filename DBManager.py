import mysql.connector
import uuid
import time
import datetime



def formating_of_ID(input):
	for row in input:
		formatted_policy = {}
		formatted_policy ['FQDN'] = row[0]
		formatted_policy ['MSISDN'] = row[1]
		formatted_policy ['IPV4'] = row[2]
		final={}
		final['ID'] = formatted_policy
	return final

def formating_of_table_sfqdn(input):
	for row in input:
		formatted_policy = {}
		formatted_policy ['fqdn'] = row[0]
		formatted_policy ['proxy_required'] = row[1]
		formatted_policy ['carriergrade'] = row[2]
		formatted_policy ['protocol'] = row[3]
		port_numbers = row[4].split(',')
		formatted_policy ['port'] = port_numbers
	return formatted_policy

def formating_of_table(input):
	for row in input:
		formatted_policy = {}
		trans_port = {}
		formatted_policy ['direction'] = row[0]
		formatted_policy ['src'] = row[1]
		formatted_policy ['dst'] = row[2]
		formatted_policy ['protocol'] = row[3]
		trans_port['sport']=row[4]
		trans_port['dport']=row[5]
		formatted_policy ['tcp'] = trans_port
		formatted_policy ['target'] = row[6]
		return formatted_policy

def formating_of_table_Customer_Discretion(input):
	for row in input:
		formatted_policy = {}
		trans_port = {}
		formatted_policy ['priority'] = row[0]
		formatted_policy ['direction'] = row[1]
		formatted_policy ['src'] = row[2]
		formatted_policy ['dst'] = row[3]
		formatted_policy ['protocol'] = row[4]
		trans_port['sport']=row[5]
		trans_port['dport']=row[6]
		formatted_policy ['tcp'] = trans_port
		formatted_policy ['target'] = row[7]
		formatted_policy ['schedule_end'] = str(row[9])
		return formatted_policy

def formating_of_ces(input):
	for row in input:
		formatted_policy = {}
		formatted_policy ['Policy_Required'] = row[0]
		formatted_policy ['Policy_Offered'] = row[1]
		formatted_policy ['Policy_Available'] = row[2]
		formatted_policy ['Policy_Required_Constraints'] = row[3]
		#formatted_policy ['Policy_Offered_Values'] = row[3]
		return formatted_policy


def reading_response(cursor, function):
	table_formatted_policy = []
	try:
		rows = cursor.fetchall()
		if len(rows) > 0:
			for row in rows:
				tablename = []
				tablename.append(row)
				table_formatted_policy.append(function(tablename))
		else:
			pass
	except:
		pass
	return table_formatted_policy


class DBManager:

	def __init__(self, host, user, password, dataBase):

		self.host = host
		self.password = password
		self.user = user
		self.dataBase = dataBase		

	def connect(self):

		self.cnx = mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.dataBase)
		return self.cnx
		#cnx.close()

	def deconnect(self):
		self.cnx.close()

	def retrieve_from_fqdn(self, fqdn):
		final_formatted_policy = []
		rows = []

		try:
			cursor = self.cnx.cursor()
			retrieve = "select FQDN, MSISDN, IPv4 from ID_Table where FQDN='%s'" %(fqdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_ID)
		except:
			pass
		final_formatted_policy.append(table_formatted_policy)


		try:
			cursor = self.cnx.cursor()
			retrieve = "select Subscription from ID_Table where FQDN='%s'" %(fqdn)
			cursor.execute(retrieve)
			table_formatted_policy = []
			try:
				values = {}
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						values['GROUP'] = row[0]
						table_formatted_policy = [values]
				else:
					pass
			except:
				values['GROUP'] = []
				table_formatted_policy = [values]
		except:
			pass
		final_formatted_policy.append(table_formatted_policy)	


		try:
			table_formatted_policy1 = {}
			cursor = self.cnx.cursor()
			retrieve = "select SFQDN, Proxy, Carrier_Grade, Trans_Protocol, Port from User_Services where FQDN='%s'" %(fqdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_table_sfqdn)
		except:
			table_formatted_policy = []
		table_formatted_policy1['SFQDN']=table_formatted_policy
		final_formatted_policy.append(table_formatted_policy1)
	

		table_formatted_policy2 = {}
		try:
			cursor = self.cnx.cursor()
			retrieve = "select Direction, S_IP, D_IP, Trans_Protocol, S_Port, D_Port, Action, Value from UCOP where Unique_ID = (select Unique_ID from ID_Table where FQDN = '%s') and Active=1" %(fqdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_table)
			table_formatted_policy2['FIREWALL_ADMIN'] = table_formatted_policy

		except:
			table_formatted_policy2['FIREWALL_ADMIN'] = []



		try:
			cursor = self.cnx.cursor()
			retrieve = "select Priority, Direction, S_IP, D_IP, Trans_Protocol, S_Port, D_Port, Action, Value, Schedule_End from Customer_Discretion where Unique_ID = (select Unique_ID from ID_Table where FQDN='%s') and now() between Schedule_Start and Schedule_End" %(fqdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_table_Customer_Discretion)
			table_formatted_policy2['FIREWALL_USER'] = table_formatted_policy

		except:
			table_formatted_policy2['FIREWALL_USER'] = []
		table_formatted_policy1 = {}
		table_formatted_policy1['FIREWALL']=table_formatted_policy2
		final_formatted_policy.append([table_formatted_policy1])


		try:
			cursor = self.cnx.cursor()
			retrieve_subscription = "select * from Emergency where Active=1"
			cursor.execute(retrieve_subscription)
			table_formatted_policy =  reading_response(cursor, formating_of_table)
		except:
			pass
		final_formatted_policy.append(table_formatted_policy)
	
		return final_formatted_policy




	def retrieve_from_msisdn(self, msisdn):
		final_formatted_policy = []
		rows = []

		try:
			cursor = self.cnx.cursor()
			retrieve = "select FQDN, MSISDN, IPv4 from ID_Table where MSISDN='%s'" %(msisdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_ID)
		except:
			pass
		final_formatted_policy.append(table_formatted_policy)


		try:
			cursor = self.cnx.cursor()
			retrieve = "select Subscription from ID_Table where MSISDN='%s'" %(msisdn)
			cursor.execute(retrieve)
			table_formatted_policy = []
			try:
				values = {}
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						values['GROUP'] = row[0]
						table_formatted_policy = [values]
				else:
					pass
			except:
				values['GROUP'] = []
				table_formatted_policy = [values]
		except:
			pass
		final_formatted_policy.append(table_formatted_policy)	


		try:
			table_formatted_policy1 = {}
			cursor = self.cnx.cursor()
			retrieve = "select SFQDN, Proxy, Carrier_Grade, Trans_Protocol, Port from User_Services where FQDN= (select FQDN from ID_Table where MSISDN = '%s')" %(msisdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_table_sfqdn)
		except:
			table_formatted_policy = []
		table_formatted_policy1['SFQDN']=table_formatted_policy
		final_formatted_policy.append(table_formatted_policy1)
	

		table_formatted_policy2 = {}
		try:
			cursor = self.cnx.cursor()
			retrieve = "select Direction, S_IP, D_IP, Trans_Protocol, S_Port, D_Port, Action, Value from UCOP where Unique_ID = (select Unique_ID from ID_Table where MSISDN = '%s') and Active=1" %(msisdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_table)
			table_formatted_policy2['FIREWALL_ADMIN'] = table_formatted_policy

		except:
			table_formatted_policy2['FIREWALL_ADMIN'] = []



		try:
			cursor = self.cnx.cursor()
			retrieve = "select Priority, Direction, S_IP, D_IP, Trans_Protocol, S_Port, D_Port, Action, Value, Schedule_End from Customer_Discretion where Unique_ID = (select Unique_ID from ID_Table where MSISDN='%s') and now() between Schedule_Start and Schedule_End" %(msisdn)
			cursor.execute(retrieve)
			table_formatted_policy =  reading_response(cursor, formating_of_table_Customer_Discretion)
			table_formatted_policy2['FIREWALL_USER'] = table_formatted_policy

		except:
			table_formatted_policy2['FIREWALL_USER'] = []
		table_formatted_policy1 = {}
		table_formatted_policy1['FIREWALL']=table_formatted_policy2
		final_formatted_policy.append([table_formatted_policy1])


		try:
			cursor = self.cnx.cursor()
			retrieve_subscription = "select * from Emergency where Active=1"
			cursor.execute(retrieve_subscription)
			table_formatted_policy =  reading_response(cursor, formating_of_table)
		except:
			pass
		final_formatted_policy.append(table_formatted_policy)
	
		return final_formatted_policy


	def retrieve_from_ces(self, transport_protocol, link_alias, direction, ces_fqdn):
		rows = []
		table_formatted_policy = []
		
		try:
			cursor = self.cnx.cursor()
			retrieve_policy_parameters = "select Reputation from CES_Reputation_Table where CES_FQDN = '%s'" %(ces_fqdn)
			cursor.execute(retrieve_policy_parameters)
			try:
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						tablename = []
						tablename.append(row)
					pass
				else:
					pass
			except:
				pass
		except:
			pass

		reputation = float(tablename[0][0])

		if reputation>=0 and reputation<0.25:
			reputation=0.25
		elif reputation>=0.25 and reputation<0.5:
			reputation=0.5
		elif reputation>=0.5 and reputation<0.75:
			reputation=0.75
		elif reputation>=0.75 and reputation<1:
			reputation=0.1

		try:
			retrieve_policy_parameters = "select Policy_Required, Policy_Offer, Policy_Available, Policy_Required_Constraints from CES_Negotiation_Policy where Reputation = '%s' and Trans_Protocol = '%s' and Link_Alias = '%s' and Direction = '%s'" %(reputation, transport_protocol, link_alias, direction)
			cursor.execute(retrieve_policy_parameters)
			table_formatted_policy = []
			try:
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						tablename = []
						tablename.append(row)
						table_formatted_policy.append(formating_of_ces(tablename))
					pass
				else:
					pass
			except:
				pass
		except:
			pass


		offer_parameters = tablename[0][1].split(',')
		values_ces={}
		for i in range (0, len(offer_parameters)):
			retrieve_policy_parameters = "select Value from CES_Policy_Params where Reputation = '%s' and Trans_Protocol = '%s' and Link_Alias = '%s' and Direction = '%s' and Parameters = '%s'" %(reputation, transport_protocol, link_alias, direction, offer_parameters[i])
			cursor.execute(retrieve_policy_parameters)
			try:
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						values_ces[offer_parameters[i]]=row[0]
					pass
				else:
					pass
			except:
				pass
		table_formatted_policy[0]['Policy_Offered'] = values_ces

		available_parameters = tablename[0][2].split(',')
		values_ces={}
		for i in range (0, len(available_parameters)):
			retrieve_policy_parameters = "select Value from CES_Policy_Params where Reputation = '%s' and Trans_Protocol = '%s' and Link_Alias = '%s' and Direction = '%s' and Parameters = '%s'" %(reputation, transport_protocol, link_alias, direction, available_parameters[i])
			cursor.execute(retrieve_policy_parameters)
			try:
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						values_ces[available_parameters[i]]=row[0]
					pass
				else:
					pass
			except:
				pass

		table_formatted_policy[0]['Policy_Available'] = values_ces
		
		return table_formatted_policy


	def retrieve_from_host(self, local_fqdn, remote_fqdn, direction):
		rows = []
		table_formatted_policy = []
		
		try:
			cursor = self.cnx.cursor()
			retrieve_policy_parameters = "select Reputation from Host_Reputation_Table where Host_FQDN = '%s'" %(remote_fqdn)
			cursor.execute(retrieve_policy_parameters)
			try:
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						tablename = []
						tablename.append(row)
					pass
				else:
					pass
			except:
				pass
		except:
			pass

		reputation = float(tablename[0][0])

		if reputation>=0 and reputation<0.25:
			reputation=0.25
		elif reputation>=0.25 and reputation<0.5:
			reputation=0.5
		elif reputation>=0.5 and reputation<0.75:
			reputation=0.75
		elif reputation>=0.75 and reputation<1:
			reputation=0.1


		try:
			retrieve_policy_parameters = "select Policy_Required, Policy_Offer, Policy_Available, Policy_Required_Constraints from HOST_Negotiation_Policy where Local_FQDN= '%s' and Remote_FQDN = '%s' and Reputation = '%s' and Direction = '%s'" %(local_fqdn, remote_fqdn, reputation, direction)
			cursor.execute(retrieve_policy_parameters)
			table_formatted_policy = []
			try:
				rows = cursor.fetchall()
				if len(rows) > 0:
					for row in rows:
						tablename = []
						tablename.append(row)
						table_formatted_policy.append(formating_of_ces(tablename))
					pass
				else:
					pass
			except:
				pass
		except:
			pass


		offer_parameters = tablename[0][1].split(',')
		values_host={}

		try:
			for i in range (0,len(offer_parameters)):
				offer_parameter_values = offer_parameters[i].split('.')

				if offer_parameter_values[0]== 'HOST_ID':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and ID_Type = '%s'" %(offer_parameter_values[0], local_fqdn, offer_parameter_values[1])
				elif offer_parameter_values[0]== 'Control_Policy_Params':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and Remote_FQDN = '%s' and Direction = '%s' and Parameters = '%s'" %(offer_parameter_values[0], local_fqdn, remote_fqdn, direction, offer_parameter_values[1])
				elif offer_parameter_values[0]== 'Payload_Policies':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and Remote_FQDN = '%s' and Direction = '%s' and Parameters = '%s'" %(offer_parameter_values[0], local_fqdn, remote_fqdn, direction, offer_parameter_values[1])
				elif offer_parameter_values[0]== 'RLOC_Policies':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and Remote_FQDN = '%s' and RLOC_Type = '%s'" %(offer_parameter_values[0], local_fqdn, remote_fqdn, offer_parameter_values[1])


				cursor.execute(retrieve_values)
				try:
					rows = cursor.fetchall()
					if len(rows) > 0:
						for row in rows:
							values_host[offer_parameters[i]]=row[0]
						pass
					else:
						pass
				except:
					pass
		except:
			pass
		table_formatted_policy[0]['Policy_Offered'] = values_host


		available_parameters = tablename[0][2].split(',')
		values_host={}
		try:
			for i in range (0,len(available_parameters)):
				available_parameter_values = available_parameters[i].split('.')
				if available_parameter_values[0]== 'HOST_ID':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and ID_Type = '%s'" %(available_parameter_values[0], local_fqdn, available_parameter_values[1])
				elif available_parameter_values[0]== 'Control_Policy_Params':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and Remote_FQDN = '%s' and Direction = '%s' and Parameters = '%s'" %(available_parameter_values[0], local_fqdn, remote_fqdn, direction, available_parameter_values[1])
				elif available_parameter_values[0]== 'Payload_Policies':
					retrieve_values = "select Valid from %s where Local_FQDN = '%s' and Remote_FQDN = '%s' and Payload_Type = '%s'" %(available_parameter_values[0], local_fqdn, remote_fqdn, available_parameter_values[1])
				elif available_parameter_values[0]== 'RLOC_Policies':
					retrieve_values = "select Value from %s where Local_FQDN = '%s' and Remote_FQDN = '%s' and RLOC_Type = '%s'" %(available_parameter_values[0], local_fqdn, remote_fqdn, available_parameter_values[1])
				cursor.execute(retrieve_values)
				try:
					rows = cursor.fetchall()
					if len(rows) > 0:
						for row in rows:
							values_host[available_parameters[i]]=row[0]
						pass
					else:
						pass
				except:
					pass
		except:
			pass



		table_formatted_policy[0]['Policy_Available'] = values_host
		return table_formatted_policy



