import requests, csv, json
import mu_data
from wikibaseintegrator import wbi_core, wbi_login

from wikibaseintegrator.wbi_config import config as wbi_config

wbi_config['MEDIAWIKI_API_URL'] = 'http://localhost:8181/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'http://localhost:8989/bigdata/sparql'
wbi_config['WIKIBASE_URL'] = 'http://wikibase.svc'
wbi_config['PROPERTY_CONSTRAINT_PID'] = 'P39'
wbi_config['DISTINCT_VALUES_CONSTRAINT_QID'] = 'Q1651'


# ********************************************
# read in a CSV file as a list of dictionaries
def readDict(filename):
	fileObject = open(filename, 'r', newline='', encoding='utf-8')
	dictObject = csv.DictReader(fileObject)
	dictList = []
	for row in dictObject:
		dictList.append(row)
	fileObject.close()
	return dictList

def quantity(filename):
	my_file = readDict(filename)
	result = len(my_file)
	return result

# return the list of values from the i line of a csv file
def get_data_line (filename,i):
	my_file = readDict(filename) # une liste de  dictionnaires
	line = my_file[i]
	line_values = line.values()
	result = []
	for t in line_values:
		result.append(t)
	return result


def get_key_line (filename):
	my_file = readDict(filename) # une liste de  dictionnaires
	line = my_file[0]
	line_keys = line.keys()
	result = []
	for t in line_keys:
		result.append(t)
	return result


def get_prop_line (key_list,prop_type):
	result = []
	for t in key_list:
		z = prop_type.get(t)
		result.append(z)
	return result

class csv_importator (object):
	def __init__(self,filename,prop_type):
   		self.top_list = get_key_line (filename)
   		self.prop_list = get_prop_line(self.top_list,prop_type)	
   		self.len = len(self.top_list)
   		self.quantity = quantity(filename)
   		
   		self.item_list_list = []
   		for t in range (0,self.quantity):
   			z = get_data_line(filename,t)
   			self.item_list_list.append(z)




# ************** main script *****************


filename = 'pays_short.csv'
prop_type = mu_data.prop_type
login_bot = wbi_login.Login(user='WikibaseAdmin@bot_1', pwd='xxx')

pays = csv_importator(filename,prop_type)

references_z = [[wbi_core.ItemID(value='Q1646', prop_nr='P36', is_reference=True),
			wbi_core.ItemID(value='Q1647', prop_nr='P38', is_reference=True)]]

for i in pays.item_list_list:

	data_z = []
    
        
	for j in range (pays.len):
		if pays.top_list[j] in mu_data.no_ref:
			if pays.prop_list[j] == 'String':
				data_obj = wbi_core.String(i[j],pays.top_list[j], references=references_z)
				data_z.append(data_obj)
		
			elif pays.prop_list[j] == 'ItemID':
				data_obj = wbi_core.ItemID(i[j],pays.top_list[j])
				data_z.append(data_obj)

			elif pays.prop_list[j] == 'ExternalID':
				data_obj = wbi_core.ExternalID(i[j],pays.top_list[j])
				data_z.append(data_obj)

			elif pays.prop_list[j] == 'Url':
				data_obj = wbi_core.Url(i[j],pys.top_list[j])
				data_z.append(data_obj)
		
			elif pays.prop_list[j] == 'GeoShape':
				data_obj = wbi_core.GeoShape(i[j],pays.top_list[j])
				data_z.append(data_obj)

			else:
				if pays.prop_list[j] != 'Label' and pays.prop_list[j] != 'Description':
					print('Erreur, propriété inconnue : '+pays.prop_list[j])

		else:
			if pays.prop_list[j] == 'String':
				data_obj = wbi_core.String(i[j],pays.top_list[j], references=references_z)
				data_z.append(data_obj)
		
			elif pays.prop_list[j] == 'ItemID':
				data_obj = wbi_core.ItemID(i[j],pays.top_list[j],  references=references_z)
				data_z.append(data_obj)

			elif pays.prop_list[j] == 'ExternalID':
				data_obj = wbi_core.ExternalID(i[j],pays.top_list[j], references=references_z)
				data_z.append(data_obj)

			elif pays.prop_list[j] == 'Url':
				data_obj = wbi_core.Url(i[j],pays.top_list[j], references=references_z)
				data_z.append(data_obj)
		
			elif pays.prop_list[j] == 'GeoShape':
				data_obj = wbi_core.GeoShape(i[j],pays.top_list[j], references=references_z)
				data_z.append(data_obj)

			else:
				if pays.prop_list[j] != 'Label' and pays.prop_list[j] != 'Description':
					print('Erreur, propriété inconnue : '+pays.prop_list[j])

	new_item = wbi_core.ItemEngine(data=data_z)
	
	for j in range (pays.len):
		if pays.prop_list[j] == 'Label':
			lang = pays.top_list[j].replace("Label", "")
			new_item.set_label(i[j],lang,'REPLACE')

		elif pays.prop_list[j] == 'Description':
			lang = pays.top_list[j].replace("Description", "")
			new_item.set_description(i[j],lang,'REPLACE')			
    		
	new_item.write(login_bot)


# next step : deam with /usr/local/lib/python3.8/dist-packages/wikibaseintegrator-0.10.0.dev0-py3.8.egg/wikibaseintegrator/wbi_core.py:1394: UserWarning: Warning: No distinct value properties found 
#Please set P2302 and Q21502410 in your Wikibase or set `core_props` manually.
#Continuing with no core_props
