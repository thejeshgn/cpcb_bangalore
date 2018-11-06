# coding=utf-8
import sys
import os
import sqlite3 as lite
import csv
import dataset
import json
import string
import json
import datetime
root_raw_data_folder = '/home/thej/code/cpcb/data/raw'


def step1_import_metadata():
	db = dataset.connect('sqlite:////home/thej/code/cpcb/data/db/data.sqlite3')
	
	parameters = db['metadata']

	
	for path, subdirs, files in os.walk(root_raw_data_folder):
		for name in files:
			csv_file_path = os.path.join(path, name)
			print csv_file_path
			
			print csv_file_path[-4:]
			if csv_file_path[-4:] == ".csv":
				row_no = 0
				parameter_details = {}
				got_parameter_header_row = False
				got_parameter_footer_row = False
				#insert if it doesnt exist
				if parameters.find_one(file_path=csv_file_path) is None:
					with open(csv_file_path, "r") as csv_file:
						reader = csv.reader(csv_file)
						for row in reader:
							#print row
							row_no = row_no + 1
							

							if got_parameter_header_row:
								if row[0] != '':
									parameter_details['parameter'] = row[0]
									parameter_details['file_path'] = csv_file_path
									parameter_details['row_no'] = row_no
									parameter_details['full_csv_parsed'] = 0
									break

							else:
								row_zero_text = (row[0]).lower() 
								if row_zero_text.startswith('station'):
									station_text = row[0]
									parameter_details['station'] = ((station_text.split(":"))[1]).strip()

								if row_zero_text.startswith('avgperiod'):
									station_text = row[0]
									parameter_details['avg_period'] = ((station_text.split(":"))[1]).strip()

								if row_zero_text.startswith('datefrom'):
									station_text = row[0]
									parameter_details['date_from'] =( (station_text.split(":"))[1]).strip()

								if row_zero_text.startswith('dateto'):
									station_text = row[0]
									parameter_details['date_to'] = ((station_text.split(":"))[1]).strip()

								if row_zero_text.startswith('timefrom'):
									station_text = row[0]
									parameter_details['time_from'] = station_text.replace("TimeFrom: ","")

								if row_zero_text.startswith('timeto'):
									station_text = row[0]
									parameter_details['time_to'] = station_text.replace("TimeTo: ","")

								if row_zero_text == 'parameter':
									got_parameter_header_row = True


						print str(parameter_details)
						parameters.insert(parameter_details)
						print "============================================================="
				else:
					print "EXISTS"
			else:
				print "in else"
				if csv_file_path[-5:] == ".json":
					print "INSIDE JSON"
					if parameters.find_one(file_path=csv_file_path) is None:
						with open(csv_file_path, "r") as json_file:
							data = json_file.read()
							json_data = json.loads(data)
							parameter_details = {}
							parameter_details['parameter'] = "MULTI"
							parameter_details['file_path'] = csv_file_path
							parameter_details['full_csv_parsed'] = 0
							parameter_details['avg_period'] = "1 Hours"
							pm25_first = json_data["Particulate Matter<2.5"]["data"][1]
							pm25_last = json_data["Particulate Matter<2.5"]["data"][-1]

							pm25_first_date_time = pm25_first[0]
							pm25_last_date_time = pm25_last[0]

							parameter_details['date_from'] = pm25_first_date_time.split(" ")[0]
							parameter_details['time_from'] = pm25_first_date_time.split(" ")[1] 

							parameter_details['date_to'] = pm25_last_date_time.split(" ")[0]
							parameter_details['time_to'] = pm25_last_date_time.split(" ")[1]
							new_stations = ["HEBBAL", "JAYANAGAR", "KAVIKA","NIHMANS","SILKBOARD"]
							for station in new_stations:
								if station in csv_file_path.upper():
									parameter_details['station'] =  station
									break
							print parameter_details
							parameters.insert(parameter_details)
							print "============================================================="
					else:
						print "EXISTS"

def step2_import_values():
	db = dataset.connect('sqlite:////home/thej/code/cpcb/data/db/data.sqlite3')	
	metadata_table = db['metadata']
	parameters_table = db['parameters']
	data_table = db['data']	
	db.begin()
	for metadata in metadata_table:
		full_csv_parsed = metadata['full_csv_parsed']
		if full_csv_parsed == 1:
			continue

		file_path 	 = metadata['file_path']
		

		if file_path[-4:] == ".csv": 
			parameter_full_name 	= metadata['parameter']
			print str(parameter_full_name)
			parameters_data = parameters_table.find_one(full_name=parameter_full_name)
			#print str(parameters_data)

			parameter_short_name 	= parameters_data['short_name']
			station 	= metadata['station']			
			start_row_no		 = metadata['row_no']
			row_no = 0
			print str(file_path)

			with open(file_path, "r") as csv_file:
				reader = csv.reader(csv_file)			
				for row in reader:
					#print row
					row_no = row_no + 1
					if row_no < start_row_no:
						continue
					else:
						#print str(row)
						data = {}
						from_time = str(row[1]).strip()
						from_date = str(row[3]).strip()

						if from_time =="" or from_date == "":
							continue
						data["from_time"]=from_time
						data["date"]=from_date

						data["station"]= station.strip()
						data["year"]=str((str(row[3]).split("/"))[2])
						data[parameter_short_name]=str(row[4]).strip()

						key = data["station"]+"_"+data["date"]+"_"+data["from_time"]
						key = key.replace(':', '_')
						key = key.replace('/', '_')
						data['key'] = key.strip()
						print str(data)
						data_table.upsert(data, ['key'])
						print "---------------------------------------------------"
					#end of for loop
			#end of file - csv closed	 

		elif file_path[-5:] == ".json":
			with open(file_path, "r") as json_file:
				data = json_file.read()
				json_data = json.loads(data)
				parameters = json_data.keys()
				for parameter in parameters:
					parameters_data = parameters_table.find_one(alt_name=parameter)
					parameter_short_name = parameters_data['short_name']
					station = metadata['station']			
					first_item = True
					for data_item in json_data[parameter]["data"]:
						if first_item:
							first_item = False
							continue

						data = {}
						from_datetime = data_item[0]
						
						#JSON date format "2018-01-01 0:00"
						from_time = str(from_datetime.split(" ")[1]).strip()

						#convert from yyyy-dd-mm to mm/dd/yyyy
						from_date = str(from_datetime.split(" ")[0]).strip()
						from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d').strftime('%d/%m/%Y')

						if from_time =="" or from_date == "":
							continue
						data["from_time"]=from_time
						data["date"]=from_date

						data["station"]= station.strip()
						data["year"]=str((str(from_date).split("/"))[2])
						data[parameter_short_name]=str(data_item[1]).strip()

						key = data["station"]+"_"+data["date"]+"_"+data["from_time"]
						key = key.replace(':', '_')
						key = key.replace('/', '_')
						data['key'] = key.strip()
						print str(data)
						data_table.upsert(data, ['key'])

		else:
			print "UNSUPPORTED" 	
		#lets commit things
		metadata['full_csv_parsed']=1
		metadata_table.update(metadata,['id'])
		db.commit()
		print "*************************************************************"
		






def main():
	#step1_import_metadata()
	step2_import_values()

if __name__ == "__main__":
	main()
