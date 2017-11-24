# coding=utf-8
import sys
import os
import sqlite3 as lite
import csv
import requests
import time
from BeautifulSoup import BeautifulSoup
import dataset
import time
import datetime
import json
from datetime import datetime
#from bs4 import BeautifulSoup
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
								break

						else:
							row_zero_text = (row[0]).lower() 
							if row_zero_text.startswith('station'):
								station_text = row[0]
								parameter_details['station'] = (station_text.split(":"))[1]

							if row_zero_text.startswith('avgperiod'):
								station_text = row[0]
								parameter_details['avg_period'] = (station_text.split(":"))[1]

							if row_zero_text.startswith('datefrom'):
								station_text = row[0]
								parameter_details['date_from'] = (station_text.split(":"))[1]

							if row_zero_text.startswith('dateto'):
								station_text = row[0]
								parameter_details['date_to'] = (station_text.split(":"))[1]

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
				print "SKIPPING"

	#long_name
	#file

def main():
	step1_import_metadata()

if __name__ == "__main__":
	main()
