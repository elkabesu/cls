#!/usr/bin/python

from utils import db
from BeautifulSoup import BeautifulSoup
import csv
import os

"""
we look through our downloaded DEF 14A proxy statements and we print all its HTML tables into a text file
"""

def output_to_csv(title_list, row_list, csv_name):
    csv_save_path = os.path.join(os.getcwd(), csv_name)

    if not os.path.exists(csv_save_path): 
        writer = csv.writer(open(csv_save_path, 'wb', buffering=0))
        writer.writerow(title_list)
        writer.writerow(row_list)
    else:
        writer = csv.writer(open(csv_save_path, 'a', buffering=0)) # add more rows
        writer.writerow(row_list)


def14a_list = []

years = [str(i) for i in range(2002,2007)]
quarters = [str(j) for j in range(1,5)]

for year in years:
	for quarter in quarters:
		def14a_list.extend(db.search(year, quarter, "", "", "DEF 14A", "*"))

non_accessable = []
for def14a in def14a_list:
	temp_filename = def14a[6]
	cik = str(def14a[2])
	accession = temp_filename.split('/')[3]
	filename = "/local/" + temp_filename
	directory = "/local/def14a_extract/tables_dates/" + cik + "/"
	path = directory + accession.replace(".txt", "_") + str(def14a[5]).replace("-","") + ".txt"
	
	if os.path.exists(path):
		continue
	
	try:
		filename = "/local/" + temp_filename
		with open(filename) as f:
			html = f.read()
			soup = BeautifulSoup(html)
			if not soup.find("html"):
				continue
			else:
				if not soup.find("table"):
					continue
				if not os.path.exists(directory):
					os.makedirs(directory)

				f = open(path, 'w+')
				for table in soup.findAll("table"):
					f.write(str(table))
				f.close()
	except:
		title_list = ['year', 'quarter', 'cik', 'company name', 'form type', 'date', 'filename']
		row_list = [def14a[0], def14a[1], def14a[2], def14a[3], def14a[4], def14a[5], def14a[6]]
		output_to_csv(title_list, row_list, 'non_accessable_files.csv')
		continue

