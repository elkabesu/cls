#!/usr/bin/python

import os
import urllib2
import httplib
from utils import db

"""
for the purposes of creating timeaware links between public issuing companies and institutional investors
to look at all of the 13F HR and 13F NT filings in the past 15 years, this script finishes populating the directory that holds the 13F text/xml files
derek sanz
"""

tf_list = []

years = [str(i) for i in range(2000,2016)]
quarters = [str(j) for j in range(1,5)]

for year in years:
        for quarter in quarters:
                tf_list.extend(db.search(year, quarter, "", "", "13F-HR", "*"))

for tf in tf_list:
	cik = tf[2]
	filename = "/local/" + tf[6]

	if not os.path.isfile(filename):
		accession = tf[6].split('/')[3]
		url = "http://www.sec.gov/Archives/edgar/data/" + cik + '/' + accession.replace(".txt", "/").replace("-", "") + accession
		
		opened_url = urllib2.urlopen(url)
		text = ""
		while True:
			try:
				next_chunk = opened_url.read(4096)
			except httplib.IncompleteRead, e:
				next_chunk = e.partial

			if not next_chunk:
				break
			text += next_chunk
		opened_url.close()

		directory = "/local/edgar/data/" + cik
		if not os.path.exists(directory):
			os.makedirs(directory)

		try:
			with open(filename, 'w+') as f:
				f.write(text)
				f.close()
		except:
			continue
