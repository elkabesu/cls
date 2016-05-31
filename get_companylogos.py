

#!/usr/bin/python

import os
import MySQLdb
from BeautifulSoup import BeautifulSoup

"""
in attempt to add a company logo to every company page on the CROWN website
we scraped the first page of the DEF 14A proxy statements to see if we could grab the logos of each company
while scraping the first page of these proxy statements, we made sure to grab the first image
once we were able to find an image in the statement, we would add the link of that image to a MySQL table.
"""

mysql = MySQLdb.connect("localhost", "root", pass, "website")
cursor = mysql.cursor()
query = "select cik from icons"
cursor.execute(query)
icons_ciks = cursor.fetchall()

icons_cik_set = set()
for ciks in icons_ciks:
	icons_cik_set.add(ciks)

mysql = MySQLdb.connect("localhost", "root", pass, "edgar")
cursor = mysql.cursor()
query = "select * from (select * from edgar.edgar where form_type = \"DEF 14A\" and year > 2002 and year < 2006  group by company_name order by date_filed DESC) as temp_table" 
cursor.execute(query)

def14A = cursor.fetchall()
img_list = ["IMG", "img", "IMAGE", "image"]

for d in def14A:
	cik = str(d[2])
	filename = d[6]
	accession = filename.split('/')[3]
	path = "/local/" + filename
	if os.path.isfile(path):
		try:
			with open(path) as f:
				html = f.read()
				soup = BeautifulSoup(html)
				for i in img_list:
					if soup.find(i):
						mysql = MySQLdb.connect("localhost", "root", pass, "website")
						cursor = mysql.cursor()
						url = "http://www.sec.gov/Archives/edgar/data/" + cik + "/" + accession.replace('-', '')[0:-4] +  "/" + soup.find(i)['src']
						query_1 = "insert into icons (cik, link) values (%s,%s)"
						cursor.execute(query_1, [cik, url])
						mysql.commit()
						break
		except:
			continue
