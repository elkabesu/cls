

#!/usr/bin/python

import MySQLdb
import requests
from BeautifulSoup import BeautifulSoup

mysql = MySQLdb.connect("localhost", "root", pass, "derek")
cursor = mysql.cursor()
query = "insert into neighborhoods (hood, latitude, longitude, zindex) values (\"%s\", %s, %s, %s)"

r = requests.get("http://www.zillow.com/webservice/GetRegionChildren.htm", params = (("zws-id", "X1-ZWz1a4hg2hm6tn_1r7kw"), ("state", "ny"), ("city", "nyc"), ("childtype", "neighborhood")))
r = str(r.text).split("<!--")[0]

soup = BeautifulSoup(r)
for region in soup.findAll('region'):
	if region:
		name = region.find('name')
		if name:
			zindex = region.zindex
			if not zindex:
				zindex = 0
			else:
				zindex = zindex.text
			cursor.execute(query % (name.text, region.latitude.text, region.longitude.text, zindex))
			mysql.commit()
