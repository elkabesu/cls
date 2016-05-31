

#!/usr/bin/python

from BeautifulSoup import BeautifulSoup
import MySQLdb
import urllib2
import os
import httplib
import re

"""
first step was to upload the requested M&A's into MySQL
the set of M&a's at hand this time around has the following structure:
deal_id,date_announced,date_effective,target_name,target_primary_ticker,target_cusip,target_cik,acquiror_name,acquiror_primary_ticker,acquiror_cusip,acquiror_cik
and are stored in derek.ricks_deals
using the target cik and the acquiror cik, look through all the filings from these companies between the date_announced and the date_effective
look at each filing and it's exhibits to find whether one of these components contain an agreement and plan of merger
"""

def getlinktext(filename):
	url = edgarpath + filename

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
	return text

edgarpath = "http://www.sec.gov/Archives/"
serverdatapath = "/local/edgar/data/"
localdirectory = "/local/"

regex1 = re.compile(ur'<description>[^<]*ex-2.1', re.IGNORECASE)
regex2 = re.compile(ur'<type>[^<]*ex-2.1$', re.IGNORECASE)
regex3 = re.compile(ur'<description>[^<]*agreement[^<]*plan[^<]*merger', re.IGNORECASE)
regex4 = re.compile(ur'<description>[^<]*agreement[^<]*?plan[^<]*?reorganization', re.IGNORECASE)
regex5 = re.compile(ur'<description>[^<]*plan[^<]*?agreement[^<]*?merger', re.IGNORECASE)
regex6 = re.compile(ur'<description>[^<]*plan[^<]*?agreement[^<]*?reorganization', re.IGNORECASE)
regex7 = re.compile(ur'<title>[^<]*ex-2.1$', re.IGNORECASE)
regex8 = re.compile(ur'<title>[^<]*agreement[^<]*?plan[^<]*?merger', re.IGNORECASE)
regex9 = re.compile(ur'<title>[^<]*agreement[^<]*?plan[^<]*?reorganization', re.IGNORECASE)
regex10 = re.compile(ur'<title>[^<]*plan[^<]*?agreement[^<]*?merger', re.IGNORECASE)
regex11 = re.compile(ur'<title>[^<]*plan[^<]*?agreement[^<]*?reorganization', re.IGNORECASE)

mysql = MySQLdb.connect("localhost", "root", pass, "derek")
cursor = mysql.cursor()
rickslinks_query = "select deal_id, date_filed, filename from ricks_deals, edgar.edgar where (target_cik = cik or acquiror_cik = cik) and date_filed >= date_announced and date_filed <= date_effective"
cursor.execute(rickslinks_query)
ricks_links = cursor.fetchall()

mergersinsert_query = "insert into mergers (deal_id, date_filed, edgar_url, filepath) values (\"%s\", \"%s\", \"%s\", \"%s\")"
checkmergers_query = "select * from mergers where deal_id = \"%s\" and date_filed = \"%s\""
updatericksdeals_query = "update ricks_deals set found = 1 where deal_id = \"%s\""

for ricks_link in ricks_links:
	try:
		if cursor.execute(checkmergers_query % (deal_id, str(date_filed))):
			continue
	except:
		continue
	try:
		text = getlinktext(filename)
	except:
		continue

	soup = BeautifulSoup(text)
	for num, tag in enumerate(soup.findAll('type')):
		tag = str(tag)
		if re.search(regex1, tag) or re.search(regex2, tag) or re.search(regex3, tag) or re.search(regex4, tag) or re.search(regex5, tag) or re.search(regex6, tag) or re.search(regex7, tag) or re.search(regex8, tag) or re.search(regex9, tag) or re.search(regex10, tag) or re.search(regex11, tag):
			filepath = '/local/mergers/' + deal_id + '_' + filename.split('/')[3].replace('.txt', '_') + str(num) + '.txt'
			f = open(filepath, 'w')
			f.write(tag)
			f.close()
			
			try:
				cursor.execute(updatericksdeals_query % (deal_id))
				mysql.commit()
				cursor.execute(mergersinsert_query % (deal_id, str(date_filed), "http://www.sec.gov/Archives/" + filename.replace('.txt', '-index.htm'), filepath))
				mysql.commit()
			except:
				mysql.rollback()
