

import os
import re
from nltk.tokenize import sent_tokenize
import MySQLdb

firmprofileregex = re.compile(ur'Firm Profile[^<]*?Firm History')
thisfirmregex = re.compile(ur'This [Ff]irm.*?\.')
typesbusiness = re.compile(ur'Types\sof\sBusiness\n\n[^<]*?FINRA')
beforeotherregex = re.compile(ur'([^<]*(?=other -))', re.IGNORECASE)
dateregex = re.compile(ur'\d\d\/\d\d\/\d\d\d\d')
s = set()

mysql = MySQLdb.connect("localhost", "root", pass, "derek")
cursor = mysql.cursor()
insertquery = "insert into brokercheck_firm (crd, classification, formed, typebusiness) values (%s, \"%s\", \"%s\", \"%s\")"

for root, dirnames, filenames in os.walk('/home/ds3437/brokercheck_firm/texts'):
	for filename in filenames:
		myDict = {}
		path = os.path.join(root, filename)
		text = open(path, 'r').read()
		for fp in re.findall(firmprofileregex, text)[1:2]:
			classified = formed = ''
			secondcolumn = ''
			for f in fp.splitlines()[1:-1]:
				columns = re.split('\s{2,100}', f)
				if len(columns) > 2:
					secondcolumn += columns[2] + ' '
			for this in re.findall(thisfirmregex, secondcolumn):
				if 'clas' in this:
					classified = this
				if 'formed' in this:
					formed = this
		crd = path.split('/')[5]
		myDict['crd'] = crd
		myDict['classification'] = classified
		myDict['formed'] = formed
		date = ''
		try:
			date = re.findall(dateregex, formed)[0]
		except:
			pass
		myDict['d'] = date

		for tb in re.findall(typesbusiness, text):
			#print path.split('/')[5]
			tbsplit = tb.split('\n\n')
			businesses = tbsplit[1]
			try:
				businesses = re.findall(beforeotherregex, businesses)[0]
			except:
				pass
			for b in businesses.splitlines():
				line = re.sub('\(.*?\)', ' ', b.lower())
				line = re.sub('[^A-Za-z0-9 ]+', '', line)
				line = re.sub(' (or|of|a|in|or|for) ', ' ', line)
				#cursor.execute(insertquery % (crd, classified, formed, line))
				#mysql.commit()
				if len(line) <= 64:
					finalline = '_'.join(line.split())
				else:
					finalline = '_'.join(line[:64].split()[:-1])
				
				myDict[finalline] = 1
			
			s.add(finalline)
			placeholders = ', '.join(['%s'] * len(myDict))
			columns = ', '.join(myDict.keys())
			sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ("brokercheckfirm", columns, placeholders)
			cursor.execute(sql, myDict.values())
			mysql.commit()
			#print myDict
			#print '-'*10
print s
