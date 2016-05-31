

#!/usr/bin/python

import re
import MySQLdb
from BeautifulSoup import BeautifulSoup
import subprocess
import urllib2
import os

"""
baseurl = "http://www.supremecourt.gov/oral_arguments/argument_transcript/%s"
path = "/home/ds3437/code_files/scotus_oral_arguments/pdf/%s.pdf"

for year in range(2000, 2015):
	html = urllib2.urlopen(baseurl % (str(year))).read()
	soup = BeautifulSoup(html)
	for href in soup.findAll('div', {'class': 'panel panel-scus'}):
		for a in href.findAll('a'):
			pdflink = "https://www.supremecourt.gov/oral_arguments/argument_transcripts/%s" % (a['href'].split('/')[2])
			pdf = urllib2.urlopen(pdflink).read()
			docket_num = a.text[:-1]
			print docket_num
			f = open(path % (docket_num), 'wb')
			f.write(pdf)
			f.close()

shelltext = "pdftotext -layout %s %s"
pdfpath = "/home/ds3437/code_files/scotus_oral_arguments/pdf/"
for root, dirnames, filenames in os.walk(pdfpath):
	for filename in filenames:
		pdffile = pdfpath + filename
		subprocess.call(shelltext % (pdffile, pdffile.replace("pdf", "txt")), shell = True)

"""

with open("/home/ds3437/scotus/cases_toerase.txt", 'r') as f:
	casestoerase = f.readlines()

casestoerase = ([c.rstrip(' \n') for c in casestoerase])

mysql = MySQLdb.connect("localhost", "root", pass, "derek")
cursor = mysql.cursor()
sgenquery = "select sgeneral, docket, dateArgument from justicedata_concise where instr(docket,\"%s\") and instr(docket, \"%s\") limit 1"

uniquelinenum = 0
pdftotextpath = "/home/ds3437/code_files/scotus_oral_arguments/txt/"
for root, dirnames, filenames in os.walk(pdftotextpath):
	for filename in filenames:
		docket = ''
		usedefaultpresentation = False

		txtpath = pdftotextpath + filename
		caseid = filename.replace('.txt', '').split('-')
		try:
			caseid1 = caseid[0]
			caseid2 = caseid[1]
		except:
			caseid1 = caseid[0][:3]
			caseid2 = caseid[0][3:]


		try:
			cursor.execute(sgenquery % (caseid1, caseid2))
			cu = cursor.fetchall()[0]
			sgeneral = cu[0]
			docket = cu[1]
		except:
			pass #sgen = "FALSE"

		if docket in casestoerase:
			continue

		text = ''
		with open(txtpath) as f:
			textlist = f.readlines()
	
		for tl in textlist:
			text = text + ' '.join(tl.split()[1:]) + '\n'
			#print f.readline()
	
		text = text.decode("utf-8").replace(u'\xa0', u' ')
		
		companyaddresslabel = re.compile(ur'1111.*?20005')
		addressregex1 = re.compile(ur'14th street.*?20005', re.IGNORECASE)
		addressregex2 = re.compile(ur'REPORTING COMPANY.*?20005')
		reportingcompanyregex = re.compile('reporting company', re.IGNORECASE)
		text = re.sub(companyaddresslabel, '', text)
		text = re.sub(addressregex1, '', text)
		text = re.sub(addressregex2, '', text)
		text = re.sub(reportingcompanyregex, '', text)
		
		presentationcounter = 0
		proregex = re.compile(ur'P R O C E E D I')
		timeregex = re.compile(ur'\(\d{1,2}:\d\d (a|p)\.m\.\)')
		tworegex = re.compile(ur'25\s{0,100}2')
		manualfix031693 = re.compile(ur'behalf of the petitioner\s*?47', re.IGNORECASE)
		speakerregex = re.compile(ur'[A-Z]+\.? [A-Z]+\'?[A-Z]+:')
		endregex = re.compile(ur'whereupon.*?at', re.IGNORECASE)
		presentationregex = re.compile(ur'(REBUTTAL|ORAL) ARGUMENT OF')
		removepresentationregex = re.compile(ur'(REBUTTAL|ORAL) ARGUMENT OF.*? BEHALF.*')
		asitmayregex = re.compile(ur'as it may please the court:', re.IGNORECASE)
		justiceregex = re.compile(ur'JU')
		contentregex = re.compile(ur'C O N T E N T')
		oralargumentregex = re.compile(ur'oral argument of page', re.IGNORECASE)
		
		prosplit = re.split(proregex, text)

		if len(prosplit) != 2:
			continue #print text + '\n'
		
		heading = prosplit[0]
		content = re.split(contentregex, heading)
		if len(content) != 2:
			content = re.split(oralargumentregex, heading)
				
		content = '\n'.join(content[1:])
		content = ' '.join(content.lower().split())
		
		petitionerlist = []
		respondentlist = []
		for c in re.finditer('(petitioner|appellant|plaintiff)', content):
			petitionerlist.append(str(c.start()) + 'p')

		for c in re.finditer('(respondent|appellee|defendant)', content):
			respondentlist.append(str(c.start()) + 'r')
	
		for c in re.finditer('amic.*?!support', content):
			respondentlist.append(str(c.start()) + 'a')
		
		plen = len(petitionerlist) 
		rlen = len(respondentlist) 
		if plen == 0:
			usedefaultpresentation = True
		else:
			petitionerlist.extend(respondentlist)
			newlist = sorted(petitionerlist, key=lambda x: int(x[:-1]))
			prlist = ['NA']
			for nl in newlist:
				if 'p' in nl:
					prlist.append("PETITIONER")
				elif 'r' in nl:
					prlist.append("RESPONDENT")
				else:
					prlist.append("NA")

		proceeding = prosplit[1]

		speakers = re.findall(speakerregex, proceeding)
		utterances = re.split(speakerregex, proceeding)[1:]


		query = "select partyWinning, majority, docket from justicedata_concise where instr(docket,\"%s\") and instr(docket, \"%s\") and instr(justiceName, \"%s\") limit 1"
		addpresonnext = False
		for index, speaker in enumerate(speakers):
			sgen = "FALSE"

			speakersplit = speaker.split()
			sp = ' '.join(speakersplit[len(speakersplit) - 2:]).replace(':', '').replace('-', '')
			fixed_utterance = ' '.join(utterances[index].split())

			if re.search(endregex, fixed_utterance) or fixed_utterance.count(':') > 25:
				break
			
			case_id = filename.replace('.txt', '')
			#print sp, [fixed_utterance]
			justice_vote = "NA"

			if re.search(justiceregex, sp):
				is_justice = "JUSTICE"
				try:
					cursor.execute(query % (caseid1, caseid2, sp.replace('\'', '').split()[1]))
					queryrow = cursor.fetchall()[0]
					if queryrow[0] == '1' and queryrow[1] == '2':
						justice_vote = "PETITIONER"
					elif queryrow[0] == '1' and queryrow[1] == '1':
						justice_vote = "RESPONDENT"
					elif queryrow[0] == '0' and queryrow[1] == '2':
						justice_vote = "RESPONDENT"
					elif queryrow[0] == '0' and queryrow[1] == '1':
						justice_vote = "PETITIONER"

					case_id = queryrow[2]
					
					
				except:
					continue
			else:
				is_justice = "NOT JUSTICE"
			
			try:
				if sgeneral in sp.lower() and is_justice == "NOT JUSTICE":
					sgen = "TRUE"
			except:	
				pass

			if re.search(asitmayregex, fixed_utterance) or presentationcounter == 0:
				after_previous = "FALSE"
			else:
				after_previous = "TRUE"
			
			if usedefaultpresentation:
				if presentationcounter == 0:
					presentation = "NA"
				elif presentationcounter % 2 == 1:
					presentation = "PETITIONER"
				elif presentationcounter % 2 == 0:
					presentation = "RESPONDENT"
			else:
				try:
					presentation = prlist[presentationcounter]
				except:
					if presentationcounter == 0:
						presentation = "NA"
					elif presentationcounter % 2 == 1:
						presentation = "PETITIONER"
					elif presentationcounter % 2 == 0:
						presentation = "RESPONDENT"

			
			if re.search(presentationregex, fixed_utterance):
				presentationcounter += 1
				fixed_utterance = re.sub(removepresentationregex, '', fixed_utterance)
			
			uniquelinenum += 1
			
			print ("%s +++$+++ %s +++$+++ %s +++$+++ %s +++$+++ %s +++$+++ %s +++$+++ %s +++$+++ %s +++$+++ %s" % (docket, str(uniquelinenum), after_previous, sp, is_justice, justice_vote, presentation, sgen, fixed_utterance)).encode('ascii', 'ignore')
