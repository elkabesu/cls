

#!/usr/bin/python

import re
import os
from BeautifulSoup import BeautifulSoup
import time
import MySQLdb

regex_financing = re.compile(ur'FINANCING.{0,40}SECURITY\s+AGREEMENT')
regex_creditsecurity = re.compile(ur'CREDIT.{0,40}SECURITY\s+AGREEMENT')
regex_loansecurity = re.compile(ur'LOAN.{0,40}SECURITY\s+AGREEMENT')
regex_creditguarantee = re.compile(ur'CREDIT.{0,40}GUARANT(EE|Y)\s+AGREEMENT')
regex_loanguarantee = re.compile(ur'LOAN.{0,40}GUARANT(EE|Y)\s+AGREEMENT')
regex_credit = re.compile(ur'CREDIT\s+AGREEMENT')
regex_loan = re.compile(ur'LOAN\s+AGREEMENT')
regex_creditfacility = re.compile(ur'CREDIT\s+FACILITY')
regex_security = re.compile(ur'SECURITY\s+AGREEMENT')
regex_revolvingcredit = re.compile(ur'REVOLVING\s+CREDIT')
regex_agr_revolver1 = re.compile(ur'REVOLV.{0,100}AGREEMENT')
regex_agr_term1 = re.compile(ur'TERM.{0,100}AGREEMENT')
regex_agr_revolver2 = re.compile(ur'AGREEMENT.{0,100}REVOLV')
regex_agr_term2 = re.compile(ur'AGREEMENT.{0,100}TERM')
regex_collateral = re.compile(ur'COLLATERAL\s+AGREEMENT')
regex_pledge = re.compile(ur'PLEDGE\s+AGREEMENT')
regex_ex10_1 = re.compile(ur'ex-?10', re.IGNORECASE)
regex_ex10_2 = re.compile(ur'dex10', re.IGNORECASE)
regex_ex10_3 = re.compile(ur'exhibit\s*10', re.IGNORECASE)
regex_toc1 = re.compile(ur'table\s+of\s+contents', re.IGNORECASE)
regex_toc2 = re.compile(ur't a b l e\s+o f\s+c o n t e n t s', re.IGNORECASE)
regex_amended1 = re.compile(ur'amendment', re.IGNORECASE)
regex_amended2 = re.compile(ur'AMENDED')


securitydirectory = "/local/securityagreements/"
creditdirectory = "/local/creditagreements/"

mysql = MySQLdb.connect("localhost", "root", pass, "edgar")
cursor = mysql.cursor()
query = """	select cik, form_type, date_filed, concat(\"/local/all_data/\", year(date_filed), \"/\", right(filename, 24))  as f, 
		filename  from edgar.edgar where (form_type = \"8-k\" or form_type = \"8-k/a\" and form_type = \"s-1\" or 
		form_type = \"s-1/a\" or form_type = \"s-2\" or form_type = \"s-2/a\" or form_type = \"s-4\" or form_type = \"s-4/a\" or 
		form_type = \"10-k\"  or form_type = \"10-k/a\" or form_type = \"10-q\" or form_type = \"10-q/a\") order by form_type, cik desc, date_filed """
cursor.execute(query)
filings = cursor.fetchall()

creditagreementsquery = "insert into derek.creditagreements (cik, form_type, date_filed, path, filename) values (%s, \"%s\", \"%s\", \"%s\", \"%s\")"
securityagreementsquery = "insert into derek.securityagreements (cik, form_type, date_filed, path, filename) values (%s, \"%s\", \"%s\", \"%s\", \"%s\")"

def print_prettytext(tag, directory, path):
	prettytext = ''.join(BeautifulSoup(tag.prettify()).findAll(text=True))
	f = open(directory + path, 'w')
	f.write(prettytext)
	f.close()
	return

def handling_credit(regex, text, title, tag, path, filing):
	if re.search(regex, text[:1000]):
		if re.search(regex_toc1, text) or re.search(regex_toc2, text):
			try:
				print_prettytext(tag, creditdirectory, path)

				cursor.execute(creditagreementsquery % (filing[0], filing[1], filing[2], creditdirectory + p, filing[4]))
				mysql.commit()
				return True
			except Exception, e:
				return False
	else:
		return False

def handling_security(regex, text, title, tag, path, filing):
	if re.search(regex, text[:1000]):
		if re.search(regex_amended1, exhibit1000) or re.search(regex_amended2, exhibit1000):
			return False
		else:
			try:
				print_prettytext(tag, securitydirectory, path)
			
				cursor.execute(securityagreementsquery % (filing[0], filing[1], filing[2], securitydirectory + p, filing[4]))
				mysql.commit()
				return True
			except:
				return False


count = 0
start = time.time()
for filing in filings:
	
	count += 1
	if count % 100 == 0:
		print count, time.time() - start

	path = filing[3]
	try:
		with open(path) as opened_text:
			content = opened_text.read()
		soup = BeautifulSoup(content)
	except:
		continue
	
	for num, tag in enumerate(soup.findAll('type')):
		exhibit = ''.join(tag.findAll(text=True))
		exhibit4000 = ' '.join(exhibit.replace('-', '').replace('.','').replace('=','')[:4000].split())
		exhibit1000 = exhibit4000[:1000]
		exhibit1000b = ' '.join(exhibit[:1000].split())
		
		if re.search(regex_ex10_1, exhibit1000b) or re.search(regex_ex10_2, exhibit1000b) or re.search(regex_ex10_3, exhibit1000b):
			
			p = "%s_%s_%s_%s.txt" % (str(filing[0]), filing[1], str(filing[2]), str(num + 1))
			p = p.replace('-', '').replace('/', '')
			worked = False

			while True:
				worked = handling_credit(regex_financing, exhibit4000, "Financing and Security Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_creditsecurity, exhibit4000, "Credit and Security Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_loansecurity, exhibit4000, "Loan and Security Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_creditguarantee, exhibit4000, "Credit and Guarantee Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_loanguarantee, exhibit4000, "Loan and Guarantee Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_credit, exhibit4000, "Credit Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_loan, exhibit4000, "Loan Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_creditfacility, exhibit4000, "Credit Facility", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_revolvingcredit, exhibit4000, "Revolving Credit", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_agr_term1, exhibit4000, "Term Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_agr_term2, exhibit4000, "Term Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_agr_revolver1, exhibit4000, "Revolving/Revolver Agreement", tag, p, filing)
				if worked: break
				worked = handling_credit(regex_agr_revolver2, exhibit4000, "Revolving/Revolver Agreement", tag, p, filing)
				if worked: break
			
				worked = handling_security(regex_security, exhibit4000, "Security Agreement", tag, p, filing)
				if worked: break
				worked = handling_security(regex_collateral, exhibit4000, "Collateral Agreement", tag, p, filing)
				if worked: break
				worked = handling_security(regex_pledge, exhibit4000, "Pledge Agreement", tag, p, filing)
				if worked: break

