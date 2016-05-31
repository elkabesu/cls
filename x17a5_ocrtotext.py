

#!/usr/bin/pythin

import MySQLdb
import glob, os
import subprocess
import urllib2
from BeautifulSoup import BeautifulSoup
import re
import time

pdfregex = re.compile(ur'\.pdf')

mysql = MySQLdb.connect("localhost", "root", pass, "edgar")
cursor = mysql.cursor()
query = "select filename from edgar where form_type = \"X-17A-5\" order by rand() limit 10"
cursor.execute(query)
filenames = cursor.fetchall()

start_time = time.time()
for filename in filenames:
	ident = filename[0].split('/')[3].replace('.txt', '')

	landingpage = "https://www.sec.gov/Archives/" + filename[0].replace('.txt', '-index.htm')
	try:
                opened_url = urllib2.urlopen(landingpage)
        except Exception, e:
                #print str(e), landingpage
                continue

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
        soup = BeautifulSoup(text)
        #for a in soup.findAll('table', {"class" : "tableFile"}, {"summary" : "Document Format Files"}):
        for a in soup.findAll('table', {"summary" : "Document Format Files"}):	
		links = a.findAll('a')
		for l in links:
			if re.search(pdfregex, str(l)):
				pdflink = "https://www.sec.gov/" + l['href']
				pdfname = l.text.replace('.pdf', '')
		
				try:
					print '-'
					opened_url = urllib2.urlopen(pdflink)
					pdf_downloadpath = "/home/ds3437/code_files/x17a5_project/downloaded_pdfs/%s_%s.pdf" % (ident, pdfname)
					f = open(pdf_downloadpath, "wb")
					f.write(opened_url.read())
					f.close()	
				except Exception, e:
					#print str(e), landingpage
					continue
			
				directory = "/home/ds3437/code_files/x17a5_project/tifs/%s_%s" % (ident, pdfname)
				if not os.path.exists(directory):
    					os.makedirs(directory)
				task1 = "convert -density 300 " + pdf_downloadpath + " -type Grayscale -compress lzw -background white +matte -depth 32 " + directory + "/page_%05d.tif"
				subprocess.call(task1, shell=True)

				task2 = "for i in " + directory + "/page_*.tif; do echo $i; tesseract $i " + directory + "/$(basename $i .tif) pdf; done"
				subprocess.call(task2, shell=True)

				task3 = "pdftk " + directory + "/page_*.pdf cat output " + directory + "/merged.pdf"
				subprocess.call(task3, shell=True)
	
				task4 = "pdftotext " + directory + "/merged.pdf /home/ds3437/code_files/x17a5_project/pdf_astext/%s_%s.txt" % (ident, pdfname)
				subprocess.call(task4, shell=True)

print "--- %s seconds ---" % (time.time() - start_time)
