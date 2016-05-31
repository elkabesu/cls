import os
import calendar
from datetime import datetime
from collections import defaultdict
from BeautifulSoup import BeautifulSoup
import difflib

#lines = open('diff_analysis.txt', 'r').readlines()
lines = open('tempt.txt', 'r').readlines()

text = ''
for root, dirs, files in os.walk("/local/adv/files"):
        for f in files:
                print os.path.join(root, f)

totals = defaultdict(int)

for line in lines:
	linelist = line.rstrip().split()
	date1 = linelist[0]
	#date2 = linelist[2]
	date1day = calendar.day_name[datetime.strptime(date1, '%Y-%m-%d').weekday()]
	#date2day = calendar.day_name[datetime.strptime(date2, '%Y-%m-%d').weekday()]
	#print linelist[0], date1, date1day, date2, date2day, linelist[3]
	date1weeknum = (int(datetime.strptime(date1, '%Y-%m-%d').strftime('%d')) - 1) / 7 + 1
	totals[str(date1weeknum) + date1day] += 1
	#totals[date2day] += 1

print [(k, totals[k]) for k in sorted(totals, key = totals.get, reverse = True)][0:5]

text = open('alladvs.txt', 'r').readlines()
dd = defaultdict(int)

for line in lines:
	linelist = line.rstrip().split()
	crd = linelist[0]
	date1 = linelist[1]
	date2 = linelist[2]
	for t in text:
		if crd in t:
			if date1 in t:
				first = t.rstrip()
			if date2 in t:
				second = t.rstrip()
	
	#print first, second, linelist[3]

	firsthtml = open(first, 'r').read()
	secondhtml = open(second, 'r').read()
	
	firstsoup = BeautifulSoup(firsthtml)	
	secondsoup = BeautifulSoup(secondhtml)	

	firstlist = firstsoup.findAll('table', {'class': "flatBorderTable"})
	secondlist = secondsoup.findAll('table', {'class': "flatBorderTable"})


	if len(firstlist) != len(secondlist):
		continue
	

	for index, sl in enumerate(secondlist):
		try:
			header = sl.find('td', {'class':'GreyCenterTD'}).find('div').text
		except:
			try:
				header = sl.find('td', {'class':'GreyLeftTD'}).find('div').text
			except:
				continue
		secondlines = ''.join(sl.findAll(text=True))
		firstlines = ''.join(firstlist[index].findAll(text=True))
		
		diff = difflib.ndiff(firstlines.splitlines(), secondlines.splitlines())
		changes = [d for d in diff if d.startswith('+ ') or d.startswith('- ')]
		
		if len(changes) > 0:
			print crd, date1, date2, header#print header, len(changes)
	

#for key, value in dd.items():
	#print key, value	
#print [(k, d[k]) for k in sorted(d, key = d.get, reverse = True)]
