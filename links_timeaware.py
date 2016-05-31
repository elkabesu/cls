

#!/usr/bin/python

import MySQLdb
from build_links_for_adv_13f_groups_2 import *
from functions.general import *
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from multiprocessing import *

"""
for the purpose of creating time aware links between public issuing companies and institutional investors
compare the names of the public issuing companies to the names of the adv filing instituional investors 
using a cosine similarity algorithm combined with a global term frequency algorithm to ignore 'stopwords'
derek sanz
"""

def linking((tfidx, tf)):
	temp = []
	for advidx, adv in adv_names.items():
		d = get_distance_from_sklearn(tf.split(), adv.split(), idf_words)
		if d > .59:
			print d, "|||", tf, "|||", adv
			temp.append([tfidx, advidx])
	return temp

mysql = MySQLdb.connect("localhost", "root", pass, "adv_new")
cursor = mysql.cursor()
query = "select group_id, crd, name2 from adv_groups_components_revised where year = 2008"
cursor.execute(query)
adv_groups = cursor.fetchall()

mysql = MySQLdb.connect("localhost", "root", pass, "form_13F")
cursor = mysql.cursor()
query = "select year, group_id, cik, company_name from 13f_timeaware where year = 2008"
cursor.execute(query)
tf_groups = cursor.fetchall()

filers_set = set()
adv_names = defaultdict(str)
tf_names = defaultdict(str)
names_list = []


for tfg in tf_groups:
	if tfg[3]:
		tf_name = preprocess(tfg[3])
		names_list.append(tf_name)
		tf_names[tfg[1]] += tf_name + " "

for advg in adv_groups:
	adv_name = preprocess(advg[2])
	names_list.append(adv_name)
	adv_names[advg[0]] += adv_name + " "

vectorizer = TfidfVectorizer(min_df=1)
X = vectorizer.fit_transform(names_list)
idf = vectorizer._tfidf.idf_
idf_words = dict(zip(vectorizer.get_feature_names(), idf))
print sorted(idf_words.items(), key = lambda x: x[1])[:150]

p = Pool(3)
links = p.map(linking, tf_names.items())

tgset = set()
advset = set()

group_links = []
for link in links:
        if len(link) > 0:
                for l in link:
			group_links.append(l)


query = "insert into links6 (year, 13f, adv, link_id) values (%s, %s, %s, %s)"
for idx, gl in enumerate(group_links):
        tgset.add(gl[0])
        advset.add(gl[1])
        count = idx + 1
        try:
		cursor.execute(query % (2008, gl[0], gl[1], idx + 1))
        	mysql.commit()
	except:
		mysql = MySQLdb.connect("localhost", "root", pass, "form_13F")
		cursor = mysql.cursor()
		cursor.execute(query % (2008, gl[0], gl[1], idx + 1))
        	mysql.commit()

for t in tf_groups:
        if t[1] not in tgset:
                count = count + 1
                try:
			cursor.execute(query % (2008, t[1], 0, count))
                	mysql.commit()
                except:
			mysql = MySQLdb.connect("localhost", "root", pass, "form_13F")
			cursor = mysql.cursor()
			cursor.execute(query % (2008, t[1], 0, count))
                	mysql.commit()
		tgset.add(t[1])

for a in adv_groups:
        if a[0] not in advset:
                count = count + 1
                try:
			cursor.execute(query % (2008, 0, a[0], count))
                	mysql.commit()
		except:
			mysql = MySQLdb.connect("localhost", "root", pass, "form_13F")
			cursor = mysql.cursor()
			cursor.execute(query % (2008, 0, a[0], count))
                	mysql.commit()
                advset.add(a[0])
