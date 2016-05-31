

#!/usr/bin/python

import MySQLdb
from collections import defaultdict
from build_13f_groups_firstpass import *

"""
derive a name for the adv and 13f links for crown website purposes
Naming Algo.
1st pass: Choose at random among the 13F HR filers that filed for a 13F NT filer in a link
2nd pass: Choose at random among the ADV filers in a link
3rd pass: Choose at random among any 13F HR filers
"""

mysql = MySQLdb.connect("localhost", "root", pass, "form_13F")
cursor = mysql.cursor()
query = ("select group_id, cik, company_name from 13f_timeaware2 where year = 2014")
cursor.execute(query)
thirteenf_groups = cursor.fetchall()

query = ("select * from links where year = 2014")
cursor.execute(query)
group_links = cursor.fetchall()

mysql = MySQLdb.connect("localhost", "root", pass, "adv_new")
cursor = mysql.cursor()
query = ("select * from adv_new.adv_groups_2014_final_678")
cursor.execute(query)
adv_groups = cursor.fetchall()

query = ("select 1E from dereks_full_baseform where year(submitted_date) = 2014")
cursor.execute(query)
adv_crds = cursor.fetchall()

mysql = MySQLdb.connect("localhost", "root", pass, "edgar")
cursor = mysql.cursor()
query = ("select cik, filename from edgar where year = 2014 and form_type = \"13F-NT\" ")
cursor.execute(query)
nt_ciks = cursor.fetchall()

mysql = MySQLdb.connect("localhost", "root", pass, "edgar")
cursor = mysql.cursor()
query = ("select cik from edgar where year = 2014 and form_type = \"13F-HR\" ")
cursor.execute(query)
hr_ciks = cursor.fetchall()

crd_name_dict = {}
cik_name_dict = {}
all_ciks_set = set()
group_ciks_dict = defaultdict(list)
crd_filer_set = set()
group_hr_dict = defaultdict(list)
nt_cik_dict = defaultdict(list)
hr_set = set()
group_crd_dict = defaultdict(list)
nt_parents_set = set()

for cik in hr_ciks:
	hr_set.add(int(cik[0]))

for tg in thirteenf_groups:
	group_ciks_dict[tg[0]].append(tg[1])
	all_ciks_set.add(tg[1])
	cik_name_dict[tg[1]] = tg[2]


for crd in adv_crds:
	crd_filer_set.add(int(crd[0]))


for ag in adv_groups:
	if ag[1] in crd_filer_set:
		group_crd_dict[ag[0]].append(ag[1])
		crd_name_dict[ag[1]] = ag[2]

for cik in nt_ciks:
	nt_parent =  get_thirteenf_nt_parents_from_extract(cik[1])
	if nt_parent:
		if nt_parent[0][0] and nt_parent[0][0].strip():
			nt_parent = int(nt_parent[0][0])
			if nt_parent in all_ciks_set:
				nt_parents_set.add(nt_parent)

link_names_dict_1 = {}
link_names_dict_2 = {}
link_names_dict_3 = {}

for gl in group_links:
	ciks = group_ciks_dict[gl[1]]
	if ciks:
		for cik in ciks:
			if cik in nt_parents_set:
				link_names_dict_1[gl[2]] = cik_name_dict[cik].replace("\"", "").replace("\'", "")
			if cik in hr_set:
				link_names_dict_2[gl[2]] = cik_name_dict[cik].replace("\"", "").replace("\'", "")

	crds = group_crd_dict[gl[0]]
	if crds:
		for crd in crds:
			if crd in crd_filer_set:
				link_names_dict_3[gl[2]] = crd_name_dict[crd].replace("\"", "").replace("\'", "")

print link_names_dict_1
"""
mysql = MySQLdb.connect("localhost", "root", pass, "adv_13f_match")
cursor = mysql.cursor()
for gl in group_links:
	if gl[2] in link_names_dict_1:
		query = "update group_links set link_name = \"%s\" where link_id = %s" % (link_names_dict_1[gl[2]], gl[2])
		cursor.execute(query)
		mysql.commit()
	elif gl[2] in link_names_dict_3:
		query = "update group_links set link_name = \"%s\" where link_id = %s" % (link_names_dict_3[gl[2]], gl[2])
		cursor.execute(query)
		mysql.commit()
	elif gl[2] in link_names_dict_2:
		query = "update group_links set link_name = \"%s\" where link_id = %s" % (link_names_dict_2[gl[2]], gl[2])
		cursor.execute(query)
		mysql.commit()
"""
