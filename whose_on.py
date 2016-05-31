from pygeocoder import Geocoder
import socket
import json
import urllib2
import subprocess

commandstring = "who -a | grep + | awk \'{print $1\" \"$4\" \"$5\" \" $6\" \"$8}\' | grep -v \"old\" > whose_on_temp.txt"
subprocess.call(commandstring, shell = True)

commandresults = open('whose_on_temp.txt', 'r').read().splitlines()
commandresults = commandresults[:len(commandresults) - 1]

for commandresult in commandresults:
	crsplit = commandresult.split()
	user = crsplit[0]
	date = crsplit[1]
	starttime = crsplit[2]
	endtime = crsplit[3]
	hostname = crsplit[4].replace(')', '').replace('(', '')
	ip = socket.gethostbyaddr(hostname)[2][0]
	data = json.load(urllib2.urlopen("http://geoip.nekudo.com/api/%s" % (ip)))
	latitude = data['location']['latitude']
	longitude = data['location']['longitude']
	address = Geocoder.reverse_geocode(float(latitude), float(longitude))

	print "User: ", user 
	print "Date: ", date 
	print "Start Time: ", starttime
	print "Duration: ", endtime
	print "Address: ", address
	print '-'*40
	
