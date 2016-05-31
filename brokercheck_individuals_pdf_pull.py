#!/usr/bin/python

from pyvirtualdisplay import Display
from selenium import webdriver
# import selenium
from selenium.webdriver.common.keys import Keys
import os, sys
import time
from bs4 import *
import unicodedata
import re
from bs4 import NavigableString
import csv
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
import subprocess

if __name__ == '__main__':
    with open('dis_crd.csv', 'r') as f:
        reader = csv.reader(f)
        crds = list(reader)
        crds = [i[0] for i in crds]
        crds = [i.zfill(7) for i in crds]

    if len(sys.argv)>1:
        print(sys.argv)
        start = int(sys.argv[1])
    else:
        start = 0

    end = len(crds)

    for i_crd, crd in enumerate(crds[start:]):
        try:
	
	    print("Processing " + str(crds.index(crd)) + ', ' + str(crd) + " from " + str(start)+ " to " + str(end) )
	   
	    
	    p = '/local/brokercheck/PDF/%s' % (str(int(crd)))
	    
	    if os.listdir(p):
		continue
	
	    display = Display(visible=0, size=(800, 600))
	    display.start()

	    dc = DesiredCapabilities.CHROME
	    dc['loggingPrefs'] = {'browser': 'ALL'}
	    
	    d = '/local/brokercheck/PDF/%s' % (str(int(crd)))

	    chrome_profile = webdriver.ChromeOptions()
	    profile = {"download.default_directory": d,
		       "download.prompt_for_download": False,
		       "download.directory_upgrade": True,
		       "plugins.plugins_disabled": ["Chrome PDF Viewer"]}
	    chrome_profile.add_experimental_option("prefs", profile)

	    #Helpful command line switches
	    # http://peter.sh/experiments/chromium-command-line-switches/
	    chrome_profile.add_argument("--disable-extensions")
	    chrome_profile.binary_location = '/opt/google/chrome/google-chrome'
	    driver = webdriver.Chrome(executable_path='/usr/local/share/chromedriver',
					   chrome_options=chrome_profile,
					   service_args=['--log-path=chromedriver.log', '--verbose'],
					   desired_capabilities=dc)
	    
	    url = "http://brokercheck.finra.org/Individual/Summary/" + crd
	    driver.get(url)

	    elem = driver.find_elements_by_xpath("//div[div[input[@type = 'submit' and @id = "
						 "'ctl00_phContent_TermsAndCondUC_BtnAccept']]]//input")
	    if len(elem) > 1:
		driver.execute_script("$(arguments[0]).click();", elem[0] )
		time.sleep(1)
	    else:
		pass

	    try:
		elem = driver.find_element_by_xpath("//*[@id='contentTable']/div[2]/div[1]/div/div[5]/a")
		elem.click()
		time.sleep(10)
	    except Exception, e:
		pass


	    display.stop()
	    driver.quit()

	except:
	    continue
