__author__ = 'Lolhaven'

#

import sensative_info as si
# Station Service
url_service = "http://web.mta.info/status/serviceStatus.txt"

url_gtfs = " http://datamine.mta.info/mta_esi.php?key="+si.api_key+"&feed_id=1"

url_sir = "http://datamine.mta.info/mta_esi.php?key="+si.api_key+"&feed_id=11"

import pandas as pd
from mtaGTFS import mtaGTFS
import threading
t = threading.Timer(30.0,run_subway_gather)
t.start()
count =0

def run_subway_gather():
    t.start()
    subs = mtaGTFS(url_gtfs)

    with open('dumps/sept2_scheduled.csv', 'a') as f:
        wcsv = subs.scheduledStops
        wcsv.ix[:,'timeFeed'] = subs.timeFeed
        wcsv.to_csv(f, index = False, header = False)

    with open('dumps/sept2_enroute.csv', 'a') as f:
        wcsv = subs.enrouteTrains
        wcsv.ix[:,'timeFeed'] = subs.timeFeed
        wcsv.to_csv(f, index = False, header = False)
    count = count +1
    #print(count+"\n")
