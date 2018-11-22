import sensative_info as si
import mtaGTFS
import sqlalchemy
import pandas as pd
import numpy as np
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
import datetime
import threading
import time



engine = mtaGTFS.connect_to_mysql(si, echo =False)
metadata = sqlalchemy.MetaData()
metadata.reflect(bind=engine)

# we can then produce a set of mappings from this MetaData.
Base = automap_base(metadata=metadata)

# calling prepare() just sets up mapped classes and relationships.
Base.prepare()

Stops, TrainID  = Base.classes.stops, Base.classes.trainID
Enroute_trains, Sched_stops = Base.classes.enroute_trains, Base.classes.sched_stops

Session =sessionmaker(bind = engine)
session = Session()


def createEnrouteObject(tup, tf):
        return Enroute_trains(full_id = tup[0], stop_id = tup[4],
        current_status = tup[1], last_ping = tup[3], timeFeed = tf,
        stop_sequence = tup[2])

def createTrainIdObject(tup):
    return TrainID(full_id = tup[0], route_plan = tup[5] , direction = tup[3], start_date = tup[4] , route_id = tup[2])

def createSchedStopObject(tup,tf):
    arr = tup[3]
    dep = tup[4]
    #attempts to catch any NaT's that don't mesh well with SQLalchemy
    if type(arr) != pd.tslib.Timestamp:
        arr = None
    if type(dep) != pd.tslib.Timestamp:
        dep = None

    return Sched_stops(full_stop_id = tup[0], full_id = tup[1],  stop_id = tup[2], arrival = arr, departure = dep, timeFeed = tf)

def updateTrainIds(mta_obj):
    unique_trains = mta_obj.trainIds.ix['scheduled']
    ids = unique_trains.index.values

    trains_in_db = session.query(TrainID).filter(TrainID.full_id.in_(ids))
    ids_in_db = [row.full_id for row in trains_in_db]

    new_trains_sel = unique_trains.index.isin(ids_in_db)
    new_trains_sel = np.logical_not(new_trains_sel)
    trainsobj_to_add = [createTrainIdObject(tup) for tup in
        unique_trains.ix[new_trains_sel].itertuples()]
    session.add_all(trainsobj_to_add)

def updateSchedStops(mta_obj):
    sched_ids = mta_obj.scheduledStops.index.values
    updated_ids  = []
    stops_in_db = session.query(Sched_stops).filter(Sched_stops.full_stop_id.in_(sched_ids))

    for row in stops_in_db:
        db_update = mta_obj.scheduledStops.ix[row.full_stop_id]
        row.arrival = db_update.arrival
        row.departure = db_update.departure
        updated_ids.append(row.full_stop_id)

    new_trains_sel = mta_obj.scheduledStops.index.isin(updated_ids)
    new_trains_sel = np.logical_not(new_trains_sel)

    stops_to_add = [createSchedStopObject(tup, mta_obj.timeFeed) for tup in mta_obj.scheduledStops.ix[new_trains_sel].itertuples()]
    session.add_all(stops_to_add)

def updateEnrouteTrains(mta_obj):
    temp = [createEnrouteObject(tup, mta_obj.timeFeed) for tup in mta_obj.enrouteTrains.itertuples()]
    session.add_all(temp)

    enroute_trains = mta_obj.enrouteTrains.reset_index()
    enroute_trains['full_stop_id'] = enroute_trains.full_id+enroute_trains.stop_id
    enroute_trains.set_index('full_stop_id', inplace =True)
    enroute_ids = enroute_trains.index.values
    stops_in_db = session.query(Sched_stops).\
        filter(Sched_stops.full_stop_id.in_(enroute_ids)).\
        filter(Sched_stops.enroute_conf== 0)
    for row in stops_in_db:
        #db_update = enroute_trains.ix[row.full_stop_id]
        row.enroute_conf = enroute_trains.ix[row.full_stop_id,'current_stop_sequence']


def push_to_db(mta_obj, to_log = True):
    #think about moving the following line to loop_update
    if(to_log):
        logger.info(mta_obj.subway_group +' - Start')
    mta_obj.updateFeed(single_id = True)

    if(to_log):
        logger.info('Feed Updated')
    updateTrainIds(mta_obj)

    if(to_log):
        logger.info('Train Ids Merged')
    updateEnrouteTrains(mta_obj)

    if(to_log):
        logger.info('Enroute Trains Update')
    updateSchedStops(mta_obj)

    if(to_log):
        logger.info('Scheduled Stops Merged')
    session.commit()

    if(to_log):
        logger.info('Data Pushed to Database')

import logging
logger = logging.getLogger('full_log')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('logs/'+datetime.date.today().isoformat() +'.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


irt = mtaGTFS.mtaGTFS(subway_group = 'irt', api_key= si.api_key, single_id=True)
sir = mtaGTFS.mtaGTFS(subway_group = 'sir', api_key= si.api_key, single_id=True)
l = mtaGTFS.mtaGTFS(subway_group = 'l', api_key= si.api_key, single_id=True)
nqrw = mtaGTFS.mtaGTFS(subway_group = 'nqrw', api_key= si.api_key, single_id=True)

next_call = time.time()
def loop_update():
    global next_call
    next_call += 30
    try:
        session.execute(metadata.tables['enroute_trains'].delete())

        push_to_db(irt)
        push_to_db(l)
        push_to_db(sir)
        # push_to_db(nqrw)

    except Exception, e:
        logger.error(str(e))
        # added to revert any database push errors that may occur
        session.rollback()

    threading.Timer( next_call - time.time(), loop_update).start()

loop_update()
