import sensative_info as si
import mtaGTFS
import sqlalchemy
import pandas as pd
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
        return Enroute_trains(full_id = tup[3], stop_id = tup[5], 
        current_status = tup[1], last_ping = tup[4], timeFeed = tf,  
        stop_sequence = tup[2])

def createTrainIdObject(tup):
    return TrainID(full_id = tup[1], route_plan = tup[7] , direction = tup[5], 
    start_date = tup[6] , route_id = tup[4])
    
def createSchedStopObject(tup,tf):
        return Sched_stops(full_stop_id = (tup[1] +tup[2]), full_id = tup[1], 
        stop_id = tup[2], arrival = tup[3], departure = tup[4], timeFeed = tf)

next_call = time.time()        
def run_subway_gather():
    global next_call
    next_call += 30
    start = datetime.datetime.now().ctime()
    print start
    try:
        nice_irt = mtaGTFS.mtaGTFS(api_key = si.api_key)  
    except DecodeError:
        with open('logs/sept-15.csv', 'a') as f:
            f.write(start +",DecodeError\n")
        threading.Timer( next_call - time.time(), run_subway_gather ).start()
        return
    
    
    ok1 = session.query(TrainID).all()
    ok2 = session.query(Sched_stops).all()

    trainIds_unique = nice_irt.trainIds.drop_duplicates('full_id')
    trains_to_add = [createTrainIdObject(tup) for tup in trainIds_unique.itertuples()]
    trains_to_add = [session.merge(trainIds) for trainIds in trains_to_add]

    session.execute(metadata.tables['enroute_trains'].delete())
    temp = [createEnrouteObject(tup, nice_irt.timeFeed) for tup in nice_irt.enrouteTrains.itertuples()]        
    session.add_all(temp)

    sched_stops_new = [createSchedStopObject(tup, nice_irt.timeFeed) for tup in nice_irt.scheduledStops.itertuples()]
    sched_stops_new = [session.merge(sched_stop) for sched_stop in sched_stops_new]


    session.commit()
    stop = datetime.datetime.now().ctime()
    print stop
    with open('logs/sept-15.csv', 'a') as f:
            f.write(start +","+stop+"\n")
    threading.Timer( next_call - time.time(), run_subway_gather ).start()

    


run_subway_gather()
#nice_irt.trainIds  

        
# all_stops = session.query(Stops).order_by(Stops.stop_id)
# for instance in all_stops:
    # print instance.stop_id, instance.stop_name
    

#only written to database with session.commit()
#stops = sqlalchemy.Table('stops', metadata, autoload = True)
#stops.select().execute().fetchone()
