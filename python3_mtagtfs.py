from google.transit import gtfs_realtime_pb2
import requests
import time # imports module for Epoch/GMT time conversion
import os
# import sensative_info as si
from datetime import datetime
import pandas as pd
import numpy as np

# api_key = si.api_key

def try_date(str_date, str_format=["%D"], log=False):
    # function takes a list of possible date formats (str_format) and tries them
    # on a single string (str_date). It's possible to get a read out of not cast strings by setting
    # log = True

    # Returns a string in ISO 8601 format
    out = "invalid"
    for test_form in str_format:
        try:
            out = datetime.strptime(str_date, test_form)
            break
        except:
            continue
    if out == "invalid" and log:
        print(str_date)
    return out

def connect_to_mysql(si, echo = False):
    import sqlalchemy
    engine_text ='mysql+mysqldb://' + si.db_user + ':'+si.db_pass+'@' + si.db_host + ':'+si.db_port+'/'+si.db_table
    return sqlalchemy.create_engine(engine_text, echo = echo)


class mtaGTFS(object):
    def __init__(self, subway_group = "irt", api_key=None, buildTables = True, single_id = False, past = None):
        """ Inits mtaGTFS with the following arguments
        Args:
            subway_group (str): which subway type to read in. {"irt", "l", "sir"}
            default "irt"
            api_key (str): mta given api key
        Attributes:
            subway_group: defined in init
            api_key: defined in init
            feed:
        """
        self.subway_group = subway_group
        self.feed = gtfs_realtime_pb2.FeedMessage()
        if(past is None):
            self.urls = {
                'irt' : "http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=1",
                'l':"http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=2",
                'sir' : "http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=11",
                'nqrw': "http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=16",
                'ace': "http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=26",
                'bdfm':"http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=21",
                'g': "http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=31",
                'jz': "http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=36",
                '7':"http://datamine.mta.info/mta_esi.php?key="+api_key+"&feed_id=51"
        }
        else:
            self.urls = {'irt' : 'https://datamine-history.s3.amazonaws.com/gtfs-' + past}

        self.timePulled = None
        self.trainIds = None
        self.stationlkp = None
        self.enrouteTrains = None
        self.unscheduledTrains = None
        self.updateFeed(buildTables,single_id)

    def updateFeed(self, buildTables = True, single_id = False):
        self.timePulled = datetime.now()
        self.response = requests.get(self.urls[self.subway_group])

        self.feed.ParseFromString(self.response.content)
        self.timeFeed = datetime.fromtimestamp(self.feed.header.timestamp)
        if buildTables:
            self.buildTrainIds()
            self.buildAllStops(single_id)
            self.buildAllEnroute()
    def jsonDump(self):
        import json
        #local files
        import protobuf_json
        self.json = json.dumps(protobuf_json.pb2json(self.feed), separators=(',',':'))

    def getEntity(self,id):
        try:
            return self.feed.entity[id-1]
        except IndexError:
            print("Here is the id attempted: " + str(id))
            print("Max size:" + str(len(self.feed.entity)))
            raise


    def buildTrainIds(self, log = False):
        #creates a full list of all train ids
        # outputs into self.trainIds
        raw_list = []
        raw_unscheduled_list = []
        entity_id = 0
        for entity in self.feed.entity:
            entity_id += 1
            if entity.HasField('trip_update'):
                type = 'scheduled'
                ent = entity.trip_update
            elif entity.HasField('vehicle'):
                type = 'enroute'
                ent = entity.vehicle
            else:
                if(log):
                    print(entity)
                continue
            # assigned = ent.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor].is_assigned

            trip_id = ent.trip.trip_id
            start_date = ent.trip.start_date
            full_id = start_date + "_" + trip_id
            split_full_id = full_id.split('..')
            if len(split_full_id) >1:
                route_plan = split_full_id[1]
            else:
                route_plan = ''
            # full_id = self._makeFullId(ent)
            #entity_id = int(entity.id)
            route_id= ent.trip.route_id

            if(len(route_plan) > 0):
                direction = route_plan[0]
            else:
                direction = ''
            #Direction 1 = North, Direction 3 = South
            # direction = ent.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor].direction
            # if direction==1:
            #     direction="N"
            # elif direction ==3:
            #     direction ="S"
            # else:
            #     direction = "U"
            #SIR vs IRT lines have different date formates so we need to try both.
            start_date = try_date(start_date, ["%Y-%m-%d %H:%M:%S","%Y%m%d"]).date()

            raw_data = [full_id,entity_id,type,route_id,direction, start_date,route_plan]
            # if assigned:
            raw_list.append(raw_data)
            # else:
                # raw_unscheduled_list.append(raw_data)
        pd_columns = ['full_id', 'entity_id', 'type','route_id','direction','start_date','route_plan']
        pd_index = ['type','full_id']
        self.trainIds = pd.DataFrame(columns = pd_columns,
            data = raw_list).set_index(pd_index)
        self.unscheduledTrains = pd.DataFrame(columns = pd_columns,
            data = raw_unscheduled_list).set_index(pd_index)
    def _getIndividualTrain(self, full_id, log = False):
        #finds all the entities with particular full_id, returns them as a list.
        #self.buildTrainIds()
        ent_iter = self.trainIds.entity_id[self.trainIds.full_id == full_id]
        out = [self.getEntity(ent_id) for ent_id in ent_iter]
        if log:
            for item in out:
                print(item)
        return out
    def getStops(self, full_id):
        # gets all the scheduled stops for a train with a particular id. These
        # stops will only be stops that are to be made, and past stops are not listed.
        # returned as a DataFrame
        raw_list = []
        #select_row = (self.trainIds.full_id == full_id) & (self.trainIds.type=='scheduled')
        #if(select_row.sum() !=1):
            #print(full_id +" has multiple entries in trainIds.")

        #entity_num = int(self.trainIds.ix[select_row,'entity_id'])
        entity_num = self.trainIds.loc[('scheduled',full_id),'entity_id']
        if(type(entity_num) is not np.int64):
            print('some weird double, choosing first one:' + full_id)
            entity_num = int(entity_num[0])

        stops = self.getEntity(entity_num).trip_update.stop_time_update

        # first/last stops to do not have departure/arrival times, respectfully
        for stop in stops:
            if stop.HasField('arrival'):
                arrival = datetime.fromtimestamp(stop.arrival.time)
            else:
                 arrival=None
            if stop.HasField('departure'):
                departure = datetime.fromtimestamp(stop.departure.time)
            else:
                departure=None
            stop_id = stop.stop_id
            raw_list.append([full_id,stop_id,arrival,departure])
        out = pd.DataFrame(columns=['full_id','stop_id','arrival','departure'], data = raw_list)
        #out.set_index(['full_id','stop_id'],inplace =True)
        return out

    def getEnroute(self, full_id):
        """ Get's all the enroute information for an enroute train

        Args:
            full_id (str): an id for a specific train

        Returns:
            a dictionary with keys = full_id, stop_id, current_status,
                last_ping, current_stop_sequence.

        Raises:
            N/A
        """
        type(1)
        # select_row = (self.trainIds.full_id == full_id) & (self.trainIds.type =='enroute')
        entity_num = self.trainIds.loc[('enroute',full_id),'entity_id']
        if(type(entity_num) is not np.int64):
            print('some weird double, choosing first one:' + full_id)
            entity_num = int(entity_num[0])
        vehicle = self.getEntity(entity_num).vehicle

        stop_id = vehicle.stop_id
        if(stop_id==""):
            stop_id = self.getStops(full_id).head(1).stop_id[0]

        current_status = vehicle.current_status

        if current_status == 0:
            current_status = "INCOMING_AT"
        elif current_status == 1:
            current_status = "STOPPED_AT"
        elif current_status == 2:
            current_status = "IN_TRANSIT_TO"
        else:
            current_status = "unknown"

        last_ping = datetime.fromtimestamp(vehicle.timestamp)
        current_stop_sequence = vehicle.current_stop_sequence

        out = {'full_id': full_id, 'stop_id': stop_id,
                   'current_status':current_status, 'last_ping':last_ping,
                   'current_stop_sequence':current_stop_sequence}
        return out
    def getUniqueTrains(self, type):
        train_ids = self.trainIds.loc[type].index
        if(not train_ids.is_unique):
            #raise an error here
            print(type + " trains' Ids are not unique")
        return train_ids.unique()

    def buildAllEnroute(self):
        # sched_trains_ids = self.trainIds.ix['enroute'].index
        # #sched_trains_ids = sched_trains_ids.order()
        # if(not sched_trains_ids.is_unique):
            # #raise an error here
            # print "Scheduled trains' Ids are not unique"

        # select_row = (self.trainIds.type=='enroute')
        enroute_train_ids = self.getUniqueTrains(type ='enroute')

        #enroute_train_ids = np.unique(self.trainIds.ix[select_row,'full_id'])
        dict_list =[self.getEnroute(fullid_train) for fullid_train in enroute_train_ids]
        out = pd.DataFrame(data = dict_list).set_index('full_id')
        # if(self.stationlkp is None):
            # self.fetchStationNames()
        # self.enrouteTrains  = out.merge(self.stationlkp, how ='left',
            # on = 'stop_id')
        self.enrouteTrains = out.sort_values('full_id')
    def buildAllStops(self, single_id = False):
        # builds all scheduled stops
        #select_row = (self.trainIds.type=='scheduled')
        #sched_train_ids  = np.unique(self.trainIds.ix[select_row,'full_id'])
        # sched_trains_ids = self.trainIds.ix['scheduled'].index
        # #sched_trains_ids = sched_trains_ids.order()
        # if(not sched_trains_ids.is_unique):
            # #raise an error here
            # print "Scheduled trains' Ids are not unique"
        unique_ids = self.getUniqueTrains(type ='scheduled')

        df_list = [self.getStops(fullid_train) for fullid_train in unique_ids]
        out =pd.concat(df_list, ignore_index=True)
        # if(self.stationlkp is None):
            # self.fetchStationNames()
        # self.scheduledStops  = out.merge(self.stationlkp, how ='left',
            # on = 'stop_id')
        if single_id:
            out['full_stop_id'] = out['full_id'] + out['stop_id']
            out.set_index('full_stop_id', inplace = True)
            self.scheduledStops = out.sort_index()
        else:
            out.set_index(['full_id','stop_id'], inplace = True)
            self.scheduledStops = out.sort_index()



    def _fetchStationNames(self):
        # get station names. This should only happen once when we have the update script
        import sqlalchemy
        import os

        import sensative_info as si
        engine_text ='mysql+mysqldb://'+si.db_user+':'+si.db_pass+'@' + si.db_host +':'+si.db_port+'/'+si.db_table


        if os.path.isfile('stops.txt'):
            # check http://web.mta.info/developers/data/nyct/subway/google_transit.zip for updated stops.txt
            self.stationlkp= pd.read_csv('stops.txt')
            self.stationlkp = self.stationlkp[['stop_id','stop_name','stop_lat','stop_lon']]
        else:
            engine = sqlalchemy.create_engine('engine_text')
            self.stationlkp = pd.read_sql_table('stops',engine, columns = ['stop_id','stop_name','stop_lat','stop_lon'])

    def _filterTrains(self, route_id = None, direction = None, start_date=datetime.now().date(), type_t = 'scheduled'):
        # returns full_ids for trains that fulfill specfic criteria
        if  route_id is not None:
            route_sel = (self.trainIds.route_id==str(route_id))
        else:
            route_sel = True
        if  direction is not None:
            if isinstance(direction,   str):
                # Convert string direction to int selector
                north_re = re.compile("[nN](orth)?")
                if(len(north_re.findall(direction))>0):
                    direction = "N"
                else:
                    direction = "S"

            dir_sel = (self.trainIds.direction==direction)
        else:
            dir_sel = True

        date_sel = self.trainIds.start_date >= start_date
        type_sel = (self.trainIds.type == type_t)

        return self.trainIds.full_id[(route_sel * dir_sel *date_sel * type_sel)==1]
