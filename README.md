# mtaGTFS Object Introduction and Description
The object defined in mtaGTFS allows the user to easily pull and analyze data from the NYC MTA site. This currently includes the 1, 2, 3, 4, 5, 6, S and SIR lines.

## MTA Feed Description
It helps parse the [GTFS file provided by the MTA](http://datamine.mta.info/sites/all/files/pdfs/GTFS-Realtime-NYC-Subway%20version%201%20dated%207%20Sep.pdf). In order to obtain the data, you must [first register for an api key](http://datamine.mta.info/user/register). More information on the feeds can be [found here on the mta website](http://datamine.mta.info/).

## Dependencies
To use the object you must include `mtaGTFS.py` along with `protobuf_json.py` and nyct_subway_pb2.py`. This object also depends on functionality provided by the following packages:
- [google.transit](https://github.com/google/gtfs-realtime-bindings/tree/master/python)
- pandas
- numpy

Sqlalchemy is required for some specific functions, but not necessary for the object to function properly

## Quick Start Guide
See `mtaGTFS.py` for details. Import mtaGTFS.py and create a a new object with:
`new_object = mtaGTFS(subway_group = "irt", api_key='*************')`
subway_group can be either 'irt', 'l', or 'sir' for each train type. Once the object is loaded the enrouteTrains, scheduledStops, and trainIds can be accessed as properties of new_object. The feeds can be updated with the updateFeed method.


# Database Setup
1. Install mysql
2. create a user
3. run the mysql/model_generation_script.sql
4. Grant the mta_user all privilages:
     GRANT ALL PRIVILEGES ON mta_trains.* to 'mta_user'@'%' identified by 'userpassword';
5. Open up your mysql database to remote connections
   This probably involves updating the bind_address
6. Write the stops.txt file to the stops TABLE
