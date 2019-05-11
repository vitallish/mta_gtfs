import pandas as pd
import sqlalchemy
import sensative_info as si


engine_text ='mysql+mysqldb://'+si.db_user+':'+si.db_pass+'@' + si.db_host +':'+si.db_port+'/'+si.db_table
engine = sqlalchemy.create_engine(engine_text)

stops = pd.read_csv('stops.txt')
stops = stops[['stop_id','stop_name','stop_lat','stop_lon']]


stops.to_sql('stops', engine, index = False, if_exists = 'append')
