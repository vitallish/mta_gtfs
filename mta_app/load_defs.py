import pandas as pd
import sqlalchemy
#import sensative_info as si
import os
db_user = os.environ['MYSQL_USER']
db_pass = os.environ['MYSQL_PASSWORD']
db_table = os.environ['MYSQL_DATABASE']


engine_text ='mysql+mysqldb://'+db_user+':'+db_pass+'@db:3306/'+db_table
engine = sqlalchemy.create_engine(engine_text)

test_table = pd.read_sql_table(table_name = 'stops', con  = engine)

stops = pd.read_csv('stops.txt')
stops = stops[['stop_id','stop_name','stop_lat','stop_lon']]

try:
    stops.to_sql('stops', engine, index = False, if_exists = 'append')
except:
    print('stops table already populated')
