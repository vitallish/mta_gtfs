import sensative_info as si
import mtaGTFS
import sqlalchemy
import pandas as pd
import numpy as np

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import table, column, select
from sqlalchemy import text

import os
import gzip

engine = mtaGTFS.connect_to_mysql(si, echo =False)


def fetchTrainIDsByDate(early_date, end_date, engine):
    train_ids_text = select('*').select_from(table('trainID')).where(column('start_date').between(early_date, end_date))

    return pd.read_sql(train_ids_text, engine)


def fetchSchedStopsByTrainID(train_ids, engine):
    # train_ids is an array or pandas column

    sched_stops_text = select('*').select_from(table('sched_stops')).where(column('full_id').in_(train_ids))

    return pd.read_sql(sched_stops_text, engine)

def pullWeekTrains(date_ser, engine, overwrite = False):
    folder_name = format(date_ser.year) + '_' + format(date_ser.week, '02')
    full_path = 'stored_data/' + folder_name

    if os.path.exists(full_path) and not overwrite:
        return False
    elif not os.path.exists(full_path):
        os.makedirs(full_path)

    train_id_fn =full_path+'/' + format(date_ser.early_date) + '_' + format(date_ser.end_date)+'_trainID.csv.gz'

    sched_stop_fn =full_path+'/' + format(date_ser.early_date) + '_' + format(date_ser.end_date)+'_schedStop.csv.gz'

    train_ids_df = fetchTrainIDsByDate(date_ser.early_date, date_ser.end_date, engine)

    sched_stops_df = fetchSchedStopsByTrainID(train_ids_df.full_id, engine)

    with gzip.open(train_id_fn, 'wb') as f:
        train_ids_df.to_csv(f)

    with gzip.open(sched_stop_fn, 'wb') as f:
        sched_stops_df.to_csv(f)

    return True


# sunday starts a new week
#this will run on sunday at 1 am
# and leave two weeks (counting today)
current_week_text  = text(
"SELECT  year(start_date) as year, week(start_date) as week, min(start_date) as early_date, max(start_date) as end_date from trainID WHERE week(start_date) is not null group by year(start_date), week(start_date) order by year(start_date) desc, week(start_date) desc;")

all_weeks_df = pd.read_sql(current_week_text, engine)

weeks_to_store = all_weeks_df[2:]


for row in weeks_to_store.iterrows():
    pullWeekTrains(row[1], engine, overwrite=False)
