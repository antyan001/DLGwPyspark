#!/home/shubochkin1-ei_ca-sbrf-ru/bin/python35
import os
import sys
curruser = os.environ.get('USER')

# sys.path.insert(0, '/opt/workspace/{user}/system/support_library/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/libs/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/system/labdata/lib/'.format(user=curruser))


sys.path.insert(0, './../src')
sys.path.insert(0, '/home/{user}/notebooks/support_library/'.format(user=curruser))
sys.path.insert(0, '/home/{user}/python35-libs/lib/python3.5/site-packages/'.format(user=curruser))
sys.path.insert(0, '/home/{user}/notebooks/labdata/lib/'.format(user=curruser))

import tendo.singleton
import warnings
warnings.filterwarnings('ignore')

import logging
logging.basicConfig(filename='./logs/__export2iskra__.log',level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


import joblib
import json
from joblib import Parallel, delayed

from time import sleep
from itertools import islice
from multiprocessing import Pool, Process, JoinableQueue
from multiprocessing.pool import ThreadPool
from functools import partial
import subprocess
from threading import Thread
import time
from datetime import datetime

from spark_connector import SparkConnector
from sparkdb_loader import spark
from connector import OracleDB
import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

import re
import pandas as pd
import numpy as np
from tqdm._tqdm_notebook import tqdm_notebook
from pathlib import Path
import shutil
import loader as load

sing = tendo.singleton.SingleInstance()

# os.chdir('/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/Clickstream_Analytics/AutoUpdate/')
os.chdir('/home/{}/notebooks/NRT_RUN/'.format(curruser))

def stop_spark(sp):
    print_and_log("Stopping Spark context...")
    sp.sc.stop()

    print_and_log(">" * 90)
    print_and_log("END of DBs UPDATE")
    print_and_log(">" * 90)
    return None

def print_and_log(message: str):
    print(message)
    logger.info(message)
    return None

table_name = 'NRT_ALL_SCENARIOS_HIST'
conn_schema = 'sbx_team_digitcamp'


print("### Starting spark context. Run!")
sp = spark(schema='sklod_external_google_analytics',
           queue = 'ektov1-av_ca-sbrf-ru',
           process_label="ISKRA_EXPORT",
           numofinstances=12,
           numofcores=8,
           dynamic_alloc=False,
           kerberos_auth=True
          )
hive = sp.sql
print(sp.sc.version)

print_and_log("### Find last timestamp - max(load_dt) in NRT_GA_ALL_SCENARIOS_HIST Oracle datamart")

sql = """
(
select /*+ parallel (6) */ max(load_dt) as maxdt from {}
)""".format(table_name)

cntconn = 0

while True:
    try:
        max_load_dt = sp.get_oracle(OracleDB('iskra4'), sql).collect()
        break
    except Exception as ex:
        if 'could not open socket: ["tried to connect to' in str(ex):
            sleep(10)
            cntconn+=1
            if cntconn >2:
                print_and_log("could not open socket [Errno 111] Connection refused")
                break

max_resp_dt_str = datetime.strftime(max_load_dt[0]['MAXDT'], format='%Y-%m-%d %H:%M:%S')
print_and_log("### max load_dt is {}".format(max_resp_dt_str))


print_and_log("### Updating NRT_GA_ALL_SCENARIOS_HIST datamart. Perform Synchronization between Oracle and Hive dbs")
print_and_log("### Selecting records from relevant Hive table-->")

sql = '''select * from {}.{} where load_dt > timestamp('{}')'''.format(conn_schema,table_name,max_resp_dt_str)
sdf = hive.sql(sql)
sdf = sdf.select([col.upper() for col in sdf.columns])
cnt_sdf = sdf.count()
print_and_log("### Number of records to insert into ORacle table are: {}".format(cnt_sdf))

if cnt_sdf == 0:
    print_and_log("### Nothing to insert. BYE!!!")
    stop_spark(sp)
    # sing.__del__()
    exit()

typesmap={}
for column_name, column in sdf.dtypes:
    if column == 'string':
        if 'INN' in column_name.upper() or 'KPP' in column_name.upper():
            typesmap[column_name] = 'VARCHAR(20)'
        elif 'commonSegmentoUID'.upper() in column_name.upper():
            typesmap[column_name] = 'CLOB'
        else:
            typesmap[column_name] = 'VARCHAR(900)'
    elif column == 'int':
        typesmap[column_name] = 'INTEGER'
    elif column == 'bigint':
        typesmap[column_name] = 'INTEGER'
    elif column == 'timestamp':
        typesmap[column_name] = 'TIMESTAMP'
    elif column == 'float' or column == 'double':
        typesmap[column_name] = 'FLOAT'
    else:
        None

cols = ', '.join([col + ' ' + typesmap[col] for col in sdf.columns])

db = OracleDB('iskra4')
mode = 'append'
sdf \
    .write \
    .format('jdbc') \
    .mode(mode) \
    .option('url', 'jdbc:oracle:thin:@//'+db.dsn) \
    .option('user', db.user) \
    .option('password', db.password) \
    .option('dbtable', table_name) \
    .option('createTableColumnTypes', cols)\
    .option('driver', 'oracle.jdbc.driver.OracleDriver') \
    .save()

# print_and_log("### Dropping temp table {}".format(out_table_name))
# hive.sql("drop table if exists {schema}.{tbl}".format(schema=conn_schema,
#                                                       tbl=out_table_name))

stop_spark(sp)

sing.__del__()














