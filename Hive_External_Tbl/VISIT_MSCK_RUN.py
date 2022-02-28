#!/home/shubochkin1-ei_ca-sbrf-ru/bin/python36

import os, sys
import argparse

from lib.tools import *
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Catalog


parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-create', '--createExtTbl', type=bool, required=True, default=False)
parser.add_argument('-h', '--help',
                    action='help', default=argparse.SUPPRESS,
                    help='set createExtTbl param to True if you wanna recreate external table')
args = parser.parse_args()

SPARK_CONFIG={}

SPARK_CONFIG["process_label"] = "RECSYS_STORY_AB_TEST_DESIGN"
SPARK_CONFIG["numofcores"] = 10
SPARK_CONFIG["numofinstances"] = 6
SPARK_CONFIG["kerberos_auth"] = False

sp = spark(**SPARK_CONFIG)
hive = sp.sql

hive.setConf("mapreduce.input.fileinputformat.input.dir.recursive","true")
hive.setConf("mapred.input.dir.recursive","true")
hive.setConf("hive.mapred.supports.subdirectories","true")
# hive.setConf('spark.sql.parquet.binaryAsString', 'true')
# hive.setConf('spark.sql.hive.convertMetastoreParquet', 'false')

hive.setConf('hive.metastore.fshandler.threads', 30)
hive.setConf('hive.msck.repair.batch.size', 1000)
hive.setConf('hive.merge.smallfiles.avgsiz', 256000000)
hive.setConf('hive.merge.size.per.task', 256000000)


if args.createExtTbl == True:
    ## CREATE EXTERNAL TABLE
    print("DROP OLD GOOGLE_ANALYTICS_VISIT TABLE AND CREATE NEW ONE")
    sdf = hive.sql("select * from prx_google_analytics_part_external_google_analytics.visit where ctl_loading = 15256269")
    tbl_descr = ", ".join(["{} {}".format(col, tp) for col, tp in sdf.dtypes if col not in ('ctl_loading')])
    HDFS_LOC = "hdfs://hdfsgw/clsklod5/data/core/external/google_analytics/pa/snp/visit/"
    hive.sql("DROP TABLE GOOGLE_ANALYTICS_VISIT PURGE")

    create_ext_tbl_sql = \
    '''
    CREATE EXTERNAL TABLE GOOGLE_ANALYTICS_VISIT ({})
    --ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    PARTITIONED BY (ctl_loading integer)
    --STORED AS PARQUET
    LOCATION '{}'
    '''.format(tbl_descr, HDFS_LOC)

    hive.sql(create_ext_tbl_sql)

print("UPDATE METASTORE WITH NEW PARTITIONS")
Catalog(SparkSession(sp.sc)).recoverPartitions('GOOGLE_ANALYTICS_VISIT')
# hive.sql("MSCK REPAIR TABLE GOOGLE_ANALYTICS_VISIT")
print("GOOGLE_ANALYTICS_VISIT HAS BEEN UPDATED SUCCESFULLY")


