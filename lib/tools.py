#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

################################## Import ##################################

import os
import sys
import warnings
import subprocess
import re
import abc
import argparse

warnings.filterwarnings('ignore')

curruser = os.environ.get('USER')

if curruser in os.listdir("/opt/workspace/"):
    sys.path.insert(0, '/opt/workspace/{user}/system/support_library/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/libs/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/system/labdata/lib/'.format(user=curruser))
else:
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import inspect
import tendo.singleton
from collections import Counter
import time

from spark_connector import SparkConnector
from sparkdb_loader import spark
from connector import OracleDB
import pyspark
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pyspark.sql.types as stypes
from pyspark.sql import Window
import loader

from lib.logger import *

################################## Functions ##################################

def typed_udf(return_type):
    return lambda func: f.udf(func, return_type)


def show(self, n=10):
    return self.limit(n).toPandas()

pyspark.sql.dataframe.DataFrame.show = show


################################## Table functions ##################################


def load_table(schema, table, hive):
    return hive.table("{schema}.{table}".format(schema=schema, table=table))


def drop_table(schema, table, hive):
    hive.sql("drop table if exists {schema}.{table} purge" \
                .format(schema=schema, table=table))

    # subprocess.call(['hdfs', 'dfs', '-rm', '-R', '-skipTrash',
    #     "hdfs://clsklsbx/user/team/team_digitcamp/hive/{}".format(table.lower())])


def create_table_from_tmp(schema_out, table_out, table_tmp, hive):
    hive.sql("create table {schema_out}.{table_out} select * from {table_tmp}" \
            .format(schema_out=schema_out, table_out=table_out, table_tmp=table_tmp))

    # subprocess.call(['hdfs', 'dfs', '-chmod', '-R', '777',
    #     "hdfs://clsklsbx/user/team/team_digitcamp/hive/{}".format(table_out.lower())])


def create_table_from_df(schema, table, df, hive):
    df.registerTempTable(table)
    create_table_from_tmp(schema, table, table, hive)


def insert_into_table_from_df(schema, table, df, hive):
    df.registerTempTable(table)
    hive.sql('insert into table {schema}.{table} select * from {table}' \
                .format(schema=SBX_TEAM_DIGITCAMP, table=table))


def rename_table(schema, old_name, new_name, hive):
    hive.sql("alter table {schema}.{old_name} rename to {schema}.{new_name}" \
                .format(schema=schema, old_name=old_name, new_name=new_name))


def exception_restart(num_of_attempts: int = 3,
                      delay_time_sec: int = 10):

    def decorator(func):

        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(num_of_attempts):
                try:
                    func_return = func(*args, **kwargs)
                    return func_return
                except Exception as exc:
                    time.sleep(delay_time_sec)
                    last_exception = exc
                    continue
            raise last_exception

        return wrapper

    return decorator


################################## Resourses ##################################

#####################################################################
SBX_TEAM_DIGITCAMP = 'sbx_team_digitcamp'
SBX_T_TEAM_CVM = 'sbx_t_team_cvm'
DEFAULT = 'default'
##-----------------------------------------------------------------##
INTERNAL_PANHASH_COOKIES = "internal_panhash_cookies"
URLS_TO_PARSE_FOR_BUSINESS = "urls_to_parse_for_business"

ALL_COOKIES = "all_cookies"

MA_PRODUCT_DICT = 'ma_product_dict'
MA_CMDM_MA_DEAL = "ma_cmdm_ma_deal_new"
MA_MMB_OFFER_NONTOP = "ma_mmb_offer_nontop"

MW_ATB_SEGMEN_PROD_GROUP = "MW_ATB_SEGMEN_PROD_GROUP"

OFFER_PRIORITY = "offer_priority"

#GA_CID_SBBOL_INN = "ga_cid_sbbol_inn"
# GA_CID_SBBOL_INN = "all_cookie_inn_match"
GA_CID_SBBOL_INN = "u_recsys360_cookie_matching"
GA_ALL_SCENARIOS_HIST = 'ga_all_scenarios_hist_part'
#GA_ALL_SCENARIOS_HIST = 'ga_all_scenarios_hist'
GA_ALL_SCENARIOS_HIST_RESULT = 'ga_all_scenarios_hist'
GA_ALL_SCENARIOS_STATS = "ga_all_scenarios_stats"
GA_SITE_ALL_PRODS = "GA_SITE_ALL_PRODS_SLICE"

TMP_VISIT_PART = 'tmp_visit_part'
TMP_TEST_VISIT_PART = 'TMP_VISIT_PART'
TMP_EXC_COPY_TABLE = "tmp_exc_copy_table"
TMP_SITE_ALL_PRODS_INTERMEDIATE = "TMP_SITE_ALL_PRODS_INTERMEDIATE"

PRX_UNIFIED_CUSTOMER_SCHEME = "prx_unified_customer_internal_kakb_kakb_od"
UNIFIED_CUSTOMER = 'ma_unified_customer'
GA_MA_CUSTOMER_SBBOL = "GA_MA_CUSTOMER_SBBOL"
GA_MA_USER_PROFILE_SBBOL = "GA_MA_USER_PROFILE_SBBOL"
MA_CUSTOMER_COOKIE_MAP = "MA_CUSTOMER_COOKIE_MAP"
#####################################################################

#####################################################################
DEFAULT_GOOGLE_ANALYTICS = 'google_analytics_visit'
PROXY_GOOGLE_ANALYTICS = 'GOOGLE_ANALYTICS_VISIT' #"prx_google_analytics_part_external_google_analytics"
SKLOD_EXTERNAL_GOOGLE_ANALYTICS = "sklod_external_google_analytics"
CAP_DDA_GOOGLE_ANALYTICS = "cap_external_google_dda_external_google_analytics"
##-----------------------------------------------------------------##
VISIT = None #'visit'
#####################################################################

#####################################################################
SKLOD_EXTERNAL_CLICKSTREAM = "sklod_external_clickstream"
##-----------------------------------------------------------------##
CLICKSTREAM = 'clickstream'
#####################################################################

#####################################################################
SKLOD_NRT_SBBOL_CLICKSTREAM = "sklod_nrt_sbbol_clickstream"
##-----------------------------------------------------------------##
SBBOL_EVENTS = 'sbbol_events'
SBBOL_EVENTS_DELTA = 'sbbol_events_delta'
#####################################################################

################################## Iskra ##################################
ISKRA = 'iskra4'
##-----------------------------------------------------------------##
ISKRA_LOGIN = 'tech_iskra[iskra]'
ISKRA_PASS = 'Uthvfy123'
##-----------------------------------------------------------------##
ISKRA_BATCH_SIZE = 700000
#####################################################################

################################## ScenarioBase Data ##################################
SLICE_JSON_FILE = 'dates/slice.json'
SCENARIOS_JSON_FILE = 'dates/scenarios.json'
##-----------------------------------------------------------------##
TEST_MIN_CTL_LOADING_DEFAULT = 3709530
TEST_MAX_CTL_LOADING_DEFAULT = 3709946
#####################################################################

################################## SourcesUpdate Data ##################################
UNIFIED_CUSTOMER_JSON_FILE = 'dates/unified_customer.json'
SOURCES_JSON_FILE = 'dates/sources.json'
##-----------------------------------------------------------------##
REWRITABLE_SOURCES_LIST = [
    #'MA_MMB_OFFER_NONTOP' ,
    'UNIFIED_CUSTOMER', #error
    # 'MA_CUSTOMER_COOKIE_MAP',
    'GA_MA_CUSTOMER_SBBOL', #error
    'GA_MA_USER_PROFILE_SBBOL',
    'MA_PRODUCT_DICT',
]
##-----------------------------------------------------------------##
UPDATABLE_SOURCES_LIST = [
    #'OFFER_PRIORITY',
    #'MA_CMDM_MA_DEAL'
]

DATE_COLUMN_IN_UPDATABLE_SOURCES = {
    'OFFER_PRIORITY': "load_dttm",
    #'MA_CMDM_MA_DEAL': "create_dt"
}
##-----------------------------------------------------------------##
FREQUNCY_0F_UPDATES = {
    'UNIFIED_CUSTOMER' : 3,
    # 'MA_CUSTOMER_COOKIE_MAP' : 3,
    'GA_MA_CUSTOMER_SBBOL' : 3,
    'GA_MA_USER_PROFILE_SBBOL' : 3,
    # 'MA_MMB_OFFER_NONTOP' : 3,
    'MA_PRODUCT_DICT' : 3,
    # 'OFFER_PRIORITY' : 3,
    # 'MA_CMDM_MA_DEAL' : 3
}
#####################################################################

################################## ScenarioStats Data ##################################
EMAIL_LIST_FILE = "mail_settings/mail_list.txt"
TEST_EMAIL_LIST_FILE = "mail_settings/test_mail_list.txt"
EMAIL_SETTINGS_FILE = "mail_settings/mail_settings.txt"
##-----------------------------------------------------------------##
ALL_SCENARIOS = [
    'STORIES_01',
    'DEPOSIT_01',
    'CREDIT_02',
    'WTA_SITE_NBS',
    'PREMIUMCARD_02',
    'HELP_SBBOL',
    'LETTERSOFCREDIT_01',
    'CORPCARD_01',
    'PREMIUMCARDMASTER_02',
    'DIGITALCARD_02',
    'CORPCARD_02',
    'DIGITALCARD_01',
    'PREMIUMCARDMASTER_01',
    'BUSINESS_CARD_01',
    'SITEALLPRODAGG_01',
    'SHOP_01',
    'PREMIUMCARD_01',
    'ACQUIRING_01',
    'CASHORDER_01',
    'ENCASHMENT_01',
    'COLLINSUR_ONLINE',
    'COLLINSUR_OTHER',
    'COLLINSUR_ALREADY',
    'RKO_OPEN_ACC',
    'RKO_BANK',
    'RKO_DOS']
#####################################################################
