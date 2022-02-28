#!/home/shubochkin1-ei_ca-sbrf-ru/bin/python35
import os
import sys
import warnings
import logging
import re

warnings.filterwarnings('ignore')
curruser = os.environ.get('USER')

# sys.path.insert(0, '/opt/workspace/{user}/system/support_library/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/libs/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/system/labdata/lib/'.format(user=curruser))

sys.path.insert(0, './../src')
# sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/python35-libs/lib/python3.5/site-packages/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))

# sys.path.insert(0, '/home/{}/notebooks/ecom_model/src/'.format(curruser))
sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))

import tendo.singleton
import pandas as pd
import json
from datetime import datetime
from spark_connector import SparkConnector
from sparkdb_loader import spark
import pyspark
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *

sing = tendo.singleton.SingleInstance()

# os.chdir('/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/Clickstream_Analytics/AutoUpdate/')
os.chdir('/home/{}/notebooks/NRT_RUN/'.format(curruser))

logging.basicConfig(filename='./logs/__upd_NRTSBBOL_ACQUIRING__.log',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------------ #
NUM_OF_STEPS = 13  # Funnel
PRODUCT_CD_NAME = "ACQUIRING"
PRODUCT_LIKE = "эквайринг"
SCENARIO_ID = "NRT_ACQUIRING"
CHANNEL = "SBBOL"
REFDATE = '2020-12-14'

CONN_SCHEMA = 'sbx_team_digitcamp'
NRT_SCHEMA = 'sklod_nrt_sbbol_clickstream'
NRT_EVENTS = 'sbbol_events'
OUT_TABLE_NAME = 'NRT_ALL_SCENARIOS_HIST'
MA_DEALS_TABLE_NAME = 'MA_CMDM_MA_DEAL'
CID_INN_TABLE_NAME = 'GA_CID_SBBOL_INN'
MA_UNIFIED_CUSTOMER = 'UNIFIED_CUSTOMER'
MA_PRODUCT_DICT = 'ma_product_dict'

fPathtoScenariosDict = './json/scenariosCtlJson'

GA_NEW_SLICE_ID = 'LAST_SESS_DATE'
fPathtoGASliceDict = './json/lastDateJson'
# ------------------------------------ #

def stop_spark(sp):
    print_and_log("Stopping Spark context...")
    sp.sc.stop()

    print_and_log(">" * 90)
    print_and_log("END of DBs UPDATE")
    print_and_log(">" * 90)
    return None

def show(self, n=10):
    return self.limit(n).toPandas()

pyspark.sql.dataframe.DataFrame.show = show

def get_value(col, key):
    if isinstance(col, str):
        js = json.loads(col)
        for item in js:
            if item['key'] == key:
                return item['value']
    elif isinstance(col, list):
        for i in range(len(col)):
            if col[i].key == key:
                return col[i].value

@udf(IntegerType())
def get_timestamp(col):
    dt = datetime.strptime(col.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    return int(dt.timestamp())

@udf(StringType())
def get_cid(col):
    try:
        return ".".join(get_value(col, "_ga").split(".")[-2:])
    except:
        return None

@udf(StringType())
def get_commonSegmentoUID(col):
    try:
        return get_value(col, "Segmento_UID")
    except:
        return None

@udf(StringType())
def get_sbbolUserId(col):
    try:
        return get_value(col, "sbbolUserId")
    except:
        return None

@udf(StringType())
def get_sbbolOrgGuid(col):
    try:
        return get_value(col, "sbbolOrgGuid")
    except:
        return None

@udf(StringType())
def get_hitPagePath(col):
    try:
        return get_value(col, "hitPagePath")
    except:
        return None

@udf(StringType())
def get_hitPageHostName(col):
    try:
        return get_value(col, "hitPageHostName").split(":")[1][2:]
    except:
        return None

@udf(StringType())
def get_eventLabel(col):
    try:
        return get_value(col, "eventLabel")
    except:
        return None

##--------------------------------------------------------
def getProdCDFromId(product_dict_nm):
    def get_product(product_id, product_dict_nm):
        if product_id is None:
            return None

        for _id in product_dict_nm:
            if product_id == _id:
                return product_dict_nm[_id]

        return None

    return f.udf(lambda x: get_product(x,product_dict_nm))
##--------------------------------------------------------

def essense(channel: str, prod_cd: str):
    message = "{}: {} retargeting".format(channel, prod_cd)
    return message

essense_udf = f.udf(essense, StringType())


def print_and_log(message: str):
    print(message)
    logger.info(message)
    return None


def get_df_sbbol(nrt_sbbol_clickstream=NRT_SCHEMA, nrt_schema=NRT_EVENTS, start_date=REFDATE, ref_ts = None, parq_id = None):
    print_and_log("Formate table")

    if ref_ts is None:
        sql=\
        '''select
                    timestampcolumn as sessiondate,
                    data_eventCategory as eventCategory,
                    data_eventType as eventAction,
                    case when data_eventAction = "event" then "EVENT"
                         when data_eventAction = "pageview" then "PAGE"
                    else "EXCEPTION" end as hitType,
                    data_properties,
                    profile_cookie,
                    from_unixtime(unix_timestamp(substring_index(kafkaTimestampField,'.',1),
                                                              "yyyy-MM-dd'T'HH:mm:ss")) as sessionStartTime,
                    from_unixtime(unix_timestamp(substring_index(kafkaTimestampField,'.',1),
                                                              "yyyy-MM-dd'T'HH:mm:ss"),"yyyy-MM-dd") as sessionDay
           from {}.{} where timestamp(timestampcolumn) == timestamp('{}')
        '''.format(nrt_sbbol_clickstream, nrt_schema, start_date)
    else:
        # sql=\
        # '''select
        #             timestampcolumn as sessiondate,
        #             data_eventCategory as eventCategory,
        #             data_eventType as eventAction,
        #             case when data_eventAction = "event" then "EVENT"
        #                  when data_eventAction = "pageview" then "PAGE"
        #             else "EXCEPTION" end as hitType,
        #             data_properties,
        #             profile_cookie,
        #             from_unixtime(unix_timestamp(substring_index(kafkaTimestampField,'.',1),
        #                                                          "yyyy-MM-dd'T'HH:mm:ss")) as sessionStartTime,
        #             from_unixtime(unix_timestamp(substring_index(kafkaTimestampField,'.',1),
        #                                                          "yyyy-MM-dd'T'HH:mm:ss"),"yyyy-MM-dd") as sessionDay
        #    from {}.{} where (timestamp(timestampcolumn) >= timestamp('{}'))
        #               and (from_unixtime(unix_timestamp(substring_index(kafkaTimestampField,'.',1),
        #                                                                            "yyyy-MM-dd'T'HH:mm:ss")) > timestamp('{}'))
        # '''.format(nrt_sbbol_clickstream, nrt_schema, start_date, ref_ts)

        sdf = hive.read.option('dropFieldIfAllNull',True)\
                       .parquet("hdfs://clsklod/data/core/nrt/sbbol_clickstream/pa/snp/sbbol-clickstream/event/timestampcolumn={}".format(parq_id))

        df_sbbol_table = \
              sdf.select(f.lit(parq_id).alias("sessiondate"),
                         f.col("data_eventCategory").alias("eventCategory"),
                         f.col("data_eventType").alias("eventAction"),
                         "data_properties",
                         "profile_cookie",
                         f.from_unixtime(f.unix_timestamp(f.substring_index("kafkaTimestampField",'.',1),
                                                                 "yyyy-MM-dd'T'HH:mm:ss")).alias("sessionStartTime"),
                         f.from_unixtime(f.unix_timestamp(f.substring_index("kafkaTimestampField",'.',1),
                                                                 "yyyy-MM-dd'T'HH:mm:ss"),"yyyy-MM-dd").alias("sessionDay"),
                         f.when(f.col("data_eventAction") == "event", "EVENT")\
                          .when(f.col("data_eventAction") == "pageview", "PAGE").otherwise("EXCEPTION").alias("hitType")
                         ).filter("sessionStartTime > timestamp('{}')".format(ref_ts))

        # df_sbbol_table = hive.sql(sql)

# df_sbbol_table.withColumn("sessionStartTime", get_timestamp("data_timeStamp")) \
    df_sbbol_table = \
    df_sbbol_table.withColumn('hitPageHostName', get_hitPageHostName('data_properties')) \
                  .withColumn('hitPagePath', get_hitPagePath('data_properties')) \
                  .withColumn('sbbolUserId', get_sbbolUserId('data_properties')) \
                  .withColumn("eventLabel", get_eventLabel('data_properties')) \
                  .withColumn("cid", get_cid('profile_cookie')) \
                  .withColumn("commonSegmentoUID", get_commonSegmentoUID('profile_cookie')) \
                  .drop('profile_cookie', 'data_properties')

    print_and_log("### Filtering fetched data")
    df_sbbol_table = df_sbbol_table.filter("sbbolUserId is not Null")
    df_sbbol_table = df_sbbol_table.filter(f.col("sessiondate") == f.col("sessionDay"))
    df_sbbol_table.cache()

#     print_and_log("### Removing records corresponding to 23:59:59 time")
#     max_sessiondate = df_sbbol_table.select(f.max("sessiondate").alias("max_sessiondate")).collect()[0]["max_sessiondate"]
#     strange_datetime = max_sessiondate + ' ' + '23:59:59'
#     df_sbbol_table = df_sbbol_table.filter(f.col("sessionStartTime") != f.to_timestamp(f.lit(strange_datetime)))

    print_and_log("Calcuating count and max sessiondate")
    cnt = df_sbbol_table.count()
    print_and_log("Number of records for treatment: {}".format(cnt))

    max_sessiondate = parq_id #df_sbbol_table.select(f.max("sessiondate").alias("max_sessiondate")).collect()[0]["max_sessiondate"]
    max_sessionStartTime = df_sbbol_table.select(f.max("sessionStartTime").alias("max_sessionStartTime")).collect()[0]["max_sessionStartTime"]

    return df_sbbol_table, max_sessiondate, max_sessionStartTime, cnt


def make_user_funnels(df_sbbol):
    print_and_log("-" * 90)
    print_and_log("### Formate funnels")

    patt0 = "\[layout.primarymenusbbol3\]: \[system_name\: menu_item_merchant\-acquiring"
    patt1 = "\[placement\: main\[main_menu\]\]\[container\:.*\[product\: merchant\-acquiring\]"

    df_sbbol = df_sbbol\
             .withColumn("isClickMenu",
                                        (f.col("eventCategory").like("%[std]: merchant acquiring%")) &
                                        (f.col("eventAction") == "click") & \
                                        (f.regexp_extract(f.trim(f.col("eventLabel")),patt0,0)!='')
                        )\
             .withColumn("isStoryImpression",
                                        (f.col("eventCategory").like("%[operations]: offers%")) &
                                        (f.col("eventAction") == "impression-card-short") & \
                                        (f.regexp_extract(f.trim(f.col("eventLabel")),patt1,0)!='')
                        )\
             .withColumn("isCreateRequest",
                                        (f.col("eventCategory") == "NBS_merchant-acquiring") &
                                        (f.col("eventAction") == "click") & \
                                        (f.trim(f.col("eventLabel")).like("%[shop card action]: [referral%"))
                        )\
             .withColumn("isClickApplication",
                                        (f.col("eventCategory") == "[std]: merchant acquiring") &
                                        (f.col("eventAction") == "click") & \
                                        (f.trim(f.col("eventLabel")).like('[merchantacquiring.malayout]%navigationtabs%acquiring%'))
                        )\
             .withColumn("isCreateNewApplication",
                                        (f.col("eventCategory") == "[std]: merchant acquiring") &
                                        (f.col("eventAction") == "click") & \
                                        (f.trim(f.col("eventLabel")).like('[merchantacquiring.malayout]: [actiondropdown]'))
                        )\
             .withColumn("isDropDownMenuClick",
                                        (f.col("eventCategory") == "[std]: merchant acquiring") &
                                        (f.col("eventAction") == "click") & \
                                        (f.trim(f.col("eventLabel")).like('[merchantacquiring.malayout]: [merchantacquiring:requestsdropdowntitles.maposregistrationrequest]'))
                        )\
             .withColumn("isLoadApplicationForm",
                                        (
                                            (f.col("eventCategory") == "[std]: merchant acquiring: contract registration request") &
                                            (f.col("eventAction") == "show") & \
                                            (f.trim(f.col("eventLabel")).like('%[contract registration request stepped edit page%create]: [page open]%'))
                                        )|\
                                        (
                                            (f.col("hitType") == "PAGE") &
                                            (f.col("hitPagePath") == "/acquiring/merchant/requests/pos-registration-request/create")
                                        )
                        )\
             .withColumn("isCarouselClick",
                                        (f.col("eventCategory") == "[std]: merchant acquiring: pos registration request") &
                                        (f.col("eventAction") == "click") & \
                                        (
                                          (f.trim(f.col("eventLabel")) == '[pos registration request stepped edit page]: [carousel-arrow prev]') |
                                          (f.trim(f.col("eventLabel")) == '[pos registration request stepped edit page]: [carousel-arrow next]')
                                        )
                        )\
             .withColumn("isRegistrationRequest",
                                        (f.col("eventCategory") == "[std]: merchant acquiring: contract registration request") &
                                        (f.col("eventAction") == "click") &
                                        (f.trim(f.col("eventLabel")) == "[contract registration request stepped edit page]: [fill address from fias]")
                        )\
             .withColumn("isFormFillError",
                                        (f.col("hitType") == "PAGE") &
                                        (f.col("hitPagePath").like("%/settings/organization/requests%/merchant-acquiring/contract-registration/create%"))
                        )\
             .withColumn("isSendApplication",
                                        (f.col("eventCategory") == "[operations]: posregistrationrequest") &
                                        (f.col("eventAction") == "create-new")
                        )\
             .withColumn("isGetSMSCode",
                                        (f.col("eventCategory") == "[std]: merchant acquiring") &
                                        (f.col("eventAction") == "click") &
                                        (f.trim(f.col("eventLabel")) == "[merchantacquiring.malayout]: [get sms code]")
                        )\
             .withColumn("isSendSMSCode",
                                        (f.col("eventCategory") == "[std]: merchant acquiring") &
                                        (f.col("eventAction") == "click") &
                                        (f.trim(f.col("eventLabel")) == "[merchantacquiring.malayout]: [action.send]")
                        )\
             .withColumn("isSMSError",
                                        (f.col("eventCategory") == "[std]: layout") &
                                        (f.col("eventAction") == "show") &
                                        (f.trim(f.col("eventLabel")).like("[system notification panel]: [action: show, type: error%")) &
                                        (f.col("hitPagePath").like("%/acquiring/merchant/requests/pos-registration-request%"))
                        )\
             .withColumn("isSMSSenOK",
                                        (f.col("eventCategory") == "[operations]: posregistrationrequest") &
                                        (f.col("eventAction") == "send")
                        )

    print_and_log("-" * 90)

    df_sbbol_product = df_sbbol \
        .where("isClickMenu == True OR \
                isStoryImpression == True OR \
                isCreateRequest == True OR \
                isClickApplication == True OR \
                isCreateNewApplication == True OR \
                isDropDownMenuClick == True OR \
                isLoadApplicationForm == True OR \
                isCarouselClick == True OR \
                isRegistrationRequest == True OR \
                isFormFillError == True OR \
                isSendApplication == True OR \
                isGetSMSCode == True OR \
                isSendSMSCode == True OR \
                isSMSError == True OR \
                isSMSSenOK == True OR"
              )

    # Group by user
    print_and_log("### Formatting groupby user")
    df_sbbol_user = df_sbbol_product \
        .groupby("sbbolUserId") \
        .agg(f.sum(f.col("isClickMenu").cast("int")).alias('sumClickMenu'),
             f.sum(f.col("isStoryImpression").cast("int")).alias('sumStoryImpression'),
             f.sum(f.col("isCreateRequest").cast("int")).alias('sumCreateRequest'),
             f.sum(f.col("isClickApplication").cast("int")).alias('sumClickApplication'),
             f.sum(f.col("isCreateNewApplication").cast("int")).alias('sumCreateNewApplication'),
             f.sum(f.col("isDropDownMenuClick").cast("int")).alias('sumDropDownMenuClick'),
             f.sum(f.col("isLoadApplicationForm").cast("int")).alias('sumLoadApplicationForm'),
             f.sum(f.col("isCarouselClick").cast("int")).alias('sumCarouselClick'),
             f.sum(f.col("isRegistrationRequest").cast("int")).alias('sumRegistrationRequest'),
             f.sum(f.col("isFormFillError").cast("int")).alias('sumFormFillError'),
             f.sum(f.col("isSendApplication").cast("int")).alias('sumSendApplication'),
             f.sum(f.col("isGetSMSCode").cast("int")).alias('sumGetSMSCode'),
             f.sum(f.col("isSendSMSCode").cast("int")).alias('sumSendSMSCode'),
             f.sum(f.col("isSMSError").cast("int")).alias('sumSMSError'),
             f.sum(f.col("isSMSSenOK").cast("int")).alias('sumSMSSenOK'),
             f.min(f.col('sessionStartTime')).cast(TimestampType()).alias('minSessionStartTime'),
             f.max(f.col('sessionStartTime')).cast(TimestampType()).alias('maxSessionStartTime'),
             f.first("sessiondate").alias("sessiondate")
             ) \
        .fillna({'sumClickMenu': 0,
                 'sumStoryImpression': 0,
                 'sumCreateRequest': 0,
                 "sumClickApplication": 0,
                 "sumCreateNewApplication": 0,
                 "sumDropDownMenuClick": 0,
                 "sumLoadApplicationForm": 0,
                 "sumCarouselClick": 0,
                 "sumRegistrationRequest": 0,
                 "sumFormFillError": 0,
                 "sumSendApplication": 0,
                 "sumGetSMSCode": 0,
                 "sumSendSMSCode": 0,
                 "sumSMSError": 0,
                 "sumSMSSenOK": 0
                })

    # Calculate funnel rates
    print_and_log("### Formatting funnel rates")
    df_sbbol_user_funnels = df_sbbol_user \
        .withColumn('funnel_rate',
                    f.when(f.col('sumSMSSenOK') > 0,
                          f.format_number(f.lit(13) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumSMSError') > 0,
                          f.format_number(f.lit(12) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumSendSMSCode') > 0,
                          f.format_number(f.lit(12) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumGetSMSCode') > 0,
                          f.format_number(f.lit(11) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumSendApplication') > 0,
                          f.format_number(f.lit(10) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumFormFillError') > 0,
                          f.format_number(f.lit(9) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumRegistrationRequest') > 0,
                          f.format_number(f.lit(9) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumCarouselClick') > 0,
                          f.format_number(f.lit(8) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumLoadApplicationForm') > 0,
                          f.format_number(f.lit(7) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumDropDownMenuClick') > 0,
                          f.format_number(f.lit(6) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumCreateNewApplication') > 0,
                          f.format_number(f.lit(5) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumClickApplication') > 0,
                          f.format_number(f.lit(4) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumCreateRequest') > 0,
                          f.format_number(f.lit(3) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumStoryImpression') > 0,
                          f.format_number(f.lit(2) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sumClickMenu') > 0,
                          f.format_number(f.lit(1) / NUM_OF_STEPS, 2)) \
                    .otherwise(0.0)
                   )

    return df_sbbol_user_funnels


def offers_priority(df_sbbol_user_inn, product_cd_name=PRODUCT_CD_NAME):
    print_and_log("### Working with OFFER_PRIORITY...")

    offer_priority = hive.table("{}.OFFER_PRIORITY".format(CONN_SCHEMA)) \
                           .select('INN', 'CRM_ID', 'PRODUCT_CD', 'START_DT', 'END_DT')

    offer_priority = offer_priority.select([col.lower() for col in offer_priority.columns])
    offer_priority_product = offer_priority.filter("PRODUCT_CD = '{}'".format(product_cd_name))
    count = offer_priority_product.count()  # всего приоритетных предложений по продукту

    # No offers for product, skip
    if count == 0:
        return None

    conditions = (df_sbbol_user_inn.cu_inn == offer_priority_product.inn) & \
                 ((offer_priority_product.start_dt >= df_sbbol_user_inn._6monthoffset) &
                  (offer_priority_product.end_dt >= pd.datetime(2022, 1, 1)))

    df_sbbol_user_offer_priority = df_sbbol_user_inn \
        .join(offer_priority_product,
              on=conditions,
              how='inner') \
        .select("cu_inn", "_crm_id", "crm_id", "sbbolUserId", "product_cd") \
        .withColumn("offer_priority", f.lit(True))

    df_sbbol_user_offer_priority = df_sbbol_user_offer_priority.withColumn('crm_id_nvl',f.coalesce('_crm_id','crm_id'))\
                                                               .withColumnRenamed('crm_id_nvl','id_crm')
    df_sbbol_user_offer_priority = df_sbbol_user_offer_priority.drop('crm_id','_crm_id')


    df_sbbol_user_offer_priority_unique = df_sbbol_user_offer_priority \
        .groupby("cu_inn", "id_crm", "sbbolUserId", "product_cd") \
        .agg(f.sum(f.col("offer_priority").cast("int")).alias("sum_offer_priority"))

    return df_sbbol_user_offer_priority_unique


def get_offers_nontop(df_sbbol_user_inn, product_cd_name=PRODUCT_CD_NAME):
    print_and_log("### Working with MA_MMB_OFFER_NONTOP...")

    mmb_offer_nontop = hive.table("{}.MA_MMB_OFFER_NONTOP".format(CONN_SCHEMA))

    product_dict_sp = hive.sql("select * from {}.{}".format(CONN_SCHEMA, MA_PRODUCT_DICT))
    # product_dict_sp = product_dict_sp.select([col.lower() for col in product_dict_sp.columns])
    product_dict_sp = product_dict_sp.filter("PRODUCT_CD_MMB is not Null").select('ID', f.col('PRODUCT_CD_MMB').alias('PRODUCT_CD'))
    product_dict_pd = product_dict_sp.collect()
    product_dict_nm = {row.ID: row.PRODUCT_CD for row in product_dict_pd}
    mmb_offer_nontop = mmb_offer_nontop.withColumn('PRODUCT_CD', getProdCDFromId(product_dict_nm)('PRODUCT_ID'))

    mmb_offer_nontop = mmb_offer_nontop.select([col.lower() for col in mmb_offer_nontop.columns])
    mmb_offer_nontop_product = mmb_offer_nontop.filter("PRODUCT_CD = '{}'".format(product_cd_name))
    count = mmb_offer_nontop_product.count()  # всего приоритетных предложений по продукту

    # No nontop offers for product, skip
    if count == 0:
        return None

    conditions = (df_sbbol_user_inn.cu_inn == mmb_offer_nontop_product.inn) & \
                 (mmb_offer_nontop_product.start_dttm >= df_sbbol_user_inn._6monthoffset)

    df_sbbol_user_offer_nontop = df_sbbol_user_inn \
        .join(mmb_offer_nontop_product,
              on=conditions,
              how='inner') \
        .select("cu_inn", "_crm_id", "crm_id", "sbbolUserId", "product_cd") \
        .withColumn("offer_nontop", f.lit(True))

    df_sbbol_user_offer_nontop = df_sbbol_user_offer_nontop.withColumn('crm_id_nvl',f.coalesce('_crm_id','crm_id'))\
                                                               .withColumnRenamed('crm_id_nvl','id_crm')
    df_sbbol_user_offer_nontop = df_sbbol_user_offer_nontop.drop('crm_id','_crm_id')


    df_sbbol_user_offer_nontop_unique = df_sbbol_user_offer_nontop \
        .groupby("cu_inn", "id_crm", "sbbolUserId", "product_cd") \
        .agg(f.sum(f.col("offer_nontop").cast("int")).alias("sum_offer_nontop"))

    return df_sbbol_user_offer_nontop_unique


def join_offers_df(df_sbbol_user_offer_priority, df_sbbol_user_offer_nontop):
    print_and_log("### Join OFFER_PRIORITY with MA_MMB_OFFER_NONTOP...")

    if df_sbbol_user_offer_priority is None and df_sbbol_user_offer_nontop is None:
        df_sbbol_user_offers = None
    elif df_sbbol_user_offer_priority is None:
        df_sbbol_user_offers = df_sbbol_user_offer_nontop.withColumn('sum_offer_priority', f.lit(0))
    elif df_sbbol_user_offer_nontop is None:
        df_sbbol_user_offers = df_sbbol_user_offer_priority.withColumn('sum_offer_nontop', f.lit(0))
    else:
        df_sbbol_user_offers = df_sbbol_user_offer_priority \
            .join(df_sbbol_user_offer_nontop,
                  on=["cu_inn", "id_crm", "sbbolUserId", "product_cd"],
                  how='outer') \
            .fillna({'sum_offer_priority': 0, 'sum_offer_nontop': 0})
    return df_sbbol_user_offers


def join_offers_inn(df_sbbol_user_inn, df_sbbol_user_offers, product_cd_name=PRODUCT_CD_NAME):
    print_and_log("### Join offers with sbbolUserId-INN datamart...")

    if df_sbbol_user_offers is None:
        df_sbbol_user_interest_offer = df_sbbol_user_inn \
            .withColumn('sum_offer_priority', f.lit(0)) \
            .withColumn('sum_offer_nontop', f.lit(0)) \
            .withColumn('id_crm', f.lit(None).cast(StringType()))
    else:
        df_sbbol_user_interest_offer = df_sbbol_user_inn \
            .join(df_sbbol_user_offers,
                  on=["cu_inn", "sbbolUserId"],
                  how="left_outer") \
            .fillna({'sum_offer_priority': 0, 'sum_offer_nontop': 0})

    df_sbbol_user_interest_offer = df_sbbol_user_interest_offer \
        .drop("product_cd") \
        .withColumn("product_cd", f.lit(product_cd_name)) \
        .drop("replicationguid", "cu_id_sbbol",
              "user_id", "cu_kpp", "cu_okpo", "locked", "load_dt")

    df_sbbol_user_interest_offer = df_sbbol_user_interest_offer.withColumn('crm_id_nvl',f.coalesce('_crm_id','id_crm'))
    df_sbbol_user_interest_offer = df_sbbol_user_interest_offer.drop('_crm_id','id_crm')
    df_sbbol_user_interest_offer = df_sbbol_user_interest_offer.withColumnRenamed('crm_id_nvl','id_crm')


    return df_sbbol_user_interest_offer


def check_product_deals(df_sbbol_user_interest_offer, product_like, conn_schema):
    print_and_log("### Check if user already has this product")

    ma_deals = hive.sql("select * from {schema}.{tbl}".format(schema=conn_schema, tbl=MA_DEALS_TABLE_NAME))
    ma_deals = ma_deals.select(*[col.lower() for col in ma_deals.columns])
    ma_deals_product = ma_deals \
        .filter(f.col('appl_stage_name').like("%Закрыта/Заключена%") &
                f.col('product_norm').like('%{}%'.format(product_like)) &
                f.isnull(f.col('close_reason_type_name'))) \
        .select(f.col('inn'),
                f.col('product_norm').alias('madeal_product_norm'),
                f.col('prod_type_name').alias('madeal_prod_type_name'),
                f.col('appl_core_txt').alias('madeal_appl_core_txt'),
                f.col('complete_dt').alias('madeal_complete_dt'))

    conditions = (df_sbbol_user_interest_offer.cu_inn == ma_deals_product.inn) & \
                 ((ma_deals_product.madeal_complete_dt >= df_sbbol_user_interest_offer._6monthoffset) &
                  (ma_deals_product.madeal_complete_dt <= df_sbbol_user_interest_offer.maxSessionStartTime))

    res = df_sbbol_user_interest_offer \
        .join(ma_deals_product, on=conditions, how='left_outer') \
        .withColumn('hasProduct', f.when(~f.isnull('inn'), 1).otherwise(0)) \
        .drop('inn')

    return res

def save_final_table(conn_schema, out_table_name, tmp_tbl):
    print_and_log("### RUN dynamic update of {} table".format(out_table_name))

    hive.setConf("hive.exec.dynamic.partition", "true")
    hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    hive.sql(
        """
            insert into table {schema}.{tbl}
            partition(sessiondate)
            select {tmp_tbl}.* from {tmp_tbl}
            distribute by sessiondate
        """.format(schema=conn_schema, tbl=out_table_name, tmp_tbl=tmp_tbl))
    print("SUCCESS INSERT")
    return None



if __name__ == '__main__':

    sp = spark(schema='sklod_external_google_analytics',
               queue = 'ektov1-av_ca-sbrf-ru',
               process_label="NRT_DLG",
               numofinstances=12,
               numofcores=8,
               dynamic_alloc=False,
               kerberos_auth=True
              )
    print(sp.sc.version)
    hive = sp.sql

    # Get last partition
    print_and_log("### Checking new data by date")
    with open(fPathtoScenariosDict,'r') as fobj:
        scenariosCtl = json.load(fobj)

    currSessionDate = scenariosCtl[SCENARIO_ID]

    partlst = hive.sql("show partitions {}.{}".format(NRT_SCHEMA, NRT_EVENTS)).orderBy(f.col("partition").desc()).collect()
    parts = [part['partition'].split('timestampcolumn=')[-1] for part in partlst]
    indx = parts.index(currSessionDate)
    unproc_parts = sorted(parts[:indx+1])

    for parquet in unproc_parts:

        with open(fPathtoGASliceDict,'r') as fobj:
            last_date_dict = json.load(fobj)
        # cnt = last_date_dict['COUNT']
        ctl = last_date_dict[SCENARIO_ID]["CTL"]
        start_date = last_date_dict[SCENARIO_ID]["LAST_SESS_DATE"]

        print_and_log("### Processing parquet: {}".format(parquet))
        optbinelem = re.compile('java.lang.ClassCastException: optional binary .* is not a group')
        try:
            df_sbbol, maxSessionDate, max_sessionStartTime, cnt = get_df_sbbol(start_date=start_date, ref_ts = ctl, parq_id = parquet)
        except Exception as ex:
            parse = optbinelem.findall(str(ex))
            if len(parse)!=0:
                print_and_log(parse[0])
            else:
                print_and_log(str(ex)[:500])
            continue

        print_and_log("### VISIT->number of new records for treatment is: {}".format(cnt))

        if (cnt == 0) or (cnt == None):
            print_and_log("### Nothing to insert. BYE!!!")
            # stop_spark(sp)
            # sing.__del__()
            continue
            # exit()

        # Group by user, make funnels and filter by them
        df_sbbol_user_funnels = make_user_funnels(df_sbbol)


        cid_inn_dict = hive.table("{schema}.{table_name}".format(schema=CONN_SCHEMA, table_name=CID_INN_TABLE_NAME)) \
            .select('SBBOLUSERID', 'commonSegmentoUID', 'CU_INN', f.col('crm_id').alias('id_crm')) \
            .distinct()
        cid_inn_dict = cid_inn_dict.select([col.lower() for col in cid_inn_dict.columns])

        df_sbbol_user_inn = df_sbbol_user_funnels.join(cid_inn_dict,
                                                       on=["sbbolUserId"],
                                                       how='inner')

        # Add 6month range
        print_and_log("### Add column corresponding to 6 month offset from `sessionStartTime`")

        dttm_offset = f.from_unixtime(f.unix_timestamp(f.col('maxSessionStartTime')) - 6 * 30 * 24 * 60 * 60,
                                      'yyyy-MM-dd HH:mm:ss').cast(TimestampType())
        df_sbbol_user_inn = df_sbbol_user_inn.withColumn('_6monthoffset', dttm_offset)

        res = check_product_deals(df_sbbol_user_inn, PRODUCT_LIKE, CONN_SCHEMA)

        cols = ['cu_inn',
                'sbbolUserId',
                'commonSegmentoUID',
                'id_crm',
                'scenario_id',
                'channel',
                'product_cd',
                'madeal_product_norm',
                'madeal_prod_type_name',
                'madeal_appl_core_txt',
                'madeal_complete_dt',
                'hasProduct',
                'essense',
                'returncnt',
                'numberOfSteps',
                'funnel_rate',
        #         'sum_offer_priority',
        #         'sum_offer_nontop',
                'minSessionStartTime',
                'maxSessionStartTime',
                'load_dt',
                'sessiondate']

        currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d %H:%M:%S')

        sbbol_final = res \
            .withColumn("product_cd", f.lit(PRODUCT_CD_NAME)) \
            .withColumn("essense", essense_udf(f.lit(CHANNEL), f.lit("'эквайринг'")))\
            .withColumn("load_dt", f.lit(f.lit(currdate).cast(TimestampType()))) \
            .withColumn("scenario_id", f.lit(SCENARIO_ID)) \
            .withColumn("channel", f.lit(CHANNEL)) \
            .withColumn("returncnt", f.col('sumClickMenu')+f.col('sumStoryImpression')) \
            .withColumn("numberOfSteps", f.lit(NUM_OF_STEPS)) \
            .select(*cols) \
            .withColumnRenamed("cu_inn", "inn")\
            .withColumnRenamed("product_cd", "id_product")


        sbbol_final_cnt = sbbol_final.count()
        print_and_log("### Final table size: {}".format(sbbol_final_cnt))

        if sbbol_final_cnt != 0:
            spStopCheck = sp.sc._jsc.sc().isStopped()
            if not spStopCheck:
                print("### Spark context is still alive!")
            else:
                sp = spark(schema=CONN_SCHEMA)
                hive = sp.sql
            # Save
            tmp_tbl = 'tmp_inn_' + PRODUCT_CD_NAME
            sbbol_final.registerTempTable(tmp_tbl)
            save_final_table(CONN_SCHEMA, OUT_TABLE_NAME, tmp_tbl)
        else:
            print_and_log("### Nothing to insert into target datamart... Bye!!!")

        print_and_log(">" * 90)
        print_and_log("### max value of sessionDate is: {}".format(maxSessionDate))
        print_and_log("### max value of sessionStartTime is: {}".format(max_sessionStartTime))
        print_and_log("### number of processed records is: {}".format(cnt))
        print_and_log("### number of final records is: {}".format(sbbol_final_cnt))
        print_and_log(">" * 90)

        print_and_log("### Updating JSONs files")

        last_date_dict[SCENARIO_ID]["LAST_SESS_DATE"] = maxSessionDate
        last_date_dict[SCENARIO_ID]["COUNT"] = sbbol_final_cnt
        last_date_dict[SCENARIO_ID]["CTL"] = max_sessionStartTime

        # Get last partition
        with open(fPathtoScenariosDict,'r') as fobj:
            scenariosCtl = json.load(fobj)

        # currSessionDate = scenariosCtl[SCENARIO_ID]

        if maxSessionDate is not None:
            scenariosCtl[SCENARIO_ID] = maxSessionDate
            with open(fPathtoScenariosDict,'w') as fobj:
                json.dump(scenariosCtl, fobj, indent=4, sort_keys=True)

            with open(fPathtoGASliceDict,'w') as fobj:
                json.dump(last_date_dict, fobj, indent=4, sort_keys=True)

    stop_spark(sp)

    sing.__del__()
