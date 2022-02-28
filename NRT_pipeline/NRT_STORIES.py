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
sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/python35-libs/lib/python3.5/site-packages/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))

# sys.path.insert(0, '/home/{}/notebooks/ecom_model/src/'.format(curruser))
# sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
# sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
# sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))

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
os.chdir('/opt/workspace/{}/notebooks/NRT_RUN/'.format(curruser))

logging.basicConfig(filename='./logs/__upd_NRTSBBOLBusinessCard__.log',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------------ #
NUM_OF_STEPS = 2  # Funnel
PRODUCT_CD_NAME = "STORIES"
# PRODUCT_LIKE = "бизнес%карт"
SCENARIO_ID = "NRT_STORIES"
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

@udf(StringType())
def extract_product(label):
    if label is not None:
        pattern = re.compile(r'product: ([\d\w]+)[\,\]]+')
        out = pattern.findall(label.lower())
        if len(out) > 0:
            return out[0]
        else:
            return None
    else:
        return None

# @udf(StringType())
def match_product(product_dict_nm):
    def get_product(product_norm, product_dict_nm):
        if product_norm is None:
            return None

        for product_nm in product_dict_nm:
            if product_nm in product_norm:
                return product_dict_nm[product_nm]
            else:
                return None

    return f.udf(lambda x: get_product(x,product_dict_nm))


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

    print_and_log("### Check if story has been shown on user screen")
    df_sbbol_show = df_sbbol \
        .withColumn('story_show',  (f.col("eventCategory").like('%[operations]: offers%')) & \
                                   (f.col("eventAction") == 'impression-card'))

    print_and_log("### Check if user has clicked on the story")
    df_sbbol_click = df_sbbol_show \
        .withColumn('is_click',  (f.col("eventCategory").like('%[operations]: offers%')) & \
                                 (f.col("eventAction") == 'impression-card-short'))

    print_and_log("### Check if user has opened the story landing")
    df_sbbol_open = df_sbbol_click \
        .withColumn('story_open',   (f.col("eventCategory").like('%[operations]: offers%')) & \
                                    (f.col("eventAction") == 'open landing'))

    # Проверка на клик по магазину продуктов
    # print_and_log("### Check if shop click was made")
    # df_sbbol_click = df_sbbol \
    #     .withColumn('is_click', (f.col('eventCategory') == '[std]: shop') & \
    #                 (f.col("eventLabel").like('%offercardsbbol3%')))

    # Проверка на целевое действие по магазину продуктов
    print_and_log("### Check if checkout was made")
    df_sbbol_checkout = df_sbbol_open \
        .withColumn('is_checkout', f.col("eventAction").like('%checkout%'))

    # Извлекаем название продукта из eventLabel
    # TODO: добавить условие, достаем продукт только для is_click и is_checkout (прибавка скорости)
    print_and_log("### Get product name from eventLabel")
    df_sbbol_product = df_sbbol_checkout \
        .withColumn('product', extract_product('eventLabel'))

    # Проверка на статус активации продукта
    # TODO: проверять только если извлечен продукт
    print_and_log("### Step three: application (operation)")
    df_sbbol_status = df_sbbol_product \
        .withColumn('status_activated', f.col("eventLabel").like("%status: activated%"))

    df_sbbol_final = df_sbbol_status \
        .fillna({'is_checkout': False,
                 'is_click': False,
                 'story_show': False,
                 'story_open': False,
                 'status_activated': False})


    print_and_log("### Formatting groupby user")
    df_shop_user = df_sbbol_final \
        .groupby("sbbolUserId", "product") \
        .agg(f.sum(f.col("is_click").cast(IntegerType())).alias("sum_click"),
             f.sum(f.col("story_open").cast(IntegerType())).alias("sum_story_open"),
             f.sum(f.col("is_checkout").cast(IntegerType())).alias("sum_checkout"),
             f.max("status_activated").alias("status_activated"),
             f.min(f.col('sessionStartTime')).cast(TimestampType()).alias('minSessionStartTime'),
             f.max(f.col('sessionStartTime')).cast(TimestampType()).alias('maxSessionStartTime'),
             f.first("sessiondate").alias("sessiondate")
             )

    dct_sum_cols = dict([(col,0) for col in df_shop_user.columns if col.startswith('sum_')])
    df_shop_user = df_shop_user.fillna(dct_sum_cols)

    # Calculate funnel rates
    print_and_log("### Formatting funnel rates")
    df_shop_user_funnels = df_shop_user \
        .withColumn('funnel_rate',
                    f.when(f.col('status_activated').cast(IntegerType()) == 1,
                           f.format_number(f.lit(2) / NUM_OF_STEPS, 2)) \
                    .when(f.col('sum_checkout') > 0,
                          f.format_number(f.lit(2) / NUM_OF_STEPS, 2))
                    .when(f.col('sum_click') > 0,
                          f.format_number(f.lit(1) / NUM_OF_STEPS, 2))
                    .when(f.col('sum_story_open') > 0,
                          f.format_number(f.lit(1) / NUM_OF_STEPS, 2))
                    .otherwise(0.0))


    print_and_log("-" * 90)
    return df_shop_user_funnels


def offers_priority(df_sbbol_user_inn, product_cd_name=PRODUCT_CD_NAME):

    print_and_log("### Working with OFFER_PRIORITY...")

    offer_priority = hive.table("{}.OFFER_PRIORITY".format(CONN_SCHEMA))
    offer_priority = offer_priority.select([col.lower() for col in offer_priority.columns])
    offer_priority = offer_priority\
        .select('inn', 'crm_id', 'product_cd', 'start_dt', 'end_dt') \
        .withColumnRenamed("inn", "offer_inn") \
        .withColumnRenamed("product_cd", "offer_product_cd")

    conditions = (df_sbbol_user_inn.cu_inn == offer_priority.offer_inn) & \
                 (df_sbbol_user_inn.product_cd == offer_priority.offer_product_cd) & \
                 ((offer_priority.start_dt >= df_sbbol_user_inn._6monthoffset) &
                  (offer_priority.end_dt >= pd.datetime(2022, 1, 1)))

    df_shop_user_inn_product_offer_priority = df_sbbol_user_inn \
        .join(offer_priority,
              on=conditions,
              how='inner') \
        .select("cu_inn", "_crm_id", "crm_id", "sbbolUserId", "product_cd") \
        .withColumn("offer_priority", f.lit(True))

    df_shop_user_inn_product_offer_priority = \
    df_shop_user_inn_product_offer_priority.withColumn('crm_id_nvl',f.coalesce('_crm_id','crm_id'))\
                                           .withColumnRenamed('crm_id_nvl','id_crm')
    df_shop_user_inn_product_offer_priority = df_shop_user_inn_product_offer_priority.drop('crm_id','_crm_id')


    if len(df_shop_user_inn_product_offer_priority.take(1)) != 0:
        df_shop_user_inn_product_offer_priority_unique = df_shop_user_inn_product_offer_priority \
                            .groupby("cu_inn", "id_crm", "sbbolUserId", "product_cd") \
                            .agg(f.sum(f.col("offer_priority").cast("int")).alias("sum_offer_priority"))
    else:
        df_shop_user_inn_product_offer_priority_unique = None

    return df_shop_user_inn_product_offer_priority_unique


def get_offers_nontop(df_sbbol_user_inn, product_cd_name=PRODUCT_CD_NAME):
    print_and_log("### Working with MA_MMB_OFFER_NONTOP...")

    offer_nontop = hive.table("{}.MA_MMB_OFFER_NONTOP".format(CONN_SCHEMA))

    product_dict_sp = hive.sql("select * from {}.{}".format(CONN_SCHEMA, MA_PRODUCT_DICT))
    # product_dict_sp = product_dict_sp.select([col.lower() for col in product_dict_sp.columns])
    product_dict_sp = product_dict_sp.filter("PRODUCT_CD_MMB is not Null").select('ID', f.col('PRODUCT_CD_MMB').alias('PRODUCT_CD'))
    product_dict_pd = product_dict_sp.collect()
    product_dict_nm = {row.ID: row.PRODUCT_CD for row in product_dict_pd}
    offer_nontop    = offer_nontop.withColumn('PRODUCT_CD', getProdCDFromId(product_dict_nm)('PRODUCT_ID'))

    offer_nontop = offer_nontop.select([col.lower() for col in offer_nontop.columns])
    offer_nontop = offer_nontop\
        .select('inn', 'crm_id', 'product_cd', 'start_dttm') \
        .withColumnRenamed("inn", "offer_inn") \
        .withColumnRenamed("product_cd", "offer_product_cd")

    conditions = (df_sbbol_user_inn.cu_inn == offer_nontop.offer_inn) & \
                 (df_sbbol_user_inn.product_cd == offer_nontop.offer_product_cd) & \
                 (df_sbbol_user_inn._6monthoffset <= offer_nontop.start_dttm)

    df_shop_user_inn_product_offer_nontop = df_sbbol_user_inn \
        .join(offer_nontop,
              on=conditions,
              how='inner') \
        .select("cu_inn", "_crm_id", "crm_id", "sbbolUserId", "product_cd") \
        .withColumn("offer_nontop", f.lit(True))

    df_shop_user_inn_product_offer_nontop = \
    df_shop_user_inn_product_offer_nontop.withColumn('crm_id_nvl',f.coalesce('_crm_id','crm_id'))\
                                         .withColumnRenamed('crm_id_nvl','id_crm')
    df_shop_user_inn_product_offer_nontop = df_shop_user_inn_product_offer_nontop.drop('crm_id','_crm_id')


    if len(df_shop_user_inn_product_offer_nontop.take(1)) != 0:
        df_shop_user_inn_product_offer_nontop_unique = df_shop_user_inn_product_offer_nontop \
                    .groupby("cu_inn", "id_crm", "sbbolUserId", "product_cd") \
                    .agg(f.sum(f.col("offer_nontop").cast("int")).alias("sum_offer_nontop"))
    else:
        df_shop_user_inn_product_offer_nontop_unique = None

    return df_shop_user_inn_product_offer_nontop_unique


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


def check_product_deals(df_sbbol_user_interest_offer, product_dict_sp, conn_schema):
    print_and_log("### Check if user already has this product")

    # if df_sbbol_user_interest_offer is not None:
    product_dict_pd = product_dict_sp.collect()
    product_dict_nm = {row.product_short_nm: row.product_cd for row in product_dict_pd}

    ma_deals = hive.sql("select * from {schema}.{tbl}".format(schema=conn_schema, tbl=MA_DEALS_TABLE_NAME))
    ma_deals_product = ma_deals \
        .filter(f.col('appl_stage_name').like("%Закрыта/Заключена%")) \
        .filter(f.isnull(f.col('close_reason_type_name'))) \
        .withColumn("extracted_product_cd", match_product(product_dict_nm)(f.col("product_norm"))) \
        .select(f.col('inn').alias("deal_inn"),
                f.col("extracted_product_cd"),
                f.col('product_norm').alias('madeal_product_norm'),
                f.col('prod_type_name').alias('madeal_prod_type_name'),
                f.col('appl_core_txt').alias('madeal_appl_core_txt'),
                f.col('complete_dt').alias('madeal_complete_dt'))

    conditions = (df_sbbol_user_interest_offer.cu_inn == ma_deals_product.deal_inn) & \
                 (df_sbbol_user_interest_offer.product_cd == ma_deals_product.extracted_product_cd) & \
                 ((df_sbbol_user_interest_offer._6monthoffset <= ma_deals_product.madeal_complete_dt) &
                  (df_sbbol_user_interest_offer.maxSessionStartTime <= ma_deals_product.madeal_complete_dt))

    if len(ma_deals_product.take(1)) != 0:
        res = df_sbbol_user_interest_offer \
            .join(ma_deals_product, on=conditions, how='left_outer') \
            .withColumn('hasProduct', f.when(~f.isnull('deal_inn'), 1).otherwise(0)) \
            .drop('deal_inn', 'extracted_product_cd')
    else:
        print("Nothing to join by relevant `PRODUCT_CD`...")
        res = res \
                .withColumn('hasProduct',f.lit(0))\
                .withColumn('madeal_product_norm',f.lit(None))\
                .withColumn('madeal_prod_type_name',f.lit(None))\
                .withColumn('madeal_appl_core_txt',f.lit(None))\
                .withColumn('madeal_complete_dt',f.lit(None))

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
               process_label="anthony_",
               numofinstances=12,
               numofcores=8,
               dynamic_alloc=False,
               kerberos_auth=False
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

        product_dict = hive.table("{}.{}".format(CONN_SCHEMA, MA_PRODUCT_DICT))
        product_dict = product_dict.select([col.lower() for col in product_dict.columns])
        product_dict = product_dict.filter("product_cd_mmb is not Null")
        product_dict = product_dict.withColumnRenamed('product_cd_mmb','product_cd')

        df_shop_user_inn_product = df_sbbol_user_inn \
            .join(product_dict,
                  on=f.lower(df_sbbol_user_inn.product) == f.lower(product_dict.product_cd_asup),
                  how="inner") \
            .drop("crm_product_id", "product_cd_asup")


        res = check_product_deals(df_shop_user_inn_product, product_dict, CONN_SCHEMA)

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
            .withColumn("essense", essense_udf(f.lit(CHANNEL), f.lit("PRODUCT_SHORT_NM")))\
            .withColumn("load_dt", f.lit(f.lit(currdate).cast(TimestampType()))) \
            .withColumn("scenario_id", f.lit(SCENARIO_ID)) \
            .withColumn("channel", f.lit(CHANNEL)) \
            .withColumn("returncnt", f.col('sum_click') + f.col("sum_story_open")) \
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

    # sing.__del__()
