#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)


from lib.tools import *

from lib.CRMDictProds import AllParseAndRegExp
from transliterate import translit
from urllib.parse import urlparse, unquote


class GaSiteAllProds:
    def __init__(self):
        self.sing = tendo.singleton.SingleInstance()
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.CHANNEL = "SITE"
        self.SCENARIO_ID = "SITEALLPRODS_01"
        self.script_name = "GA_SITE_ALL_PRODS"

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.REFDATE = "2019-09-01"

        self.load_slice_json()
        self.load_sources_json()
        self.load_scenarios_json()

        if self.slice_empty() or self.today_was_launch():
            self.close()
            sys.exit()

        self.start_spark()

        self.parse = AllParseAndRegExp()

        log("# __init__ : begin", self.logger)


    ############################## Run method ##############################


    @class_method_logger
    def run(self):
        visit_part = self.load_visit_part(min_session_date=self.scenarios_last_date)
        
        visit_prod_gr = self.add_prod_gr(visit_part)
        visit_prod_gr.cache()

        visit_hitTime = self.add_hitTime(visit_prod_gr)

        visit_row_number = self.add_row_number(visit_hitTime)
        visit_agg_session = self.add_agg_session(visit_row_number)

        visit_interm = self.save_intermediate_res(visit_agg_session)

        visit_first_page_hit = self.add_first_page_hit(visit_interm)
        visit_trg_sess = self.add_trg_sess_between(visit_first_page_hit)

        visit_hit = self.add_hit_columns(visit_trg_sess)
        visit_eventLabel_cols = self.add_eventLabel_columns(visit_hit)

        visit_load_dt = self.add_load_dt(visit_eventLabel_cols)
    
        self.create_final_table(visit_load_dt)
        
        self.save_sources_json()
        self.save_scenarios_json()


    ############################## Load visit ##############################


    @class_method_logger
    def load_visit(self, schema, table):
        sql = \
                '''
                  select
                  cid,
                  sbbolUserId,
                  sessionId,
                  visitNumber,
                  hitType,
                  hitPagePath,
                  hitPageTitle,
                  sessionStartTime,
                  timestamp(sessionDate),
                  hitNumber,
                  hitTime,
                  eventCategory,
                  eventAction,
                  eventLabel,
                  deviceIsMobile,
                  deviceMobileDeviceModel,
                  deviceDeviceCategory,
                  geoRegion,
                  geoCity,
                  ctl_loading
                from {schema}.{table}
                '''.format(schema=schema, table=table)

        visit = self.hive.sql(sql)
        return visit


    @class_method_logger
    def load_visit_part(self, min_session_date, max_session_date : str = None):
        if min_session_date is None:
            min_session_date = self.REFDATE

        visit = self.load_visit(SBX_TEAM_DIGITCAMP, TMP_VISIT_PART) \
                    .filter("hitPageHostName like '%www.sberbank.ru%'") \
                    .filter("hitPagePath like '%/s_m_business/%'")

        visit_part = visit.filter("timestamp(sessionDate) > timestamp('{}')".format(min_session_date))

        if max_session_date is not None:
            visit_part = visit_part.filter("timestamp(sessionDate) <= timestamp('{}')".format(max_session_date))
        return visit_part


    ############################## Add columns ##############################


    @class_method_logger
    def add_prod_gr(self, df):
        df_prod_gr = df.withColumn("url_domain", get_url_domain("hitPagePath")) \
                        .withColumn("url_path", get_url_path("hitPagePath")) \
                        .withColumn('prod_gr', checkHitPageTitle_udf(self.parse.dctofrefwords)('hitPageTitle', 'url_path')) \
                        .filter("prod_gr is not Null")


        return df_prod_gr


    @class_method_logger
    def add_hitTime(self, df):
        df_hitTime = df.withColumn('hitTimefromStartTime',
                                    f.from_unixtime(f.col('sessionStartTime') + f.col('hitTime') / 1000,
                                                     format='yyyy-MM-dd HH:mm:ss').cast(stypes.TimestampType()))

        return df_hitTime


    @class_method_logger
    def add_row_number(self, df):
        w = Window.orderBy('hitTimefromStartTime')
        df_row_number = df.orderBy(f.col('hitTimefromStartTime').asc()) \
                            .withColumn('row_num', f.row_number().over(w)) \

        return df_row_number


    @class_method_logger
    def add_agg_session(self, df):
        w=Window.orderBy('cid','prod_gr','url_path','row_num')
        df_agg_session = df.orderBy(f.col('hitTimefromStartTime').asc()) \
                            .withColumn('agg_session', f.when((f.lead(f.col('hitType'), count=1).over(w) == 'PAGE') & 
                                                                   (f.lead(f.col('hitType'), count=2).over(w) == 'PAGE') & 
                                                                   (f.col('hitType') != 'EVENT'),
                                                                f.lead(f.col('visitNumber'), count=2).over(w))\
                                                        .when((f.lead(f.col('hitType'), count=1).over(w) == 'PAGE') & \
                                                                  (f.lead(f.col('hitType'), count=2).over(w) != 'PAGE') & \
                                                                  (f.col('hitType') != 'EVENT'),
                                                               f.lead(f.col('visitNumber'), count=1).over(w))\
                                                        .otherwise(f.col('visitNumber')))\
                            .drop("row_num")
        return df_agg_session


    @class_method_logger
    def add_first_page_hit(self, df):
        w = Window.partitionBy('cid','prod_gr','url_path','agg_session')

        df_page_hit = df.withColumn('firstPageHit', f.min(f.when(f.col('hitType')=='PAGE',
                                                                f.col('hitTimefromStartTime')) \
                                                           .otherwise(None)) \
                                                     .over(w))

        df_page_hit_url = df_page_hit.withColumn('firstPageHitUrl', f.first(f.when((f.col('hitType') == 'PAGE') & \
                                                                                        (f.col('hitTimefromStartTime') == f.col('firstPageHit')),
                                                                                    f.col('url_path')) \
                                                                            .otherwise(None)) \
                                                                    .over(w))
        return df_page_hit_url


    @class_method_logger
    def add_trg_sess_between(self, df):
        dt_format = 'yyyy-MM-dd HH:mm:ss'

        dttm_sess_left1  = f.from_unixtime(f.unix_timestamp(f.col('firstPageHit')) + 0*30*60, dt_format).cast(stypes.TimestampType())
        dttm_sess_right1 = f.from_unixtime(f.unix_timestamp(f.col('firstPageHit')) + 1*30*60, dt_format).cast(stypes.TimestampType())

        dttm_sess_left2  = f.from_unixtime(f.unix_timestamp(f.col('firstPageHit')) + 1*30*60, dt_format).cast(stypes.TimestampType())
        dttm_sess_right2 = f.from_unixtime(f.unix_timestamp(f.col('firstPageHit')) + 2*30*60, dt_format).cast(stypes.TimestampType())

        dttm_sess_left3  = f.from_unixtime(f.unix_timestamp(f.col('firstPageHit')) + 2*30*60, dt_format).cast(stypes.TimestampType())
        dttm_sess_right3 = f.from_unixtime(f.unix_timestamp(f.col('firstPageHit')) + 3*30*60, dt_format).cast(stypes.TimestampType())

        df_trg_sess = df.withColumn('trg_sess_between' , f.when((f.col('hitTimefromStartTime') >= dttm_sess_left1) & \
                                                                 (f.col('hitTimefromStartTime') < dttm_sess_right1), 1)\
                                                           .when((f.col('hitTimefromStartTime') >= dttm_sess_left2) & \
                                                                 (f.col('hitTimefromStartTime') < dttm_sess_right2), 2)\
                                                           .when((f.col('hitTimefromStartTime') >= dttm_sess_left3) & \
                                                                 (f.col('hitTimefromStartTime') < dttm_sess_right3), 3) \
                                                           .otherwise(0))

        return df_trg_sess


    @class_method_logger
    def add_hit_columns(self, df):
        w = Window.partitionBy('cid', 'prod_gr', 'url_path', 'agg_session','trg_sess_between')

        df_col = df.withColumn('lastPageHit',  f.max(f.when((f.col('hitType') == 'PAGE'), f.col('hitTimefromStartTime')) \
                                                      .otherwise(None)) \
                                              .over(w)) \
                .withColumn('firstEventHit', f.min(f.when(f.col('hitType') == 'EVENT', f.col('hitTimefromStartTime')) \
                                                      .otherwise(None)) \
                                              .over(w)) \
                .withColumn('lastEventHit',  f.max(f.when((f.col('hitType') == 'EVENT'), f.col('hitTimefromStartTime')) \
                                                    .otherwise(None)) \
                                              .over(w)) \
                .withColumn('countPageHit',  f.sum(f.when(f.col('hitType') == 'PAGE', 1) \
                                                    .otherwise(0)) \
                                              .over(w)) \
                .withColumn('countEventHit', f.sum(f.when(f.col('hitType') == 'EVENT', 1) \
                                                    .otherwise(0)) \
                                                .over(w))
        df_filt = df_col.filter('countPageHit > 0 and countEventHit > 0')
        return df_filt


    @class_method_logger
    def add_eventLabel_columns(self, df):
        for col, repattern in self.parse.reEventsDct['click_any']:
            transcol = translit(col, "ru", reversed=True)
            transcol = '_'.join(re.sub(r"[\'\-]+", '', transcol).split(' '))
            df = df.withColumn('{}'.format(transcol), 
                                f.regexp_extract(f.lower(f.col('eventLabel')), '({})'.format(repattern), 1))

        return df 


    @class_method_logger
    def add_load_dt(self, df):
        str_now = datetime.strftime(datetime.now(), format='%Y.%d.%m')
        dt_now = datetime.strptime(str_now,'%Y.%d.%m')
        df_load_dt = df.withColumn('load_dt',f.lit(dt_now).cast(stypes.TimestampType()))
        return df_load_dt


    ############################## Table functions ##############################


    @class_method_logger
    def create_table_from_df(self, schema, table, df):
        create_table_from_df(schema=schema, table=table, df=df, hive=self.hive)

    @class_method_logger
    def load_table(self, schema, table):
        return load_table(schema, table, self.hive)


    @class_method_logger
    def drop_table(self, schema, table):
        drop_table(schema, table, self.hive)


    ############################## Save table ##############################

    @class_method_logger
    def save_intermediate_res(self, df):
        self.drop_table(SBX_TEAM_DIGITCAMP, TMP_SITE_ALL_PRODS_INTERMEDIATE)
        self.create_table_from_df(SBX_TEAM_DIGITCAMP, TMP_SITE_ALL_PRODS_INTERMEDIATE, df)
        return self.load_table(SBX_TEAM_DIGITCAMP, TMP_SITE_ALL_PRODS_INTERMEDIATE)


    @class_method_logger
    def insert_into_final_table(self, df):
        df.registerTempTable(GA_SITE_ALL_PRODS)

        self.hive.setConf("hive.exec.dynamic.partition", "true")
        self.hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        self.hive.sql(
            """
                insert into table {schema}.{tbl}
                partition(ctl_loading)
                select {tbl}.* from {tbl}
                distribute by ctl_loading
            """.format(schema=SBX_TEAM_DIGITCAMP, tbl=GA_SITE_ALL_PRODS))
        

    @class_method_logger
    def create_final_table(self, df):
        self.drop_table(SBX_TEAM_DIGITCAMP, GA_SITE_ALL_PRODS)
        self.create_table_from_df(SBX_TEAM_DIGITCAMP, GA_SITE_ALL_PRODS, df)


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=2,
                        kerberos_auth=False,
                        numofcores=8,
                        numofinstances=20)

        self.hive = self.sp.sql


    ################################## Logging ##################################


    def init_logger(self):
        self.print_log = True

        try: 
            os.mkdir("logs/" + self.currdate)
        except:
            pass

        logging.basicConfig(filename='logs/{}/{}.log'.format(self.currdate, self.script_name),
                            level=logging.INFO,
                            format='%(asctime)s %(message)s')
        self.logger = logging.getLogger(__name__)

        log("="*54 + " {} ".format(self.currdate) + "="*54, self.logger)
        

    ############################## Load and save json ##############################


    @class_method_logger
    def load_slice_json(self):
        with open(SLICE_JSON_FILE, 'r') as f:
            self.slice_json = json.load(f)
        self.slice_count = self.slice_json['COUNT']
        self.slice_max_date = self.slice_json['MAX_DATE']


    @class_method_logger
    def load_sources_json(self):
        with open(SOURCES_JSON_FILE, 'r') as f:
            self.sources_json = json.load(f)
        self.last_launch = self.sources_json[self.script_name]


    @class_method_logger
    def save_sources_json(self):
        self.sources_json[self.script_name] = self.currdate
        with open(SOURCES_JSON_FILE, 'w') as f:
            json.dump(self.sources_json, f, indent=4, sort_keys=True)


    @class_method_logger
    def load_scenarios_json(self):
        with open(SCENARIOS_JSON_FILE, 'r') as f:
            self.scenarios_json = json.load(f)
        self.scenarios_last_date = self.scenarios_json[self.SCENARIO_ID] 

    @class_method_logger
    def save_scenarios_json(self):
        self.scenarios_json[self.SCENARIO_ID] = self.slice_max_date
        with open(SCENARIOS_JSON_FILE, 'w') as f:
            json.dump(self.scenarios_json, f, indent=4, sort_keys=True)


    ############################## Check dates ##############################


    @class_method_logger
    def today_was_launch(self):
        if self.last_launch == self.currdate:
            log("### check_currdate_eq_last_launch_date ERROR: current_date == last_launch ", self.logger)
            log("table GA_SITE_ALL_PRODS was updated early today", self.logger)
            return True
        return False


    @class_method_logger
    def slice_empty(self):
        if self.slice_count == 0:
            log("### check_slice_count ERROR: slice_count = 0 >>> nothing to process", self.logger)
            return True
        return False


    ############################## ContextManager ##############################


    def __enter__(self):
        return self


    @class_method_logger
    def close(self):
        try:
            self.drop_table(SBX_TEAM_DIGITCAMP, TMP_SITE_ALL_PRODS_INTERMEDIATE)
            self.sp.sc.stop()
        except Exception as ex:
            if "object has no attribute" in str(ex):
                log("### SparkContext was not started", self.logger)
            else:
                log("### Close exception: \n{}".format(ex), self.logger)
        finally:
            del self.sing


    def __exit__(self, exc_type, exc_value, exc_tb):
        log("# __exit__ : begin", self.logger)
        if exc_type is not None:
            log("### Exit exception ERROR:\nexc_type:\n{}\nexc_value\n{}\nexc_tb\n{}"\
                .format(exc_type, exc_value, exc_tb), self.logger)
        self.close()
        log("# __exit__ : end", self.logger)
        log("="*120, self.logger) 


############################## Parse ##############################


@udf(stypes.StringType())
def get_url_path(url):
        if url is None:
            return None

        url_parse = None
        try:
            url_parse = urlparse(url)
        except:
            url_parse = None
            
        if url_parse is None:
            return None

        url_path = None
        if url_parse.netloc != "":
            url_path = url_parse.path.strip("/")
        else:
            url = "http://" + url_parse.path.strip("/")
            try:
                url_path = urlparse(url).path.strip("/")
            except:
                return None

        result = None
        if url_path != "":
            result = url_path

        return result


@udf(stypes.StringType())
def get_url_domain(url: str):
    if url is None:
        return None

    url_parse = None
    try:
        url_parse = urlparse(url)
    except:
        url_parse = None

    if url_parse is None:
        return None    

    url_domain = None
    if url_parse.netloc != "":
        url_domain = url_parse.netloc
    else:
        url_domain = "http://" + url_parse.path.lstrip("/")
        try:
            url_domain = urlparse(url_domain).netloc
        except:
            return None

    result = None
    if url_domain != "":
        result = url_domain

    return result


def checkHitPageTitle_udf (lst):
    def checkHitPageTitle(dctofrefwords, title, path):
        finout = None
        for key, wordlst in dctofrefwords.items():
            for reword in wordlst:
                wpatt = re.compile(reword)
                if title is not None:
                    res = wpatt.findall(title.lower())
                    if len(res)!=0:
                        if path is not None:
                            finout = key
                            break

            if finout is not None:
                break

        return finout

    return f.udf(lambda t, p: checkHitPageTitle(lst, t, p))


#####################################################################
############################### Main ################################
#####################################################################


if __name__ == "__main__":
    with GaSiteAllProds() as ga_site_all_prods:
        ga_site_all_prods.run()