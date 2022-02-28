#!/bin/env python3

import subprocess
import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *


class CID_SBBOL_INN_UPDATE:

    ######################### Init and close ##############################

    def __init__(self):
        self.sing = tendo.singleton.SingleInstance()
        self.REFDATE = "2019-09-01"
        self.SCENARIO_ID = 'CIDSBBOLUSERID'
        self.script_name = "CID_SBBOL_USER_ID"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        
        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.load_scenarios_json()
        self.load_slice_json()
        self.load_sources_json()

        if self.slice_empty() or self.today_was_launch():
            pass
            #self.close()
            #sys.exit()

        self.start_spark()

        log("# __init__ : begin", self.logger)


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=2,
                        kerberos_auth=True,
                        numofcores=8,
                        numofinstances=20)

        self.hive = self.sp.sql

        self.hive.setConf("hive.exec.dynamic.partition","true")
        self.hive.setConf("hive.exec.dynamic.partition.mode","nonstrict")
        self.hive.setConf("hive.enforce.bucketing","false")
        self.hive.setConf("hive.enforce.sorting","false")
        self.hive.setConf("spark.sql.sources.partitionOverwiteMode","dynamic")
    # hive.setConf("hive.exec.stagingdir", "/tmp/{}/".format(curruser))
    # hive.setConf("hive.exec.scratchdir", "/tmp/{}/".format(curruser))
        self.hive.setConf("hive.load.dynamic.partitions.thread", 1)

    ################################## Load json ##################################


    @class_method_logger
    def load_scenarios_json(self):
        with open(SCENARIOS_JSON_FILE, 'r') as f:
            self.scenarios_json = json.load(f)

        if self.SCENARIO_ID not in self.scenarios_json:
            self.scenarios_last_date = self.REFDATE
        else:
            self.scenarios_last_date = self.scenarios_json[self.SCENARIO_ID]

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


    ################################## Check dates ##################################

    @class_method_logger
    def today_was_launch(self):
        if self.last_launch == self.currdate:
            log("### check_currdate_eq_last_launch_date ERROR: current_date == last_launch ", self.logger)
            log("table CID_SBBOL_INN was updated early today", self.logger)
            return True
        return False


    @class_method_logger
    def slice_empty(self):
        if self.slice_count == 0:
            log("### check_slice_count ERROR: slice_count = 0 >>> nothing to save", self.logger)
            return True
        return False


    ################################## Run ##################################


    @exception_restart(num_of_attempts=3, delay_time_sec=10*60)
    @class_method_logger
    def run(self):
        tmp_visit_part = self.load_visit_part()

        cid_sbbol, cid_segmento = self.get_cid_sbbol_and_segmento(tmp_visit_part)

        # Получение ИНН из связки с REPLICATIONGUID

        profile = self.load_user_profile()
        cid_sbbol_replicguid = cid_sbbol.join(profile, on=['sbbolUserId'], how='inner')


        customer = self.load_ma_customer()
        cid_sbbol_inn = cid_sbbol_replicguid.join(customer, on=['REPLICATIONGUID'], how='inner')


        # Получение ИНН из cookie_map

        #cookie_map = self.load_cookie_map()
        #active_cookie = self.get_active_cookie(cookie_map)

        #cid_segmento_inn = cid_segmento.join(active_cookie, 
        #                                   on=(cid_segmento.commonSegmentoUID == active_cookie.CUST_COOKIE), 
        #                                  how="left_outer")

        # Join

        cid_sbbol_inn_segmento = cid_sbbol_inn.join(cid_segmento, on=['cid'], how='full_outer').distinct() \
                                                #.withColumn('inn_',f.coalesce('CU_INN','CUST_INN')) \
                                                #.drop('CU_INN','CUST_INN')\
                                                #.withColumnRenamed("inn_", "CU_INN")


        unified_customer = self.load_unified_customer()

        cid_sbbol_inn_segmento_crm = cid_sbbol_inn_segmento.join(unified_customer, 
                                                                on=(cid_sbbol_inn_segmento.CU_INN == unified_customer.inn), how='leftouter') \
                                                                .dropDuplicates(['cu_inn','sbbolUserId', 'commonSegmentoUID', 'crm_id'])

        
        cid_sbbol_inn_segmento_crm_dt = self.add_load_dt(cid_sbbol_inn_segmento_crm)
        final_df = self.create_final_df(cid_sbbol_inn_segmento_crm_dt)

        # self.save_final_table(final_df)
        sdf = final_df
        conn_schema = 'sbx_team_digitcamp'
        table_name = 'ga_cid_sbbol_inn_update'
        part_tupl_lst = [('ctl_loading', 'bigint')]
        part_tupl_str = ', '.join(["{} {}".format(col, _type) for col, _type in part_tupl_lst])
                                
        self.hive.sql("drop table if exists {schema}.{tbl} purge".format(schema=conn_schema, tbl=table_name))
        insert = ', '.join(["{} {}".format(col, _type) for col, _type in sdf.dtypes if col.lower() not in part_tupl_lst[0][0]])

        self.createSDF(conn_schema, target_tbl=table_name, insert=insert, part_tupl_lst=part_tupl_str)

        self.insertToSDF(sdf,
                    conn_schema='sbx_team_digitcamp',
                    tmp_tbl='tmp_ga_cid_inn', 
                    target_tbl='ga_cid_sbbol_inn_update', 
                    part_col_lst='ctl_loading')

        data_path = 'hdfs://clsklsbx/user/team/team_digitcamp/hive/'
        parts_from = self.hive.sql("show partitions {}.{}".format('sbx_team_digitcamp','ga_cid_sbbol_inn_update')).collect()
        parts_from = [part for part in parts_from if not part['partition'].endswith('__HIVE_DEFAULT_PARTITION__')]
        parts_from = sorted(parts_from,reverse=True, key=lambda x: int(x['partition'].split('=')[-1]))
        parts_from = [part['partition'] for part in parts_from if not part['partition'].endswith('__HIVE_DEFAULT_PARTITION__')]

        parts_to = self.hive.sql("show partitions {}.{}".format('sbx_team_digitcamp','ga_cid_sbbol_inn')).collect()
        parts_to = [part for part in parts_to if not part['partition'].endswith('__HIVE_DEFAULT_PARTITION__')]
        parts_to = sorted(parts_to, reverse=True, key=lambda x: int(x['partition'].split('=')[-1]))
        # parts_to = sorted(parts_to,reverse=True)

        part_diff = set(parts_from) - set(parts_to)
        part_diff = [part.split('=')[-1]  for part in part_diff]
        part_diff = sorted(part_diff,reverse=True)
        for part_num in part_diff:
            #print('ADDING PARTITION: {}...'.format(part_num))
            self.hive.sql('''ALTER TABLE sbx_team_digitcamp.ga_cid_sbbol_inn ADD IF NOT EXISTS PARTITION(ctl_loading='{}')'''.format(part_num))


        for ctl in parts_from:
            hdfs_from = data_path+'ga_cid_sbbol_inn_update'+'/'+'{}/*'.format(ctl)
            hdfs_to   = data_path+'ga_cid_sbbol_inn'+'/'+'{}/'.format(ctl)
            subprocess.call(['hdfs', 'dfs', '-cp', '-f', hdfs_from, hdfs_to], stdout=subprocess.PIPE, stdin=subprocess.PIPE)




        self.save_scenarios_json(tmp_visit_part)
        self.save_sources_json()

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


    ################################## Load ##################################


    @class_method_logger
    def load_table(self, schema, table):
        return load_table(schema, table, self.hive)


    @class_method_logger
    def load_visit_part(self):
        tmp_visit_part = self.load_table(SBX_TEAM_DIGITCAMP, TMP_VISIT_PART) \
                            .filter("timestamp(sessionDate) > timestamp('{}')".format(self.scenarios_last_date))

        return tmp_visit_part


    @class_method_logger
    def get_cid_sbbol_and_segmento(self, visit_part):
        cid_sbbol = visit_part.filter('sbbolUserId is not Null') \
                                    .dropDuplicates(['cid', 'sbbolUserId']) \
                                    .select('cid', 'sbbolUserId', 'CTL_LOADING')

        cid_segmento = visit_part.filter('commonSegmentoUID is not Null') \
                                        .select('cid','commonSegmentoUID') \
                                        .distinct()

        return cid_sbbol, cid_segmento


    @class_method_logger
    def load_user_profile(self):
        profile = self.load_table(SBX_TEAM_DIGITCAMP, GA_MA_USER_PROFILE_SBBOL) \
                        .select('CU_ID_SBBOL', 'REPLICATIONGUID', 'USER_ID', f.col('USER_GUID').alias('sbbolUserId')) \
                        .filter("(sbboluserid is not Null) and not (sbboluserid = 'Пользователь Интернет-Клиента')")\
                        .filter(~f.col("CU_ACCOUNT_PROFILE").isin(
                                                        [
                                                         'Операционист Банка', 
                                                         'Администратор Банка', 
                                                         'Администратор Off-line Клиента',   
                                                         'Кредитный консультант ММБ',
                                                         'Учетная запись RPA',
                                                         'Пользователь Online-Клиента',
                                                         'Пользователь Off-line Клиента',
                                                         'Получатель средств ADM'   
                                                        ]
                                                     )
                   )


        return profile


    @class_method_logger
    def load_ma_customer(self):
        customer = self.load_table(SBX_TEAM_DIGITCAMP, GA_MA_CUSTOMER_SBBOL) \
                        .select('REPLICATIONGUID', 'CU_INN', 'CU_KPP', 'CU_OKPO', 'LOCKED')

        return customer


    @class_method_logger
    def load_cookie_map(self):
        cookie_map = self.load_table(SBX_TEAM_DIGITCAMP, MA_CUSTOMER_COOKIE_MAP) \
                            .select("CUST_INN", "CUST_COOKIE", "SRC_UPDATE_DTTM") \

        return cookie_map


    @class_method_logger
    def load_unified_customer(self):
        unified_customer = self.load_table(SBX_TEAM_DIGITCAMP, UNIFIED_CUSTOMER)  \
                                .select('inn', 'crm_id').distinct()

        return unified_customer


    ################################## Operations ##################################

    @class_method_logger
    def get_active_cookie(self, cookie):
        cookie_active = cookie.withColumn("days_diff", f.datediff(f.current_timestamp(), f.col("SRC_UPDATE_DTTM"))) \
                                .filter(f.col("days_diff") <= 40) \
                                .select("CUST_INN", "CUST_COOKIE").distinct()
        return cookie_active


    @class_method_logger
    def add_load_dt(self, df):
        strdate = datetime.strftime(datetime.now(), format='%Y.%d.%m')
        df_with_load_df = df.withColumn('load_dt', f.lit(datetime.strptime(strdate, '%Y.%d.%m')).cast(stypes.TimestampType()))
        return df_with_load_df

    @class_method_logger
    def createSDF(self, conn_schema, target_tbl, insert, part_tupl_lst):

        self.hive.sql('''create table {schema}.{tbl} (
                                                {fields}
                                                    )
                    PARTITIONED BY ({part_col_lst})
                '''.format(schema=conn_schema,
                            tbl=target_tbl,
                            fields=insert,
                            part_col_lst=part_tupl_lst)
                )

    @class_method_logger
    def insertToSDF(self, sdf, conn_schema, tmp_tbl, target_tbl, part_col_lst):
    
        sdf.registerTempTable(tmp_tbl)
        
        self.hive.sql("""
        insert overwrite table {schema}.{tbl}
        partition({part_col})
        select * from {tmp_tbl}
        distribute by ({part_col})
        """.format(schema=conn_schema,
                tbl=target_tbl,
                tmp_tbl=tmp_tbl,
                part_col=part_col_lst)
                )

    ############################## Create final_df ##############################




    @class_method_logger
    def create_final_df(self, df):
        cols = [ 'cid',
                'replicationguid',
                'sbboluserid',
                'commonSegmentoUID',
                'cu_id_sbbol',
                'user_id',
                'cu_inn',
                'cu_kpp',
                'crm_id',
                'cu_okpo',
                'locked',
                'load_dt',
                'ctl_loading']

        final_df = df.select([col if col != 'ctl_loading' else f.col(col).cast(stypes.LongType()) for col in cols])

        return final_df
                        

    ############################## Save final table ##############################

    @class_method_logger
    def save_final_table(self, df):
        tmp_tbl = 'tmp_inn'
        df.registerTempTable(tmp_tbl)

        self.hive.setConf("hive.exec.dynamic.partition", "true")
        self.hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        self.hive.sql(
            """
                insert into table {schema}.{tbl}
                partition(ctl_loading)
                select {tmp_tbl}.* from {tmp_tbl}
                distribute by ctl_loading
            """.format(schema=SBX_TEAM_DIGITCAMP, tbl=GA_CID_SBBOL_INN, tmp_tbl=tmp_tbl))
        return None


    @class_method_logger
    def save_scenarios_json(self, tmp_visit_part):
        ctl_loading = tmp_visit_part.select(f.max('sessionDate')).collect()
        self.slice_max_date = ctl_loading[0]['max(sessionDate)']

        self.scenarios_json[self.SCENARIO_ID] = self.slice_max_date
        with open(SCENARIOS_JSON_FILE, 'w') as obj:
            # log(self.scenarios_json[self.SCENARIO_ID])
            json.dump(self.scenarios_json, obj, indent=4, sort_keys=True)


    @class_method_logger
    def save_sources_json(self):
        self.sources_json[self.script_name] = self.currdate
        with open(SOURCES_JSON_FILE, 'w') as f:
            json.dump(self.sources_json, f, indent=4, sort_keys=True)


    ############################## ContextManager ##############################


    def __enter__(self):
        return self


    @class_method_logger
    def close(self):
        try:
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


if __name__ == '__main__':
    with CID_SBBOL_INN_UPDATE() as cid_sbbol_inn_upd:
        cid_sbbol_inn_upd.run()