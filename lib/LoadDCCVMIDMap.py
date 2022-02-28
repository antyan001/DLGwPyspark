#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from dateutil.relativedelta import relativedelta
from lib.tools import *


def get_elapsed_days_count(date_str):
        return (datetime.today()-datetime.strptime(date_str, '%Y-%m-%d')).days
        
        
def get_dccvmid(string, sub_string='dccvmid='):
    if not isinstance(string, str):
        return None
    result = re.search(sub_string+r'(\w*)', string)
    return None if result is None else result.group(1)


class LoadDCCVMIDMap:
    """ Loading DCCVMID mapping dataframe and saving the result in hive table.  
    """

    def __init__(self):
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = 'LOAD_DCCVMID_MAP'

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.init_spark()

        log("# __init__ : begin", self.logger)

    @class_method_logger
    def init_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=2,
                        kerberos_auth=False,
                        numofcores=4,
                        numofinstances=10)

        self.hive = self.sp.sql

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

    @exception_restart(num_of_attempts=3, delay_time_sec=10*60)
    @class_method_logger
    def run(self, target):
        cid_sbbol_inn_map = self.hive.sql('select cid, sbboluserid, commonSegmentoUID, cu_inn from {}.{}'.format(SBX_TEAM_DIGITCAMP, 'ga_cid_sbbol_inn')).distinct()

        visit_part = self.load_visit_part('cap_external_google_analytics_up_external_google_analytics', 'visit')
        if visit_part is None:
            return None
        cid_dccvmid_inn_map = self.get_cid_dccvmid_map(visit_part)\
                                  .join(cid_sbbol_inn_map, ['cid'], how='left') \
                                  .drop_duplicates()\
                                  .withColumn('load_dt', f.lit(self.currdate))
        
        cid_dccvmid_inn_map.select('cid', 'dccvmid', 'cu_inn', 'sbboluserid', 'commonSegmentoUID', 'load_dt').registerTempTable(target)
        self.insert_into_partitioned_table_from_df(target, 'load_dt', SBX_TEAM_DIGITCAMP, target)
        
    @class_method_logger
    def load_visit_part(self, schema, table):
        yesterday = datetime.strptime(self.currdate, '%Y-%m-%d') + relativedelta(days=-4)       
        ctl_date_map = self.hive.sql("select * from sbx_team_digitcamp.GA_VISIT_CTL_DATE_MAP") \
                   .filter("max_sessiondate>='{}'".format(yesterday.strftime('%Y-%m-%d')))
        if ctl_date_map.count() == 0:
            return None
    
        sql = \
            """
                SELECT
                cid,
                sbbolUserId,
                hitPagePath,
                hitPageTitle,
                sessionId,
                sessionDate,
                sessionStartTime,
                hitTime,
                hitType,
                eventCategory,
                eventAction,
                eventLabel,
                hitPageHostName
                from {}.{} where ctl_loading in({})
            """.format(schema, table, ','.join([str(x) for x in ctl_date_map.select('ctl_loading').toPandas()['ctl_loading']]))
        visit_part = self.hive.sql(sql).filter("regexp_extract(cid,'(\\\\d{9,11}\.\\\\d{9,11})',1) !='' ''")
        return visit_part
    
    @class_method_logger
    def get_cid_dccvmid_map(self, visit_part):
        result = visit_part.select('cid', f.udf(get_dccvmid)(f.col('hitPagePath')).alias('dccvmid')) \
                           .select('dccvmid', 'cid').distinct()
        return result
        
    @class_method_logger
    def insert_into_partitioned_table_from_df(self, part_tbl, part_col, conn_schema='sbx_team_digitcamp', 
                                              table_name='cid_dccvmid_inn_map'):
        self.hive.sql("""
                    insert into table {schema}.{table}
                    partition({part_col})
                    select * from {tmp_tbl}
                    distribute by ({part_col})
                    """.format(schema=conn_schema,
                               table=table_name,
                               tmp_tbl=part_tbl,
                               part_col=part_col)
                )

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
            pass

    def __exit__(self, exc_type, exc_value, exc_tb):
        log("# __exit__ : begin", self.logger)
        if exc_type is not None:
            log("### Exit exception ERROR:\nexc_type:\n{}\nexc_value:\n{}\nexc_tb:\n{}"\
                .format(exc_type, exc_value, exc_tb), self.logger)
        self.close()
        log("# __exit__ : end", self.logger)
        log("="*120, self.logger) 


#####################################################################
############################### Main ################################
#####################################################################

if __name__ == '__main__':
    with LoadDCCVMIDMap() as load_cid_dccvmid_inn_map:
        load_cid_dccvmid_inn_map.run('cid_dccvmid_inn_map')