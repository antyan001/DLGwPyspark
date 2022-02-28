#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *


class NewDataSlice:
    def __init__(self):
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
#         self.sing = tendo.singleton.SingleInstance()
        self.script_name = 'NEW_DATA_SLICE'
        self.REFDATE = "2019-09-01"

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.load_slice_json()
        self.load_sources_json()

        if self.today_was_launch():
            self.close()
            sys.exit()

        self.start_spark()

        log("# __init__ : begin", self.logger)


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
        self.hive.setConf("spark.sql.hive.manageFilesourcePartitions","false")


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


    ############################## Run method ##############################

    @exception_restart(num_of_attempts=3, delay_time_sec=10*60)
    @class_method_logger
    def run(self):
        visit_part = self.load_visit_part_by_date(DEFAULT, DEFAULT_GOOGLE_ANALYTICS,
                                                    min_session_date=self.slice_max_date)

        self.slice_count = self.get_visit_count(visit_part)

        if self.slice_count == 0:
            log("### slice_json ERROR : self.slice_count == 0: TMP_VISIT_PART will not create", self.logger)
            self.save_slice_json()
            sys.exit()

        self.create_tmp_table(visit_part)
        tmp_visit_part = self.load_table(SBX_TEAM_DIGITCAMP, TMP_VISIT_PART)

        self.slice_max_date = self.get_max_date(tmp_visit_part)

        self.save_slice_json()
        self.save_sources_json()


    ############################## Load and save json ##############################


    @class_method_logger
    def load_slice_json(self):
        with open(SLICE_JSON_FILE, 'r') as f:
            self.slice_json = json.load(f)
        self.slice_count = self.slice_json['COUNT']
        self.slice_max_date = self.slice_json['MAX_DATE']


    @class_method_logger
    def save_slice_json(self):
        self.slice_json['COUNT'] = self.slice_count
        self.slice_json['MAX_DATE'] = self.slice_max_date
        with open(SLICE_JSON_FILE, 'w') as f:
            json.dump(self.slice_json, f, indent=4, sort_keys=True)


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
    def today_was_launch(self):
        if self.last_launch == self.currdate:
            log("### check_currdate_eq_last_launch_date ERROR: current_date == last_launch ", self.logger)
            log("data slice was loaded early today", self.logger)
            return True
        return False


    ############################## Load visit ##############################


    @class_method_logger
    def load_visit(self, schema, table):
        if table is not None:
            sql = \
                """
                    select
                    visitNumber,
                    cid,
                    commonSegmentoUID,
                    sbbolUserId,
                    hitPagePath,
                    hitPageTitle,
                    sessionStartTime,
                    sessionDate,
                    hitType,
                    hitNumber,
                    hitTime,
                    eventCategory,
                    eventAction,
                    eventLabel,
                    hitPageHostName,
                    ctl_loading,
                    sessionId,
                    deviceIsMobile,
                    deviceMobileDeviceModel,
                    deviceDeviceCategory,
                    geoRegion,
                    geoCity
                    from {}.{}
                """.format(schema, table)
        else:
            sql = \
                """
                    select
                    visitNumber,
                    cid,
                    commonSegmentoUID,
                    sbbolUserId,
                    hitPagePath,
                    hitPageTitle,
                    sessionStartTime,
                    sessionDate,
                    hitType,
                    hitNumber,
                    hitTime,
                    eventCategory,
                    eventAction,
                    eventLabel,
                    hitPageHostName,
                    ctl_loading,
                    sessionId,
                    deviceIsMobile,
                    deviceMobileDeviceModel,
                    deviceDeviceCategory,
                    geoRegion,
                    geoCity
                    from {}
                """.format(schema)

        visit = self.hive.sql(sql)
        return visit


    @class_method_logger
    def load_visit_part_by_date(self, schema, table,
                                min_session_date, max_session_date : str = None):
        if min_session_date is None:
            min_session_date = self.REFDATE

        visit = self.load_visit(schema, table)
        visit_part = visit.filter("timestamp(sessionDate) > timestamp('{}')".format(min_session_date))
 #.filter("ctl_loading > 8749034")\

        if max_session_date is not None:
            visit_part = visit_part.filter("timestamp(sessionDate) <= timestamp('{}')".format(max_session_date))
        return visit_part


    @class_method_logger
    def get_visit_count(self, visit_part):
        return visit_part.count()

    ############################## Create tmp table ##############################

    @class_method_logger
    def get_max_date(self, df):
        ctl_loading = df.select(f.max('sessionDate')).collect()
        max_date = datetime.strftime(pd.Timestamp(ctl_loading[0]['max(sessionDate)']),
                                    format='%Y-%m-%d')
        return max_date


    @class_method_logger
    def create_tmp_table(self, df):
        self.drop_table(SBX_TEAM_DIGITCAMP, TMP_VISIT_PART)
        self.create_table_from_df(SBX_TEAM_DIGITCAMP, TMP_VISIT_PART, df)


    ######################### Table ##############################

    @class_method_logger
    def load_table(self, schema, table):
        return load_table(schema, table, self.hive)


    @class_method_logger
    def drop_table(self, schema, table):
        drop_table(schema, table, self.hive)


    @class_method_logger
    def create_table_from_df(self, schema, table, df):
        create_table_from_df(schema=schema, table=table, df=df, hive=self.hive)


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
            pass
#             del self.sing


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
    with NewDataSlice() as new_data_slice:
        new_data_slice.run()
