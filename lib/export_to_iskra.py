#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *
import loader as load


class ExportToIskra:
    ################################## Init ##################################

    def __init__(self, force_mode : bool = False):
        self.sing = tendo.singleton.SingleInstance()
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = "EXPORT_TO_ISKRA"

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.force_mode = force_mode
        log("### FORCE_MODE: {} ###".format(self.force_mode), self.logger)

        self.load_slice_json()
        self.load_scenarios_json()
        self.load_sources_json()

        if  (self.slice_empty() or (not self.all_scenarios_done()) or self.export_done()) \
                and (not self.force_mode):
            self.close()
            sys.exit()

        self.start_spark()

        log("# __init__ : begin", self.logger)


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=1,
                        kerberos_auth=False,
                        numofcores=4,
                        numofinstances=10)

        self.hive = self.sp.sql


    ############################## Run method ##############################


    @exception_restart(num_of_attempts=3, delay_time_sec=10*60)
    @class_method_logger
    def run(self):
        max_load_dt = self.get_max_load_dt_in_iskra()
        hist_part = self.load_hist_part(min_load_dt=max_load_dt)

        count = hist_part.count()
        if count == 0:
            log("### Download_count ERROR: count = 0 >>> nothing to download")
            sys.exit()

        log("## Hist_part count: {}".format(count), self.logger)

        self.save_to_iskra(hist_part)
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


    ############################## Load and save json ##############################


    @class_method_logger
    def load_slice_json(self):
        with open(SLICE_JSON_FILE, 'r') as f:
            self.slice_json = json.load(f)
        self.slice_count = self.slice_json['COUNT']
        self.slice_max_date = self.slice_json['MAX_DATE']


    @class_method_logger
    def load_scenarios_json(self):
        with open(SCENARIOS_JSON_FILE, 'r') as f:
            self.scenarios_json = json.load(f)


    @class_method_logger
    def load_sources_json(self):
        with open(SOURCES_JSON_FILE, 'r') as f:
            self.sources_json = json.load(f)
        self.export_last_date = self.sources_json[self.script_name]
        self.slice_last_date = self.sources_json["NEW_DATA_SLICE"]



    @class_method_logger
    def save_sources_json(self):
        self.sources_json[self.script_name] = self.currdate
        with open(SOURCES_JSON_FILE, 'w') as f:
            json.dump(self.sources_json, f, indent=4, sort_keys=True)


    ############################## Check dates ##############################

    @class_method_logger
    def slice_empty(self):
        if self.slice_count == 0:
            log("##### check_slice_count ERROR: slice_count = 0 >>> nothing to save", self.logger)
            return True
        return False


    @class_method_logger
    def all_scenarios_done(self):
        scenario_error_list = []
        for SCENARIO_ID in ALL_SCENARIOS:
            scenarios_last_date = self.scenarios_json[SCENARIO_ID]
            if (scenarios_last_date != self.slice_max_date):
                scenario_error_list.append(SCENARIO_ID)

        if len(scenario_error_list) != 0:
            log("### check_scenarios_date ERROR: scenarios_last_date != slice_max_date ", self.logger)
            log("scenarios {} did not work".format(str(scenario_error_list)), self.logger)
            return False

        return True


    @class_method_logger
    def export_done(self):
        if  self.slice_last_date <= self.export_last_date:
            log("### check_slice_and_iskra_date ERROR: slice_last_date <= export_last_date ", self.logger)
            log("data was loaded early", self.logger)
            return True
        return False


    ############################## Load hist ##############################

    @class_method_logger
    def get_max_load_dt_in_iskra(self):
        sql = "(select /*+ parallel (8) */ max(load_dt) as maxdt from {})".format("ISKRA_CVM.GA_ALL_SCENARIOS_HIST")
        max_load_dt = self.sp.get_oracle(OracleDB(ISKRA), sql).collect()[0]
        max_resp_str = datetime.strftime(max_load_dt['MAXDT'], format='%Y-%m-%d')
        return max_resp_str


    @class_method_logger
    def load_hist_part(self, min_load_dt: str):
        sql = "select * from {}.{} where timestamp(load_dt) > timestamp('{}')" \
                    .format(SBX_TEAM_DIGITCAMP, GA_ALL_SCENARIOS_HIST, min_load_dt)
        hist_part = self.hive.sql(sql)
        hist_part_upper = hist_part.select([col.upper() for col in hist_part.columns])
        return hist_part_upper


    ############################## Save to Iskra ##############################


#     @class_method_logger
#     def save_to_iskra(self, df):
#         pddf=df.toPandas()
#         ld = loader.Loader(init_dsn=True, encoding='cp1251',  sep=',')
#         ld.upload_df_or_csv(pddf, "GA_ALL_SCENARIOS_HIST", parallel=1,
#                     password='', path= None,
#                     isclobe=1, isuseclobdct=1, verbose=1, njobs=5)

    @class_method_logger
    def save_to_iskra(self, df):
        sdf = df.select(*[f.col(col).alias(col.upper()) for col in df.columns])
        iskra_table_name = "ISKRA_CVM.GA_ALL_SCENARIOS_HIST"
        typesmap={}
        for column_name, column in sdf.dtypes:
            if column == 'string':
                if 'INN' in column_name.upper() or 'KPP' in column_name:
                    typesmap[column_name] = 'VARCHAR(20)'
                elif 'PRODUCTS'.upper() in column_name:
                    typesmap[column_name] = 'VARCHAR(4000)'
                else:
                    typesmap[column_name] = 'VARCHAR(200)'
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
            .option('url', 'jdbc:oracle:thin:@//' + db.dsn) \
            .option('user', db.user) \
            .option('password', db.password) \
            .option('dbtable', iskra_table_name) \
            .option('createTableColumnTypes', cols)\
            .option('driver', 'oracle.jdbc.driver.OracleDriver') \
            .save()
        log("# __oracle_export__ : done", self.logger)

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


    def __exit__(self, exc_type = None, exc_value = None, exc_tb = None):
        log("# __exit__ : begin", self.logger)
        if exc_type is not None:
            log("### Exit exception ERROR:\nexc_type:\n{}\nexc_value\n{}\nexc_tb\n{}"\
                .format(exc_type, exc_value, exc_tb), self.logger)
        self.close()
        log("# __exit__ : end", self.logger)
        log("="*120, self.logger)


#####################################################################
############################### Main ################################
#####################################################################


def run_argparse():
    parser = argparse.ArgumentParser(__name__)

    parser.add_argument('-fm', "--force_mode", type=bool, required=False,
                        help='Отключить проверки дат при загрузке', default=False)

    return parser.parse_args()


if __name__ == "__main__":
    args = run_argparse()
    with ExportToIskra(args.force_mode) as export_to_iskra:
        export_to_iskra.run()
