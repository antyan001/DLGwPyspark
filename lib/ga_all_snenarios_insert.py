#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *
import subprocess

class AllScenariosHistUpdate:

    def __init__(self):
#         self.sing = tendo.singleton.SingleInstance()
        self.REFDATE = "2021-09-01"
        self.SCENARIO_ID = 'GASCENARIOHISTUPDATE'
        self.script_name = "SCENARIOHISTINSERT"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.init_logger()
        log("# __init__ : begin", self.logger)

#         self.load_scenarios_json()
#         self.load_slice_json()
        self.load_sources_json()

#         if self.slice_empty() or self.today_was_launch():
#             pass
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
                        kerberos_auth=False,
                        numofcores=8,
                        numofinstances=20)

        self.hive = self.sp.sql
        self.hive.setConf("hive.exec.dynamic.partition","true")
        self.hive.setConf("hive.exec.dynamic.partition.mode","nonstrict")
        self.hive.setConf("hive.enforce.bucketing","false")
        self.hive.setConf("hive.enforce.sorting","false")


    ################################## Load json ##################################


#     @class_method_logger
#     def load_scenarios_json(self):
#         with open(SCENARIOS_JSON_FILE, 'r') as f:
#             self.scenarios_json = json.load(f)

#         if self.SCENARIO_ID not in self.scenarios_json:
#             self.scenarios_last_date = self.REFDATE
#         else:
#             self.scenarios_last_date = self.scenarios_json[self.SCENARIO_ID]

#     @class_method_logger
#     def load_slice_json(self):
#         with open(SLICE_JSON_FILE, 'r') as f:
#             self.slice_json = json.load(f)
#         self.slice_count = self.slice_json['COUNT']
#         self.slice_max_date = self.slice_json['MAX_DATE']


    @class_method_logger
    def load_sources_json(self):
        with open(SOURCES_JSON_FILE, 'r') as f:
            self.sources_json = json.load(f)
        self.export_last_date = self.sources_json["EXPORT_TO_ISKRA"]
        self.slice_last_date = self.sources_json["NEW_DATA_SLICE"]


    ################################## Check dates ##################################


    @class_method_logger
    def slice_empty(self):
        if self.slice_count == 0:
            log("### check_slice_count ERROR: slice_count = 0 >>> nothing to save", self.logger)
            return True
        return False

    @class_method_logger
    def export_done(self):
        return self.slice_last_date == self.export_last_date

    ################################## Run ##################################
    def run(self):
        if self.export_done() == False:
            return False

        sdf = load_table(SBX_TEAM_DIGITCAMP, "GA_ALL_SCENARIOS_HIST_PART", self.hive)

        part_tupl_lst = [('ctl_loading', 'bigint')]
        part_tupl_str = ', '.join(["{} {}".format(col, _type) for col, _type in part_tupl_lst])


        conn_schema = 'sbx_team_digitcamp'
        table_name = 'ga_all_scenarios_hist_part_upd'



        self.hive.sql("drop table if exists {schema}.{tbl} purge".format(schema=conn_schema, tbl=table_name))
        insert = ', '.join(["{} {}".format(col, _type) for col, _type in sdf.dtypes if col != part_tupl_lst[0][0]])
#         insert = ', '.join(["{} {}".format(col, _type) for col, _type in sdf.dtypes])
        self.createSDF(conn_schema, target_tbl=table_name, insert=insert, part_tupl_lst=part_tupl_str)

        self.insertToSDF(sdf, SBX_TEAM_DIGITCAMP, "tmp_ga_part", "ga_all_scenarios_hist_part_upd", "ctl_loading")

        data_path = 'hdfs://clsklsbx/user/team/team_digitcamp/hive/'
        parts_from = self.hive.sql("show partitions {}.{}".format('sbx_team_digitcamp','ga_all_scenarios_hist_part_upd')).collect()
        parts_from = [part for part in parts_from if not part['partition'].endswith('__HIVE_DEFAULT_PARTITION__')]
        parts_from = sorted(parts_from,reverse=True, key=lambda x: int(x['partition'].split('=')[-1]))
        parts_from = [part['partition'] for part in parts_from]

        parts_to = self.hive.sql("show partitions {}.{}".format('sbx_team_digitcamp','GA_ALL_SCENARIOS_HIST')).collect()
        parts_to = [part for part in parts_to if not part['partition'].endswith('__HIVE_DEFAULT_PARTITION__')]
        parts_to = sorted(parts_to, reverse=True, key=lambda x: int(x['partition'].split('=')[-1]))
        parts_to = [part['partition'] for part in parts_to]
        # parts_to = sorted(parts_to,reverse=True)
        part_diff = set(parts_from) - set(parts_to)
        part_diff = [part.split('=')[-1]  for part in part_diff]
        part_diff = sorted(part_diff,reverse=True)

        for part_num in part_diff:
#             print('ADDING PARTITION: {}...'.format(part_num))
            self.hive.sql('''ALTER TABLE sbx_team_digitcamp.GA_ALL_SCENARIOS_HIST ADD IF NOT EXISTS PARTITION(ctl_loading='{}')'''.format(part_num))

        for ctl in parts_from:

            hdfs_from = data_path+'ga_all_scenarios_hist_part_upd'+'/'+'{}/*'.format(ctl)
            hdfs_to   = data_path+'ga_all_scenarios_hist'+'/'+'{}/'.format(ctl)

            subprocess.call(['hdfs', 'dfs', '-cp', '-f', hdfs_from, hdfs_to], stdout=subprocess.PIPE, stdin=subprocess.PIPE)




        self.hive.sql("drop table if exists {schema}.{tbl} purge".format(schema=conn_schema, tbl="ga_all_scenarios_hist_part"))

        sdf_hist = load_table(SBX_TEAM_DIGITCAMP, "ga_all_scenarios_hist", self.hive)
        insert_hist = ', '.join(["{} {}".format(col, _type) for col, _type in sdf_hist.dtypes])
        self.createHIST_PART(conn_schema=conn_schema, target_tbl="ga_all_scenarios_hist_part", insert=insert_hist)



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

    def createHIST_PART(self, conn_schema, target_tbl, insert):

        self.hive.sql('''create table {schema}.{tbl} (
                                             {fields}
                                                )
             '''.format(schema=conn_schema,
                        tbl=target_tbl,
                        fields=insert)
            )

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
#     ############################## Save final table ##############################


#     @class_method_logger
#     def save_scenarios_json(self, tmp_visit_part):
#         ctl_loading = tmp_visit_part.select(f.max('sessionDate')).collect()
#         self.slice_max_date = ctl_loading[0]['max(sessionDate)']

#         self.scenarios_json[self.SCENARIO_ID] = self.slice_max_date
#         with open(SCENARIOS_JSON_FILE, 'w') as obj:
#             json.dump(self.scenarios_json, obj, indent=4, sort_keys=True)


#     @class_method_logger
#     def save_sources_json(self):
#         self.sources_json[self.script_name] = self.currdate
#         with open(SOURCES_JSON_FILE, 'w') as f:
#             json.dump(self.sources_json, f, indent=4, sort_keys=True)


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
            # del self.sing
            pass


    def __exit__(self, exc_type, exc_value, exc_tb):
        log("# __exit__ : begin", self.logger)
        if exc_type is not None:
            log("### Exit exception ERROR:\nexc_type:\n{}\nexc_value\n{}\nexc_tb\n{}"\
                .format(exc_type, exc_value, exc_tb), self.logger)
        self.close()
        log("# __exit__ : end", self.logger)
        log("="*120, self.logger)


if __name__ == '__main__':
    with AllScenariosHistUpdate() as AllScenariosHist:
        AllScenariosHist.run()
