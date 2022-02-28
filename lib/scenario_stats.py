#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *
from lib.mail_sender import *


class ScenarioStats:

    ######################### Init and close ##############################


    def __init__(self,
                date : str = None,
                test_mode : bool = False):
        self.sing = tendo.singleton.SingleInstance()
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = "SCENARIO_STATS"

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.test_mode = test_mode
        log("### TEST_MODE: {} ###".format(self.test_mode), self.logger)

        self.load_slice_json()
        self.load_scenarios_json()
        self.load_sources_json()

        self.process_scenario_status()
        self.export_to_iskra = self.export_done()

        self.start_spark()

        if date is None:
            self.date = self.slice_last_date
        else:
            self.date = date

        log("# __init__ : end", self.logger)


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

    @class_method_logger
    def run(self, send_email : bool = True,
                  save_table : bool = True):

        hist = load_table(SBX_TEAM_DIGITCAMP, GA_ALL_SCENARIOS_HIST, self.hive)

        hist_stats = self.get_hist_stats(hist)

        if send_email:
            self.send_email(hist_stats)

        if self.export_to_iskra and save_table and not self.test_mode:
            self.save_final_table(hist_stats)


    ############################## Logging ##############################


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


    ############################## Load and check dates ##############################


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
        self.export_last_date = self.sources_json["EXPORT_TO_ISKRA"]
        self.slice_last_date = self.sources_json["NEW_DATA_SLICE"]


    @class_method_logger
    def load_scenarios_json(self):
        with open(SCENARIOS_JSON_FILE, 'r') as f:
            self.scenarios_json = json.load(f)

    ############################## Check ##############################

    @class_method_logger
    def process_scenario_status(self):
        self.done_scenarios = []
        self.error_scenarios = []
        for scenario in ALL_SCENARIOS:
            if self.scenarios_json[scenario] == self.slice_max_date:
                self.done_scenarios.append(scenario)
            else:
                self.error_scenarios.append(scenario)
                log("### Scenario_status ERROR: scenario {} did not work".format(scenario), self.logger)


    @class_method_logger
    def export_done(self):
        return self.slice_last_date <= self.export_last_date


    ############################## Stats ##############################


    @udf(stypes.StringType())
    def get_bins_float(concat_str):
        arr = list(map(float, concat_str.split(",")))
        return str(dict(Counter(arr)))


    @udf(stypes.StringType())
    def get_bins_int(concat_str):
        arr = list(map(int, concat_str.split(",")))
        return str(dict(Counter(arr)))


    @class_method_logger
    def get_hist_stats(self, hist):
        hist_filt = hist.filter(f.col("load_dt") >= self.date)

        hist_agg = hist_filt.groupBy("load_dt", "scenario_id", "id_product", "channel", "numberOfSteps") \
                            .agg(f.count(f.col("returncnt")).alias("count"),
                                 f.avg("funnel_rate").alias("funnel_rate_avg"),
                                 f.avg("returncnt").alias("returncnt_avg"),
                                 f.avg("sum_offer_priority").alias("sum_offer_priority_avg"),
                                 f.avg("sum_offer_nontop").alias("sum_offer_nontop_avg"),
                                 f.concat_ws(",", f.collect_list("funnel_rate")).alias("concat_fr"),
                                 f.concat_ws(",", f.collect_list("returncnt")).alias("concat_rc"),
                                 f.concat_ws(",", f.collect_list("sum_offer_priority")).alias("concat_sop"),
                                 f.concat_ws(",", f.collect_list("sum_offer_nontop")).alias("concat_son")) \
                            .orderBy(f.col("count").desc())

        hist_bins  = hist_agg.withColumn("funnel_rate_bins", ScenarioStats.get_bins_float(f.col("concat_fr"))) \
                                .withColumn("returncnt_bins", ScenarioStats.get_bins_int(f.col("concat_rc"))) \
                                .withColumn("sum_offer_priority_bins", ScenarioStats.get_bins_int(f.col("concat_sop"))) \
                                .withColumn("sum_offer_nontop_bins", ScenarioStats.get_bins_int(f.col("concat_son"))) \
                                .drop("concat_fr", "concat_rc", "concat_sop", "concat_son")

        return hist_bins


    ############################## Table ##############################


    @class_method_logger
    def save_final_table(self, df):
        df.registerTempTable(GA_ALL_SCENARIOS_STATS)

        self.hive.sql(
            """
                insert into table {schema}.{tbl}
                select {tbl}.* from {tbl}
            """.format(schema=SBX_TEAM_DIGITCAMP, tbl=GA_ALL_SCENARIOS_STATS))
        return None


    ############################## Email ##############################


    @class_method_logger
    def get_receivers(self):
        if self.test_mode:
            receivers_file = TEST_EMAIL_LIST_FILE
        else:
            receivers_file = EMAIL_LIST_FILE

        receivers = []
        with open(receivers_file, "r") as f:
            for line in f:
                line = line.strip()
                if line[0] == "-":
                    continue

                receivers.append(line.strip())
        return receivers


    @class_method_logger
    def get_settings(sefl):
        with open(EMAIL_SETTINGS_FILE, "r") as f:
            settings = json.load(f)
        return settings


    @class_method_logger
    def get_message(self, hist, all_count):
        hist = hist.toPandas()

        scenario_count_dict = dict()
        for scenario, count in hist.to_numpy():
            scenario_count_dict[scenario] = count

        line = "-" * 50 + "\n"
        def header(name, target, all_):
            return "#"*10 + " {} ".format(name) + "{}/{} ".format(target, all_) + "#"*10

        msg = ""
        msg += "Статистика лидогенерации {}\n".format(self.date)
        msg += line

        msg += "VISIT_COUNT: " + str(self.slice_count) +"\n"
        msg += "VISIT_MAX_DATE: " + str(self.slice_max_date) + "\n"
        msg += line + "\n"

        msg += header("DONE", len(self.done_scenarios), len(ALL_SCENARIOS)) + "\n"
        counter = 1
        for scenario in sorted(ALL_SCENARIOS):
            if scenario in scenario_count_dict.keys():
                count = scenario_count_dict[scenario]
            elif scenario in self.done_scenarios:
                count = 0
            else:
                continue
            msg += str(counter) + ") " + scenario + " : " + str(count) + "\n"
            counter += 1
        msg += "### ALL : " + str(all_count) + "\n"

        msg += "\n" + header("ERROR", len(self.error_scenarios), len(ALL_SCENARIOS)) + "\n"
        counter = 1
        for scenario in self.error_scenarios:
            msg += str(counter) + ") " + scenario + "\n"
            counter += 1


        msg += "\n" + line
        msg += "EXPORT TO ISKRA: " + str(self.export_to_iskra)

        return msg


    @class_method_logger
    def send_email(self, hist_stats):
        hist_agg = hist_stats.groupBy("scenario_id").agg(f.sum("count"))
        all_count = hist_agg.select(f.sum("sum(count)").alias("count")).collect()[0]["count"]

        msg = self.get_message(hist_agg, all_count)

        settings = self.get_settings()
        receivers = self.get_receivers()

        authorization = Authorization(user=settings["user"], domain='ALPHA', mailbox=settings["mailbox"],
                                      server='cab-vop-mbx2053.omega.sbrf.ru')
        mail_receiver = MailReceiver(settings["password"], auth_class=authorization)

        mail_receiver.send_message(receivers, 'DLG_SCENARIO_STATS {}'.format(self.date), msg)
        return msg


    ############################## Close ##############################


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


#####################################################################
############################### Main ################################
#####################################################################

def run_argparse():
    parser = argparse.ArgumentParser(__name__)

    parser.add_argument('-tm', "--test_mode", action='store_true', required=False,
                        help='Активация тестового режима', default=False)

    parser.add_argument('-nse', "--not_send_email", action='store_true', required=False,
                        help='Включение почтовой рассылки', default=False)

    parser.add_argument('-nst', "--not_save_table", action='store_true', required=False,
                        help='Включение сохранения в таблицу', default=False)

    parser.add_argument('-d', "--date", type=str, required=False,
                        help='Указание даты загрузки (YYYY-mm-dd) данных в таблице', default=None)

    return parser.parse_args()


if __name__ == "__main__":
    args = run_argparse()
    with ScenarioStats(test_mode=args.test_mode, date=args.date) as scenarioStats:
        scenarioStats.run(send_email=(not args.not_send_email), save_table=(not args.not_save_table and not args.test_mode))
