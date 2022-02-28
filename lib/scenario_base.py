#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *


class ScenarioBase:
    """ Base abstract scenario class """

    ######################### Init ##############################

    def __init__(self,
                 SCENARIO_ID : str,
                 CHANNEL : str,
                 NUM_OF_STEPS : int,
                 REFDATE : str ,
                 test_mode : bool = True):
        # self.sing = tendo.singleton.SingleInstance()
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = SCENARIO_ID

        self.NUM_OF_STEPS = NUM_OF_STEPS
        self.CHANNEL = CHANNEL
        self.REFDATE = REFDATE
        self.SCENARIO_ID = SCENARIO_ID

        self.test_mode = test_mode
        self.print_stage_count = self.test_mode
        
        self.ml_360_flg = False

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.load_slice_json()
        self.load_scenarios_json()

        if self.test_mode:
            log("### TEST_MODE IS ON ###", self.logger)

        if (self.slice_empty() or self.scenario_was_launch()) and (not self.test_mode):
            self.close()
            sys.exit()

        self.start_spark()

        log("# __init__ : end", self.logger)

    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=3,
                        kerberos_auth=False,
                        numofcores=8,
                        numofinstances=20)

        self.hive = self.sp.sql


    ############################## Run ##############################

    @exception_restart(num_of_attempts=3, delay_time_sec=3*60)
    @class_method_logger
    def run(self, print_final_count=False):
        visit_part = self.load_visit_part()

        users = self.get_funnels(visit_part)

        users_cid_sbbol_inn = self.join_cid_sbbol_inn_users(users)

        users_offers = self.join_offers_users(users_cid_sbbol_inn)

        users_deals = self.join_deals_users(users_offers)

        final_df = self.create_final_df(users_deals)

        if not self.test_mode:
            self.save_final_result(final_df)

        if print_final_count or self.test_mode:
            log("##### Final count: {}".format(final_df.count()), self.logger)


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


    def print_count_if_test(self, df, msg):
        if not self.print_stage_count:
            return

        log("### TEST OUTPUT ### => function: " + msg, self.logger)
        count = df.count()
        log("### TEST OUTPUT ### => return count: " + str(count), self.logger)
        return count


    ################################## Load  json ##################################


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

        if self.SCENARIO_ID not in self.scenarios_json:
            self.scenarios_last_date = self.REFDATE
        else:
            self.scenarios_last_date = self.scenarios_json[self.SCENARIO_ID]


    ################################## Check dates ##################################

    @class_method_logger
    def slice_empty(self):
        if self.slice_count == 0:
            log("##### check_slice_count ERROR: slice_count = 0 >>> nothing to save", self.logger)
            return True
        return False


    @class_method_logger
    def scenario_was_launch(self):
        if self.scenarios_last_date == self.slice_max_date:
            log("### scenarios_last_date ERROR: scenarios_last_date == slice_max_date >>> nothing to process", self.logger)
            return True
        return False


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


    @class_method_logger
    def insert_into_table_from_df(self, schema, table, df):
        insert_into_table_from_df(schema=schema, table=table, df=df, hive=self.hive)


    ######################### Load visit ##############################


    @class_method_logger
    def load_visit(self, schema, table):
        sql = \
                '''
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
                  ctl_loading
                from {schema}.{table}
                '''.format(schema=schema, table=table)

        if self.CHANNEL == "SBBOL":
            sql_where = "where hitPageHostName = 'sbi.sberbank.ru'"
        elif self.CHANNEL == "SITE":
            sql_where = "where hitPageHostName not in ('sbi.sberbank.ru', 'localhost')"
        sql += sql_where

        visit = self.hive.sql(sql)
        return visit


    @class_method_logger
    def load_visit_part_by_date(self, schema, table,
                                min_session_date, max_session_date : str = None):
        if min_session_date is None:
            min_session_date = self.REFDATE

        visit = self.load_visit(schema, table)
        visit_part = visit.filter("timestamp(sessionDate) > timestamp('{}')".format(min_session_date))

        if max_session_date is not None:
            visit_part = visit_part.filter("timestamp(sessionDate) <= timestamp('{}')".format(max_session_date))
        return visit_part


    @class_method_logger
    def load_visit_part_by_ctl(self, schema, table,
                               min_ctl_loading, max_ctl_loading : int = None):

        visit = self.load_visit(schema, table)
        visit_part = visit.filter("(ctl_loading > {})".format(min_ctl_loading))

        if max_ctl_loading is not None:
            visit_part = visit.filter("(ctl_loading <= {})".format(max_ctl_loading))
        return visit_part


    @class_method_logger
    def load_visit_part(self):
        if self.test_mode:
            visit_part = self.load_visit(SBX_TEAM_DIGITCAMP, TMP_TEST_VISIT_PART)
        else:
            visit_part = self.load_visit_part_by_date(SBX_TEAM_DIGITCAMP, TMP_VISIT_PART,
                                                        min_session_date=self.scenarios_last_date)

        self.print_count_if_test(visit_part, "load_visit_part")
        return visit_part


    ######################### Test visit table ##############################


    @class_method_logger
    def create_test_tmp_table(self,
                             tmp_schema : str = SBX_TEAM_DIGITCAMP,
                             tmp_table : str = TMP_TEST_VISIT_PART,
                             min_ctl_loading : int = TEST_MIN_CTL_LOADING_DEFAULT,
                             max_ctl_loading : int = TEST_MAX_CTL_LOADING_DEFAULT):

        visit_part = self.load_visit_part_by_ctl(SKLOD_EXTERNAL_GOOGLE_ANALYTICS, VISIT,
                                                min_ctl_loading, max_ctl_loading)

        self.drop_test_tmp_table()
        self.create_table_from_df(tmp_schema, tmp_table, visit_part)

        self.print_count_if_test(visit_part, "create_test_tmp_table")


    @class_method_logger
    def drop_test_tmp_table(self,
                            tmp_schema : str = SBX_TEAM_DIGITCAMP,
                            tmp_table : str = TMP_TEST_VISIT_PART):
        self.drop_table(tmp_schema, tmp_table)


    ############################## Get funnels ##############################

    @abc.abstractmethod
    def make_funnels(self, visit_part):
        """
        Return DataFrame with column:
            sbboluserid/cid, product_cd,
            funnel_rate, returncnt,
            minSessionStartTime, maxSessionStartTime, ctl_loading
        """
        pass


    @class_method_logger
    def get_funnels(self, visit_part):
        users =  self.make_funnels(visit_part)

        self.print_count_if_test(users, "get_funnels")
        return users


    ############################## Join cid_sbbol_inn_dict ##############################


    @class_method_logger
    def load_ga_cid_sbbol_inn(self):
        """ml_360_flg = False - флаг, который определенн в конструкторе класса для выбора витрины с куками
           Важно: В сценариях по РКО данный метод переопределен для получения ЕПК идентификатора куки, 
           соответственно, любые изменения должны дублироваться и в этих сценариях.
        """


        if self.ml_360_flg == True:
            cid_sbbol_inn = self.load_table(SBX_TEAM_DIGITCAMP, GA_CID_SBBOL_INN) \
                                .select(f.col("sbbol_user_id").alias('sbboluserid'),
                                        f.col("segmento_client_id").alias('commonsegmentouid'),
                                        f.col("inn").alias('cu_inn'),
                                        f.col("google_client_id").alias( "cid")) \
                                .distinct()
        else:
            cid_sbbol_inn = self.load_table(SBX_TEAM_DIGITCAMP, "all_cookie_inn_match") \
                                .select(f.col('sbboluserid'),
                                        f.col('commonsegmentouid'),
                                        f.col("inn").alias('cu_inn'),
                                        f.col( "cid")) \
                                .distinct()
            

        unified_customer = self.load_table(PRX_UNIFIED_CUSTOMER_SCHEME, UNIFIED_CUSTOMER)

        cid_inn_df = cid_sbbol_inn.join(unified_customer, 
                                         on=(cid_sbbol_inn.cu_inn == unified_customer.inn), how='inner') \
                                  .dropDuplicates(['cu_inn','sbbolUserId', 'commonSegmentoUID', 'crm_id'])

        cid_inn_df = cid_inn_df.select("sbboluserid",
                                        "commonsegmentouid",
                                        "cu_inn",
                                        "crm_id",
                                        "cid").distinct()
        return cid_inn_df


    @class_method_logger
    def join_cid_sbbol_inn_users(self, users):
        cid_sbbol_inn = self.load_ga_cid_sbbol_inn().withColumnRenamed("crm_id", "_crm_id")

        if self.CHANNEL == "SBBOL":
            join_column = "sbboluserid"
            cid_sbbol_inn = cid_sbbol_inn.filter("sbboluserid is not Null")
            groupby_column = ['sbboluserid', 'cu_inn']
        elif self.CHANNEL == 'SITE':
            join_column = "cid"
            groupby_column = ["cid", 'sbboluserid', 'cu_inn']
            cid_sbbol_inn = cid_sbbol_inn.filter("cid is not Null")
        else:
            log("### Wrong channel: {}".format(self.CHANNEL), self.logger)
            sys.exit()

        cid_sbbol_inn_agg = cid_sbbol_inn.groupBy(groupby_column) \
                                         .agg(f.first('_crm_id').alias('_crm_id'),
                                                f.concat_ws(',', f.collect_set('commonSegmentoUID')).alias('commonSegmentoUID'))



        users_cid_sbbol_inn = users.join(cid_sbbol_inn_agg, on=[join_column], how='inner')

        self.print_count_if_test(users_cid_sbbol_inn, "get_cid_sbbol_inn")
        return users_cid_sbbol_inn


    ############################## Offers priority ##############################


    @class_method_logger
    def load_offers_priority(self):
        offer_priority = self.load_table(SBX_T_TEAM_CVM, OFFER_PRIORITY) \
                             .select('inn', 'crm_id', 'product_cd', 'start_dt', 'end_dt')

        return offer_priority


    @class_method_logger
    def get_offers_priority(self, users):
        offer_priority = self.load_offers_priority() \
                             .withColumnRenamed("product_cd", "_product_cd")

        open_date = pd.datetime(2199, 1, 1)
        conditions = ((users.cu_inn == offer_priority.inn) &
                     (users.product_cd == offer_priority._product_cd) &
                     (offer_priority.start_dt <= datetime.now()) &
                      (offer_priority.end_dt >= open_date))

        users_offer_priority = users.join(offer_priority, on=conditions, how='inner') \
                                    .select("cu_inn", "_crm_id", "crm_id", "sbboluserid", "product_cd") \
                                    .withColumn('id_crm',f.coalesce('_crm_id','crm_id')) \
                                    .drop('crm_id','_crm_id')

        users_offer_priority_agg = users_offer_priority.withColumn("offer_priority", f.lit(1)) \
                                                        .groupby("cu_inn", "id_crm", "sbboluserid", "product_cd") \
                                                        .agg(f.sum("offer_priority").alias("sum_offer_priority"))

        self.print_count_if_test(users_offer_priority_agg, "offers_priority")
        return users_offer_priority_agg


    ############################## Offers nontop ##############################


    @class_method_logger
    def load_offers_nontop(self):
        offer_nontop = self.load_table(SBX_T_TEAM_CVM, MA_MMB_OFFER_NONTOP) \
                            .select('inn', 'crm_id', 'product_id')

        return offer_nontop


    @class_method_logger
    def load_product_dict(self):
        ma_product_dict = self.load_table(SBX_TEAM_DIGITCAMP, MA_PRODUCT_DICT)

        ma_product_dict_filt = ma_product_dict.select([col.lower() for col in ma_product_dict.columns]) \
                                            .filter("product_cd_mmb is not Null") \
                                            .withColumnRenamed('product_cd_mmb','product_cd') \


        return ma_product_dict_filt


    def getProdCDFromId(self, product_dict):
        def get_product(product_id, product_dict):
            if product_id is None:
                return None

            for _id in product_dict:
                if product_id == _id:
                    return product_dict[_id]

            return None

        return f.udf(lambda x: get_product(x, product_dict))


    @class_method_logger
    def get_offers_nontop(self, users):
        product_dict = {row.id: row.product_cd for row in self.load_product_dict().collect()}

        offer_nontop = self.load_offers_nontop() \
                           .withColumn('_product_cd', self.getProdCDFromId(product_dict)('product_id'))

        conditions = (users.cu_inn == offer_nontop.inn) & \
                     (users.product_cd == offer_nontop._product_cd)

        users_offer_nontop = users.join(offer_nontop, on=conditions, how='inner') \
                                    .select("cu_inn", "_crm_id", "crm_id", "sbboluserid", "product_cd") \
                                    .withColumn('id_crm',f.coalesce('_crm_id','crm_id'))\
                                    .drop('crm_id','_crm_id')

        users_offer_nontop_agg = users_offer_nontop.withColumn("offer_nontop", f.lit(1)) \
                                                    .groupby("cu_inn", "id_crm", "sbboluserid", "product_cd") \
                                                    .agg(f.sum("offer_nontop").alias("sum_offer_nontop"))

        self.print_count_if_test(users_offer_nontop_agg, "get_offers_nontop")
        return users_offer_nontop_agg


    ############################## Join offers ##############################


    @class_method_logger
    def get_offers(self, users):
        offer_priority = self.get_offers_priority(users)
        offer_nontop = self.get_offers_nontop(users)

        offers = offer_priority.join(offer_nontop,
                                      on=["cu_inn", "id_crm", "sbboluserid", "product_cd"],
                                      how='outer') \
                                .fillna({'sum_offer_priority': 0, 'sum_offer_nontop': 0})

        self.print_count_if_test(offers, "get_offers")
        return offers


    @class_method_logger
    def join_offers_users(self, users):
        try:
            offers = self.get_offers(users)
        except:
            log("### Create temp table followed by read from one (to avoid spark2.2 TreeNode Catalyst copy errors)", self.logger)

            self.drop_table(SBX_TEAM_DIGITCAMP, TMP_EXC_COPY_TABLE)
            self.create_table_from_df(SBX_TEAM_DIGITCAMP, TMP_EXC_COPY_TABLE, users)
            users = self.load_table(SBX_TEAM_DIGITCAMP, TMP_EXC_COPY_TABLE)

            offers = self.get_offers(users)

        users_offers = users.join(offers,
                                  on=["cu_inn", "sbbolUserId", "product_cd"],
                                  how="left_outer") \
                            .fillna({'sum_offer_priority': 0, 'sum_offer_nontop': 0})

        users_offers_crm = users_offers.withColumn('crm_id_nvl',f.coalesce('_crm_id','id_crm')) \
                                        .drop('_crm_id','id_crm') \
                                        .withColumnRenamed('crm_id_nvl','id_crm')

        self.print_count_if_test(users_offers_crm, "join_offers_users")
        return users_offers_crm


    ############################## Join deals ##############################


    def match_product(self, product_dict_nm):
        def get_product(product_norm, product_dict_nm):
            if product_norm is None:
                return None

            for product_nm in product_dict_nm:
                if product_nm.lower() in product_norm.lower():
                    return product_dict_nm[product_nm]

            return None

        return f.udf(lambda x: get_product(x,product_dict_nm))


    @class_method_logger
    def load_ma_deals(self):
        ma_deals = self.load_table(SBX_TEAM_DIGITCAMP, MA_CMDM_MA_DEAL) \
                        .select(f.col('inn'),
                            f.col("product_group").alias('madeal_product_norm'),
                            f.col('prod_type_name').alias('madeal_prod_type_name'),
                            f.col('appl_core_txt').alias('madeal_appl_core_txt'),
                            f.col('complete_dt').alias('madeal_complete_dt'),
                            f.col("appl_stage_name"),
                            f.col("close_reason_type_name"), 
                            f.col("PRODUCT_CD_MMB").alias("product_cd"))

        return ma_deals


    @class_method_logger
    def add_offset(self, df):
        sec = 4 * 30 * 24 * 60 * 60
        dttm_offset = f.from_unixtime(f.unix_timestamp(f.col('maxSessionStartTime')) - sec, 'yyyy-MM-dd HH:mm:ss') \
                       .cast(stypes.TimestampType())
        return df.withColumn('_6monthoffset', dttm_offset)
        

    @class_method_logger
    def join_deals_users(self, users_offers):
        users_offers = self.add_offset(users_offers)

        ma_deals = self.load_ma_deals().filter(f.col('appl_stage_name').like("%Закрыта/Заключена%") &
                                                f.isnull(f.col('close_reason_type_name'))) \
                                        .drop("appl_stage_name", "close_reason_type_name")

        conditions = ((ma_deals.inn == users_offers.cu_inn) &
                     (ma_deals.product_cd == users_offers.product_cd) &
                     (ma_deals.madeal_complete_dt >= users_offers._6monthoffset) &
                     (ma_deals.madeal_complete_dt <= users_offers.maxSessionStartTime))

        users_offers_deals = users_offers.join(ma_deals, on=conditions, how='left_outer') \
                                            .withColumn('hasProduct', f.when(~f.isnull('inn'), 1).otherwise(0)) \
                                            .drop('inn')\
                                            .drop(ma_deals.product_cd)

        self.print_count_if_test(users_offers_deals, "join_deals_users")
        return users_offers_deals


    ############################## Create final_df ##############################


#     @udf(stypes.StringType())
#     def essense(channel: str, product_cd: str, scenario_id: str):
#         message = "Scenario {scenario} retargeting product {product} in channel {channel}" \
#                     .format(scenario=scenario_id, product=product_cd, channel=channel)
#         return message

    def essense(self, df):
        """Данный метод может быть переопределен в классе сценарии-наследнике, требуется, 
        чтобы колнки, используемые при формировании essense были в датафрейме df
        
        Базовый текст сейчас выглядит так:
        Scenario {scenario} retargeting product {product} in channel {channel}"""
        
        df_with_ess = df.withColumn("essense", f.concat(f.lit("Scenario "), 
                                                        f.col("scenario_id"),
                                                        f.lit(" retargeting product "),
                                                        f.col("product_cd"),
                                                        f.lit(" in channel "),
                                                        f.col("channel")))
        return df_with_ess


    @class_method_logger
    def create_final_df(self, df):
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
                'sum_offer_priority',
                'sum_offer_nontop',
                'minSessionStartTime',
                'maxSessionStartTime',
                'load_dt',
                'ctl_loading']
        
        df_col = df.withColumn("scenario_id", f.lit(self.SCENARIO_ID)) \
                   .withColumn("channel", f.lit(self.CHANNEL))\
                   .withColumn("scenario_id", f.lit(self.SCENARIO_ID)) \
                   .withColumn("channel", f.lit(self.CHANNEL))\
                    .withColumn("numberOfSteps", f.lit(self.NUM_OF_STEPS))\
                    .withColumn("load_dt", f.lit(f.current_date().cast(stypes.TimestampType())))
                    
        df_with_ess = self.essense(df_col)
        
        final_df = df_with_ess.select(*cols) \
                              .withColumnRenamed("cu_inn", "inn") \
                              .withColumnRenamed("product_cd", "id_product")
        
#         final_df = df.withColumn("scenario_id", f.lit(self.SCENARIO_ID)) \
#                         .withColumn("channel", f.lit(self.CHANNEL)) \
#                         .withColumn("essense", ScenarioBase.essense(f.col("channel"), f.col("product_cd"), f.col("scenario_id"))) \
#                         .withColumn("numberOfSteps", f.lit(self.NUM_OF_STEPS)) \
#                         .withColumn("load_dt", f.lit(f.current_date().cast(stypes.TimestampType()))) \
#                         .select(*cols) \
#                         .withColumnRenamed("cu_inn", "inn") \
#                         .withColumnRenamed("product_cd", "id_product")

        return final_df


    ############################## Save final table ##############################


    @class_method_logger
    def save_scenarios_json(self):
        self.scenarios_json[self.SCENARIO_ID] = self.slice_max_date
        with open(SCENARIOS_JSON_FILE, 'w') as f:
            json.dump(self.scenarios_json, f, indent=4, sort_keys=True)


    @class_method_logger
    def save_final_result(self, final_df):
        self.insert_into_table_from_df(SBX_TEAM_DIGITCAMP, GA_ALL_SCENARIOS_HIST, final_df)
        self.save_scenarios_json()


    ############################## ContextManager ##############################


    def __enter__(self):
        return self


    @class_method_logger
    def close(self):
        try:
            self.drop_table(SBX_TEAM_DIGITCAMP, TMP_EXC_COPY_TABLE)
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