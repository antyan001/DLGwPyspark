#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *

class ScenarioOpenAcckRko(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID='RKO_OPEN_ACC',
                        CHANNEL="SITE",
                        NUM_OF_STEPS=5,
                        REFDATE='2019-09-01',
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):
        df_rko = visit_part.filter(f.col("hitpagepath").like('/ru/s_m_business/open-accounts'))

        #Этап 1 - посещение страницы с открытием счета
        df_rko = df_rko.withColumn("page_visit", (f.col("hitpagepath").like('/ru/s_m_business/open-accounts')) & 
                                                         (f.col("hitType").like('PAGE')))
        #Этап 2 - скролл 
        df_rko = df_rko.withColumn("scroll",
                                           (f.col("hitpagepath").like('/ru/s_m_business/open-accounts')) & 
                                           (f.col("hitType").like('EVENT')) &
                                           (f.col("eventCategory").like('SITE_Corporate_open-accounts')) &
                                           (f.col("eventAction").like('informing_leads')) &
                                           (f.col("eventLabel").like('%scroll%'))
                                          )
        #Этап 3 - время посещения больше 30 секунд
        df_rko_pag_time_visit = self._get_page_visit_time(df_rko)
        cols = df_rko.columns
        df_rko = df_rko.join(df_rko_pag_time_visit, on="cid", how="inner")\
                       .withColumn("page_visit_time", f.when(f.col("page_visit_time") >= 15,
                                                                     f.lit(1)).otherwise(f.lit(0)))
        #Этап 4 - мальчик с бумажками
        df_rko = df_rko.withColumn("click_rko",
                                           (f.col("hitpagepath").like('/ru/s_m_business/open-accounts')) & 
                                           (f.col("hitType").like('EVENT')) &
                                           (f.col("eventCategory").like('SITE_Corporate_open-accounts')) &
                                           (f.col("eventAction").like('click_any')) &
                                           (f.col("eventLabel").like('%Открыть счёт%'))
                                          )
        #Этап 5 - отправить заявку по созданию
        df_rko = df_rko.withColumn("create_rko",
                                           (f.col("hitpagepath").like('/ru/s_m_business/open-accounts')) & 
                                           (f.col("hitType").like('EVENT')) &
                                           (f.col("eventCategory").like('SITE_Corporate_open-accounts')) &
                                           (f.col("eventAction").like('click_any')) &
                                           (f.col("eventLabel").like('%Отправить заявку%'))
                                          )
        # Group by user
        df_rko_user = df_rko \
                            .groupby("cid") \
                            .agg(f.sum(f.col("create_rko").cast("int")).alias('sum_create_rko'),
                                 f.sum(f.col("click_rko").cast("int")).alias('sum_click_rko'),
                                 f.sum(f.col("page_visit").cast("int")).alias('sum_page_visit'),
                                 f.sum(f.col("scroll").cast("int")).alias('sum_scroll'),
                                 f.max(col("page_visit_time")).alias("sum_page_visit_time"),
                                 f.from_unixtime(f.min(f.col('sessionStartTime'))) \
                                    .cast(stypes.TimestampType()).alias('minSessionStartTime'),
                                 f.from_unixtime(f.max(f.col('sessionStartTime')))\
                                    .cast(stypes.TimestampType()).alias('maxSessionStartTime'),
                                 f.first('ctl_loading').alias('ctl_loading'))
        # Calculate funnel rates
        dct_sum_cols = dict([(col,0) for col in df_rko_user.columns if col.startswith('sum_')])
        df_rko_user = df_rko_user.fillna(dct_sum_cols)

        num_of_steps = self.NUM_OF_STEPS
        df_rko_user = df_rko_user.withColumn('funnel_rate', f.when(f.col('sum_create_rko')>0, f.format_number(f.lit(num_of_steps)/num_of_steps,2))\
                                                       .when(f.col('sum_click_rko')>0, f.format_number(f.lit(num_of_steps-1)/num_of_steps,2))\
                                                       .when(f.col('sum_page_visit_time')>0, f.format_number(f.lit(num_of_steps - 2)/num_of_steps,2))\
                                                       .when(f.col('sum_scroll')>0, f.format_number(f.lit(num_of_steps-3)/num_of_steps,2))\
                                                       .when(f.col('sum_page_visit')>0, f.format_number(f.lit(num_of_steps-4)/num_of_steps,2))\
                                                       .otherwise(0.0)) \
                                    .withColumn("returncnt", f.col('sum_page_visit')) \
                                    .withColumn("product_cd", f.lit("RKO"))
        return df_rko_user
    
    
    ######################### get page visit time ##############################
    
    
    def _get_page_visit_time(self, page_slice):
        # return cid time visiting rko page
        page_slice_time = page_slice.select("sessionStartTime", "cid", "hitNumber", "hitTime")
        
        w = Window().partitionBy("cid", "sessionStartTime").orderBy("hitNumber")
        
        df_rko_pag_time_visit = page_slice_time.withColumn("order_number",
                                                           f.row_number().over(w))\
                                               .withColumn("hit_num_delta",
                                                           f.col("hitNumber") - f.col("order_number"))\
                                               .groupBy(f.col("sessionStartTime"), f.col("cid"), f.col("hit_num_delta"))\
                                               .agg(f.min(f.col("hitTime")).alias("first_hit_time"),
                                                    f.max(f.col("hitTime")).alias("last_hit_time"))\
                                               .withColumn("page_visit_time_by_session",
                                                           (f.col("last_hit_time") - f.col("first_hit_time")) / f.lit(1000))
        df_rko_pag_time_visit = df_rko_pag_time_visit.groupBy("cid")\
                                                     .agg(f.sum(f.col("page_visit_time_by_session")).alias("page_visit_time"))
        return df_rko_pag_time_visit
    
    
    ######################### Join cid_sbbol_inn_epk_id_dict ##############################
    
    
    def load_ga_cid_sbbol_inn(self):
        # Этот метод возвращает ЕПК ИД там, где не подцепился ИНН
        if self.ml_360_flg == True:
            cid_sbbol_inn = self.load_table(SBX_TEAM_DIGITCAMP, GA_CID_SBBOL_INN)\
                                .select(f.col("sbbol_user_id").alias('sbboluserid'),
                                        f.col("segmento_client_id").alias('commonsegmentouid'),
                                        f.when(f.col("inn").isNull(), f.col("client_epk_id")).otherwise(f.col("inn")).alias('cu_inn'),
                                        f.col("google_client_id").alias("cid")) \
                                .distinct()
        else:
            cid_sbbol_inn = self.load_table(SBX_TEAM_DIGITCAMP, "all_cookie_inn_match") \
                                .select(f.col('sbboluserid'),
                                        f.col('commonsegmentouid'),
                                        f.when(f.col("inn").isNull(), f.col("EPK_ID")).otherwise(f.col("inn")).alias('cu_inn'),
                                        f.col( "cid")) \
                                .distinct()
        unified_customer = self.load_table(PRX_UNIFIED_CUSTOMER_SCHEME, UNIFIED_CUSTOMER)
        cid_inn_df = cid_sbbol_inn.join(unified_customer, 
                                         on=(cid_sbbol_inn.cu_inn == unified_customer.inn), how='left') \
                                  .dropDuplicates(['cu_inn','sbboluserid', 'commonsegmentouid', 'crm_id'])
        cid_inn_df = cid_inn_df.filter("cu_inn is not null")
        cid_inn_df = cid_inn_df.select("sbboluserid",
                                        "commonsegmentouid",
                                        "cu_inn",
                                        "crm_id",
                                        "cid").distinct()
        return cid_inn_df
    
    
#####################################################################
############################### Main ################################
#####################################################################


def run_argparse():
    parser = argparse.ArgumentParser(__name__)

    parser.add_argument('-tm', "--test_mode", action='store_true', required=False,
                        help='Активация тестового режима', default=False)

    return parser.parse_args()


if __name__ == '__main__':
    args = run_argparse()
    with ScenarioOpenAcckRko(test_mode=args.test_mode) as scenario:
        scenario.run()