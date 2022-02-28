#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *


class ScenarioAcquiring01(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID="ACQUIRING_01",
                        CHANNEL="SBBOL",
                        NUM_OF_STEPS=3,
                        REFDATE='2019-09-01',
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):
        df_sbbol = visit_part.withColumn("click_acquiring",
                                        (f.col("eventAction") == "click") & \
                                        (f.col("eventCategory") == "[std]: merchant acquiring"))

        df_sbbol = df_sbbol.withColumn("click_acquiring_alt",
                                        (f.col('hitPagePath') == '/acquiring/merchant/pos-list'))

        df_sbbol = df_sbbol.withColumn("click_reg_request",
                                        (f.col("eventAction") == "show")& \
                                        (f.col("eventCategory").like("[std]: merchant acquiring: pos registration request")))


        df_sbbol = df_sbbol.withColumn("click_reg_request_alt",
                                        (f.col("eventAction") == "show")& \
                                        (f.col("eventCategory").like("[std]: merchant acquiring: equipment registration request ")))


        df_sbbol = df_sbbol.withColumn("click_acquiring_create",
                                        (f.col("eventAction") == "create-new")& \
                                       (f.col("eventCategory").like("[operations]: posregistrationrequest")))

        df_sbbol = df_sbbol.withColumn("click_acquiring_create_alt",
                                        (f.col("eventAction") == "create-new")& \
                                       (f.col("eventCategory").like("[operations]: equipmentregistrationrequest")))


        df_sbbol = df_sbbol.fillna(dict([(col, 0) for col, _type in df_sbbol.dtypes if _type == 'boolean']))

        cols = [col for col, _type in df_sbbol.dtypes if _type == 'boolean']
        _str = ''
        for col in cols:
            _str += '({} = True) OR '.format(col)
        filter_request = _str[:-4]

        df_sbbol = df_sbbol.filter(filter_request)

        df_sbbol_user = df_sbbol.groupby("sbbolUserId") \
                                .agg(f.sum(f.col("click_acquiring").cast("int")).alias('sum_click_acquiring'),
                                     f.sum(f.col("click_acquiring_alt").cast("int")).alias('sum_click_acquiring_alt'),
                                     f.sum(f.col("click_reg_request").cast("int")).alias('sum_click_reg_request'),
                                     f.sum(f.col("click_reg_request_alt").cast("int")).alias('sum_click_reg_request_alt'),
                                     f.sum(f.col("click_acquiring_create").cast("int")).alias('sum_click_acquiring_create'),
                                     f.sum(f.col("click_acquiring_create_alt").cast("int")).alias('sum_click_acquiring_create_alt'),
                                     f.from_unixtime(f.min(f.col('sessionStartTime'))) \
                                        .cast(stypes.TimestampType()).alias('minSessionStartTime'),
                                     f.from_unixtime(f.max(f.col('sessionStartTime')))\
                                        .cast(stypes.TimestampType()).alias('maxSessionStartTime'),
                                     f.first('ctl_loading').alias('ctl_loading'),
                                    )

        dct_sum_cols = dict([(col,0) for col in df_sbbol_user.columns if col.startswith('sum_')])
        df_sbbol_user = df_sbbol_user.fillna(dct_sum_cols)


        df_sbbol_user = df_sbbol_user.withColumn('funnel_rate',
                                                f.when((f.col('sum_click_acquiring_create')>0)|
                                                       (f.col('sum_click_acquiring_create_alt')>0),
                                                           f.format_number(f.lit(self.NUM_OF_STEPS)/self.NUM_OF_STEPS,2))\
                                                .when((f.col('sum_click_reg_request')>0)|
                                                       (f.col('sum_click_reg_request_alt')>0),
                                                           f.format_number(f.lit(self.NUM_OF_STEPS-1)/self.NUM_OF_STEPS,2))\
                                                .when((f.col('sum_click_acquiring')>0)|
                                                     (f.col('sum_click_acquiring_alt')>0),
                                                         f.format_number(f.lit(self.NUM_OF_STEPS-2)/self.NUM_OF_STEPS,2))\
                                                .otherwise(0.0)) \
                                    .withColumn("returncnt", f.col('sum_click_acquiring')+f.col('sum_click_acquiring_alt')) \
                                    .withColumn("product_cd", f.lit("ACQUIRING"))

        return df_sbbol_user



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
    with ScenarioAcquiring01(test_mode=args.test_mode) as scenario:
        scenario.run()
