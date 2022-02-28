#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *

class ScenarioCredit2(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID='CREDIT_02',
                        CHANNEL="SBBOL",
                        NUM_OF_STEPS=5,
                        REFDATE='2019-09-01',
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):

# Клик на кредиты в главном меню СББОЛ   

        df_sbbol = visit_part.withColumn("click_credit",(f.col("eventcategory").like('[std]: credits')) &
                                                        (f.col("eventAction").like('click')))
    
# Кнопка "Создать заявку" на странице с кредитными продуктами        

        df_sbbol = df_sbbol.withColumn("new_request", (f.col("hitpagepath").like('/credits/request-lb')) & 
                                                      (f.col("eventlabel").like('%[credits main page]: [open credit product request button]%')))


# Выбор любого возможного продукта по кредиту 
        df_sbbol = df_sbbol.withColumn("credit_product", (f.col("eventLabel").like('%[cps product selection]: [credit product selected%'))&
                                                         (f.col("eventcategory").like('%[std]: credits%')))


# Кнопка "далее" в процессе заполнения заявок
        df_sbbol = df_sbbol.withColumn("credit_application", (f.col("eventcategory").like('[sq]: credits')) &
                                                             (f.col("eventaction").like('cpsform conditions')) &
                                                             (f.col("eventlabel").like('%[start]>%[submit]>[end]%'))
                                      )

# Создание итоговой заявки
        df_sbbol = df_sbbol.withColumn("credit_success", (f.col("eventCategory").like('[std]: credits')) & \
                                                        (f.col("eventAction") == 'click') & \
                                                        (f.col("eventLabel").like('[credit product selection page]: [documents: create standard form]')))

# Выделеие продукта, выбронного в меню
        pattern1 = '^\[cps product selection\]\: \[credit product selected '
        pattern2 = '\]$'
        df_sbbol = df_sbbol.withColumn('chosen_programm', f.when(f.col("credit_product").cast("int")>0,
                                                                 f.col("eventLabel"))\
                                                           .otherwise(f.lit(None)))
        df_sbbol = df_sbbol.withColumn('chosen_programm', f.regexp_replace("chosen_programm", pattern1, ""))
        df_sbbol = df_sbbol.withColumn('chosen_programm', f.regexp_replace("chosen_programm", pattern2, ""))

        df_sbbol = df_sbbol.fillna(dict([(col, 0) for col, _type in df_sbbol.dtypes if _type == 'boolean']))

        cols = [col for col, _type in df_sbbol.dtypes if _type == 'boolean']
        _str = ''
        for col in cols:
            _str+='({} = True) OR '.format(col)
        filter_request = _str[:-4]

        df_sbbol = df_sbbol.filter(filter_request)

        df_sbbol_user = df_sbbol \
                            .groupby("sbbolUserId") \
                            .agg(f.sum(f.col("click_credit").cast("int")).alias('sum_click_credit'),
                                 f.sum(f.col("new_request").cast("int")).alias('sum_new_request'),
                                 f.sum(f.col("credit_product").cast("int")).alias('sum_credit_product'),
                                 f.sum(f.col("credit_application").cast("int")).alias('sum_credit_application'),
                                 f.sum(f.col("credit_success").cast("int")).alias('sum_credit_success'),
                                 f.collect_set("chosen_programm").alias("chosen_programm"),
                                 f.from_unixtime(f.min(f.col('sessionStartTime'))) \
                                    .cast(stypes.TimestampType()).alias('minSessionStartTime'),
                                 f.from_unixtime(f.max(f.col('sessionStartTime')))\
                                    .cast(stypes.TimestampType()).alias('maxSessionStartTime'),
                                 f.first('ctl_loading').alias('ctl_loading'))
                
# добавление choosen_programm для формирования essense
        df_sbbol_user = df_sbbol_user.withColumn("chosen_programm",
                                                 f.concat(f.lit("["),
                                                          f.array_join("chosen_programm", ", "),
                                                          f.lit("]")))

        dct_sum_cols = dict([(col,0) for col in df_sbbol_user.columns if col.startswith('sum_')])
        df_sbbol_user = df_sbbol_user.fillna(dct_sum_cols)

        num_of_steps = self.NUM_OF_STEPS
        df_sbbol_user = df_sbbol_user.withColumn('funnel_rate', f.when(f.col('sum_credit_success')>0, f.format_number(f.lit(num_of_steps)/num_of_steps,2))\
                                                       .when(f.col('sum_credit_application')>0,
                                                             f.format_number(f.lit(num_of_steps-1)/num_of_steps,2))\
                                                       .when(f.col('sum_credit_product')>0,
                                                             f.format_number(f.lit(num_of_steps-2)/num_of_steps,2))\
                                                       .when(f.col('sum_new_request')>0,
                                                             f.format_number(f.lit(num_of_steps-3)/num_of_steps,2))\
                                                       .when(f.col('sum_click_credit')>0,
                                                             f.format_number(f.lit(num_of_steps-4)/num_of_steps,2))\
                                                       .otherwise(0.0)) \
                                    .withColumn("returncnt", f.col('sum_click_credit')) \
                                    .withColumn("product_cd", f.lit("CREDIT"))
        return df_sbbol_user
    
    
    def essense(self, df):
        "Scenario {scenario} retargeting product {product} in channel {channel}"
        
        df_with_ess = df.withColumn("essense", f.concat(f.lit("Scenario "), 
                                                        f.col("scenario_id"),
                                                        f.lit(" retargeting product "),
                                                        f.col("product_cd"),
                                                        f.lit(" programm chosen "),
                                                        f.col("chosen_programm"),
                                                        f.lit(" in channel "),
                                                        f.col("channel")))
        return df_with_ess
    
    
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
    with ScenarioCredit2(test_mode=args.test_mode) as scenario:
        scenario.run()
