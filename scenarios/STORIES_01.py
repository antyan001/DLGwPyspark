#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *


class ScenarioStories(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID="STORIES_01",
                        CHANNEL="SBBOL",
                        NUM_OF_STEPS=2,
                        REFDATE="2019-09-01",
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):

        df_sbbol_show = visit_part.withColumn('story_show',
                                              (f.col("eventCategory").like('%[operations]: offers%')) & \
                                              (f.col("eventAction") == 'impression-card'))

        df_sbbol_click = df_sbbol_show.withColumn('is_click',
                                                  (f.col("eventCategory").like('%[operations]: offers%')) & \
                                                  (f.col("eventAction") == 'impression-card-short'))

        df_sbbol_open = df_sbbol_click.withColumn('story_open',
                                                (f.col("eventCategory").like('%[operations]: offers%')) & \
                                                (f.col("eventAction") == 'open landing'))

        # Проверка на целевое действие по магазину продуктов
        df_sbbol_checkout = df_sbbol_open.withColumn('is_checkout',
                                                    f.col("eventAction").like('%checkout%'))

        # Извлекаем название продукта из eventLabel
        # TODO: добавить условие, достаем продукт только для is_click и is_checkout (прибавка скорости)
        df_sbbol_product = df_sbbol_checkout.withColumn('product',
                                                        extract_product('eventLabel'))

        # Проверка на статус активации продукта
        # TODO: проверять только если извлечен продукт
        df_sbbol_status = df_sbbol_product.withColumn('status_activated',
                                                      f.col("eventLabel").like("%status: activated%"))

        df_sbbol_final = df_sbbol_status.fillna({'is_checkout': False, 'is_click': False, 'story_show': False,
                                                'story_open': False, 'status_activated': False})

        df_shop_user = df_sbbol_final.groupby("sbbolUserId", "product") \
                                    .agg(f.sum(f.col("is_click").cast(stypes.IntegerType())).alias("sum_click"),
                                         f.sum(f.col("story_open").cast(stypes.IntegerType())).alias("sum_story_open"),
                                         f.sum(f.col("is_checkout").cast(stypes.IntegerType())).alias("sum_checkout"),
                                         f.max("status_activated").alias("status_activated"),
                                         f.from_unixtime(f.min(f.col('sessionStartTime'))) \
                                            .cast(stypes.TimestampType()).alias('minSessionStartTime'),
                                         f.from_unixtime(f.max(f.col('sessionStartTime')))\
                                            .cast(stypes.TimestampType()).alias('maxSessionStartTime'),
                                         f.first('ctl_loading').alias('ctl_loading'))

        dct_sum_cols = dict([(col,0) for col in df_shop_user.columns if col.startswith('sum_')])
        df_shop_user = df_shop_user.fillna(dct_sum_cols)

        df_shop_user_funnels = df_shop_user .withColumn('funnel_rate',
                                                      f.when(f.col('status_activated').cast(stypes.IntegerType()) == 1,
                                                             f.format_number(f.lit(2) / self.NUM_OF_STEPS, 2)) \
                                                      .when(f.col('sum_checkout') > 0,
                                                            f.format_number(f.lit(2) / self.NUM_OF_STEPS, 2))
                                                      .when(f.col('sum_click') > 0,
                                                            f.format_number(f.lit(1) / self.NUM_OF_STEPS, 2))
                                                      .when(f.col('sum_story_open') > 0,
                                                            f.format_number(f.lit(1) / self.NUM_OF_STEPS, 2))
                                                      .otherwise(0.0)) \
                                          .withColumn("returncnt", f.col('sum_click'))

        df_shop_user_filt = df_shop_user_funnels.where("(sbbolUserId is not Null) AND (product is not Null)") \
                                                .where('funnel_rate > 0.0')

        product_dict = self.load_product_dict()

        df_shop_user_inn_product = df_shop_user_filt.join(product_dict,
                                                          on=f.lower(df_shop_user_filt.product) == f.lower(product_dict.product_cd_asup),
                                                          how="inner") \
                                                    .drop("crm_product_id", "product_cd_asup")

        return df_shop_user_inn_product



######################### Scenario values ##############################


######################### Scenario functions ##############################


@typed_udf(stypes.StringType())
def extract_product(label):
    if label is not None:
        pattern = re.compile(r'product: ([\d\w]+)[\,\]]+')
        out = pattern.findall(label.lower())
        if len(out) > 0:
            return out[0]
        else:
            return None
    else:
        return None


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
    with ScenarioStories(test_mode=args.test_mode) as scenario:
      scenario.run()
