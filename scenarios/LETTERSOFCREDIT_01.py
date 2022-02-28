#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *


class ScenarioLettersOfCredit01(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID="LETTERSOFCREDIT_01",
                        CHANNEL="SBBOL",
                        NUM_OF_STEPS=5,
                        REFDATE='2019-09-01',
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):
        df_sbbol = visit_part.withColumn("click_tradefinance",
                                        (f.col("eventAction") == "click") & \
                                        (f.col("eventLabel") == '[layout.primarymenusbbol3]: [productcode:tradefinance_lettersofcredit]'))

        df_sbbol = df_sbbol.withColumn("click_tradefinance_alt",
                                        (f.col('hitPagePath') == '/trade-finance/lettersOfCredit'))

        df_sbbol = df_sbbol.withColumn("lettersOfCredit",
                                        (f.col("eventAction") == "click")& \
                                        (f.col("eventCategory").like("%[std]: trade finance: letters of credit%"))& \
                                        (f.col("eventLabel") == '[productslist]: [new_lc]'))


        ### SCENARIO 1"

        df_sbbol = df_sbbol.withColumn("documentsCreationAsk",
                                        (f.col("eventAction") == "click")& \
                                         (f.col("eventCategory").like("%[std]: trade finance: letters of credit%"))& \
                                         (f.col("eventLabel") == '[productslist]: [tradefinance.lettersofcredit:documents.creation.sendtobank]'))


        df_sbbol = df_sbbol.withColumn("documentsCreationAsk_alt",
                                        (f.col('hitPagePath') == '/trade-finance/lettersOfCredit/products/:documentTypeRoute/draft'))


        df_sbbol = df_sbbol.withColumn("documentsSendToFill",
                                        (f.col("eventAction") == "click")& \
                                        (f.col("eventCategory").like("%[std]: trade finance: letters of credit%"))& \
                                        (f.col("eventLabel") == '[issued_domestic_lc_issue_applicationdraftpage]: [отправить на заполнение]'))


        ### SCENARIO 2

        df_sbbol = df_sbbol.withColumn("documentsCreationSelf",
                                        (f.col("eventAction") == "click")& \
                                      (f.col("eventCategory").like("%[std]: trade finance: letters of credit%"))& \
                                      (f.col("eventLabel") == '[productslist]: [tradefinance.lettersofcredit:documents.creation.manual]'))

        df_sbbol = df_sbbol.withColumn("documentsCreationSelf_alt",
                                        (f.col('hitPagePath') == '/trade-finance/lettersOfCredit/products/:documentTypeRoute/create'))

        df_sbbol = df_sbbol.withColumn("documentsCreationSave",
                                        (f.col("eventAction") == "click")& \
                                      (f.col("eventCategory").like("%[std]: trade finance: letters of credit%"))& \
                                      (f.col("eventLabel") == '[issued_domestic_lc_issue_application createpage]: [action.create]'))


        df_sbbol = df_sbbol.withColumn("documentsCreationSave_alt",
                                        (f.col('hitPagePath') == '/trade-finance/lettersOfCredit/products/:documentTypeRoute/:documentId/details'))



        df_sbbol = df_sbbol.fillna(dict([(col, 0) for col, _type in df_sbbol.dtypes if _type == 'boolean']))

        cols = [col for col, _type in df_sbbol.dtypes if _type == 'boolean']
        _str = ''
        for col in cols:
            _str+='({} = True) OR '.format(col)
        filter_request = _str[:-4]

        df_sbbol = df_sbbol.filter(filter_request)

        df_sbbol_user = df_sbbol.groupby("sbbolUserId") \
                                .agg(f.sum(f.col("click_tradefinance").cast("int")).alias('sum_click_tradefinance'),
                                     f.sum(f.col("click_tradefinance_alt").cast("int")).alias('sum_click_tradefinance_alt'),
                                     f.sum(f.col("lettersOfCredit").cast("int")).alias('sum_lettersOfCredit'),
                                     f.sum(f.col("documentsCreationAsk").cast("int")).alias('sum_documentsCreationAsk'),
                                     f.sum(f.col("documentsCreationAsk_alt").cast("int")).alias('sum_documentsCreationAsk_alt'),
                                     f.sum(f.col("documentsSendToFill").cast("int")).alias('sum_documentsSendToFill'),
                                     f.sum(f.col("documentsCreationSelf").cast("int")).alias('sum_documentsCreationSelf'),
                                     f.sum(f.col("documentsCreationSelf_alt").cast("int")).alias('sum_documentsCreationSelf_alt'),
                                     f.sum(f.col("documentsCreationSave").cast("int")).alias('sum_documentsCreationSave'),
                                     f.sum(f.col("documentsCreationSave_alt").cast("int")).alias('sum_documentsCreationSave_alt'),
                                     f.from_unixtime(f.min(f.col('sessionStartTime'))) \
                                        .cast(stypes.TimestampType()).alias('minSessionStartTime'),
                                     f.from_unixtime(f.max(f.col('sessionStartTime')))\
                                        .cast(stypes.TimestampType()).alias('maxSessionStartTime'),
                                     f.first('ctl_loading').alias('ctl_loading'),
                                    )

        dct_sum_cols = dict([(col,0) for col in df_sbbol_user.columns if col.startswith('sum_')])
        df_sbbol_user = df_sbbol_user.fillna(dct_sum_cols)


        df_sbbol_user = df_sbbol_user.withColumn('funnel_rate',
                                                f.when((f.col('sum_documentsSendToFill')>0)|
                                                       (f.col('sum_documentsCreationSave')>0)|
                                                       (f.col('sum_documentsCreationSave_alt')>0),
                                                           f.format_number(f.lit(self.NUM_OF_STEPS)/self.NUM_OF_STEPS,2))\
                                                 .when((f.col('sum_documentsCreationSelf')>0)|
                                                       (f.col('sum_documentsCreationSelf_alt')>0)|
                                                       (f.col('sum_documentsCreationAsk')>0)|
                                                       (f.col('sum_documentsCreationAsk_alt')>0),
                                                           f.format_number(f.lit(self.NUM_OF_STEPS-1)/self.NUM_OF_STEPS,2))\
                                               .when(f.col('sum_lettersOfCredit')>0,
                                                         f.format_number(f.lit(self.NUM_OF_STEPS-2)/self.NUM_OF_STEPS,2))\
                                               .when((f.col('sum_click_tradefinance')>0)|
                                                     (f.col('sum_click_tradefinance_alt')>0),
                                                         f.format_number(f.lit(self.NUM_OF_STEPS-3)/self.NUM_OF_STEPS,2))\
                                               .otherwise(0.0)) \
                                    .withColumn("returncnt", f.col('sum_click_tradefinance')+f.col('sum_click_tradefinance_alt')) \
                                    .withColumn("product_cd", f.lit("CREDLETTER"))



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
    with ScenarioLettersOfCredit01(test_mode=args.test_mode) as scenario:
        scenario.run()
