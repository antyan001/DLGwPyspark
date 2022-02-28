#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *


class ScenarioHelp(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID="HELP_SBBOL",
                        CHANNEL="SBBOL",
                        NUM_OF_STEPS=1,
                        REFDATE="2020-08-06",
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):
        visit_step1 = visit_part.filter("sbboluserid is not NULL")

        visit_step2 = visit_step1.withColumn("step2_trigger",
                                         (f.col("eventCategory").isin(event2_eventCategory) &
                                         (f.col("eventAction") == event2_eventAction) &
                                         f.col("eventLabel").isin(event2_eventLabel)).cast("int"))

        visit_step3 = visit_step2.withColumn("step3_trigger",
                                         ((f.col("eventCategory") == event3_eventCategory) &
                                         (f.col("eventAction") == event3_eventAction) &
                                         f.col("eventLabel") \
                                          .rlike("\[faq list\].*\[.*group: ({})+.*".format("|".join(product_asup)))).cast("int"))

        visit_asup = visit_step3.withColumn("asup_product",
                                            get_product(f.col("eventLabel"), f.col("step2_trigger"), f.col("step3_trigger"))) \
                                .filter("asup_product is not Null")

        visit_agg = visit_asup.groupby("sbboluserid", "asup_product") \
                            .agg(f.from_unixtime(f.min(f.col('sessionStartTime'))) \
                                    .cast(stypes.TimestampType()).alias('minSessionStartTime'),
                                 f.from_unixtime(f.max(f.col('sessionStartTime')))\
                                    .cast(stypes.TimestampType()).alias('maxSessionStartTime'),
                                 f.first('ctl_loading').alias('ctl_loading'))

        visit_fin = visit_agg.withColumn("product_cd", get_product_cd(f.col('asup_product'))) \
                            .filter("product_cd is not Null").drop("asup_product") \
                            .withColumn("returncnt", f.lit(9999)) \
                            .withColumn("funnel_rate", f.lit(1.00))

        return visit_fin



######################### Scenario values ##############################

## Этап 2
event2_eventCategory = ['[std]: help', '[std]: tutorial']
event2_eventAction = "click"

event2_eventLabel = \
['[help light box]: [group: statements]',
 '[help light box]: [group: salaryagreement]',
 '[tutorial groups list]: [group: bussinesscards]',
 '[tutorial groups list]: [group: credits]',
 '[tutorial groups list]: [group: admcashier]',
 '[tutorial groups list]: [group: deposits]',
 '[tutorial groups list]: [group: einvoicing]',
 '[tutorial groups list]: [group: tokens]',
 '[tutorial groups list]: [group: acquiring]',
 '[tutorial groups list]: [group: cashorder]',
 '[tutorial groups list]: [group: curpayments]',
 '[tutorial groups list]: [group: lkpu]']

## Этап 3

event3_eventCategory = "[std]: help"
event3_eventAction = "click"
event3_eventLabel = \
['[faq list]: [%group: statements%]',
 '[faq list]:[%group: salaryagreement%]',
 '[faq list]:[%group: bussinesscards%]',
 '[faq list]:[%group: bussinesscards%]',
 '[faq list]:[%group: admcashier%]',
 '[faq list]:[%group: deposits%]',
 '[faq list]:[%group: einvoicing%]',
 '[faq list]:[%group: tokens%]',
 '[faq list]:[%group: acquiring%]',
 '[faq list]:[%group: cashorder%]',
 '[faq list]:[%group: curpayments%]',
 '[faq list]:[%group: lkpu%]']

# убираем ошибку в ТЗ
event3_eventLabel.remove('[faq list]:[%group: bussinesscards%]')
event3_eventLabel.append('[faq list]:[%group: credits%]')

product_asup = \
['statements',
 'salaryagreement',
 'bussinesscards',
 'admcashier',
 'deposits',
 'einvoicing',
 'tokens',
 'acquiring',
 'cashorder',
 'curpayments',
 'lkpu',
 'credits']

## Словарь перевода извлечённых продуктов в product_cd
product_to_cd = {
    'cashorder':  'CASHORDER',
    'bussinesscards': 'CORPCARD',
    'curpayments':  'VED',
    'deposits': 'DEPOSIT',
    'einvoicing': 'EINVOICING',
    'statements': 'STATEMENT_SCHEDULE',
    'acquiring':  'ACQUIRING',
    'lkpu': 'SPPU',
    'credits':  'CREDIT',
    'admcashier': 'ENCASHSELF',
    'salaryagreement': 'SALARYPROJ',
}


######################### Scenario functions ##############################

@udf(stypes.StringType())
def get_product(eventLabel, step2_trigger, step3_trigger):
    if eventLabel is None:
        return None

    if step2_trigger:
        pattern = ".*\[group: (.*)\].*"
        res = re.search(pattern, eventLabel)
        return res.group(1)

    elif step3_trigger:
        pattern = ".*group: (.*),.*"
        res = re.search(pattern, eventLabel)
        return res.group(1)

    return None


@udf(stypes.StringType())
def get_product_cd(extracted):
    if extracted is None or extracted == "tokens": # исключение - не найдено в словаре
        return None
    return product_to_cd[extracted]


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
    with ScenarioHelp(test_mode=args.test_mode) as scenario:
        scenario.run()
