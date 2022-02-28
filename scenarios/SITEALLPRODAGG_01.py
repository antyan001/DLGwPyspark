#!/bin/env python3

import sys
sys.path.insert(0, "./")
from lib.scenario_base import *

from lib.CRMDictProds import AllParseAndRegExp
from transliterate import translit

class ScenarioSiteAllProdAgg(ScenarioBase):

    def __init__(self, test_mode : bool = False):
        super().__init__(SCENARIO_ID='SITEALLPRODAGG_01',
                        CHANNEL="SITE",
                        NUM_OF_STEPS=9999,
                        REFDATE='2019-09-01',
                        test_mode=test_mode)


    ######################### Funnels ##############################

    def make_funnels(self, visit_part):
        interest_cols = self.get_insterest_cols()

        exprarr = \
        [f.sum(f.when(f.col(_col) != '', 1).otherwise(0)).alias('trg_' + _col) for _col in interest_cols]+\
        [
             f.from_unixtime(f.min(f.col('sessionStartTime'))).cast(stypes.TimestampType()).alias('minSessionStartTime'),
             f.from_unixtime(f.max(f.col('sessionStartTime'))).cast(stypes.TimestampType()).alias('maxSessionStartTime'),
             f.first('ctl_loading').alias('ctl_loading')\
        ]


        visit_agg = visit_part.groupBy('cid', 'sbboluserid', 'prod_gr').agg(*exprarr) \
                                .drop(*interest_cols) \
                                .withColumnRenamed('prod_gr','product_cd') \
                                .withColumn("funnel_rate", f.lit(1.00)) \
                                .withColumn("returncnt", f.lit(9999)) \
                                .drop("sbboluserid")


        _str = ''
        for col in interest_cols:
            _str+='(trg_{} > 0) OR '.format(col)
        filter_request = _str[:-4]

        visit_filt = visit_agg.filter(filter_request)

        return visit_filt


    def get_insterest_cols(self):
        parse = AllParseAndRegExp()
        interest_cols = []
        for col, repattern in parse.reEventsDct['click_any']:
            transcol = translit(col, "ru", reversed=True)
            transcol = '_'.join(re.sub(r"[\'\-]+",'',transcol).split(' '))
            interest_cols+=[transcol]
        return interest_cols


    @class_method_logger
    def load_visit(self, schema, table):
        sql = \
                '''
                  select * from {schema}.{table}
                '''.format(schema=schema, table=table)

        visit = self.hive.sql(sql)
        return visit


    @class_method_logger
    def load_visit_part(self):
        if self.test_mode:
            visit_part = self.load_visit(SBX_TEAM_DIGITCAMP, GA_SITE_ALL_PRODS)
        else:
            visit_part = self.load_visit_part_by_date(SBX_TEAM_DIGITCAMP, GA_SITE_ALL_PRODS,
                                                        min_session_date=self.scenarios_last_date)

        self.print_count_if_test(visit_part, "load_visit_part")
        return visit_part


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
    with ScenarioSiteAllProdAgg(test_mode=args.test_mode) as scenario:
      scenario.run()









