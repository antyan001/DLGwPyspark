#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

from lib.tools import *


class SourcesUpdate:
    ################################## Init ##################################

    def __init__(self):
        self.sing = tendo.singleton.SingleInstance()
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = "SOURCES_UPDATE"

        self.init_logger()
        log("# __init__ : begin", self.logger)

        self.load_sources_json()

        self.start_spark()

        self.ld = loader.Loader(init_dsn=True, encoding='cp1251',  sep=',')
        self.db = loader.OracleDB(ISKRA_LOGIN, ISKRA_PASS, self.ld._get_dsn(ISKRA))

        self.db.connect()
        self.cursor = self.db.cursor

        log("# __init__ : begin", self.logger)


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=2,
                        kerberos_auth=True,
                        numofcores=8,
                        numofinstances=10)

        self.hive = self.sp.sql


    @class_method_logger
    def revive_spark(self):
        if self.sp.sc._jsc.sc().isStopped():
            self.start_spark()
            return True
        return False



    ############################## Run method ##############################


    @class_method_logger
    def run(self):
        self.run_rewritable_sourses(table_list=REWRITABLE_SOURCES_LIST)
        self.run_updatable_sourses(table_list=UPDATABLE_SOURCES_LIST)


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


    ############################## Load and save json ##############################


    @class_method_logger
    def load_sources_json(self):
        with open(SOURCES_JSON_FILE, 'r') as f:
            self.sources_json = json.load(f)


    @class_method_logger
    def save_sources_json(self):
        with open(SOURCES_JSON_FILE, 'w') as f:
            json.dump(self.sources_json, f, indent=4, sort_keys=True)


    ############################## Table ##############################

    @class_method_logger
    def load_table(self, schema, table):
        return load_table(schema=schema, table=table, hive=self.hive)


    @class_method_logger
    def drop_table(self, schema, table):
        drop_table(schema=schema, table=table, hive=self.hive)


    @class_method_logger
    def create_table_from_df(self, schema, table, df):
        create_table_from_df(schema=schema, table=table, df=df, hive=self.hive)


    @class_method_logger
    def insert_into_table_from_df(self, schema, table, df):
        insert_into_table_from_df(schema=schema, table=table, df=df, hive=self.hive)


    @class_method_logger
    def rename_table(self, schema, old_name, new_name):
        rename_table(schema=schema, old_name=old_name, new_name=new_name, hive=self.hive)


    ############################## Rewritable ##############################


    @class_method_logger
    def load_pddf_from_iskra(self, table, size : int = None):
        sql = 'select /*+ parallel (8) */ * from {} '.format(table)
        if size is not None:
            sql += "where rownum < {}".format(size)
        pddf = pd.read_sql(sql, con=self.db.connection)
        return pddf


    @class_method_logger
    def run_rewritable_sourses(self, table_list):
        for table in table_list:
            log("-"*54 + " {} ".format(table) + "-"*54, self.logger)
            if not self.is_need_to_update(table=table):
                continue

            self.tmp_table = "tmp_" + table
            self.drop_table(SBX_TEAM_DIGITCAMP, self.tmp_table)

            pddf = self.load_pddf_from_iskra(table, size=1000)
            typesmap_rdd, typesmap_pd = self.create_hive_table(pddf=pddf, table=self.tmp_table)

            was_updating = False
            self.cursor.execute("select /*+ parallel (8) */ * from {}".format(table))
            counter = 0
            success = 0
            while True:
                log('### {}. Fetchmany from Oracle table {}'.format(counter, table), self.logger)

                data = self.cursor.fetchmany(ISKRA_BATCH_SIZE)
                if (data is None) or (len(data) == 0):
                    break
                else:
                    pddf = pd.DataFrame(data, columns=pddf.columns.values)
                    was_updating = True

                log('Get bucket of size {} from Oracle table {}'.format(pddf.shape, table), self.logger)

                self.revive_spark()
                try:
                    self.update_hive_table(pddf, typesmap_rdd, typesmap_pd, self.tmp_table)
                except Exception as ex:
                    log("##### EXCEPTION in update_hive_table: \n{}".format(str(ex)), self.logger)
                    success -= 1
                    pass

                counter += 1
                success += 1

            log("### Table done: {table}\n### Stage status => All: {all}; Success: {suc}; Error: {err}" \
                    .format(table=table, all=counter, suc=success, err=counter-success), self.logger)


            if was_updating:
                self.drop_table(SBX_TEAM_DIGITCAMP, table)
                self.rename_table(SBX_TEAM_DIGITCAMP, self.tmp_table, table)

                self.sources_json[table] = self.currdate
                self.save_sources_json()


    ############################## Updatable ##############################


    @class_method_logger
    def get_max_date_in_hive(self, date_col : str, schema : str, table : str):
        sql = '''select max({date_col}) from {schema}.{table}''' \
                .format(date_col=date_col, schema=schema, table=table)
        max_dt = self.hive.sql(sql).collect()[0]

        return datetime.strftime(max_dt['max({date_col})'.format(date_col=date_col)],
                                format='%Y-%m-%d %H:%M:%S.%f')


    @class_method_logger
    def run_updatable_sourses(self, table_list):
        for table in table_list:
            log("-"*54 + " {} ".format(table) + "-"*54, self.logger)
            if not self.is_need_to_update(table=table):
                continue

            date_col = DATE_COLUMN_IN_UPDATABLE_SOURCES[table]
            max_date = self.get_max_date_in_hive(date_col=date_col, schema=SBX_TEAM_DIGITCAMP, table=table)

            if '.000000' in max_date:
                max_date = max_date.split('.000000')[0]
                time_format = 'yyyy-mm-dd hh24:mi:ss'
            elif '.' in max_date:
                time_format = 'yyyy-mm-dd hh24:mi:ss.ff6'

            sql = """(select /*+ parallel (8) */ * from {table}
                      where {date_col} > to_timestamp('{max_date}', '{format}'))""" \
                    .format(date_col=date_col, max_date=max_date, format=time_format, table=table)
            df = self.sp.get_oracle(OracleDB(ISKRA), sql)

            if table == 'MA_CMDM_MA_DEAL':
                df = self.add_load_dt(df)
                df = self.add_product_norm(df)

            self.revive_spark()
            cols = self.load_table(SBX_TEAM_DIGITCAMP, table).columns

            self.insert_into_table_from_df(SBX_TEAM_DIGITCAMP, table, df.select(*cols))

            self.sources_json[table] = self.currdate
            self.save_sources_json()


    ############################## Create table ##############################


    @class_method_logger
    def get_typesmap(self, df):
        typesmap_rdd={}
        typesmap_pd={}
        for column_name, column in df.iteritems():
            if column.dtype.kind == 'O':
                typesmap_rdd[column_name] = stypes.StringType()
                typesmap_pd[column_name]  = str
            elif column.dtype.kind == 'i':
                typesmap_rdd[column_name] = stypes.IntegerType()
                typesmap_pd[column_name]  = np.int
            elif column.dtype.kind == 'M':
                typesmap_rdd[column_name] = stypes.LongType()
                typesmap_pd[column_name]  = np.datetime64
            elif column.dtype.kind == 'f':
                typesmap_rdd[column_name] = stypes.FloatType()
                typesmap_pd[column_name]  = np.float

        return typesmap_rdd, typesmap_pd


    @class_method_logger
    def create_hive_table(self, pddf, table):
        typesmap_rdd, typesmap_pd = self.get_typesmap(pddf)
        while True:
            try:
                spdf = self.hive.createDataFrame(pddf, schema=stypes.StructType([stypes.StructField(col, typesmap_rdd[col]) for col in pddf.columns]))
                break
            except TypeError as ex:
                if 'LongType can not accept object' in str(ex):
                    datecols = [k for k,v in typesmap_pd.items() if np.issubdtype(v, np.datetime64)]
                    for col in datecols:
                        typesmap_rdd[col] = stypes.TimestampType()
            except ValueError as ex:
                    if 'object of IntegerType out of range' in str(ex):
                        intcols = [k for k,v in typesmap_pd.items() if np.issubdtype(v, np.int64)]
                        for col in intcols:
                            typesmap_rdd[col] = stypes.LongType()
            finally:
                try:
                    spdf = self.hive.createDataFrame(pddf, schema=stypes.StructType([stypes.StructField(col, typesmap_rdd[col]) for col in df.columns]))
                    break
                except:
                    pass

        for column_name, _type in spdf.dtypes:
            if _type == 'bigint' and (typesmap_pd[column_name] != np.int):
                tmp = spdf.select(column_name).take(1)
                if (len(str(tmp[0][column_name])) > 10) & (len(str(tmp[0][column_name])) <= 13):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000).cast(TimestampType()))
                elif (len(str(tmp[0][column_name])) > 13) & (len(str(tmp[0][column_name])) <= 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000).cast(TimestampType()))
                elif (len(str(tmp[0][column_name])) > 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000000).cast(TimestampType()))
                else:
                  spdf = spdf.withColumn('Date', (f.col(column_name)).cast(TimestampType()))
                spdf = spdf.drop(column_name)
                spdf = spdf.withColumnRenamed('Date', column_name)

        self.create_table_from_df(SBX_TEAM_DIGITCAMP, table, spdf.limit(0))

        return typesmap_rdd, typesmap_pd


    ############################## Update table ##############################


    @class_method_logger
    def is_need_to_update(self, table):
        freq = FREQUNCY_0F_UPDATES[table]

        last_update_dt = datetime.strptime(self.sources_json[table], '%Y-%m-%d')
        currdate_dt = datetime.strptime(self.currdate, '%Y-%m-%d')

        log("last_update {}, currdate {}".format(last_update_dt, currdate_dt), self.logger)

        return (currdate_dt - last_update_dt).days >= freq


    @class_method_logger
    def update_hive_table(self, pddf, typesmap_rdd, typesmap_pd, table):
        def datetime2timestamp(x):
            if (x is not None) and isinstance(x, pd.datetime):
                if (x.year < 2040) and (x.year > 2000):
                    return pd.to_datetime(x)
                else:
                    return pd.to_datetime(datetime(2199, 1, 1, 0, 0))
            else:
                return pd.to_datetime(pd.NaT)

        for col in pddf.columns:
            if pddf[col].dtype.kind == 'i' or pddf[col].dtype.kind == 'f':
                pddf[col] = pddf[col].fillna(0)
                pddf[col] = pddf[col].astype(typesmap_pd[col])
            elif pddf[col].dtype.kind == 'M':
                pass
                #pddf[col] = pddf[col].apply(lambda x: pd.to_datetime(x))
            elif ( (pddf[col].dtype.kind == 'O') and
                   (typesmap_pd[col] != str) and
                   (typesmap_pd[col] == np.datetime64) ):
                pddf[col] = pddf[col].apply(lambda x: datetime2timestamp(x))

        try:
            spdf = self.hive.createDataFrame(pddf, schema=stypes.StructType([stypes.StructField(col, typesmap_rdd[col]) for col in pddf.columns]))
        except ValueError as ex:
            if 'object of IntegerType out of range' in str(ex):
                intcols = [k for k,v in typesmap_pd.items() if np.issubdtype(v,np.int64)]
                for col in intcols:
                    typesmap_rdd[col] = stypes.LongType()
                spdf = self.hive.createDataFrame(pddf, schema=stypes.StructType([stypes.StructField(col, typesmap_rdd[col]) for col in pddf.columns]))

        for column_name, _type in spdf.dtypes:
            if _type == 'bigint' and (typesmap_pd[column_name]!=np.int):
                tmp = spdf.select(column_name).take(1)
                if (len(str(tmp[0][column_name])) > 10)&(len(str(tmp[0][column_name])) <= 13):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000).cast(stypes.TimestampType()))
                elif (len(str(tmp[0][column_name])) > 13)&(len(str(tmp[0][column_name])) <= 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000).cast(stypes.TimestampType()))
                elif (len(str(tmp[0][column_name])) > 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000000).cast(stypes.TimestampType()))
                else:
                  spdf = spdf.withColumn('Date', (f.col(column_name)).cast(stypes.TimestampType()))
                spdf = spdf.drop(column_name)
                spdf = spdf.withColumnRenamed('Date', column_name)

        self.insert_into_table_from_df(SBX_TEAM_DIGITCAMP, table, spdf)

    ############################## Ma_deals ##############################


    def add_load_dt(self, df):
        currdate_dt = datetime.strptime(self.currdate, '%Y-%m-%d')
        df = df.withColumn('LOAD_DT',f.lit(currdate_dt).cast(stypes.TimestampType()))
        return df


    def udf_map_product_group(self, b_rdd, cols):
        def map_product_group(p_crm_prod, b_rdd, cols):
            if (p_crm_prod is not None) and (p_crm_prod not in ('', ' ')):
                v_crm_prod = p_crm_prod.upper()
                if v_crm_prod in ('ЭКСПРЕСС ОВЕРДРАФТ','ОВЕРДРАФТНОЕ КРЕДИТОВАНИЕ',
                                  'ОБОРОТНОЕ КРЕДИТОВАНИЕ', 'КРЕДИТ'):
                    v_crm_prod =  'КРЕДИТОВАНИЕ'
                elif v_crm_prod in ('ДИСТАНЦИОННОЕ БАНКОВСКОЕ ОБСЛУЖИВАНИЕ'):
                    v_crm_prod =  'ДИСТАНЦИОННОЕ ОБСЛУЖИВАНИЕ';
                elif v_crm_prod in ('ОТКРЫТИЕ СЧЕТА В ВАЛЮТЕ','ОТКРЫТИЕ СЧЕТА В РУБЛЯХ',
                                    'СПЕЦСЧЕТА','РКО','РАСЧЕТНО-КАССОВОЕ ОБСЛУЖИВАНИЕ'):
                    v_crm_prod = 'РКО'
                elif v_crm_prod in ('ПРОДУКТЫ ИНКАССАЦИИ'):
                    v_crm_prod =  'ИНКАССАЦИЯ'
                elif v_crm_prod in ('НЕСНИЖАЕМЫЙ ОСТАТОК'):
                    v_crm_prod = 'ПРОДУКТЫ ПРИВЛЕЧЕНИЯ СРЕДСТВ';
                elif 'ПАКЕТ УСЛУГ' in v_crm_prod:
                    v_crm_prod = 'ПАКЕТЫ УСЛУГ'
                elif v_crm_prod in ('ВЭД И ВАЛЮТНЫЙ КОНТРОЛЬ'):
                    v_crm_prod = 'ВАЛЮТНЫЙ КОНТРОЛЬ'
                elif v_crm_prod in ('АККРЕДИТИВЫ'):
                    v_crm_prod = 'ДОКУМЕНТАРНЫЕ ОПЕРАЦИИ'
                elif v_crm_prod in ('КОРПОРАТИВНАЯ КАРТА'):
                    v_crm_prod = 'БИЗНЕС-КАРТА'
                elif v_crm_prod in ('БИЗНЕС-КАРТА'):
                    v_crm_prod = 'БИЗНЕС-КАРТА'
                elif v_crm_prod in ('ДЕПОЗИТЫ (СРОЧНЫЕ)'):
                    v_crm_prod = 'ПРОДУКТЫ ПРИВЛЕЧЕНИЯ СРЕДСТВ'
                elif v_crm_prod in ('АКТИВИЗАЦИЯ РКО'):
                    v_crm_prod = None

                dct = {col: [b_rdd.value[i][col] for i in range(len(b_rdd.value))] for col in cols}
                df = pd.DataFrame.from_dict(dct, orient='columns')
                if v_crm_prod is not None:
                    try:
                        v_prod_group_trg = df.loc[df['P_NAME'].str.contains(v_crm_prod),'P_GROUP'].tolist()[0]
                        return v_prod_group_trg.lower()
                    except:
                        v_prod_group_trg = v_crm_prod
                        return v_prod_group_trg.lower()
                else:
                    return None
            else:
                return None

        return f.udf(lambda p: map_product_group(p, b_rdd, cols))


    def add_product_norm(self, df):
        prod_group = self.hive.sql('''select * from {schema}.{table}'''.format(schema=SBX_TEAM_DIGITCAMP, table=MW_ATB_SEGMEN_PROD_GROUP))
        prod_group_df = prod_group.take(prod_group.count())

        b_rdd = self.sp.sc.broadcast(prod_group_df)
        cols = prod_group.columns

        df = df.withColumn('PRODUCT_NORM', self.udf_map_product_group(b_rdd, cols)(f.col('PROD_TYPE_NAME')))
        return df


    ############################## ContextManager ##############################


    def __enter__(self):
        return self


    @class_method_logger
    def close(self):
        try:
            self.drop_table(SBX_TEAM_DIGITCAMP, self.tmp_table)
            self.sp.sc.stop()
        except Exception as ex:
            if "object has no attribute" in str(ex):
                log("### SparkContext was not started", self.logger)
            else:
                log("### Close exception: \n{}".format(ex), self.logger)
        finally:
            self.db.close()
            self.save_sources_json()
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


if __name__ == "__main__":
    with SourcesUpdate() as sources_update:
        sources_update.run()
