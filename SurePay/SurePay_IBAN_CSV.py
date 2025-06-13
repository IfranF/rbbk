# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# from datetime import timedelta as td
from dateutil.relativedelta import relativedelta
import datetime as dt

# accept incoming ADF parameters
dbutils.widgets.text("p_processdate","2024-11-13","p_processdate :")
p_processdate = dbutils.widgets.get('p_processdate')
today = dt.datetime.strptime(p_processdate, "%Y-%m-%d").date()
print(f"ADF param: {today}")

# define function for getting time range for 2 years
def set_yr_mo():
    global yr_mo
    global today
    try:
        subYears = 2;
        subDate = today - relativedelta(years=subYears)
        yr_mo = subDate.strftime("%Y%m")
        print(f"yr_mo is now set to: {yr_mo}")
    except Exception as e:
        print(f"An error occurred: {e}")

# execute function
set_yr_mo()

# dbfs path to CAC_ACG_ENTR
input_path = "/mnt/gdp/cna/CAC_ACG_ENTR/0/data/"

# define all the columns to select from GDP dataset
cols_to_select =["year_month","bookg_cdt_dbt_ind","dtld_tx_tp","ctpty_acct_id_iban","ctpty_nm","bookg_dt_tm_gmt","acct_id","bookg_amt_nmrc"]

# set CSV filename
csv_filename = f"{today}_Iban_Unvalidated.csv"
print(csv_filename)

# define output path for CSV file
output_path = "dbfs:/mnt/exchange/prod/surepay/current/"

# define all national bank codes to filter on       # TODO: Stephan is in communication to get a dynamic solution for this
df_bank_codes = spark.read.csv("dbfs:/mnt/ifp-exchange/surepay/SurePay Banks List.csv", header=True, inferSchema=True)
df_bank_codes = df_bank_codes.withColumnRenamed("National bank code found in IBAN", "bank_codes")
df_bank_codes.createOrReplaceTempView("vw_bank_codes")

# define function for renaming output csv file and deleting obsolete spark log files
def rename_csv_file(output_path, csv_filename):
    try:
        get_files = dbutils.fs.ls(output_path)
        df_filelist = spark.createDataFrame(get_files)
        get_logs = df_filelist.filter(col('name').startswith("_")).select('name').collect()
        filename = df_filelist.filter(col('name').endswith(".csv")).select('name').collect()[0][0]  # TODO: Alter logic in case there are more then file present (in case ADF moving file to archive goes wrong)
        old_name = output_path + filename
        new_name = output_path + csv_filename

        for row in get_logs:
            row = row['name']
            remove_log_file = output_path + row
            dbutils.fs.rm(remove_log_file, True)
            print("Deleted:",row)
        dbutils.fs.mv(old_name, new_name)
        print("CSV file renamed successfully.",
              "\nOld path:", old_name, 
              "\nNew path:", new_name)
    except Exception as e:
        print(f"An error occurred: {e}")

# print for ADF pipeline?
print("CSV filename:", csv_filename,
"\nOutput path:", output_path)

# COMMAND ----------

# create init dataframe from GDP
iban_check_gdp = spark.read.parquet(input_path) \
                    .select(*cols_to_select)

# filter entire GDP dataset based on the 2 years filter for SurePay
iban_check_gdp_filtered = iban_check_gdp.filter("year_month >= " + yr_mo)

# step 1
iban_check_1 = iban_check_gdp_filtered.filter("not (bookg_cdt_dbt_ind = 'DBIT' and substring(ctpty_acct_id_iban,1,2) = 'NL')")
iban_check_1 = iban_check_1.filter("dtld_tx_tp in (64,79,86,100,134,541,544,545,547)") 
iban_check_1 = iban_check_1.filter("not (bookg_cdt_dbt_ind = 'CRDT' and substring(ctpty_acct_id_iban,1,2) = 'NL' and substring(ctpty_acct_id_iban,5,4) in (select bank_codes from vw_bank_codes))")
iban_check_1 = iban_check_1.filter("ctpty_acct_id_iban is not null and trim(ctpty_acct_id_iban) != ''") \
                           .filter("ctpty_nm is not null and trim(ctpty_nm) != ''")
iban_check_1 = iban_check_1.groupBy(trim("ctpty_acct_id_iban").alias("iban"), \
                                trim("ctpty_nm").alias("name"), \
                                substring("bookg_cdt_dbt_ind", 1, 1).alias("dc")) \
                            .agg(max("bookg_dt_tm_gmt").alias("lastseen"), \
                                count("*").alias("aantal"), \
                                countDistinct("acct_id").alias("aantal_users"), \
                                max("bookg_amt_nmrc").alias("max_bookg_amt_nmrc"), \
                                max(datediff("CURRENT_DATE", "bookg_dt_tm_gmt")).alias("max_delay"))

# COMMAND ----------

# subselect incoming payments
iban_check_2_window = Window.partitionBy(iban_check_1.iban) \
                .orderBy(iban_check_1.lastseen.desc())
iban_check_2 = iban_check_1.filter(iban_check_1.dc == "C")
iban_check_2 = iban_check_2.select("*", row_number().over(iban_check_2_window).alias("rn"))
iban_check_2 = iban_check_2.filter(iban_check_2.rn == 1)

# COMMAND ----------

iban_check_3_df1 = iban_check_2.select("iban", "name", to_date("lastseen").alias("lastseen"), "dc", iban_check_2.aantal.alias("count"))

iban_check_3_df2 = iban_check_1.filter(iban_check_1.dc == "D")
iban_check_3_df2 = iban_check_3_df2.filter(iban_check_3_df2.aantal > 4)
iban_check_3_df2 = iban_check_3_df2.filter(iban_check_3_df2.aantal_users > 1)
iban_check_3_df2 = iban_check_3_df2.filter(iban_check_3_df2.max_bookg_amt_nmrc > 499)
iban_check_3_df2 = iban_check_3_df2.filter(iban_check_3_df2.max_delay > 14)
iban_check_3_df2 = iban_check_3_df2.join(iban_check_3_df1, (iban_check_3_df1.iban == iban_check_3_df2.iban) & (iban_check_3_df1.name == iban_check_3_df2.name), "left_anti")

iban_check_3_df2_window = Window.partitionBy(iban_check_3_df2.iban) \
                                .orderBy(iban_check_3_df2.aantal.desc())

iban_check_3_df2 = iban_check_3_df2.select("*", row_number().over(iban_check_3_df2_window).alias("rn"))
iban_check_3_df2 = iban_check_3_df2.filter(iban_check_3_df2.rn < 21)
iban_check_3_df2 = iban_check_3_df2.select("iban", "name", \
                                    to_date("lastseen").alias("lastseen"), "dc", iban_check_3_df2.aantal.alias("count"))

iban_check_3 = iban_check_3_df1.union(iban_check_3_df2)

# COMMAND ----------

iban_check_3.coalesce(1).write.format('csv').option('header',True) \
                                            .option('delimter','\t') \
                                            .mode('append') \
                                            .save(output_path)

# COMMAND ----------

rename_csv_file(output_path, csv_filename)

# COMMAND ----------

surepay_csv_file = output_path + csv_filename
surepay_iban = spark.read.format('csv').option('header',True) \
                                       .option('delimiter','\t') \
                                       .load(surepay_csv_file)
count = surepay_iban.count()

# COMMAND ----------

dbutils.notebook.exit(count)