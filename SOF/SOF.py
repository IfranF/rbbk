# Databricks notebook source
# MAGIC %md
# MAGIC ### Statement of Fees

# COMMAND ----------

# -------------
# Description | 
# -------------

# Every year, for each payment account the Rabobank sends on a yearly basis information about the use of an account to the customer. The main target is: increasing of the quality and transparency of payment services in order to simplify the consumer’s search for cheaper payment services.

# SOF microservices gathers the information from the various systems like ART, COK and POD and eventually composes the ‘Statement of Fees’ for every customer. Eventually it will be processed by CEA and the ‘Printstraat’ and sent digitally or by paper to the customer.

# ---------------------------
# Transaction group details |
# ---------------------------
# 9101 - Debit card, Pay by debit card (POS), in foreign currency, exchange rate surcharge
# 9107 - Debit card, Cash withdrawal (ATM), in foreign currency, exchange rate surcharge
# 9104 - Credit card, Pay by credit card (POS), in foreign currency
# 9106 - Credit card, Pay by credit card (POS), in foreign currency, exchange rate surcharge
# 9108 - Credit card, Cash withdrawal (ATM), in euro
# 9109 - Credit card, Cash withdrawal (ATM), in foreign currency
# 9110 - Credit card, Cash withdrawal (ATM), in foreign currency, exchange rate surcharge
# 03DT - Overdraft credit card
# 9111 - Cross Border, outgoing transactions
# 9112 - Cross Border, incoming transactions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify Mount Points

# COMMAND ----------

# config settings
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="key-vault-secrets",key="ifpDatabricksSpn"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="key-vault-secrets",key="ifpDatabricksSpnSecret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/token"}

# Define a function to receive the container name as the input and mount the same.
def mount(container):
  dbutils.fs.mount(
  source = f"abfss://{container}@edlcorestdeuprod0001.dfs.core.windows.net",
  mount_point = f"/mnt/gdp/{container}",
  extra_configs = configs)

# COMMAND ----------

# This cell determines if a mount point is available or not. If not, then it invokes the previously defined function with appropriate container to mount the same. If the mount point is already available, then appropriate information is displayed and no action is taken

# input variables of all mount points needed for SOF processing
aci_issuer_mount    = '/mnt/gdp/aci-issuer'
cok_mount           = '/mnt/gdp/cok'
siebel_mount        = '/mnt/gdp/siebel-idaa-cdf'
cross_border_mount  = '/mnt/gdp/gp6'
timescape_mount     = '/mnt/gdp/timescape'

# input variables of all containers
aci_issuer          = 'aci-issuer'
cok                 = 'cok'
siebel              = 'siebel-idaa-cdf'
cross_border        = 'gp6'
timescape           = 'timescape'

# Function to check if the mount already exists
def mount_exists(mount_point):
    mounts = [mnt.mountPoint for mnt in dbutils.fs.mounts()]
    return mount_point in mounts

# Mount point check function is invoked for all mount points. The variable <mount_point>_exists gets the value True or False from the function depending on if the mount point exists or not.
aci_issuer_mount_exists     = mount_exists(aci_issuer_mount)
cok_mount_exists            = mount_exists(cok_mount)
siebel_mount_exists         = mount_exists(siebel_mount)
cross_border_mount_exists   = mount_exists(cross_border_mount)
timescape_mount_exists      = mount_exists(timescape_mount)

# If the mount point is already available, appropriate message is displayed. If not, then the function to mount the mount_point is invoked. This is done for all the mount points needed for SOF.
if aci_issuer_mount_exists:
    print("aci_issuer mount already exists. No action needed.")
else:
    mount(aci_issuer)
    print(f"aci_issuer is successfully mounted.")

if cok_mount_exists:
    print("cok mount already exists. No action needed.")
else:
    mount(cok)
    print(f"cok is successfully mounted.")

if siebel_mount_exists:
    print("siebel mount already exists. No action needed.")
else:
    mount(siebel)
    print(f"siebel is successfully mounted.")

if cross_border_mount_exists:
    print("cross_border mount already exists. No action needed.")
else:
    mount(cross_border)
    print(f"cross_border is successfully mounted.")

if timescape_mount_exists:
    print("timescape mount already exists. No action needed.")
else:
    mount(timescape)
    print(f"timescape is successfully mounted.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initial Set-up and Identify the date range for the SOF run

# COMMAND ----------

#import necessary files
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import StorageLevel

# from datetime import timedelta as td
from dateutil.relativedelta import relativedelta
import datetime

# input mounts for prod -- What if the version changes? Investigate further! 
dc_trx_clrg_issng     = '/mnt/gdp/aci-issuer/dc_trx_clrg_issng_di/0/data'
bsc_pymt_agrm         = '/mnt/gdp/cok/bsc_pymt_agrm_ds/0/data'
#siebel_dir_path       = '/mnt/gdp/siebel-idaa-cdf/cdf_ggm_ar_hist/1/data' #--> Due to version2 changes, the logic related to sibel is replaced
siebel_dir_path       = '/mnt/gdp/siebel-idaa-cdf/cdf_ggm_rel_x_ar_hist/2/data'
cross_border_incoming = '/mnt/gdp/gp6/IXB_PO_MSG_INF_V2_DI/0/data'
cross_border_outgoing = '/mnt/gdp/gp6/OXB_PO_MSG_INF_DI/0/data'
fx_rate               = '/mnt/gdp/timescape/fx_rate_rslt_pct_mi/1/data'

# ---------------------------------------------------------------------------------
# The following logic is needed to identify the latest version of the input files
# ---------------------------------------------------------------------------------
# List all files in the directory
files = dbutils.fs.ls(siebel_dir_path)
# Find the file with the latest modification time
latest_file = sorted(files, key=lambda f: f.modificationTime, reverse=True)[0]
# Get the path of the latest file
cdf_ggm_ar_hist = latest_file.path

# ---------------------------------------------------------------------------------
# The following logic is needed to identify the latest version of the input files
# ---------------------------------------------------------------------------------
# List all files in the directory
files = dbutils.fs.ls(fx_rate)
# Find the file with the latest modification time
latest_file = sorted(files, key=lambda f: f.modificationTime, reverse=True)[0]
# Get the path of the latest file
fx_rate_abspath = latest_file.path

# The Statement of Fee is always run from 1st of April of the previous year till 31st of March of the current year
# Start range = 01-04-XXXX where XXXX is the previous year during the time of run
# Start range = 01-04-YYYY where YYYY is the current year during the time of run

# Calculate the current year
current_year = (datetime.date.today()).year

# Calculate the previous year
previous_year = current_year - 1

# Calculate the current date and yesterday date
today_date = spark.sql("SELECT current_date()").collect()[0][0]
yesterday_date = spark.sql("SELECT date_format(date_sub(current_date(), 1), 'yyyy-MM-dd')").collect()[0][0]

# Build the start date and end date along with start range and end range for filtration purposes
start_date  = str(previous_year) +'-04-01'
end_date    = str(current_year) + '-03-31'
start_range = str(previous_year) +'-04-01 00:00:00'
end_range   = str(current_year) + '-03-31 23:59:59'

#!!!! Uncomment if you want to run for 2023-2024 - For Testing purpose !!!!
#start_date     = '2023-04-01'
#end_date       = '2024-03-31'
#start_range    = '2023-04-01 00:00:00'
#end_range      = '2024-03-31 23:59:59'
#previous_year  = '2023'
#current_year   = '2024'

# -----------------------------------------------------------------------------------
# For better performance, extract only the needed columns for all input datasets
# -----------------------------------------------------------------------------------

# define all the columns to select from dc_trx_clrg_issng_di dataset
cols_from_dc_trx_clrg_issng =[  "CARDHDR_IBAN",\
                                "CARD_TXN_LCL_DT",\
                                "CARD_TXN_LCL_TM",\
                                "CARD_TP_CODE", \
                                "CARD_TXN_ATM_POS_CODE",\
                                "CARD_TXN_ORIG_N3_CCY_CODE",\
                                "CLRG_CARDHDR_BILL_EC_AMT",\
                                "CLRG_CARDHDR_BILL_NO_MU_EC_AMT",\
                                "CARD_TXN_DB_CR_CODE",\
                                "CTXAT_CARDHDR_ORIG_CCY_SU_AMT",\
                                "CARD_TXN_ACQR_FEE_ORIG_CCY_AMT",\
                                "CARD_TXN_ORIG_CCY_SU_AMT",\
                                "CLRG_MU_TRFF_PCT", \
                                "CARD_TXN_ORIG_CCY_DCM_PNT_POS", \
                                "ACT_DTS",  \
                                "CARD_TXN_TP_CODE", \
                                "CARD_AC_Y_EFF_INT_RATE_PCT", \
                                "CARD_TXN_ORIG_BK_NO", \
                                "CLRG_TXN_BK_DT",\
                                "CARD_TXN_BK_NO", \
                                "CARD_TXN_ORIG_PCS_DCSN_CODE" ]

# define all the columns to select from cdf_ggm_ar_hist dataset
cols_from_rel_x_ar_hist = [     "AR_AC_IBAN",\
                                "DEL_F",\
                                "SBL_IP_TP_CODE", \
                                "EDL_VALID_TO_DTS", \
                                "EDL_VALID_FROM_DTS", ]

# define all the columns to select from bsc_pymt_agrm_ds dataset
cols_from_bsc_pymt_agrm = [ "IBAN",\
                            "CCY_CODE",\
                            "ACT_DTS"]

# define all the columns to select from cross border outgoing dataset
cols_from_xb_outgoing   = ["OXB_PO_CCY_DB_AC_BOOK_AMT",\
                            "OXB_PO_CR_CCY_CODE",\
                            "OXB_PO_CR_EXG_RATE",\
                            "OXB_PO_DB_AC_CCY_CODE",\
                            "OXB_PO_DB_AC_IBAN",\
                            "OXB_PO_DT_EXG_RATE",\
                            "OXB_PO_ORIG_CCY_AMT",\
                            "OXB_PO_ORIG_CCY_CODE",\
                            "XB_PO_CR_CCY_AMT",\
                            "XB_PO_FNL_ST_CODE",\
                            "XB_PO_INR_MSG_ID",\
                            "XB_PO_PD_CODE",\
                            "XB_PO_FX_RATE_AGRM_F",\
                            "XB_PO_MSG_CLSS_CODE",\
                            "XB_PO_PYMT_ENG_FNL_ST_DTS"
                            ]

# define all the columns to select from cross border incoming dataset
cols_from_xb_incoming   =["IXB_PO_CR_EXG_RATE",\
                          "IXB_PO_CRDR_AC_NO",\
                          "IXB_PO_DB_AC_BOOK_CCY_AMT",\
                          "IXB_PO_DT_AC_CCY_CODE",\
                          "XB_PO_CR_CCY_AMT",\
                          "XB_PO_FNL_ST_CODE",\
                          "XB_PO_FX_RATE_AGRM_F",\
                          "XB_PO_PD_CODE",\
                          "XB_PO_PYMT_ENG_FNL_ST_DTS",\
                          "XB_PO_RABO_CR_AC_CCY_CODE"
                          ]

# define all the columns to select from foreign exchange rate dataset
cols_from_fx_rate   =["Currency",\
                      "HalfSpread"
                      ]
# define output path for CSV file   
Output_folder   = '/FileStore/SOF/Output/'
output_path     = '/mnt/ifp-exchange/SOF/'

# Build the needed filters to filter only the data pertaining to the particular period
txn_lcl_dt_start_filter = f"""CARD_TXN_LCL_DT >= '{start_range}'"""
txn_lcl_dt_end_filter   = f"""CARD_TXN_LCL_DT <= '{end_range}'"""

act_dts_start_filter    = f"""ACT_DTS >= '{start_range}'"""
act_dts_end_filter      = f"""ACT_DTS <= '{end_range}'"""

pymt_eng_start_filter   = f"""XB_PO_PYMT_ENG_FNL_ST_DTS >= '{start_range}'"""
pymt_eng_end_filter     = f"""XB_PO_PYMT_ENG_FNL_ST_DTS <= '{end_range}'"""


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Load the input data into dataframes and filter data for required period

# COMMAND ----------

# For transaction clearing issuing, we need a workaround to access the timestamp field i.e. CARD_TXN_LCL_TM. 
# The value of this field only has time part before 2023*, when CGW data was onboarded to GDP and full timestamp afterwards, when ACI cards data was onboarded to GDP. Due to the discrepancy in the format, you get format error, if you try accessing the timestamp field when you simply use the base path as "/mnt/gdp/aci-issuer/dc_trx_clrg_issng_di/0/data" even though you apply filter to restrict the data. If we explicitly refer the underlying partitions for the SOF period (i.e after 2023), we can avoid this issue as all values will be uniform for the selected period.


from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Function to generate list of paths for the date range
def generate_paths(start_date, end_date, base_path):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    paths = []
    while start <= end:
        paths.append(f"{base_path}/loaddate={start.strftime('%Y-%m-%d')}")
        start += relativedelta(days=1)
    return paths


# Define the base path and date range
base_path = "/mnt/gdp/aci-issuer/dc_trx_clrg_issng_di/0/data"
start_date  = str(previous_year) +'-03-01'
if today_date.month > 4 and today_date.day >= 2:
    end_date = str(current_year) + '-04-02'
else: 
    end_date = yesterday_date 

# Generate the list of paths
paths = generate_paths(start_date, end_date, base_path)

# Read the data from the generated paths
dc_temp = spark.read.parquet(*paths) 

#filter the needed data
dc_trx_clrg_issng_df = dc_temp \
                        .select(*cols_from_dc_trx_clrg_issng) \
                        .filter(txn_lcl_dt_start_filter) \
                        .filter(txn_lcl_dt_end_filter)   

# COMMAND ----------


# Read the bsc_pymt_agrm data into a dataframe and extract only the needed columns
# Also, extract the data for only the required timeframe
bsc_pymt_agrm_df     = spark.read.parquet(bsc_pymt_agrm) \
                                    .select(*cols_from_bsc_pymt_agrm) \
                                    .filter(act_dts_start_filter) \
                                    .filter(act_dts_end_filter)   

# Read the cdf_ggm_ar_hist data into a dataframe and extract only the needed columns
rel_x_ar_hist_df    = spark.read.format('delta') \
                                .load(cdf_ggm_ar_hist) \
                                .select(*cols_from_rel_x_ar_hist) 

# Read the incoming cross border data into a dataframe 
cross_border_incoming_df = spark.read.parquet(cross_border_incoming) \
                                  .select(*cols_from_xb_incoming) \
                                  .filter(pymt_eng_start_filter)  \
                                  .filter(pymt_eng_end_filter)    

# Read the outgoing cross border data into a dataframe 
cross_border_outgoing_df = spark.read.parquet(cross_border_outgoing) \
                                  .select(*cols_from_xb_outgoing) \
                                  .filter(pymt_eng_start_filter)  \
                                  .filter(pymt_eng_end_filter)   

# Read the foreign exchange rate data into a dataframe 
fx_rate_df = spark.read.parquet(fx_rate_abspath) \
                               .select(*cols_from_fx_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add/Transform columns in dataframe and create temporary views

# COMMAND ----------

# Add a column in the dataframe to hold value of 1 as the number of transactions which will be used in the calculations within SQL query

dc_modified_df = dc_trx_clrg_issng_df \
                 .withColumn("Number_Of_Transactions",lit(1))
                
xb_incoming_df = cross_border_incoming_df \
                 .withColumn("Number_Of_Transactions",lit(1))         

xb_outgoing_df = cross_border_outgoing_df \
                 .withColumn("Number_Of_Transactions",lit(1))

# Create views on the dataframes that are to be used for SQL queries to extract the needed data

dc_modified_df.createOrReplaceTempView('v_temp_transactions')
bsc_pymt_agrm_df.createOrReplaceTempView('v_temp_basicPaymentAgrm')
rel_x_ar_hist_df.createOrReplaceTempView('v_relation')
xb_incoming_df.createOrReplaceTempView('v_temp_xb_incoming')
xb_outgoing_df.createOrReplaceTempView('v_temp_xb_outgoing')
fx_rate_df.createOrReplaceTempView('v_temp_fx_rate')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract data for each transaction groups into dataframe

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9101                                                                              #   
# Debit card, Pay by debit card, in foreign currency, exchange rate surcharge       #
# ----------------------------------------------------------------------------------#

df_9101  = spark.sql("""
                         WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                          select 
                          recid
                         ,iban
                         ,currencycode
                         ,transactiongroup
                         ,transactiondate
                         ,transactioncount
                         ,ratedtransactioncount
                         ,feeorinterestbasecode
                         ,costperunitcurrencysmallestunitamount
                         ,costorinterestrate
                         ,costorinterestcurrencysuamount
                         ,costorinterestcurrencycode
                         ,costorinterestamountdecimalsnumber
                         from
                         (select
                         '20'                                                             as recid
                         ,temp.IBAN                                                       as iban
                         ,temp.CurrencyCode                                               as currencycode
                         ,temp.TransactionGroup                                           as transactiongroup
                         ,date_format(temp.TransactionDate,'dd-MM-yyyy')                  as transactiondate
                         ,temp.DebitCardIndicator                                         as DebitCardIndicator
                         ,REPLACE(CONCAT('+',SUM(temp.TransactionCount)),'+-', '-')       as transactioncount
                         ,REPLACE(CONCAT('+',SUM(temp.RatedTransactionCount)),'+-', '-')  
                                                                       as ratedtransactioncount
                         ,'2'                                          as feeorinterestbasecode
                         ,'+0.00000'                                   as costperunitcurrencysmallestunitamount
                         ,CONCAT('+',temp.CostOrInterestRate)          as costorinterestrate
                         ,REPLACE(CONCAT('+',cast(SUM(temp.CostOrInterestCurrencySUAmount) as decimal(18,0) )),'+-', '-')
                                                                       as costorinterestcurrencysuamount
                         ,'EUR'                                        as costorinterestcurrencycode
                         ,'2'                                          as costorinterestamountdecimalsnumber
                         from
                         (select 
                              t1.CARDHDR_IBAN                                   as IBAN
                              ,'EUR'                                            as CurrencyCode
                              ,'9101'                                           as TransactionGroup
                              ,t1.CARD_TXN_LCL_DT                               as TransactionDate        
                              ,substr(t1.CARD_TXN_DB_CR_CODE,1,1)               as DebitCardIndicator
                              ,SUM(t1.Number_Of_Transactions) * (CASE t1.CARD_TXN_DB_CR_CODE WHEN 'CRED' THEN -1 ELSE 1 END)                                    as TransactionCount
                              ,SUM((CASE WHEN (( IFNULL(t1.CLRG_CARDHDR_BILL_EC_AMT, 0)
                                   - IFNULL(t1.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0)
                                   THEN t1.Number_Of_Transactions
                                   ELSE 0
                                   END) *
                                   (CASE t1.CARD_TXN_DB_CR_CODE WHEN 'CRED' THEN -1 ELSE 1 END))
                                                                                as RatedTransactionCount
                              ,CAST(ifnull(t1.CLRG_MU_TRFF_PCT,0) AS DECIMAL(9,5))        as CostOrInterestRate
                              ,SUM((CASE WHEN (( IFNULL(t1.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                         - IFNULL(t1.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0) 
			                    THEN ( IFNULL(t1.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                         - IFNULL(t1.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0))
                                   ELSE 0
                                   END) * 
                                   (CASE t1.CARD_TXN_DB_CR_CODE WHEN 'CRED' THEN -1 ELSE 1 END))
                                                                                as CostOrInterestCurrencySUAmount
                              
                              from v_temp_transactions t1 
                    
                              where 
                              t1.CARD_TXN_ORIG_N3_CCY_CODE <> 978 
                              and 
                              t1.CARD_TP_CODE = 'DBT' 
                              and 
                              t1.CARD_TXN_ATM_POS_CODE = 'POS'
                              and
                              t1.CARDHDR_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                              where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              
                              group by 
                               t1.CARDHDR_IBAN 
                              ,t1.CARD_TXN_LCL_DT   
                              ,t1.CLRG_MU_TRFF_PCT
                              ,t1.CARD_TXN_DB_CR_CODE
                              ,t1.CARD_TXN_ORIG_CCY_DCM_PNT_POS
                              ) temp
                         
                         WHERE temp.CostOrInterestRate > 0 

                         GROUP by 
                         temp.IBAN
                         ,temp.CurrencyCode
                         ,temp.TransactionGroup
                         ,temp.TransactionDate
                         ,temp.DebitCardIndicator
                         ,temp.CostOrInterestRate) result
                         
                         ORDER BY 
                          iban
                         ,currencycode
                         ,transactiongroup
                         ,to_date(transactiondate)
                         
                     """) ;

# Extract the count of records retrieved and store it in a variable
df_9101_count = 0
df_9101_count = df_9101.count()

display(f"Records extracted for 9101 : {df_9101_count}")
#For reference: 7.701.539 records were extracted for 2024 i.e. 01 Apr 2023 to 31 Mar 2024

# Count after changes to replace siebel arrangement with relation =>7701700

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9107                                                                              #   
# Debit card, Cash withdrawal, in foreign currency, exchange rate surcharge         #
# ----------------------------------------------------------------------------------#

df_9107  = spark.sql("""
                         WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                         select 
                          recid
                         ,iban
                         ,currencycode
                         ,transactiongroup
                         ,transactiondate
                         ,transactioncount
                         ,ratedtransactioncount
                         ,feeorinterestbasecode
                         ,costperunitcurrencysmallestunitamount
                         ,costorinterestrate
                         ,costorinterestcurrencysuamount
                         ,costorinterestcurrencycode
                         ,costorinterestamountdecimalsnumber
                         from
                         (select
                         '20'                                                             as recid
                         ,temp.IBAN                                                       as iban
                         ,temp.CurrencyCode                                               as currencycode
                         ,temp.TransactionGroup                                           as transactiongroup
                         ,date_format(temp.TransactionDate,'dd-MM-yyyy')                  as transactiondate
                         ,temp.DebitCardIndicator                                         as DebitCardIndicator
                         ,REPLACE(CONCAT('+',SUM(temp.TransactionCount)),'+-', '-')       as transactioncount
                         ,REPLACE(CONCAT('+',SUM(temp.RatedTransactionCount)),'+-', '-')  as ratedtransactioncount
                         ,'2'                                                             as feeorinterestbasecode
                         ,'+0.00000'                                         as costperunitcurrencysmallestunitamount
                         ,CONCAT('+',temp.CostOrInterestRate)                             as costorinterestrate
                         ,REPLACE(CONCAT('+',cast(SUM(temp.CostOrInterestCurrencySUAmount) as decimal(18,0) )),'+-', '-')
                                                                                as costorinterestcurrencysuamount
                         ,'EUR'                                                 as costorinterestcurrencycode
                         ,'2'                                                   as costorinterestamountdecimalsnumber
                         from
                         (select 
                              t1.CARDHDR_IBAN                                   as IBAN
                              ,'EUR'                                            as CurrencyCode
                              ,'9107'                                           as TransactionGroup
                              ,t1.CARD_TXN_LCL_DT                               as TransactionDate        
                              ,substr(t1.CARD_TXN_DB_CR_CODE,1,1)               as DebitCardIndicator
                              ,SUM(t1.Number_Of_Transactions) * (CASE t1.CARD_TXN_DB_CR_CODE WHEN 'CRED' THEN -1 ELSE 1 END)                                    as TransactionCount
                              ,SUM((CASE WHEN (( IFNULL(t1.CLRG_CARDHDR_BILL_EC_AMT, 0)
                                   - IFNULL(t1.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0)
                                   THEN t1.Number_Of_Transactions
                                   ELSE 0
                                   END) *
                                   (CASE t1.CARD_TXN_DB_CR_CODE WHEN 'CRED' THEN -1 ELSE 1 END))
                                                                                as RatedTransactionCount
                              ,CAST(ifnull(t1.CLRG_MU_TRFF_PCT,0) AS DECIMAL(9,5))        as CostOrInterestRate
                              ,SUM((CASE WHEN (( IFNULL(t1.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                         - IFNULL(t1.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0) 
			                    THEN ( IFNULL(t1.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                         - IFNULL(t1.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0))
                                   ELSE 0
                                   END) * 
                                   (CASE t1.CARD_TXN_DB_CR_CODE WHEN 'CRED' THEN -1 ELSE 1 END))
                                                                                as CostOrInterestCurrencySUAmount
                              
                              from v_temp_transactions t1 
                    
                              where 
                              t1.CARD_TXN_ORIG_N3_CCY_CODE <> 978 
                              and 
                              t1.CARD_TP_CODE = 'DBT' 
                              and 
                              t1.CARD_TXN_ATM_POS_CODE = 'ATM'
                              and
                              t1.CARDHDR_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                              where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              
                              group by 
                              t1.CARDHDR_IBAN
                              ,t1.CARD_TXN_LCL_DT   
                              ,t1.CLRG_MU_TRFF_PCT
                              ,t1.CARD_TXN_DB_CR_CODE
                              ) temp
                         
                         WHERE temp.CostOrInterestRate > 0 

                         GROUP by 
                         temp.IBAN
                         ,temp.CurrencyCode
                         ,temp.TransactionGroup
                         ,temp.TransactionDate
                         ,temp.DebitCardIndicator
                         ,temp.CostOrInterestRate
                         ) result

                     """) ;

# Extract the count of records retrieved and store it in a variable
df_9107_count = 0
df_9107_count = df_9107.count()

display(f"Records extracted for 9107 : {df_9107_count}")
# prev 1189792

# Count after changes to replace siebel arrangement with relation =>1189813

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9104, 9108 and 9109                                                               #   
# 9104 - Debit card, Cash withdrawal, in foreign currency, exchange rate surcharge  #
# 9108 - Credit card, Cash withdrawal, in euro                                      #
# 9109 - Credit card, Cash withdrawal, in foreign currency                          #
# ----------------------------------------------------------------------------------#

df_9104_08_09 = spark.sql("""
                         WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                         select
                         '20'                                                             as recid
                         ,result.iban                                                     as iban
                         ,result.currencycode                                             as currencycode
                         ,result.transactiongroup                                         as transactiongroup
                         ,date_format(result.transactiondate,'dd-MM-yyyy')                as transactiondate
                         ,REPLACE(CONCAT('+',sum(result.transactioncount)),'+-', '-')     as transactioncount
                         ,REPLACE(CONCAT('+',sum(result.ratedtransactioncount)),'+-', '-')     
                                                                                          as ratedtransactioncount
                         ,'1'                                                             as feeorinterestbasecode
                         ,REPLACE(CONCAT('+',CAST(result.costperunitcurrencysmallestunitamount  as DECIMAL(9,5))),'+-', '-')                                               as costperunitcurrencysmallestunitamount
                         ,'+0.00000'                                        as costorinterestrate
                         ,REPLACE(CONCAT('+',round(CAST(sum(result.costorinterestcurrencysuamount) AS DECIMAL(18,0)),0)),'+-', '-')                                            as costorinterestcurrencysuamount
                         ,'EUR'                                          as costorinterestcurrencycode
                         ,'2'                                            as costorinterestamountdecimalsnumber

                         from                      
                        
                        (select
                          temp.IBAN                                                       as iban
                         ,temp.CurrencyCode                                               as currencycode
                         ,case when temp.AtmPosCode = 'POS' then '9104'
                               when temp.AtmPosCode = 'ATM' then 
                                    case when temp.OriginalCurrencyCode = 978 then '9108'
                                                                              else '9109' 
                                    end 
                            end                                                           as transactiongroup
                         ,temp.TransactionDate                                            as transactiondate
                         ,temp.DebitCreditIndicator                                       as debitcreditindicator
                         ,(SUM(temp.NumberOfTransactions) * 
                                              (CASE  
                                               WHEN temp.DebitCreditIndicator = 'CRED' 
                                               THEN -1 
                                               ELSE 1 
                                               END) )                                     as transactioncount
                         ,(SUM(case when temp.AtmPosCode = 'ATM' then temp.NumberOfTransactions
                                                                 else 0
                               end) * 
                                                  (CASE 
                                                   WHEN temp.DebitCreditIndicator = 'CRED' 
                                                   THEN -1 
                                                   ELSE 1 
                                                   END))                                  as ratedtransactioncount
                         ,'1'                                                             as feeorinterestbasecode
                            
                         ,case when temp.AtmPosCode = 'ATM' 
                               then CAST(ifnull(t2.CARD_TXN_ORIG_CCY_SU_AMT,0) / POWER(10,2)  as DECIMAL(9,5))
                               else 0
                               end                                       as costperunitcurrencysmallestunitamount

                         ,round((       (case when temp.AtmPosCode = 'ATM' then
                                        (CAST(t2.CARD_TXN_ORIG_CCY_SU_AMT AS DECIMAL(18,0))
                                        * SUM(temp.NumberOfTransactions)) else 0 end ) * 
                                        (CASE WHEN temp.DebitCreditIndicator = 'CRED' THEN -1 ELSE 1 END)),0)
                                                                         as costorinterestcurrencysuamount
                         from
                         (select 
                               t1.CARD_TP_CODE                                  as CardTypeCode 
                              ,t1.CARDHDR_IBAN                                  as IBAN
                              ,t1.CARD_TXN_LCL_DT                               as TransactionDate
                              ,'EUR'                                            as CurrencyCode
                              ,t1.CARD_TXN_LCL_TM                               as TransactionTimestamp
                              ,t1.CARD_TXN_BK_NO                                as BookingNumber
                              ,t1.CARD_TXN_ATM_POS_CODE                         as AtmPosCode
                              ,t1.CARD_TXN_ORIG_N3_CCY_CODE                     as OriginalCurrencyCode
                              ,t1.Number_Of_Transactions                        as NumberOfTransactions
                              ,t1.CARD_TXN_DB_CR_CODE                           as DebitCreditIndicator
                              
                              from v_temp_transactions t1 
                    
                              where 
                              t1.CARD_TP_CODE = 'CDT' 
                              and
                              ((t1.CARD_TXN_ATM_POS_CODE = 'POS'  and t1.CARD_TXN_ORIG_N3_CCY_CODE <> 978)
                              or
                              t1.CARD_TXN_ATM_POS_CODE = 'ATM')
                              and
                              t1.CARD_TXN_TP_CODE NOT IN ('20','28') 
                              and
                              t1.CARDHDR_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                              where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              ) temp
                         
                         INNER JOIN v_temp_transactions t2
                            ON t2.CARD_TP_CODE = temp.CardTypeCode
                           and t2.CARDHDR_IBAN = temp.IBAN
                           and t2.CARD_TXN_LCL_TM = temp.TransactionTimestamp
                           and t2.CARD_TXN_LCL_DT = temp.TransactionDate
                           and t2.CARD_TXN_ORIG_BK_NO = temp.BookingNumber
                           and t2.CARD_TXN_ORIG_PCS_DCSN_CODE = 'TXNF'
                           and (( temp.AtmPosCode = 'ATM' AND t2.CLRG_MU_TRFF_PCT is NULL)
                                or 
                                (temp.AtmPosCode = 'POS' AND t2.CLRG_MU_TRFF_PCT is not NULL)
                               )
                         GROUP by 
                          temp.IBAN
                         ,temp.CurrencyCode
                         ,temp.AtmPosCode
                         ,temp.OriginalCurrencyCode
                         ,temp.TransactionDate
                         ,temp.DebitCreditIndicator
                         ,t2.CARD_TXN_ORIG_CCY_SU_AMT
                        )result 

                        GROUP by 
                          result.iban
                         ,result.currencycode
                         ,result.transactiongroup
                         ,result.transactiondate
                         ,result.debitcreditindicator
                         ,result.feeorinterestbasecode
                         ,result.costperunitcurrencysmallestunitamount
                         
                     """) ;

df_9104 = df_9104_08_09.filter("transactiongroup='9104'")

df_9108 = df_9104_08_09.filter("transactiongroup='9108'")

df_9109 = df_9104_08_09.filter("transactiongroup='9109'")

# Extract the count of records retrieved and store it in a variable
df_9104_count = 0
df_9104_count = df_9104.count()

df_9108_count = 0
df_9108_count = df_9108.count()

df_9109_count = 0
df_9109_count = df_9109.count()

display(f"Records extracted for 9104 : {df_9104_count}")
display(f"Records extracted for 9108 : {df_9108_count}")
display(f"Records extracted for 9109 : {df_9109_count}")

# Count after changes to replace siebel arrangement with relation 9104 =>6026531
# Count after changes to replace siebel arrangement with relation 9108 =>1468063
# Count after changes to replace siebel arrangement with relation 9109 =>245075

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9106                                                                              #   
# Credit card, Pay by credit card, in foreign currency, exchange rate               #
# ----------------------------------------------------------------------------------#

df_9106  = spark.sql("""
                          WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                          select
                         '20'                                                             as recid
                         ,t2.CARDHDR_IBAN                                                 as iban
                         ,temp.CurrencyCode                                               as currencycode
                         ,'9106'                                                          as transactiongroup
                         ,date_format(t2.CARD_TXN_LCL_DT ,'dd-MM-yyyy')                   as transactiondate
                         ,REPLACE(CONCAT('+', (SUM(t2.Number_Of_Transactions) * 
                                              (CASE  
                                               WHEN t2.CARD_TXN_DB_CR_CODE = 'CRED' 
                                               THEN -1 
                                               ELSE 1 
                                               END) )),'+-', '-')                    as transactioncount
                         ,REPLACE(CONCAT('+',(SUM(CASE 
                                                  WHEN (( IFNULL(t2.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                                            - IFNULL(t2.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0)
                                                  THEN t2.Number_Of_Transactions
                                                  ELSE 0 
                                                  END) * 
                                                  (CASE 
                                                   WHEN t2.CARD_TXN_DB_CR_CODE = 'CRED' 
                                                   THEN -1 
                                                   ELSE 1 
                                                   END)) ),'+-', '-')     as ratedtransactioncount
                         ,'2'                                             as feeorinterestbasecode
                         ,'+0.00000'                                      as costperunitcurrencysmallestunitamount
                         ,CONCAT('+',CAST(ifnull(t2.CLRG_MU_TRFF_PCT,0) AS DECIMAL(9,5)))
                                                                          as costorinterestrate
                         ,REPLACE(CONCAT('+',cast(SUM((CASE 
                                                  WHEN (( IFNULL(t2.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                                            - IFNULL(t2.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0)
                                                  THEN ( IFNULL(t2.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                                            - IFNULL(t2.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0))
                                                  ELSE 0
                                                  END) * (CASE WHEN t2.CARD_TXN_DB_CR_CODE = 'CRED' THEN -1 ELSE 1 END)) as decimal(18,0) )),
                                                  '+-', '-')
                                                                                as costorinterestcurrencysuamount
                         ,'EUR'                                                 as costorinterestcurrencycode
                         ,'2'                                                   as costorinterestamountdecimalsnumber
                         from
                         (select 
                               t1.CARD_TP_CODE                                  as CardTypeCode 
                              ,t1.CARDHDR_IBAN                                  as IBAN
                              ,t1.CARD_TXN_LCL_DT                               as transactiondate 
                              ,t1.CARD_TXN_LCL_TM                               as TransactionTimestamp
                              ,t1.CARD_TXN_BK_NO                                as BookingNumber
                              ,t1.CARD_TXN_ATM_POS_CODE                         as AtmPosCode
                              ,'EUR'                                            as CurrencyCode
                              
                              from v_temp_transactions t1 
                    
                              where 
                              t1.CARD_TP_CODE = 'CDT' 
                              and 
                              t1.CARD_TXN_ATM_POS_CODE = 'POS'
                              and
                              t1.CARD_TXN_ORIG_N3_CCY_CODE <> 978 
                              and
                              t1.CARDHDR_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                              where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              
                              ) temp
                         
                         INNER JOIN v_temp_transactions t2
                            ON t2.CARD_TP_CODE = temp.CardTypeCode
                           and t2.CARDHDR_IBAN = temp.IBAN
                           and t2.CARD_TXN_LCL_TM = temp.TransactionTimestamp
                           and t2.CARD_TXN_LCL_DT = temp.TransactionDate
                           and t2.CARD_TXN_ORIG_BK_NO = temp.BookingNumber
                           and t2.CARD_TXN_ORIG_PCS_DCSN_CODE = 'TXNF' 
                           and t2.CLRG_MU_TRFF_PCT is NOT null
                           

                         GROUP by 
                          t2.CARDHDR_IBAN
                         ,temp.CurrencyCode
                         ,temp.AtmPosCode
                         ,t2.CARD_TXN_LCL_DT
                         ,t2.CARD_TXN_DB_CR_CODE
                         ,t2.CLRG_MU_TRFF_PCT
                     """) ;

# Extract the count of records retrieved and store it in a variable
df_9106_count = 0
df_9106_count = df_9106.count()

display(f"Records extracted for 9106 : {df_9106_count}")
#Prev 6026499 -> 6026441

# Count after changes to replace siebel arrangement with relation 9106 =>6026509

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9110                                                                              #   
# Credit card, Cash withdrawal, in foreign currency, exchange rate surcharge        #
# ----------------------------------------------------------------------------------#

df_9110  = spark.sql("""
                          WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                          select
                         '20'                                                             as recid
                         ,t2.CARDHDR_IBAN                                                 as iban
                         ,temp.CurrencyCode                                               as currencycode
                         ,'9110'                                                          as transactiongroup
                         ,date_format(t2.CARD_TXN_LCL_DT ,'dd-MM-yyyy')                   as transactiondate
                         ,REPLACE(CONCAT('+', (SUM(t2.Number_Of_Transactions) * 
                                              (CASE  
                                               WHEN t2.CARD_TXN_DB_CR_CODE = 'CRED' 
                                               THEN -1 
                                               ELSE 1 
                                               END) )),'+-', '-')                    as transactioncount
                         ,REPLACE(CONCAT('+',(SUM(CASE 
                                                  WHEN (( IFNULL(t2.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                                            - IFNULL(t2.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0)
                                                  THEN t2.Number_Of_Transactions
                                                  ELSE 0 
                                                  END) * 
                                                  (CASE 
                                                   WHEN t2.CARD_TXN_DB_CR_CODE = 'CRED' 
                                                   THEN -1 
                                                   ELSE 1 
                                                   END)) ),'+-', '-')     as ratedtransactioncount
                         ,'2'                                             as feeorinterestbasecode
                         ,'+0.00000'                                      as costperunitcurrencysmallestunitamount
                         ,CONCAT('+',CAST(ifnull(t2.CLRG_MU_TRFF_PCT,0) AS DECIMAL(9,5)))
                                                                          as costorinterestrate
                         ,REPLACE(CONCAT('+',cast(SUM((CASE 
                                                  WHEN (( IFNULL(t2.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                                            - IFNULL(t2.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0)) > 0)
                                                  THEN ( IFNULL(t2.CLRG_CARDHDR_BILL_EC_AMT, 0)
			                                            - IFNULL(t2.CLRG_CARDHDR_BILL_NO_MU_EC_AMT, 0))
                                                  ELSE 0
                                                  END) * (CASE WHEN t2.CARD_TXN_DB_CR_CODE = 'CRED' THEN -1 ELSE 1 END)) as decimal(18,0) )),
                                                  '+-', '-')
                                                                             as costorinterestcurrencysuamount
                         ,'EUR'                                              as costorinterestcurrencycode
                         ,'2'                                                as costorinterestamountdecimalsnumber
                         from
                         (select 
                               t1.CARD_TP_CODE                                  as CardTypeCode 
                              ,t1.CARDHDR_IBAN                                  as IBAN
                              ,t1.CARD_TXN_LCL_DT                               as transactiondate 
                              ,t1.CARD_TXN_LCL_TM                               as TransactionTimestamp
                              ,t1.CARD_TXN_BK_NO                                as BookingNumber
                              ,t1.CARD_TXN_ATM_POS_CODE                         as AtmPosCode
                              ,'EUR'                                            as CurrencyCode
                              
                              from v_temp_transactions t1 
                    
                              where 
                              t1.CARD_TP_CODE = 'CDT' 
                              and 
                              t1.CARD_TXN_ATM_POS_CODE = 'ATM'
                              and
                              t1.CARD_TXN_ORIG_N3_CCY_CODE <> 978 
                              and
                              t1.CARDHDR_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                              where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              
                              ) temp
                         
                         INNER JOIN v_temp_transactions t2
                            ON t2.CARD_TP_CODE = temp.CardTypeCode
                           and t2.CARDHDR_IBAN = temp.IBAN
                           and t2.CARD_TXN_LCL_TM = temp.TransactionTimestamp
                           and t2.CARD_TXN_LCL_DT = temp.TransactionDate
                           and t2.CARD_TXN_ORIG_BK_NO = temp.BookingNumber
                           and t2.CARD_TXN_ORIG_PCS_DCSN_CODE = 'TXNF'
                           and t2.CLRG_MU_TRFF_PCT is NOT null
                           

                         GROUP by 
                          t2.CARDHDR_IBAN
                         ,temp.CurrencyCode
                         ,temp.AtmPosCode
                         ,t2.CARD_TXN_LCL_DT
                         ,t2.CARD_TXN_DB_CR_CODE
                         ,t2.CLRG_MU_TRFF_PCT
                     """) ;

# Extract the count of records retrieved and store it in a variable
df_9110_count = 0
df_9110_count = df_9110.count()

display(f"Records extracted for 9110 : {df_9110_count}")
#Prev 243145 -> 243142

# Count after changes to replace siebel arrangement with relation 9106 =>243144

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 03DT                                                                              #   
# Overdraft credit card                                                             #
# ----------------------------------------------------------------------------------#

df_03dt = spark.sql("""
                    WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                    select
                         '30'                                                    as recid
                         ,temp.IBAN                                              as iban
                         ,temp.CurrencyCode                                      as currencycode
                         ,'03DT'                                                 as transactiongroup
                         ,date_format(temp.TransactionDate,'dd-MM-yyyy')         as transactiondate
                         ,'+0'                                                   as transactioncount
                         ,'+0'                                                   as ratedtransactioncount
                         ,'1'                                                    as feeorinterestbasecode
                         ,'+0.00000'                                        as costperunitcurrencysmallestunitamount
                         ,CONCAT('+',temp.CostOrInterestRate)                    as costorinterestrate
                         ,REPLACE(CONCAT('+',cast((SUM(temp.transactionamount) * (CASE WHEN temp.DebitCreditIndicator = 'C' THEN -1 ELSE 1 END)) as decimal(18,0))),'+-', '-')   
                                                                              as costorinterestcurrencysuamount
                         ,'EUR'                                               as costorinterestcurrencycode
                         ,'2'                                                 as costorinterestamountdecimalsnumber
                         from
                         (select
                               t1.CARDHDR_IBAN                                  as IBAN
                              ,'EUR'                                            as CurrencyCode
                              ,t1.CARD_TXN_LCL_DT                               as transactiondate  
                              ,substr(t1.CARD_TP_CODE,1,1)                      as DebitCreditIndicator
                              ,case when t1.CARD_AC_Y_EFF_INT_RATE_PCT > '' 
                                    then (cast(t1.CARD_AC_Y_EFF_INT_RATE_PCT as decimal(9,5))) 
                                    else '9.90000'
                                    end                                         as CostOrInterestRate 
                              ,t1.CLRG_CARDHDR_BILL_EC_AMT                      as transactionamount          
                              
                              from v_temp_transactions t1 
                    
                              where 
                              t1.CARD_TP_CODE = 'CDT' 
                              and 
                              t1.CARD_TXN_ORIG_PCS_DCSN_CODE = 'CRVI'
                              and
                              t1.CARDHDR_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                              where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.CARDHDR_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              ) temp
                          GROUP by 
                          temp.IBAN
                         ,temp.CurrencyCode
                         ,temp.transactiondate
                         ,temp.DebitCreditIndicator    
                         ,temp.costorinterestrate
                     """) ;

# Extract the count of records retrieved and store it in a variable
df_03dt_count = 0
df_03dt_count = df_03dt.count()

display(f"Records extracted for 03DT : {df_03dt_count}")

# Count after changes to replace siebel arrangement with relation 03DT => 0

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9111                                                                              #   
# Cross Border, outgoing transactions                                               #
# ----------------------------------------------------------------------------------#

df_9111 = spark.sql("""
                    WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                         cte_fnl_st_dts AS (
                         SELECT XB_PO_INR_MSG_ID, XB_PO_PYMT_ENG_FNL_ST_DTS, XB_PO_FNL_ST_CODE
                        FROM (
                               SELECT XB_PO_INR_MSG_ID, XB_PO_PYMT_ENG_FNL_ST_DTS, XB_PO_FNL_ST_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY XB_PO_INR_MSG_ID ORDER BY XB_PO_PYMT_ENG_FNL_ST_DTS DESC) as rn
                                FROM v_temp_xb_outgoing
                              ) sub
                         WHERE rn = 1
                    )

                    select
                         '20'                               as recid
                         ,temp.IBAN                         as iban
                         ,temp.CurrencyCode                 as currencycode
                         ,'9111'                            as transactiongroup
                         ,date_format(temp.transactiondate,'dd-MM-yyyy')       as transactiondate
                         ,REPLACE(CONCAT('+',SUM(temp.transactioncount)),'+-', '-')  
                                                            as transactioncount
                         ,REPLACE(CONCAT('+',SUM(temp.ratedtransactioncount)),'+-', '-')                        
                                                            as ratedtransactioncount
                         ,'2'                               as feeorinterestbasecode
                         ,'+0.00000'                        as costperunitcurrencysmallestunitamount
                         ,'+0.00000'                        as costorinterestrate
                         ,REPLACE(CONCAT('+',cast(SUM(temp.currencysuamount) as decimal(18,0))),'+-', '-')
                                                            as costorinterestcurrencysuamount
                         ,temp.costorinterestcurrencycode   as costorinterestcurrencycode
                         ,'2'                               as costorinterestamountdecimalsnumber
                         from
                         (
                          select
                               t1.OXB_PO_DB_AC_IBAN                             as IBAN
                              ,t1.OXB_PO_DB_AC_CCY_CODE                         as CurrencyCode
                              ,to_date(t1.XB_PO_PYMT_ENG_FNL_ST_DTS)            as transactiondate
                              ,t1.Number_Of_Transactions                        as transactioncount  

                              ,CASE WHEN (CASE WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_CR_CCY_CODE
                                            THEN (t1.OXB_PO_CCY_DB_AC_BOOK_AMT - (t1.OXB_PO_ORIG_CCY_AMT / (FLOAT(t1.OXB_PO_DT_EXG_RATE) + t2.HalfSpread))) * POWER(10,2)

                                            WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_DB_AC_CCY_CODE
                                            THEN ((t1.OXB_PO_ORIG_CCY_AMT * (FLOAT(t1.OXB_PO_CR_EXG_RATE) + t2.HalfSpread)) - t1.XB_PO_CR_CCY_AMT) * POWER(10,2)

                                            ELSE 0
                                            END)  > 0
                                       THEN 1
                                       ELSE 0
                                    END                                         as ratedtransactioncount 

                              ,CASE WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_CR_CCY_CODE
                                    THEN (t1.OXB_PO_CCY_DB_AC_BOOK_AMT - (t1.XB_PO_CR_CCY_AMT / (FLOAT(t1.OXB_PO_DT_EXG_RATE) + t2.HalfSpread))) * POWER(10,2)
                                    WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_DB_AC_CCY_CODE
                                    THEN ((t1.OXB_PO_ORIG_CCY_AMT * (FLOAT(t1.OXB_PO_CR_EXG_RATE) + t2.HalfSpread)) - t1.XB_PO_CR_CCY_AMT) * POWER(10,2)
                                    ELSE 0
                                    END                                         as currencysuamount   

                              ,CASE WHEN OXB_PO_ORIG_CCY_CODE = OXB_PO_CR_CCY_CODE
                                    THEN 'EUR'
                                    WHEN OXB_PO_ORIG_CCY_CODE = OXB_PO_DB_AC_CCY_CODE
                                    THEN OXB_PO_CR_CCY_CODE
                                    END                                         as costorinterestcurrencycode
                                                           
                              from v_temp_xb_outgoing t1 
                              
                              inner join v_temp_fx_rate t2
                                 on t1.OXB_PO_CR_CCY_CODE = t2.Currency
                              
                              where 
                                  t1.XB_PO_PD_CODE = 'WB'
                              and t1.XB_PO_FNL_ST_CODE = 'COMPLETE'
                              and t1.OXB_PO_DB_AC_CCY_CODE <> t1.OXB_PO_CR_CCY_CODE 
                              and t1.OXB_PO_DB_AC_CCY_CODE = 'EUR'
                              and t1.OXB_PO_CR_CCY_CODE <> 'EUR'
                              and t1.XB_PO_FX_RATE_AGRM_F = 'N'
                              and t1.XB_PO_MSG_CLSS_CODE IN ('OSN','PAY')
                              and 
                              t1.XB_PO_PYMT_ENG_FNL_ST_DTS IN (select t2.XB_PO_PYMT_ENG_FNL_ST_DTS
                                                         from cte_fnl_st_dts t2
                                                         where t1.XB_PO_INR_MSG_ID = t2.XB_PO_INR_MSG_ID)
                              and
                              t1.OXB_PO_DB_AC_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                                                        where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))   
                          
                          UNION ALL 

                          select
                               t1.OXB_PO_DB_AC_IBAN                             as IBAN
                              ,t1.OXB_PO_DB_AC_CCY_CODE                         as CurrencyCode
                              ,to_date(t1.XB_PO_PYMT_ENG_FNL_ST_DTS)            as transactiondate
                              ,t1.Number_Of_Transactions                        as transactioncount

                              ,CASE WHEN (CASE WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_CR_CCY_CODE
                                            THEN (t1.OXB_PO_CCY_DB_AC_BOOK_AMT - (t1.OXB_PO_ORIG_CCY_AMT * (FLOAT(t1.OXB_PO_DT_EXG_RATE) - t2.HalfSpread))) * POWER(10,2)
                                            WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_DB_AC_CCY_CODE
                                            THEN ((t1.OXB_PO_ORIG_CCY_AMT / (FLOAT(t1.OXB_PO_CR_EXG_RATE) - t2.HalfSpread)) - t1.XB_PO_CR_CCY_AMT) * POWER(10,2)
                                            ELSE 0
                                            END)  > 0
                                       THEN 1
                                       ELSE 0
                                    END                                         as ratedtransactioncount 

                              ,CASE WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_CR_CCY_CODE
                                    THEN (t1.OXB_PO_CCY_DB_AC_BOOK_AMT - (t1.OXB_PO_ORIG_CCY_AMT * (FLOAT(t1.OXB_PO_DT_EXG_RATE) - t2.HalfSpread))) * POWER(10,2)
                                    WHEN t1.OXB_PO_ORIG_CCY_CODE = t1.OXB_PO_DB_AC_CCY_CODE
                                    THEN ((t1.OXB_PO_ORIG_CCY_AMT / (FLOAT(t1.OXB_PO_CR_EXG_RATE) - t2.HalfSpread)) - t1.XB_PO_CR_CCY_AMT) * POWER(10,2)
                                    ELSE 0
                                    END                                         as currencysuamount   

                              ,CASE WHEN OXB_PO_ORIG_CCY_CODE = OXB_PO_CR_CCY_CODE
                                    THEN OXB_PO_DB_AC_CCY_CODE 
                                    WHEN OXB_PO_ORIG_CCY_CODE = OXB_PO_DB_AC_CCY_CODE
                                    THEN 'EUR'
                                    END                                         as costorinterestcurrencycode
                                                           
                              from v_temp_xb_outgoing t1 
                              
                              inner join v_temp_fx_rate t2
                                 on t1.OXB_PO_DB_AC_CCY_CODE = t2.Currency
                              
                              where 
                                  t1.XB_PO_PD_CODE = 'WB'
                              and t1.XB_PO_FNL_ST_CODE = 'COMPLETE'
                              and t1.OXB_PO_DB_AC_CCY_CODE <> t1.OXB_PO_CR_CCY_CODE 
                              and t1.OXB_PO_DB_AC_CCY_CODE <> 'EUR'
                              and t1.OXB_PO_CR_CCY_CODE = 'EUR'
                              and t1.XB_PO_FX_RATE_AGRM_F = 'N'
                              and t1.XB_PO_MSG_CLSS_CODE <> 'OPI'
                              and 
                              t1.XB_PO_PYMT_ENG_FNL_ST_DTS IN (select t2.XB_PO_PYMT_ENG_FNL_ST_DTS
                                                         from cte_fnl_st_dts t2
                                                         where t1.XB_PO_INR_MSG_ID = t2.XB_PO_INR_MSG_ID)
                              and
                              t1.OXB_PO_DB_AC_IBAN IN (select IBAN from v_temp_basicPaymentAgrm
                                                        where CCY_CODE = 'EUR')
                              and
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.OXB_PO_DB_AC_IBAN
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              ) temp
                     
                         GROUP by 
                          temp.transactiondate
                         ,temp.IBAN
                         ,temp.CurrencyCode
                         ,temp.costorinterestcurrencycode
                         HAVING ROUND(SUM(temp.currencysuamount),0) > 0
                     """) 

# Extract the count of records retrieved and store it in a variable
df_9111_count = 0
df_9111_count = df_9111.count()

display(f"Records extracted for 9111 : {df_9111_count}")
#Prev 157496

# Count after changes to replace siebel arrangement with relation 9111 =>157496

# COMMAND ----------

# ----------------------------------------------------------------------------------#
# 9112                                                                              #   
# Cross Border, incoming transactions                                               #
# ----------------------------------------------------------------------------------#

#sql query
df_9112 = spark.sql("""
                    WITH CTE_Dual_TypeCodes AS (
                         SELECT AR_AC_IBAN, EDL_VALID_TO_DTS,EDL_VALID_FROM_DTS, count(distinct(SBL_IP_TP_CODE)) as count_tpcode
                         FROM v_relation
                         WHERE 1 = 1
                         and AR_AC_IBAN > ''
                         and EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                         GROUP BY AR_AC_IBAN, EDL_VALID_TO_DTS, EDL_VALID_FROM_DTS
                         ),
                        CTE_TP_CODE AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         ),
                        CTE_TP_CODE_DELFLG AS (
                         SELECT AR_AC_IBAN,  SBL_IP_TP_CODE
                         FROM (
                                SELECT AR_AC_IBAN,  SBL_IP_TP_CODE, ROW_NUMBER() 
                                OVER (PARTITION BY AR_AC_IBAN ORDER BY EDL_VALID_FROM_DTS DESC) as rn
                                FROM v_relation
                                WHERE 
                                EDL_VALID_TO_DTS = '9999-12-31T00:00:00.000+00:00'
                                AND 
                                AR_AC_IBAN > ' '
                                AND 
                                DEL_F = 'N'
                              ) sub
                         WHERE rn = 1
                         ORDER BY AR_AC_IBAN
                         )
                    select
                         '20'                               as recid
                         ,temp.IBAN                         as iban
                         ,temp.CurrencyCode                 as currencycode
                         ,'9112'                            as transactiongroup
                         ,date_format(temp.transactiondate,'dd-MM-yyyy')       as transactiondate
                         ,REPLACE(CONCAT('+',SUM(temp.transactioncount)),'+-', '-')  
                                                            as transactioncount
                         ,REPLACE(CONCAT('+',SUM(temp.ratedtransactioncount)),'+-', '-')                        
                                                            as ratedtransactioncount
                         ,'2'                               as feeorinterestbasecode
                         ,'+0.00000'                        as costperunitcurrencysmallestunitamount
                         ,'+0.00000'                        as costorinterestrate
                         ,ifnull(REPLACE(CONCAT('+',cast(SUM(temp.currencysuamount) as decimal(18,0))),'+-', '-'),'+0')               
                                                            as costorinterestcurrencysuamount
                         ,'EUR'                             as costorinterestcurrencycode
                         ,'2'                               as costorinterestamountdecimalsnumber
                         from
                         (
                          select
                               t1.IXB_PO_CRDR_AC_NO                             as IBAN
                              ,t1.XB_PO_RABO_CR_AC_CCY_CODE                     as CurrencyCode
                              ,to_date(t1.XB_PO_PYMT_ENG_FNL_ST_DTS)            as transactiondate
                              ,t1.Number_Of_Transactions                        as transactioncount  

                              ,CASE WHEN (((t1.IXB_PO_DB_AC_BOOK_CCY_AMT / (FLOAT(t1.IXB_PO_CR_EXG_RATE) - t2.HalfSpread)) - t1.XB_PO_CR_CCY_AMT) * POWER(10,2)) > 0
                                       THEN 1
                                       ELSE 0
                                    END                                         as ratedtransactioncount 

                              ,((t1.IXB_PO_DB_AC_BOOK_CCY_AMT / (FLOAT(t1.IXB_PO_CR_EXG_RATE) - t2.HalfSpread)) - t1.XB_PO_CR_CCY_AMT) * POWER(10,2)                       as currencysuamount   

                              from v_temp_xb_incoming t1 
                              
                              inner join v_temp_fx_rate t2
                                 on t1.IXB_PO_DT_AC_CCY_CODE = t2.Currency
                              
                              where 
                                  t1.XB_PO_PD_CODE = 'WI'
                              and t1.XB_PO_FNL_ST_CODE = 'COMPLETE'
                              and t1.IXB_PO_DT_AC_CCY_CODE <> t1.XB_PO_RABO_CR_AC_CCY_CODE 
                              and t1.XB_PO_RABO_CR_AC_CCY_CODE = 'EUR'
                              and t1.IXB_PO_DT_AC_CCY_CODE <> 'EUR'
                              and t1.XB_PO_FX_RATE_AGRM_F = 'N'
                              and
                              t1.IXB_PO_CRDR_AC_NO IN (select IBAN from v_temp_basicPaymentAgrm
                                                        where CCY_CODE = 'EUR')
                              and 
                              ((exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.IXB_PO_CRDR_AC_NO
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE_DELFLG t3
                                    WHERE t3.AR_AC_IBAN = t1.IXB_PO_CRDR_AC_NO
                                    AND t3.SBL_IP_TP_CODE = 'Person'
                                    )
                              )
                              OR 
                              (NOT exists(SELECT 1
                                    FROM CTE_Dual_TypeCodes t2
                                    WHERE t2.AR_AC_IBAN = t1.IXB_PO_CRDR_AC_NO
                                    )
                                AND
                                exists(SELECT 1
                                    FROM CTE_TP_CODE t4
                                    WHERE t4.AR_AC_IBAN = t1.IXB_PO_CRDR_AC_NO
                                    AND t4.SBL_IP_TP_CODE = 'Person'
                                    )
                              ))
                              ) temp
                     
                         GROUP by 
                          temp.transactiondate
                         ,temp.IBAN
                         ,temp.CurrencyCode
                     """) ;

# Extract the count of records retrieved and store it in a variable
df_9112_count = 0
df_9112_count = df_9112.count()

display(f"Records extracted for 9112: {df_9112_count}")
#Prev 44712

# Count after changes to replace siebel arrangement with relation 9112 => 44712

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write the output file

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Build Header for the output file

# COMMAND ----------

# This header needs to be added to each file that will be copied to the Azure blob container from DBFS
# Sample header record is shown below:
# 10;STF;001;20-12-2024;18:21:10;01-04-2023;31-03-2024;IFP;"";"";"";"";""
 
import datetime

run_datetime    = datetime.datetime.now()
run_date        = run_datetime.strftime("%d-%m-%Y")
run_time        = run_datetime.strftime("%H:%M:%S")

header_rec_df   = spark.createDataFrame(
[("10", "STF","001",run_date,run_time,f"01-04-{previous_year}",f"31-03-{current_year}","IFP","","","","","")],
["recid", "filecode","seqno","rundate","runtime","startdate","enddate","source","filler1","filler2","filler3","filler4","filler5"] 
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Disable the adhoc files from being written

# COMMAND ----------

# To avoid any metadata being written in the output file. When you write the output from a dataframe in an output folder, in addition to the actual folder that contains the output file, there will be additional folders created in the same path to containe meta data infomration.
# The below logic will disable the creation of _commited_xxx, _started_xxx and _SUCCESS files in the container

spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile","false") 
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generic function to write dataframe in Output folder

# COMMAND ----------

# Generic function to write dataframe in output folder in DBFS. Please note that the extracted output is first written within databricks FileStore (DBFS) before placing it in the Azure blob container. In this logic, the header record is added at the top and trailer record is added at the bottom of the file. The layout is designed to be same as that of On-Prem CGW, as requested.

# Note: Trailer record has count of total records in the file including the header and trailer record 
# Example of trailer record is shown below: 
# 90;01000009

def write_df_to_output_folder(df,transaction_group):
    count_rec = int(df.count()) + 2
    
    trailer_rec_df   = spark.createDataFrame(
    [("90", f"{count_rec:08d}","","","","","","","","","","","")],
    ["recid", "count", "filler1", "filler2", "filler3","filler4","filler5","filler6","filler7","filler8",
    "filler9","filler10","filler11"] )
    
    combined_df = header_rec_df.union(df).union(trailer_rec_df)

    # Delete the temporary files from DBFS
    temp_folder = f"{Output_folder}/Transaction_Group_{transaction_group}.csv"
    dbutils.fs.rm(temp_folder,recurse=True)
    
    combined_df \
    .coalesce(1) \
    .write \
    .mode('overwrite')     \
    .option("header",False) \
    .option("sep",";") \
    .csv(temp_folder)

    print(f"Number of records written for {transaction_group} is {count_rec}")

# COMMAND ----------

# The generic fucntion to place the output in DBFS is called here for all 10 transaction groups i.e. 9101, 9107, 9104, 9108, 9109, 9106, 9110, 03DT, 9111 and 9112. It is accepted if there is no output file created for 03DT as it might not have any data in production and we do not write empty files i.e. output files are created only if there are records to be reported for the particular transaction group. 
# Note: We did not have any 03DT transactions in 2024 SOF file.

#--------------------------
# Debit card transactions #
#--------------------------
# execute generic function to write the output of 9101 transaction group
if  df_9101_count > 0:
    write_df_to_output_folder(df_9101, "9101")

# execute generic function to write the output of 9107 transaction group
if  df_9107_count > 0:
    write_df_to_output_folder(df_9107, "9107")

#---------------------------
# Credit card transactions # 
#---------------------------

# execute generic function to write the output of 9104 transaction group
if  df_9104_count > 0:
    write_df_to_output_folder(df_9104, "9104")

# execute generic function to write the output of 9108 transaction group
if  df_9108_count > 0:
    write_df_to_output_folder(df_9108, "9108")

# execute generic function to write the output of 9109 transaction group
if  df_9109_count > 0:
    write_df_to_output_folder(df_9109, "9109")


# execute generic function to write the output of 9106 transaction group
if  df_9106_count > 0:
    write_df_to_output_folder(df_9106, "9106")

# execute generic function to write the output of 9110 transaction group
if  df_9110_count > 0:
    write_df_to_output_folder(df_9110, "9110")

#-------------------------
# Roodstan transactions  #
#-------------------------

# execute generic function to write the output of 03DT transaction group
if  df_03dt_count > 0:
    write_df_to_output_folder(df_03dt, "03DT")

#----------------------------
# Cross Border transactions #
#----------------------------

# execute generic function to write the output of 9111 transaction group
if  df_9111_count > 0:
    write_df_to_output_folder(df_9111, "9111")


# execute generic function to write the output of 9112 transaction group
if  df_9112_count > 0:
    write_df_to_output_folder(df_9112, "9112")

#--------------------------------------------------------------------------------------------------------------
# Statistics                                                                                                  #
#--------------------------------------------------------------------------------------------------------------
# This logic has display to print the number of records being processed for each transaction group. 
# To give an idea on the volume of data being processed, please find the statistics for 2024 SOF period below:
#
# Number of records written for 9101 is  7.701.052
# Number of records written for 9107 is  1.189.688
# Number of records written for 9104 is 10.694.269
# Number of records written for 9108 is  1.468.055
# Number of records written for 9109 is    246.292
# Number of records written for 9106 is  6.024.317
# Number of records written for 9110 is    243.098
# Number of records written for 9111 is    157.498
# Number of records written for 9112 is     44.714
#                                      -------------    
#              Total number of records: 27.768.983 
#--------------------------------------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC #### Copy the output to temp folder with correct filename and then to desired Azure blob container

# COMMAND ----------

# If you instruct Databricks to write a file called abc.csv, due to its distributed nature, it will create a folder with name abc.csv and the contents of the file will be written in multiple partitions in parallel for efficient and fast processing.The names of these partitions will be dynamically generated of the format part-000*. Though, we can restrict the number of partitions into 1 by using repartition or coalesce, the file name will still be a dynamically generated (part-000*) inside a folder.

# The below logic will identify the name of the part file inside the folder 'dbfs:/FileStore/SOF/Output/Transaction_Group_T<transaction_code>.csv/' that was dynamically generated and write it in "dbfs:/FileStore/SOF/Output/" folder with the file name of format 'SOF_{str(current_year)}_T{transaction_code}.csv'. 
# Example filename: SOF_2024_T9101.csv


def copy_output_to_temp(transaction_code): 
    
    # This is the folder where the output of each transaction group is saved in DBFS
    outfolder = dbutils.fs.ls('dbfs:/FileStore/SOF/Output/Transaction_Group_' + transaction_code + '.csv/')

    # Find the part file inside the output folder
    part_file = [file.path for file in outfolder if "part" in file.name][0]

    # Dynamically create the expected filename in the Azure blob storage container
    outfile   = f'SOF_{str(current_year)}_T{transaction_code}.csv'

    # Define the temporary path within DBFS to save the file with the desired file name
    temp_path = "dbfs:/FileStore/SOF/Output/"+outfile
    
    # Move the part file to the temporary location with the new file name
    dbutils.fs.mv(part_file, temp_path)


#Call the function to move the output of the supplied transaction group with desired filename to temp path
if  df_9101_count > 0:
    copy_output_to_temp('9101')

if  df_9107_count > 0:
    copy_output_to_temp('9107')

if  df_9104_count > 0:
    copy_output_to_temp('9104')

if  df_9108_count > 0:
    copy_output_to_temp('9108')

if  df_9109_count > 0:
    copy_output_to_temp('9109')

if  df_9106_count > 0:
    copy_output_to_temp('9106')

if  df_9110_count > 0:
    copy_output_to_temp('9110')        

if  df_9111_count > 0:
    copy_output_to_temp('9111')

if  df_9112_count > 0:
    copy_output_to_temp('9112')

# COMMAND ----------

# The below function moves the csv file from DBFS to azure blob container in ifp/exchange/SOF folder.
def copy_temp_to_azure(transaction_code): 

    # Dynamically create the expected filename in the Azure blob storage container
    outfile   = f'SOF_{str(current_year)}_T{transaction_code}.csv'

    # Define the temporary path within DBFS to save the file with the desired file name
    temp_path = "dbfs:/FileStore/SOF/Output/"+outfile

    # Define the location of the Azure blob container (exchange) where the file will be picked up by RFTM
    output = output_path+outfile

    # Move the data to Azure blob container
    dbutils.fs.mv(temp_path, output)
    display(f'{outfile} is written in {output_path}')

#Call the function to move the output of the supplied transaction group from DBFS to Azure blob container

if  df_9101_count > 0:
    copy_temp_to_azure('9101')

if  df_9107_count > 0:
    copy_temp_to_azure('9107')

if  df_9104_count > 0:
    copy_temp_to_azure('9104')

if  df_9108_count > 0:
    copy_temp_to_azure('9108')

if  df_9109_count > 0:
    copy_temp_to_azure('9109')

if  df_9106_count > 0:
    copy_temp_to_azure('9106')

if  df_9110_count > 0:
    copy_temp_to_azure('9110')        

if  df_9111_count > 0:
    copy_temp_to_azure('9111')

if  df_9112_count > 0:
    copy_temp_to_azure('9112')

# COMMAND ----------

# If any file(s) needs to be removed from a path i.e. DBFS, Azure blob etc, the following command can be used.
#dbutils.fs.rm('/mnt/ifp-exchange/SOF/SOF_2025_T9112.csv')

#Note: 
#There is no residual file or file at rest in this SOF flow. The files from DBFS are moved to the Azure blob container. As soon as the file is landed in Azure blob container, RFTM picks up the file and transfers them to the SOF microservices team. RFTM also deletes the file from blob container, once the transfer is successful.

# COMMAND ----------

# MAGIC %md
# MAGIC #### ********     End of Program     ******** 
# MAGIC