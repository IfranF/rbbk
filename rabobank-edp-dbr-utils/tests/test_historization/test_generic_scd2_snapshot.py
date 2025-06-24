import sys
import os
import pytest
from pathlib import Path
from datetime import date,datetime
from databricks.sdk.runtime import spark, dbutils
from pyspark.sql.functions import date_format,current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.historization import generic_scd2
from rabobank_edp_dbr_utils.transformation import covert_columns_in_upper_case
from rabobank_edp_dbr_utils.utils_historization import create_content_hash, generate_nonkey_column

def cleanup_test_results(destination='/dbfs/user/hive/warehouse/tests/foldertests/SCD2_snapshot_table'): # Cleanup the test results before starting historied delta table
    
    try:
        print("Cleaning up before testing")
        #destination = '/dbfs/user/hive/warehouse/tests/foldertests/SCD2_snapshot_table'
        print(destination)
        dbutils.fs.rm(destination, True)
    except Exception as e:
        print("Error while deleting the folders", e)

def test_generic_scd2_snapshot_day1_positive(location="test_scenario_1"):

    # Input data
    incr_data = [(1, "John", "2021-01-01", "IND", datetime(2021, 1, 1, 0, 0, 0)),
                  (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0)),
                  (3, "Doe", "2021-01-03", "NL", datetime(2021, 1, 3, 0, 0, 0))]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True),
    ])
    incr_df = spark.createDataFrame(incr_data, schema)
    upper_df = covert_columns_in_upper_case(incr_df)
    non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE'])
    source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
    source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
    print("Source Dataframe")
    source_key_nonkey_df.show()
    
    # Create Expected output data
    expected_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True),
        StructField("SOME_BUSINESS_DTS", TimestampType(), True),
        StructField("EDL_LOAD_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS_UTC", TimestampType(), True),
        StructField("EDL_LAST_UPDATE_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS_UTC", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS_UTC", TimestampType(), True),
        StructField("EDL_ACTIVE_FLG", StringType(), True),
        StructField("EDL_DELETED_FLG", StringType(), True),
        StructField("start_date_id", StringType(), True)
    ])
    current_datetime=datetime.now()
    expected_data = [
        (1, 'John', '2021-01-01', 'IND', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 1, 0, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2021-01-01'),
        (2, 'Jane', '2021-01-02', 'US', 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day,  current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 1, 0, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2021-01-02'),
        (3, 'Doe', '2021-01-03', 'NL', 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 1, 0, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2021-01-03')
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    print("Expected Dataframe")
    expected_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/SCD2_snapshot_table/{location}'
    print("destination_path: ", destination_path)

    try:
        # Convert PosixPath to string if necessary
        if isinstance(destination_path, Path):
            destination_path = str(destination_path)

        # Call the function
        generic_scd2(source_key_nonkey_df, 
                                 destination_path, 
                                 'start_date_id', 
                                 'START_DATE',
                                 '2000-01-01',
                                 'snapshot',
                                 primary_key_hash='EDL_KEY_HASH', 
                                 non_primary_key_hash='EDL_NONKEY_HASH',
                                 edl_update_datetime='EDL_LAST_UPDATE_DTS',
                                 edl_valid_from_date='EDL_VALID_FROM_DTS',
                                 edl_valid_to_date="EDL_VALID_TO_DTS",
                                 edl_valid_from_date_utc='EDL_VALID_FROM_DTS_UTC',
                                 edl_valid_to_date_utc='EDL_VALID_TO_DTS_UTC',
                                 edl_active_flag='EDL_ACTIVE_FLG',
                                 edl_deleted_flg="EDL_DELETED_FLG",
                                 edl_load_date='EDL_LOAD_DTS',
                                 edl_act_datetime='EDL_ACT_DTS',
                                 edl_act_datetime_value='SOME_BUSINESS_DTS',
                                 edl_act_datetime_utc='EDL_ACT_DTS_UTC',
                                 merge_schema=True
                                 #,timezone='CET'
                                 )

        # Read the result (from dbfs)
        result_df = spark.read.format("delta").load(destination_path)
        print("Final Dataframe")
        result_df.show()
        
    except Exception as e:
        print("Error: ", e)
    
    # Assert the result
    columns_to_compare = [col for col in result_df.columns if col not in ["EDL_KEY_HASH", "EDL_NONKEY_HASH"]]
    result_df=result_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))
    expected_df=expected_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))

    result_df_sorted=result_df.select(columns_to_compare).sort(*result_df.columns)
    expected_df_sorted=expected_df.select(columns_to_compare).sort(*expected_df.columns)
    print("result_df_sorted")
    result_df_sorted.show(5,False)
    print("expected_df_sorted")
    expected_df_sorted.show(5,False)
    assert result_df_sorted.collect() == expected_df_sorted.collect()

    #remove created files
    if location == 'test_scenario_1':
        #dbutils.fs.rm(destination_path, recurse=True)
        cleanup_test_results(destination_path)
        # pass

def test_generic_scd2_snapshot_day2_positive(location='test_scenario_2'):

    # Input data
    incr_data = [(1, "John", "2021-01-01", "FR", datetime(2021, 1, 1, 0, 0, 0)),
                  (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0)),
                  (4, "Joe", "2022-10-11", "JPN", datetime(2022, 10, 11, 0, 0, 0))]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True),
    ])
    incr_df = spark.createDataFrame(incr_data, schema)
    upper_df = covert_columns_in_upper_case(incr_df)
    non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE'])
    source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
    source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
    print("Source Dataframe")
    source_key_nonkey_df.show()
    
    # Create Expected output data
    expected_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True),
        StructField("SOME_BUSINESS_DTS", TimestampType(), True),
        StructField("EDL_LOAD_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS_UTC", TimestampType(), True),
        StructField("EDL_LAST_UPDATE_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS_UTC", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS_UTC", TimestampType(), True),
        StructField("EDL_ACTIVE_FLG", StringType(), True),
        StructField("EDL_DELETED_FLG", StringType(), True),
        StructField("start_date_id", StringType(), True)
    ])
    current_datetime=datetime.now()
    expected_data = [
        (1, 'John', '2021-01-01', 'IND', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         'N','N', '2021-01-01'),
        (1, 'John', '2021-01-01', 'FR', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0),
         'Y','N', '2021-01-01'),
        (2, 'Jane', '2021-01-02', 'US', 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 1, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2021-01-02'),
        (3, 'Doe', '2021-01-03', 'NL', 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         'N','Y', '2021-01-03'),
        (4, 'Joe', '2022-10-11', 'JPN', 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2022-10-11')
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    print("Expected Dataframe")
    expected_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/SCD2_snapshot_table/{location}'
    print("destination_path: ", destination_path)

    try:
        # Convert PosixPath to string if necessary
        if isinstance(destination_path, Path):
            destination_path = str(destination_path)

        # create test data with first day SCD2 run
        test_generic_scd2_snapshot_day1_positive(location)
        print("test data for day 1 created successfully!")

        # Call the function
        generic_scd2(source_key_nonkey_df, 
                                 destination_path, 
                                 'start_date_id', 
                                 'START_DATE',
                                 '2000-01-02',
                                 'snapshot',
                                 primary_key_hash='EDL_KEY_HASH', 
                                 non_primary_key_hash='EDL_NONKEY_HASH',
                                 edl_update_datetime='EDL_LAST_UPDATE_DTS',
                                 edl_valid_from_date='EDL_VALID_FROM_DTS',
                                 edl_valid_to_date="EDL_VALID_TO_DTS",
                                 edl_valid_from_date_utc='EDL_VALID_FROM_DTS_UTC',
                                 edl_valid_to_date_utc='EDL_VALID_TO_DTS_UTC',
                                 edl_active_flag='EDL_ACTIVE_FLG',
                                 edl_deleted_flg="EDL_DELETED_FLG",
                                 edl_load_date='EDL_LOAD_DTS',
                                 edl_act_datetime='EDL_ACT_DTS',
                                 edl_act_datetime_value='SOME_BUSINESS_DTS',
                                 edl_act_datetime_utc='EDL_ACT_DTS_UTC',
                                 merge_schema=True
                                 #,timezone='CET'
                                 )

        # Read the result (from dbfs)
        result_df = spark.read.format("delta").load(destination_path)
        print("Final Dataframe")
        result_df.show()

    except Exception as e:
        print("Error: ", e)
    
    # Assert the result
    columns_to_compare = [col for col in result_df.columns if col not in ["EDL_KEY_HASH", "EDL_NONKEY_HASH"]]
    result_df=result_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))
    expected_df=expected_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))

    result_df_sorted=result_df.select(columns_to_compare).sort(*result_df.columns).orderBy(['CUSTOMER_ID', 'EDL_VALID_FROM_DTS'])
    expected_df_sorted=expected_df.select(columns_to_compare).sort(*expected_df.columns).orderBy(['CUSTOMER_ID', 'EDL_VALID_FROM_DTS'])
    print("result_df_sorted")
    result_df_sorted.show(5,False)
    print("expected_df_sorted")
    expected_df_sorted.show(5,False)
    assert result_df_sorted.collect() == expected_df_sorted.collect()

    #remove created files
    if location == 'test_scenario_2':
        #dbutils.fs.rm(destination_path, recurse=True)
        cleanup_test_results(destination_path)
        # pass

def test_generic_scd2_snapshot_day3_reactivate_deleted(location='test_scenario_reactivate'):

    # Input data
    incr_data = [(1, "John", "2021-01-01", "FR", datetime(2021, 1, 1, 0, 0, 0)),
                  (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0)),
                  (3, "Doe", "2021-01-03", "NL", datetime(2021, 1, 3, 0, 0, 0)),
                  (4, "Joe", "2022-10-11", "JPN", datetime(2022, 10, 11, 0, 0, 0))]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True),
    ])
    incr_df = spark.createDataFrame(incr_data, schema)
    upper_df = covert_columns_in_upper_case(incr_df)
    non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE'])
    source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
    source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
    print("Source Dataframe")
    source_key_nonkey_df.show()
    
    # Create Expected output data
    expected_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True),
        StructField("SOME_BUSINESS_DTS", TimestampType(), True),
        StructField("EDL_LOAD_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS_UTC", TimestampType(), True),
        StructField("EDL_LAST_UPDATE_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS_UTC", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS_UTC", TimestampType(), True),
        StructField("EDL_ACTIVE_FLG", StringType(), True),
        StructField("EDL_DELETED_FLG", StringType(), True),
        StructField("start_date_id", StringType(), True)
    ])
    current_datetime=datetime.now()
    expected_data = [
        (1, 'John', '2021-01-01', 'IND', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         'N','N', '2021-01-01'),
        (1, 'John', '2021-01-01', 'FR', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0),
         'Y','N', '2021-01-01'),
        (2, 'Jane', '2021-01-02', 'US', 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 1, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2021-01-02'),
        (3, 'Doe', '2021-01-03', 'NL', 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         'N','Y', '2021-01-03'),
        (3, 'Doe', '2021-01-03', 'NL', 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0),  
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0),  
         'Y','N', '2021-01-03'),
        (4, 'Joe', '2022-10-11', 'JPN', 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 2, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         'Y','N', '2022-10-11')
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    print("Expected Dataframe")
    expected_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/SCD2_snapshot_table/{location}'
    print("destination_path: ", destination_path)

    try:
        # Convert PosixPath to string if necessary
        if isinstance(destination_path, Path):
            destination_path = str(destination_path)

        # create test data with first day SCD2 run
        test_generic_scd2_snapshot_day2_positive(location)
        print("test data for day 2 created successfully!")

        # Call the function
        generic_scd2(source_key_nonkey_df, 
                                 destination_path, 
                                 'start_date_id', 
                                 'START_DATE',
                                 '2000-01-02',
                                 'snapshot',
                                 primary_key_hash='EDL_KEY_HASH', 
                                 non_primary_key_hash='EDL_NONKEY_HASH',
                                 edl_update_datetime='EDL_LAST_UPDATE_DTS',
                                 edl_valid_from_date='EDL_VALID_FROM_DTS',
                                 edl_valid_to_date="EDL_VALID_TO_DTS",
                                 edl_valid_from_date_utc='EDL_VALID_FROM_DTS_UTC',
                                 edl_valid_to_date_utc='EDL_VALID_TO_DTS_UTC',
                                 edl_active_flag='EDL_ACTIVE_FLG',
                                 edl_deleted_flg="EDL_DELETED_FLG",
                                 edl_load_date='EDL_LOAD_DTS',
                                 edl_act_datetime='EDL_ACT_DTS',
                                 edl_act_datetime_value='SOME_BUSINESS_DTS',
                                 edl_act_datetime_utc='EDL_ACT_DTS_UTC',
                                 merge_schema=True
                                 #,timezone='CET'
                                 )

        # Read the result (from dbfs)
        result_df = spark.read.format("delta").load(destination_path)
        print("Final Dataframe")
        result_df.show()

    except Exception as e:
        print("Error: ", e)
    
    # Assert the result
    columns_to_compare = [col for col in result_df.columns if col not in ["EDL_KEY_HASH", "EDL_NONKEY_HASH"]]
    result_df=result_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))
    expected_df=expected_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))

    result_df_sorted=result_df.select(columns_to_compare).sort(*result_df.columns).orderBy(['CUSTOMER_ID', 'EDL_VALID_FROM_DTS'])
    expected_df_sorted=expected_df.select(columns_to_compare).sort(*expected_df.columns).orderBy(['CUSTOMER_ID', 'EDL_VALID_FROM_DTS'])
    print("result_df_sorted")
    result_df_sorted.show(5,False)
    print("expected_df_sorted")
    expected_df_sorted.show(5,False)
    assert result_df_sorted.collect() == expected_df_sorted.collect()

    #remove created files
    if location == 'test_scenario_reactivate':
        #dbutils.fs.rm(destination_path, recurse=True)
        cleanup_test_results(destination_path)
        # pass

def test_generic_scd2_snapshot_day3_merge_schema():

    # Input data
    incr_data = [(1, "John", "2021-01-01", "FR", datetime(2021, 1, 1, 0, 0, 0), "SEPA"), 
                 (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0), "DM"), 
                 (4, "Joe", "2022-10-11", "JPN", datetime(2022, 10, 11, 0, 0, 0), "IT")]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True),
        StructField("Department", StringType(), True)
    ])
    incr_df = spark.createDataFrame(incr_data, schema)
    upper_df = covert_columns_in_upper_case(incr_df)
    non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE'])
    source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
    source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
    print("Source Dataframe")
    source_key_nonkey_df.show()
    
    # Create Expected output data
    expected_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True),
        StructField("SOME_BUSINESS_DTS", TimestampType(), True),
        StructField("EDL_LOAD_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS", TimestampType(), True),
        StructField("EDL_ACT_DTS_UTC", TimestampType(), True),
        StructField("EDL_LAST_UPDATE_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS", TimestampType(), True),
        StructField("EDL_VALID_FROM_DTS_UTC", TimestampType(), True),
        StructField("EDL_VALID_TO_DTS_UTC", TimestampType(), True),
        StructField("EDL_ACTIVE_FLG", StringType(), True),
        StructField("EDL_DELETED_FLG", StringType(), True),
        StructField("start_date_id", StringType(), True),
        StructField("DEPARTMENT", StringType(), True)
    ])
    current_datetime=datetime.now()
    expected_data = [
        (1, 'John', '2021-01-01', 'IND', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59),
         'N','N', '2021-01-01', None),
        (1, 'John', '2021-01-01', 'FR', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(2000, 1, 2, 23, 59, 59), 
         datetime(2000, 1, 2, 0, 0),
         datetime(2000, 1, 2, 23, 59, 59),
         'N','N', '2021-01-01', None),
        (1, 'John', '2021-01-01', 'FR', 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(2021, 1, 1, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 3, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 3, 0, 0),
         datetime(9999, 12, 31, 0, 0),
         'Y','N', '2021-01-01', 'SEPA'),        
        (2, 'Jane', '2021-01-02', 'US', 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 2, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 2, 23, 59, 59),
         'N','N', '2021-01-02', None),
        (2, 'Jane', '2021-01-02', 'US', 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(2021, 1, 2, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 3, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 3, 0, 0),
         datetime(9999, 12, 31, 0, 0),
         'Y','N', '2021-01-02', 'DM'),
        (3, 'Doe', '2021-01-03', 'NL', 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(2021, 1, 3, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59), 
         datetime(2000, 1, 1, 0, 0),
         datetime(2000, 1, 1, 23, 59, 59),
         'N','Y', '2021-01-03', None),
        (4, 'Joe', '2022-10-11', 'JPN', 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 2, 0, 0),
         datetime(2000, 1, 2, 23, 59, 59), 
         datetime(2000, 1, 2, 0, 0),
         datetime(2000, 1, 2, 23, 59, 59),
         'N','N', '2022-10-11', None),
        (4, 'Joe', '2022-10-11', 'JPN', 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(2022, 10, 11, 0, 0, 0),
         datetime(current_datetime.year, current_datetime.month, current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second), 
         datetime(2000, 1, 3, 0, 0),
         datetime(9999, 12, 31, 0, 0), 
         datetime(2000, 1, 3, 0, 0),
         datetime(9999, 12, 31, 0, 0),
         'Y','N', '2022-10-11', 'IT')        
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)
    print("Expected Dataframe")
    expected_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/SCD2_snapshot_table/test_scenario_3'
    print("destination_path: ", destination_path)

    try:
        # Convert PosixPath to string if necessary
        if isinstance(destination_path, Path):
            destination_path = str(destination_path)

        # create test data with first day SCD2 run
        test_generic_scd2_snapshot_day2_positive("test_scenario_3")
        print("test data for day 2 created successfully!")

        # Call the function
        generic_scd2(source_key_nonkey_df, 
                                 destination_path, 
                                 'start_date_id', 
                                 'START_DATE',
                                 '2000-01-03',
                                 'snapshot',
                                 primary_key_hash='EDL_KEY_HASH', 
                                 non_primary_key_hash='EDL_NONKEY_HASH',
                                 edl_update_datetime='EDL_LAST_UPDATE_DTS',
                                 edl_valid_from_date='EDL_VALID_FROM_DTS',
                                 edl_valid_to_date="EDL_VALID_TO_DTS",
                                 edl_valid_from_date_utc='EDL_VALID_FROM_DTS_UTC',
                                 edl_valid_to_date_utc='EDL_VALID_TO_DTS_UTC',
                                 edl_active_flag='EDL_ACTIVE_FLG',
                                 edl_deleted_flg="EDL_DELETED_FLG",
                                 edl_load_date='EDL_LOAD_DTS',
                                 edl_act_datetime='EDL_ACT_DTS',
                                 edl_act_datetime_value='SOME_BUSINESS_DTS',
                                 edl_act_datetime_utc='EDL_ACT_DTS_UTC',
                                 merge_schema=True
                                 #,timezone='CET'
                                 )

        #Read the result (from dbfs)
        result_df = spark.read.format("delta").load(destination_path)
        print("Final Dataframe")
        result_df.show()
    except Exception as e:
        print("Error: ", e)
    
    # Assert the result
    columns_to_compare = [col for col in result_df.columns if col not in ["EDL_KEY_HASH", "EDL_NONKEY_HASH"]]
    result_df=result_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))
    expected_df=expected_df.withColumn("EDL_LOAD_DTS", date_format("EDL_LOAD_DTS",'yyyy-MM-dd hh'))\
        .withColumn("EDL_LAST_UPDATE_DTS", date_format("EDL_LAST_UPDATE_DTS",'yyyy-MM-dd hh'))

    result_df_sorted=result_df.select(columns_to_compare).sort(*result_df.columns).orderBy(['CUSTOMER_ID', 'EDL_VALID_FROM_DTS'])
    expected_df_sorted=expected_df.select(columns_to_compare).sort(*expected_df.columns).orderBy(['CUSTOMER_ID', 'EDL_VALID_FROM_DTS'])
    print("result_df_sorted")
    result_df_sorted.show(100,False)
    print("expected_df_sorted")
    expected_df_sorted.show(100,False)
    assert result_df_sorted.collect() == expected_df_sorted.collect()

    #remove created files
    #dbutils.fs.rm(destination_path, recurse=True)
    cleanup_test_results(destination_path)
    
def test_generic_scd2_snapshot_day2_merge_schema_false():

    # Input data
    incr_data = [(1, "John", "2021-01-01", "FR", datetime(2021, 1, 1, 0, 0, 0), "SEPA"), 
                 (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0), "DM"), 
                 (4, "Joe", "2022-10-11", "JPN", datetime(2022, 10, 11, 0, 0, 0), "IT")]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True),
        StructField("Department", StringType(), True)
    ])
    incr_df = spark.createDataFrame(incr_data, schema)
    upper_df = covert_columns_in_upper_case(incr_df)
    non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE'])
    source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
    source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
    print("Source Dataframe")
    source_key_nonkey_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/SCD2_snapshot_table/test_scenario_4'
    print("destination_path: ", destination_path)

    # Convert PosixPath to string if necessary
    if isinstance(destination_path, Path):
        destination_path = str(destination_path)

    # create test data with first day SCD2 run
    test_generic_scd2_snapshot_day1_positive("test_scenario_4")
    print("test data for day 1 created successfully!")


    # Assert the result
    with pytest.raises(Exception, match="Error occured during upsert: SCD2 snapshot failed"):
        generic_scd2(source_key_nonkey_df, 
                                 destination_path, 
                                 'start_date_id', 
                                 'START_DATE',
                                 '2000-01-04',
                                 'snapshot',
                                 primary_key_hash='EDL_KEY_HASH', 
                                 non_primary_key_hash='EDL_NONKEY_HASH',
                                 edl_update_datetime='EDL_LAST_UPDATE_DTS',
                                 edl_valid_from_date='EDL_VALID_FROM_DTS',
                                 edl_valid_to_date="EDL_VALID_TO_DTS",
                                 edl_valid_from_date_utc='EDL_VALID_FROM_DTS_UTC',
                                 edl_valid_to_date_utc='EDL_VALID_TO_DTS_UTC',
                                 edl_active_flag='EDL_ACTIVE_FLG',
                                 edl_deleted_flg="EDL_DELETED_FLG",
                                 edl_load_date='EDL_LOAD_DTS',
                                 edl_act_datetime='EDL_ACT_DTS',
                                 edl_act_datetime_value='SOME_BUSINESS_DTS',
                                 edl_act_datetime_utc='EDL_ACT_DTS_UTC',
                                 merge_schema=False
                                 #,timezone='CET'
                                 )
    
    #remove created files
    cleanup_test_results(destination_path)

if __name__ == "__main__":
    cleanup_test_results()
    test_generic_scd2_snapshot_day1_positive()
    test_generic_scd2_snapshot_day2_positive()
    test_generic_scd2_snapshot_day3_reactivate_deleted()
    test_generic_scd2_snapshot_day3_merge_schema()
    test_generic_scd2_snapshot_day2_merge_schema_false()
    cleanup_test_results()