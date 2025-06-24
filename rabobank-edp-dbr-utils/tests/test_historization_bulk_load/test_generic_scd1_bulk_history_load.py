import sys
import os
import pytest
from pathlib import Path
from datetime import datetime,date
from databricks.sdk.runtime import spark, dbutils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.historization_bulk_load import generic_scd1_bulk_history_load
from rabobank_edp_dbr_utils.transformation import covert_columns_in_upper_case
from rabobank_edp_dbr_utils.utils_historization import create_content_hash, generate_nonkey_column

def cleanup_test_results(): # Cleanup the test results before starting historied delta table
    
    try:
        print("Cleaning up before testing")
        destination = '/dbfs/user/hive/warehouse/tests/foldertests/scd1_bulk_table'
        print(destination)
        dbutils.fs.rm(destination, True)
    except Exception as e:
        print("Error while deleting the folders", e)

def test_generic_scd1_bulk_history_load(location="test_scenario_1"):

    # Input data
    initial_data = [(1, "John", "2021-01-01", "IND", datetime(2021, 1, 1, 0, 0, 0)),
                    (1, "John", "2024-01-01", "IND", datetime(2024, 1, 1, 0, 0, 0)),
                    (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0)),
                    (2, "June", "2021-01-03", "US", datetime(2021, 1, 3, 0, 0, 0)),
                    (2, "Jane", "2021-01-03", "US", datetime(2021, 1, 3, 0, 0, 0)),
                    (2, "Jane", "2021-01-03", "USA", datetime(2024, 1, 3, 0, 0, 0)),
                    (3, "Doe", "2021-01-03", "NL", datetime(2021, 1, 3, 0, 0, 0))]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True)
    ])
    initial_data_df = spark.createDataFrame(initial_data, schema)
    upper_df = covert_columns_in_upper_case(initial_data_df)
    source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
    non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE'])
    source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')

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
        StructField("EDL_LAST_UPDATE_DTS_UTC", TimestampType(), True),
        StructField("start_date_id", StringType(), True)
    ])
    current_datetime = datetime.now()
    expected_data = [
        (1, "John", "2024-01-01", "IND", 
            datetime(2024, 1, 1, 0, 0, 0),
            current_datetime, 
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 0),
            current_datetime,
            "2024-01-01"),
        (2, "Jane", "2021-01-03", "USA", 
            datetime(2024, 1, 3, 0, 0, 0),
            current_datetime,
            datetime(2024, 1, 3, 0, 0, 0),
            datetime(2024, 1, 3, 0, 0, 0),
            current_datetime,
            "2021-01-03"),
        (3, "Doe", "2021-01-03", "NL", 
            datetime(2021, 1, 3, 0, 0, 0),
            current_datetime, 
            datetime(2021, 1, 3, 0, 0, 0),
            datetime(2021, 1, 3, 0, 0, 0),
            current_datetime,
            "2021-01-03")
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/scd1_bulk_table/{location}'
    print("destination_path: ", destination_path)

    try:
        # Convert PosixPath to string if necessary
        if isinstance(destination_path, Path):
            destination_path = str(destination_path)

        # Call the function
        generic_scd1_bulk_history_load(source_key_nonkey_df, 
                                  destination_path, 
                                  'start_date_id', 
                                  'START_DATE',
                                  #'2000-01-01', 
                                  {'SOME_BUSINESS_DTS':'desc', 'START_DATE':'asc'},
                                  primary_key_hash='EDL_KEY_HASH',
                                  timezone='',
                                  edl_act_datetime_value='SOME_BUSINESS_DTS')

        # Read the result (from dbfs)
        result_df = spark.read.format("delta").load(destination_path)
    except Exception as e:
        print("Error: ", e)
    
    # Assert the result
    columns_to_compare = [col for col in result_df.columns if col not in ["EDL_KEY_HASH", "EDL_NONKEY_HASH"]]
    result_df_sorted=result_df.select(columns_to_compare).sort(*result_df.columns).orderBy(['CUSTOMER_ID', 'EDL_LAST_UPDATE_DTS_UTC'])
    expected_df_sorted=expected_df.select(columns_to_compare).sort(*expected_df.columns).orderBy(['CUSTOMER_ID', 'EDL_LAST_UPDATE_DTS_UTC'])
    print("Actual Dataframe")     
    result_df_sorted.display()
    print("Expected Dataframe")     
    expected_df_sorted.display()
    assert result_df_sorted.drop("EDL_LOAD_DTS","EDL_LAST_UPDATE_DTS_UTC").collect() == expected_df_sorted.drop("EDL_LOAD_DTS","EDL_LAST_UPDATE_DTS_UTC").collect()

    #remove created files
    if location == 'test_scenario_1':
        print(f"Remove data from {location}")
        dbutils.fs.rm(destination_path, recurse=True)

def test_exception_generic_scd1_bulk_history_load():
    # Input data
    initial_data = [(1, "John", "2021-01-01", "IND"),
                    (1, "John", "2024-01-01", "IND"),
                    (2, "Jane", "2021-01-02", "US"),
                    (2, "June", "2021-01-03", "US"),
                    (2, "Jane", "2021-01-03", "US"),
                    (2, "Jane", "2021-01-03", "USA"),
                    (3, "Doe", "2021-01-03", "NL")]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True)
    ])
    initial_data_df = spark.createDataFrame(initial_data, schema)
	
    with pytest.raises(Exception) as excinfo:
        generic_scd1_bulk_history_load(initial_data_df, 
                                  'somepath', 
                                  'start_date_id', 
                                  'start_date',
                                  {'start_date':'aesc'},
                                  primary_key_hash='EDL_KEY_HASH',
                                  edl_act_datetime_value='start_date')
    assert str(excinfo.value) == "SCD1 Initial Load failed: Invalid sorting order. It should be desc or asc"	

if __name__ == "__main__":
    cleanup_test_results()
    test_generic_scd1_bulk_history_load()
    test_exception_generic_scd1_bulk_history_load()
    cleanup_test_results()