import sys
import os
import pytest
from pathlib import Path
from datetime import datetime,date
from databricks.sdk.runtime import spark, dbutils
from delta import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.historization import create_merge_key
from rabobank_edp_dbr_utils.transformation import covert_columns_in_upper_case
from rabobank_edp_dbr_utils.utils_historization import create_content_hash, generate_nonkey_column

def cleanup_test_results(destination='/dbfs/user/hive/warehouse/tests/foldertests/delta_table_path'): # Cleanup the test results
    
    try:
        print("Cleaning up before testing")
        #destination = '/dbfs/user/hive/warehouse/tests/foldertests/delta_table_path'
        print(destination)
        dbutils.fs.rm(destination, True)
    except Exception as e:
        print("Error while deleting the folders", e)

def test_create_merge_key_positive():

    # Input data
    incr_data = [(1, "John", "2021-01-01", "FR"), (2, "Jane", "2021-01-02", "US"), (4, "Joe", "2024-01-02", "JPN")]
    incr_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True)
    ])
    incr_df = spark.createDataFrame(incr_data, incr_schema)
    incr_df.show()
    hist_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True),
        StructField("EDL_ACTIVE_FLG", StringType(), True)
    ])

    hist_data = [
        (1, 'John', '2021-01-01', 'IND', 'Y'),
        (2, 'Jane', '2021-01-02', 'US', 'Y'),
        (3, 'Doe', '2021-01-03', 'NL', 'Y')
    ]

    hist_df = spark.createDataFrame(hist_data, hist_schema)
    hist_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/delta_table_path'
    print("destination_path: ", destination_path)

    hist_df.write.mode("overwrite").format("delta").save(str(destination_path))
    hist_df = DeltaTable.forPath(spark, str(destination_path))

    # Expected data
    expected_schema = StructType([
        StructField("_merge_key", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True)
    ])
    expected_data = [
        (None, 1, "John", "2021-01-01", "FR"),
        (1, 1, "John", "2021-01-01", "FR"),
        (2, 2, "Jane", "2021-01-02", "US"),
        (4, 4, "Joe", "2024-01-02", "JPN")
    ]

    # Expected DataFrame
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    #expected_df.show()
    result_df = create_merge_key(incr_df, hist_df, "CUSTOMER_ID", "COUNTRY_CODE", "EDL_ACTIVE_FLG")
    #result_df.show()

    result_df_sorted=result_df.sort(*result_df.columns)
    expected_df_sorted=expected_df.sort(*expected_df.columns)
    result_df_sorted.show()
    expected_df.show()
    assert result_df_sorted.collect() == expected_df_sorted.collect()

    cleanup_test_results(destination_path)

def test_create_merge_key_negetive():

    # Input data
    incr_data = [(1, "John", "2021-01-01", "FR"), (2, "Jane", "2021-01-02", "US"), (4, "Joe", "2024-01-02", "JPN")]
    incr_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True)
    ])
    incr_df = spark.createDataFrame(incr_data, incr_schema)
    incr_df.show()
    hist_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("COUNTRY_CODE", StringType(), True),
        StructField("EDL_ACTIVE_FLG", StringType(), True)
    ])

    hist_data = [
        (1, 'John', '2021-01-01', 'IND', 'Y'),
        (2, 'Jane', '2021-01-02', 'US', 'Y'),
        (3, 'Doe', '2021-01-03', 'NL', 'Y')
    ]

    hist_df = spark.createDataFrame(hist_data, hist_schema)
    hist_df.show()

    # Get the destination path
    base_path = "/dbfs/user/hive/warehouse" #Path(os.getcwd()).parent.parent
    destination_path = f'{base_path}/tests/foldertests/delta_table_path'
    print("destination_path: ", destination_path)

    hist_df.write.mode("overwrite").format("delta").save(str(destination_path))
    hist_df = spark.read.format("delta").load(str(destination_path))

    with pytest.raises(Exception, match="mergeKey generation failed, check log for more info"):
        create_merge_key(incr_df, hist_df, "CUSTOMER_ID", "COUNTRY_CODE", "EDL_ACTIVE_FLG")

    cleanup_test_results(destination_path)

if __name__ == "__main__":
    cleanup_test_results()
    test_create_merge_key_positive()
    test_create_merge_key_negetive()
    cleanup_test_results()