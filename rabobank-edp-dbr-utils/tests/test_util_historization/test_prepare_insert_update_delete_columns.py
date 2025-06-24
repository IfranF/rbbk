import sys
import os
import pytest
from pathlib import Path
from datetime import datetime,date
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    #print("base_path...........", base_path)
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.utils_historization import prepare_insert_update_delete_columns


def test_prepare_insert_update_delete_columns_scd1():

    # Input data
    input_data = [(1, "John", "2021-01-01", "IND",datetime(2021, 1, 1, 0, 0, 0))]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True)])
    input_data_df = spark.createDataFrame(input_data, schema)

    try:
        # Call the function
        insert_col,update_col,_ = prepare_insert_update_delete_columns(input_data_df,
                                                                                'start_date_id',
                                                                                'start_date',
                                                                                'scd1',
                                                                                '2024-01-01',
                                                                                edl_act_datetime_value='some_business_dts')

        # Assert the column names
        assert list(insert_col.keys()) ==  ['customer_id', 
                                            'name', 
                                            'start_date', 
                                            'country_code', 
                                            'some_business_dts', 
                                            'start_date_id', 
                                            'EDL_LOAD_DTS', 
                                            'EDL_ACT_DTS', 
                                            'EDL_ACT_DTS_UTC', 
                                            'EDL_LAST_UPDATE_DTS_UTC']
        
        assert list(update_col.keys()) == ['EDL_LAST_UPDATE_DTS_UTC', 
                                           'customer_id', 
                                           'name', 
                                           'start_date', 
                                           'country_code', 
                                           'some_business_dts']
    except Exception as e:
        print(f"Error: {e.__str__}")

def test_prepare_insert_update_delete_columns_scd2():

    # Input data
    input_data = [(1, "John", "2021-01-01", "IND",datetime(2021, 1, 1, 0, 0, 0))]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("some_business_dts", TimestampType(), True)])
    input_data_df = spark.createDataFrame(input_data, schema)

    try:
        # Call the function
        insert_col,update_col,delete_col = prepare_insert_update_delete_columns(input_data_df,
                                                                                'start_date_id',
                                                                                'start_date',
                                                                                'scd1',
                                                                                '2024-01-01',
                                                                                edl_act_datetime_value='some_business_dts')
        # Assert the column names
        assert list(insert_col.keys()) == ['customer_id', 
                                           'name', 
                                           'start_date', 
                                           'country_code', 
                                           'some_business_dts', 
                                           'start_date_id', 
                                           'EDL_LOAD_DTS', 
                                           'EDL_ACT_DTS', 
                                           'EDL_ACT_DTS_UTC', 
                                           'EDL_LAST_UPDATE_DTS_UTC'] 
        assert list(update_col.keys()) == ['EDL_LAST_UPDATE_DTS_UTC', 
                                           'customer_id', 
                                           'name', 
                                           'start_date', 
                                           'country_code', 
                                           'some_business_dts']
        assert list(delete_col.keys()) == ['EDL_LAST_UPDATE_DTS_UTC', 
                                           'EDL_VALID_TO_DTS', 
                                           'EDL_VALID_TO_DTS_UTC', 
                                           'EDL_ACTIVE_FLG', 
                                           'EDL_DELETED_FLG']
    except Exception as e:
        print(f"Error: {e.__str__}")

if __name__ == "__main__":
    test_prepare_insert_update_delete_columns_scd1()
    test_prepare_insert_update_delete_columns_scd2()