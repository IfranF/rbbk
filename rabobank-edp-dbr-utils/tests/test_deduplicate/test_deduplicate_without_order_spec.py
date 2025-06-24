import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if (__name__=="__main__"):
    sys.path.append(os.path.dirname(os.getcwd()))
    sys.path.append(os.getcwd())
else:
    sys.path.append(str(Path(__file__).parent.parent.parent))

from rabobank_edp_dbr_utils.deduplicate import deduplicate_without_order_specs


def test_deduplicate_without_order_specs_no_primary_keys():
    # Create a test DataFrame
    input_data = [("John", 25, "Male"), 
                  ("John", 25, "Female"),
                  ("Jane", 30, "Female"), 
                  ("Jane", None, "Female"),
                  ("Vicky", 33, "Female"),
                  ("einstein", 55, "Male"),
                  ("einstein", 55, None),
                  ]
    input_schema = StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Gender", StringType(), True)
                    ])
    df = spark.createDataFrame(input_data, schema=input_schema)

    # Define the primary keys
    primary_keys = []


    # Call the deduplicate_without_order_specs function    
    data_object = "test_object_without_order_specs_no_pks"
    try:
        df_actual = deduplicate_without_order_specs(df, primary_keys,data_object)
        df_actual_sorted = df_actual.sort(*df_actual.columns).collect()
    except Exception as e:
        error_message = e.__str__()
    
    # Compare the actual and expected DataFrames
    assert error_message == f"Error in Deduplication of data-object test_object_without_order_specs_no_pks: Primary keys not provided or not list of strings"


def test_deduplicate_without_order_specs_primary_keys_not_present_in_inputdf():
    # Create a test DataFrame
    input_data = [("John", 25, "Male"), 
                  ("John", 25, "Female"),
                  ("Jane", 30, "Female"), 
                  ("Jane", None, "Female"),
                  ("Vicky", 33, "Female"),
                  ("einstein", 55, "Male"),
                  ("einstein", 55, None),
                  ]
    input_schema = StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Gender", StringType(), True)
                    ])
    df = spark.createDataFrame(input_data, schema=input_schema)

    # Define the primary keys
    primary_keys = ["Name","Height"]

    # Call the deduplicate_without_order_specs function
    try:
        data_object = "test_deduplicate_without_order_specs_primary_keys_not_present_in_inputdf"
        df_actual = deduplicate_without_order_specs(df, primary_keys, data_object)
        df_actual_sorted = df_actual.sort(*df_actual.columns).collect()
    except Exception as e:
        error_message = e.__str__()
    
    # Compare the actual and expected DataFrames
    print(error_message)
    assert error_message == f"Error in Deduplication of data-object {data_object}: these column(s):Height not in parent-list: name,age,gender"

def test_deduplicate_without_order_specs_positive():
    # Create a test DataFrame
    input_data = [("John", 25, "Male"), 
                  ("John", 25, "Female"),
                  ("Jane", 30, "Female"), 
                  ("Jane", None, "Female"),
                  ("Vicky", 33, "Female"),
                  ("einstein", 55, "Male"),
                  ("einstein", 55, None),
                  ]
    input_schema = StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Gender", StringType(), True)
                    ])
    df = spark.createDataFrame(input_data, schema=input_schema)

    # Define the primary keys
    primary_keys = ["Name","Age"]

    # Call the deduplicate_without_order_specs function
    df_actual = deduplicate_without_order_specs(df, primary_keys, "test_object_without_order_specs_positive")
    df_actual_sorted = df_actual.sort(*df_actual.columns).collect()

    # Create the expected DataFrame
    expected_data = [("John",25,"Female"),
                     ("Jane",None,"Female"), 
                     ("Jane", 30, "Female"), 
                     ("Vicky",33,"Female"),
                     ("einstein", 55, None)
                    ]
    df_expected = spark.createDataFrame(expected_data, schema=input_schema)
    df_expected_sorted = df_expected.sort(*df_expected.columns).collect()

    # Compare the actual and expected DataFrames
    assert df_actual_sorted == df_expected_sorted