
import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if (__name__=="__main__"):
    sys.path.append(os.path.dirname(os.getcwd()))
    sys.path.append(os.getcwd())
    print(os.path.dirname(os.getcwd()))
else:
    sys.path.append(str(Path(__file__).parent.parent))


from rabobank_edp_dbr_utils.deduplicate import deduplicate_with_order_specs

print(sys.path)

def test_deduplicate_with_order_specs_no_primary_keys():
    # Create a test DataFrame
    input_data = [("John", 25, "Male"), 
                  ("John", 25, "Female"),
                  ("Jane", 30, "Female"), 
                  ("Jane", None, "Female"),
                  ("Vicky", 33, "Female"),
                  ("einstein", 55, "Male"),
                  ("einstein", 55, None)
                  ]
    input_schema = StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Gender", StringType(), True)
                    ])
    df = spark.createDataFrame(input_data, schema=input_schema)

    # Define the primary keys
    primary_keys = []
    order_specs = [("col1","asc"),("col2","desc")]

    data_object = "test_object_with_order_specs_no_pks"
    try:
        df_actual = deduplicate_with_order_specs(df, primary_keys,order_specs,data_object)
        df_actual_sorted = df_actual.sort(*df_actual.columns).collect()
    except Exception as e:
        error_message = e.__str__()
    
    # Compare the actual and expected DataFrames
    assert error_message == f"Error in Deduplication of data-object test_object_with_order_specs_no_pks: Primary keys not provided or not list of strings"



def test_deduplicate_with_order_specs_sortcolumn_not_present():
    # Create a test DataFrame
    input_data = [("John", 25, "Male"), 
                  ("John", 25, "Female"),
                  ("Jane", 30, "Female"), 
                  ("Jane", None, "Female"),
                  ("Vicky", 33, "Female"),
                  ("einstein", 55, "Male"),
                  ("einstein", 55, None)
                  ]
    input_schema = StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Gender", StringType(), True)
                    ])
    df = spark.createDataFrame(input_data, schema=input_schema)

    # Define the primary keys
    primary_keys = ["Name","Age"]
    order_specs = [("Name","asc"),("Height","desc"),("Weight","asc")]
    data_object = "test_object_with_order_specs_sortcolumn_not_present"
    try:
        df_actual = deduplicate_with_order_specs(df, primary_keys,order_specs,data_object)
        df_actual_sorted = df_actual.sort(*df_actual.columns).collect()
    except Exception as e:
        error_message = e.__str__()
    
    # Compare the actual and expected DataFrames
    assert error_message == f"Error in Deduplication of data-object {data_object}: these column(s):height,weight not in parent-list: name,age,gender"



def test_deduplicate_with_order_specs_positive():
    # Create a test DataFrame
    input_data = [("John", 25, "Male","amsterdam"), 
                  ("John", 25, "Female","utrecht"),
                  ("Jane", 30, "Female","maastricht"),	 
                  ("Jane", None, "Female","eindhoven"),
                  ("Vicky", 33, "Female","hoofddorp"),
                  ("einstein", 55, "Male","rotterdam"),
                  ("einstein", 55, None,"Almere")
                  ]
    input_schema = StructType([
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("Gender", StringType(), True),
                        StructField("City", StringType(), True),
                    ])
    df = spark.createDataFrame(input_data, schema=input_schema)

    # Define the primary keys
    primary_keys = ["Name","Age"]
    order_specs = [("city","asc"),("Gender","desc")]
    data_object = "test_object_with_order_specs_positive"

    # Define the expected output DataFrame
    expected_data = [("John", 25, "Male","amsterdam"),
                     ("Jane", 30, "Female","maastricht"),	 	 
                  ("Jane", None, "Female","eindhoven"),
                  ("Vicky", 33, "Female","hoofddorp"),
                  ("einstein", 55, None,"Almere")
                  ]
    
    df_expected = spark.createDataFrame(expected_data, schema=input_schema)
    df_expected_sorted = df_expected.sort(*df_expected.columns).collect()
    try:
        df_actual = deduplicate_with_order_specs(df, primary_keys,order_specs,data_object)
        df_actual_sorted = df_actual.sort(*df_actual.columns).collect()        
    except Exception as e:
        error_message = e.__str__()
    
    # Compare the actual and expected DataFrames
    assert df_expected_sorted == df_actual_sorted