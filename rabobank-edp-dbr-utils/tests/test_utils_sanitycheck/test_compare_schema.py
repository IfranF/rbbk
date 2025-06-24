import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.util_sanitycheck import compare_schema

# Sample DataFrames for testing
schema1 = StructType([StructField("name", StringType(), True),StructField("age", IntegerType(), True),StructField("city", StringType(), True)])
schema2 = StructType([StructField("name", StringType(), True),StructField("age", IntegerType(), True),StructField("country", StringType(), True)])
df1 = spark.createDataFrame([], schema1)
df2 = spark.createDataFrame([], schema2)

# Test case 1: where schemas are different without ignore column
def test_compare_schema_wo_ignore_col():
    try:
        compare_schema(df1,df2)
    except Exception as e:
        error_message = e.__str__()
    assert error_message == f"Attributes and data type check Failed. Missing: [('city', StringType(), True)], Additional: [('country', StringType(), True)]"

# Test case 2: where schemas are different with ignore column
def test_compare_schema():
    try:
        compare_schema(df1,df2,["city"])
    except Exception as e:
        error_message = e.__str__()
    assert error_message == f"Attributes and data type check Failed. Missing: [], Additional: [('country', StringType(), True)]"

# Test case 3: case sensitivity in ignored columns
def test_compare_schema_ignorecase():
    try:
        compare_schema(df1,df2,["CITY","name","NAME"])
    except Exception as e:
        error_message = e.__str__()
    assert error_message == f"Attributes and data type check Failed. Missing: [('city', StringType(), True)], Additional: [('country', StringType(), True)]"
        

if __name__ == "__main__":
    test_compare_schema_wo_ignore_col()
    test_compare_schema()
    test_compare_schema_ignorecase()