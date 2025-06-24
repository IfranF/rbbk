import sys
import os
from pathlib import Path
from pyspark.sql.functions import col
from pyspark.testing.utils import assertDataFrameEqual
from databricks.sdk.runtime import spark
from pyspark.sql.types import *
import datetime
from pyspark.sql.types import StructType, StructField, StringType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent.parent
    sys.path.append(str(base_path))
    from src.rabobank_edp_dbr_utils.transformation import select_notNull_column
else:
    base_path='dbfs:/FileStore/ccaas-genesys-cloud/pytest/tests'
    sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))
    from rabobank_edp_dbr_utils.transformation import select_notNull_column

def test_select_notNull_column_positive():
    
    input_df = spark.createDataFrame([("a", "b", "c", "d"), ("e", "f", None, "h")], ["col1", "col2", "col3", "col4"])
    multi_case_columns = {'col1_final': ['col1', 'col2'], 'col2_final': ['col3', 'col4']}

    output_df = select_notNull_column(input_df, multi_case_columns)
    #output_df.display()

    expected_df = spark.createDataFrame([("a", "b", "c", "d", "a", "c"), ("e", "f", None, "h", "e", "h")], ["col1", "col2", "col3", "col4", "col1_final", "col2_final"])
    #expected_df.display()

    assertDataFrameEqual(expected_df, output_df)

def test_select_notNull_column_not_present():
    
    input_df = spark.createDataFrame([("a", "b", "c", "d"), ("e", "f", None, "h")], ["col1", "col2", "col3", "col4"])
    multi_case_columns = {'col1_final': ['col1', 'col2'], 'col2_final': ['col3', 'col5']}

    output_df = select_notNull_column(input_df, multi_case_columns)
    #output_df.display()

    # Define schema
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
        StructField("col4", StringType(), True),
        StructField("col5", StringType(), True),
        StructField("col1_final", StringType(), True),
        StructField("col2_final", StringType(), True)
    ])

    # Define data
    data = [
        ("a", "b", "c", "d", None, "a", "c"),
        ("e", "f", None, "h", None, "e", None)
    ]

    # Create DataFrame
    expected_df = spark.createDataFrame(data, schema)
    #expected_df.display()

    assertDataFrameEqual(expected_df, output_df)

if (__name__=="__main__"):
    test_select_notNull_column_positive()
    test_select_notNull_column_not_present()