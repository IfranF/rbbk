import sys
import os
from pathlib import Path
from pyspark.sql.functions import col
from pyspark.testing.utils import assertDataFrameEqual
from databricks.sdk.runtime import spark
from pyspark.sql.types import *
import datetime

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent.parent
    sys.path.append(str(base_path))
    from src.rabobank_edp_dbr_utils.transformation import rename_columns
else:
    base_path='dbfs:/FileStore/ccaas-genesys-cloud/pytest/tests'
    sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))
    from rabobank_edp_dbr_utils.transformation import rename_columns

def test_rename_columns_positive():

    input_df = spark.createDataFrame([("a", "b"), ("c", "d")], ["col1", "col2"])
    columns_dict = {"col1": "new_col1", "col2": "new_col2"}

    output_df = rename_columns(input_df, columns_dict)
    #output_df.display()

    expected_df = spark.createDataFrame([("a", "b"), ("c", "d")], ["new_col1", "new_col2"])
    #expected_df.display()

    assertDataFrameEqual(expected_df, output_df)

def test_rename_columns_not_present():

    input_df = spark.createDataFrame([("a", "b"), ("c", "d")], ["col1", "col2"])
    columns_dict = {"col1": "new_col1", "col2": "new_col2", "col3": "new_col3"}

    output_df = rename_columns(input_df, columns_dict)
    #output_df.display()

    expected_df = spark.createDataFrame([("a", "b"), ("c", "d")], ["new_col1", "new_col2"])
    #expected_df.display()
    
    assertDataFrameEqual(expected_df, output_df)

if (__name__=="__main__"):
    test_rename_columns_positive()
    test_rename_columns_not_present()