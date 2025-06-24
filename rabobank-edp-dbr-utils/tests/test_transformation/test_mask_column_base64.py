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
    from src.rabobank_edp_dbr_utils.transformation import mask_column_base64
else:
    base_path='dbfs:/FileStore/ccaas-genesys-cloud/pytest/tests'
    sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))
    from rabobank_edp_dbr_utils.transformation import mask_column_base64

def test_mask_column_base64_positive():
    
    input_df = spark.createDataFrame([("a", "b"), ("c", "d")], ["col1", "col2"])
    mask_columns = ["col1", "col2"]

    output_df = mask_column_base64(input_df, mask_columns)
    #output_df.display()

    expected_df = spark.createDataFrame([("YQ==", "Yg=="), ("Yw==", "ZA==")], ["col1", "col2"])
    #expected_df.display()

    assertDataFrameEqual(expected_df, output_df)

if (__name__=="__main__"):
    test_mask_column_base64_positive()