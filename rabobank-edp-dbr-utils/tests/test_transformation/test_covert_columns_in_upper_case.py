import sys
import os
import pytest
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    #print("base_path...........", base_path)
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

#print("sys.path:", sys.path)

from rabobank_edp_dbr_utils.transformation import covert_columns_in_upper_case


def test_covert_columns_in_upper_case_positive():
    data = [(1, "US", "2021-01-01"), (2, "NL", "2021-01-02"), (3, "CA", "2021-01-03")]
    schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("Country_code", StringType(), True),
    StructField("joining date $", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    #df.show()
    try:
        upper_case_df = covert_columns_in_upper_case(df)
        #upper_case_df.show()
    except Exception as e:
        print(f"Error: ", {e.__str__})
    
    # Compare the actual and expected Columns
    assert upper_case_df.columns == ["CUSTOMER_ID", "COUNTRY_CODE", "JOINING DATE $"]

def test_covert_columns_in_upper_case_empty_dataframe():
    data = []
    schema = StructType([])
    df = spark.createDataFrame(data, schema)
    #df.show()
    try:
        upper_case_df = covert_columns_in_upper_case(df)
        #upper_case_df.show()
    except Exception as e:
        print(f"Error: ", {e.__str__})

    # Compare the actual and expected Columns
    assert upper_case_df.columns == df.columns


if __name__ == "__main__":
    test_covert_columns_in_upper_case_positive()
    test_covert_columns_in_upper_case_empty_dataframe()