import pytest
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

from rabobank_edp_dbr_utils.utils_historization import generate_nonkey_column

def test_generate_nonkey_column_positive():
    data = [(1, "US", "2021-01-01", "IT"), (2, "NL", "2021-01-02", "Data Science"), (3, "CA", "2021-01-03", "Finance")]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("country_code", StringType(), True),
        StructField("joining_date", StringType(), True),
        StructField("department", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    #df.show()
    try:
        result_nonkey= generate_nonkey_column(df, ["customer_id"], ["joining_date"])
        #print(result_nonkey)
    except Exception as e:
        print(f"Error: {e.__str__}")
    
    assert result_nonkey == ["country_code","department"]

def test_generate_nonkey_column_invalid_column():
    data = [(1, "US", "2021-01-01", "IT"), (2, "NL", "2021-01-02", "Data Science"), (3, "CA", "2021-01-03", "Finance")]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("country_code", StringType(), True),
        StructField("joining_date", StringType(), True),
        StructField("department", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    #df.show()
    result_nonkey=[]
    error_message=""
    try:
        result_nonkey= generate_nonkey_column(df, ["customer"], ["joining_date"])
        #print(result_nonkey)
    except Exception as e:
        error_message=e.__str__()
        #print(f"Error: {error_message}")
    
    assert error_message == "NonKey generation failed from columns primary_key: ['customer'] and exclude_column: ['joining_date']"

def test_generate_nonkey_column_invalid_argument_list():
    data = [(1, "US", "2021-01-01", "IT"), (2, "NL", "2021-01-02", "Data Science"), (3, "CA", "2021-01-03", "Finance")]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("country_code", StringType(), True),
        StructField("joining_date", StringType(), True),
        StructField("department", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    #df.show()
    result_nonkey=[]
    error_message=""
    try:
        result_nonkey= generate_nonkey_column(df, ["customer"], [])
        #print(result_nonkey)
    except Exception as e:
        error_message=e.__str__()
        #print(f"Error: {error_message}")
    
    assert error_message == f"NonKey generation failed from columns primary_key: ['customer'] and exclude_column: []"


if __name__ == "__main__":
    test_generate_nonkey_column_positive()
    test_generate_nonkey_column_invalid_column()
    test_generate_nonkey_column_invalid_argument_list()