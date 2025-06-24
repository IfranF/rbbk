import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.util_sanitycheck import compare_data_count

data1 = [(1, "2023-01-01", "A"),(2, "2023-01-02", "B"),(3, "2023-01-03", "C")]
data2 = [(1, "2023-01-01", "A", "extra1"),(2, "2023-01-02", "B", "extra2"),(4, "2023-01-04", "D", "extra3")]
    
df1 = spark.createDataFrame(data1, ["id", "date", "value"])
df2 = spark.createDataFrame(data2, ["id", "date", "value", "extra_column"])
df3 = df2.select("date","value","id", "extra_column")

# Test case 1: with ignore column and without date range
def test_compare_data_count_wo_daterange():
    try:
       compare_data_count(df1,df2,ignore_columns=["extra_column"])
    except Exception as e:
        error_message = e.__str__()
    assert error_message == f"An error occurred while compare_data_count: Data check Failed. 1 row(s) is/are missing in the 2nd dataframe. 1 additional row(s) is/are present in the 2nd dataframe"

# Test case 2: with ignore column and date range
def test_compare_data_count_daterange():
    try:
       compare_data_count(df1, df2, date_column="date", start_date="2023-01-01", end_date="2023-01-03", ignore_columns=["extra_column"])
    except Exception as e:
        error_message = e.__str__()
    assert error_message == f"An error occurred while compare_data_count: Data check Failed. 1 row(s) is/are missing in the 2nd dataframe. 0 additional row(s) is/are present in the 2nd dataframe" 

# Test case 3: without ignore column  and different schema
def test_compare_data_count_schema_mismatch():
    try:
       compare_data_count(df1, df2)
    except Exception as e:
        error_message = e.__str__()
    assert error_message == f"this method only be performed on inputs with the same number of columns" 

# Test case 4: without ignore column  and schema in diffrent order
def test_compare_data_count_schema_order():
    try:
       compare_data_count(df1, df3, ignore_columns=["extra_column"])
    except Exception as e:
        error_message = e.__str__()
    assert error_message == "An error occurred while compare_data_count: Data check Failed. 1 row(s) is/are missing in the 2nd dataframe. 1 additional row(s) is/are present in the 2nd dataframe" 

if __name__ == "__main__":
    test_compare_data_count_wo_daterange()
    test_compare_data_count_daterange()
    test_compare_data_count_schema_mismatch()
    test_compare_data_count_schema_order()