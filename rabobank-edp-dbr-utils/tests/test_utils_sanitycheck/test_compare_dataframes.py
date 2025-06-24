import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.util_sanitycheck import compare_dataframes

data1 = [(1, '2021-01-01', 10, 'A'), (2, '2021-01-02', 20, 'B')]
data2 = [(1, '2021-01-01', 15, 'B'), (2, '2021-01-02', 20, 'B')]
input_schema = StructType([StructField('id', IntegerType(), True), StructField('date', StringType(), True),  StructField('value', IntegerType(), True), StructField('tag', StringType(), True)]) 
df1 = spark.createDataFrame(data1, input_schema)
df2 = spark.createDataFrame(data2, input_schema)

# Test case 1: with ignore column and with date range
def test_compare_dataframes():
    expected_data = [(1, '2021-01-01', '2021-01-01', True, 10, 15, False)]
    expected_schema = StructType([StructField('id', IntegerType(), True), StructField('date_df1', StringType(), True), StructField('date_df2', StringType(), True), StructField('date_uniformity', BooleanType(), True), StructField('value_df1', IntegerType(), True), StructField('value_df2', IntegerType(), True), StructField('value_uniformity', BooleanType(), True)]) 
    expected_result_df = spark.createDataFrame(expected_data, expected_schema)

    actual_result_df = compare_dataframes(df1, df2, key_columns=['id'], date_column='date', start_date='2021-01-01', end_date='2021-01-02',ignore_columns=['tag'])
    assertDataFrameEqual(actual_result_df, expected_result_df, "DataFrames do not match")

# Test case 2: without ignore column and date range
def test_compare_dataframes_wo_daterange():
    expected_data = [(1, '2021-01-01', '2021-01-01', True, 'A', 'B',False, 10, 15, False)]
    expected_schema = StructType([StructField('id', IntegerType(), True),StructField('date_df1', StringType(), True), StructField('date_df2', StringType(), True), StructField('date_uniformity', BooleanType(), True),StructField('tag_df1', StringType(), True), StructField('tag_df2', StringType(), True), StructField('tag_uniformity', BooleanType(), True),StructField('value_df1', IntegerType(), True), StructField('value_df2', IntegerType(), True), StructField('value_uniformity', BooleanType(), True)]) 
    expected_result_df = spark.createDataFrame(expected_data, expected_schema)

    actual_result_df = compare_dataframes(df1, df2, key_columns=['id'])
    assertDataFrameEqual(actual_result_df, expected_result_df, "DataFrames do not match")

# Test case 3: with select column
def test_compare_dataframes_select():
    expected_data = [(1, 'A', 'B',False, 10, 15, False)]
    expected_schema = StructType([StructField('id', IntegerType(), True), StructField('tag_df1', StringType(), True), StructField('tag_df2', StringType(), True), StructField('tag_uniformity', BooleanType(), True), StructField('value_df1', IntegerType(), True), StructField('value_df2', IntegerType(), True), StructField('value_uniformity', BooleanType(), True)]) 
    expected_result_df = spark.createDataFrame(expected_data, expected_schema)

    actual_result_df = compare_dataframes(df1, df2, key_columns=['id'], select_columns=['value','tag'])
    assertDataFrameEqual(actual_result_df, expected_result_df, "DataFrames do not match")

if __name__ == "__main__":
    test_compare_dataframes()
    test_compare_dataframes_wo_daterange()
    test_compare_dataframes_select()