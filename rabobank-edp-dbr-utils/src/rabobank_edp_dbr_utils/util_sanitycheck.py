from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, expr
from rabobank_edp_dbr_utils.common.shared_spark_session import get_spark
from rabobank_edp_dbr_utils.logger import logging

utils_logger = logging.getLogger(__name__)
spark = get_spark("sanityCheck")


def compare_schema(df1: DataFrame, df2: DataFrame, ignore_columns: list=[], check_nullable=True):
    """
    Compares the schema of two DataFrames and identifies differences in attributes and data types.

    This function compares the schema of two DataFrames, optionally ignoring specified columns,
    and checks for differences in attributes and data types. It can also check the nullable property of columns.

    Args:
        df1 (DataFrame): The first DataFrame to compare.
        df2 (DataFrame): The second DataFrame to compare.
        ignore_columns (list, optional): List of column names to ignore during comparison. Defaults to an empty list.
        check_nullable (bool, optional): Flag to check the nullable property of columns. Defaults to True.

    Returns:
        None

    Raises:
        AssertionError: If there are missing or additional attributes in df2 compared to df1.

    Example:
        >>> compare_schema(df1, df2, ignore_columns=['id'], check_nullable=True)
        Attributes and data type check matches in both version
    """
    def field_list(fields):
        return (fields.name, fields.dataType, fields.nullable)
    
    df1_schema = [field_list(field) for field in df1.schema.fields if field.name not in ignore_columns]
    df2_schema = [field_list(field) for field in df2.schema.fields if field.name not in ignore_columns]
    
    missing_attributes = [field for field in df1_schema if field not in df2_schema]
    additional_attributes = [field for field in df2_schema if field not in df1_schema]
    
    if not missing_attributes and not additional_attributes:
        utils_logger.info(f"Attributes and data type check matches in both version")
    else:
        if missing_attributes:
            utils_logger.info(f"Missing attributes in the df2 : {missing_attributes}")
        if additional_attributes:
            utils_logger.info(f"Additional attributes in the df2 : {additional_attributes}")
        utils_logger.error(f"Attributes and data type check Failed. Missing: {missing_attributes}, Additional: {additional_attributes}")
        raise AssertionError(f"Attributes and data type check Failed. Missing: {missing_attributes}, Additional: {additional_attributes}")
    

def compare_data_count(df1: DataFrame, df2: DataFrame, date_column: str = None, start_date: str = None, end_date: str = None, ignore_columns: list = []):
    """
    Compares data counts between two DataFrames within a specified date range and ignoring specified columns.

    This function compares the data counts between two DataFrames, optionally filtering by a date range and ignoring specified columns.

    Args:
        df1 (DataFrame): The first DataFrame to compare.
        df2 (DataFrame): The second DataFrame to compare.
        date_column (str, optional): The name of the date column to filter by. Default is None.
        start_date (str, optional): The start date for filtering the DataFrames. Default is None.
        end_date (str, optional): The end date for filtering the DataFrames. Default is None.
        ignore_columns (list, optional): A list of columns to ignore during the comparison. Default is an empty list.

    Returns:
        None

    Raises:
        AssertionError: If there are differences in the data counts between the two DataFrames.
        Exception: If an error occurs during the comparison process.

    Example:
        >>> compare_data_count(df1, df2, 'date', '2023-01-01', '2023-12-31', ['ignore_column1', 'ignore_column2'])
        >>> compare_data_count(df1, df2, ignore_columns=['ignore_column1', 'ignore_column2'])
        >>> compare_data_count(df1, df2)       
    """
    try:
        # Filter data based on the date range if date_column, start_date, and end_date are provided
        if date_column and start_date and end_date:
            df1_filtered = df1.filter((col(date_column) >= start_date) & (col(date_column) <= end_date))
            df2_filtered = df2.filter((col(date_column) >= start_date) & (col(date_column) <= end_date))
        else:
            df1_filtered = df1
            df2_filtered = df2
        
        # Select only the columns that are not in the ignore list
        df1_filtered = df1_filtered.select([col for col in df1_filtered.columns if col not in ignore_columns])
        df2_filtered = df2_filtered.select([col for col in df2_filtered.columns if col not in ignore_columns])

        # Sort the dataframes to ensure the order is the same
        df1_filtered = df1_filtered.select(sorted(df1_filtered.columns))
        df2_filtered = df2_filtered.select(sorted(df2_filtered.columns))
        
        # Find rows that are in old data but not in df2 data
        missing_in_df2 = df1_filtered.subtract(df2_filtered)
        missing_in_df2_count = missing_in_df2.count()
        
        # Find rows that are in df2 data but not in old data
        additional_in_df2 = df2_filtered.subtract(df1_filtered)
        additional_in_df2_count = additional_in_df2.count()
        
        if missing_in_df2_count == 0 and additional_in_df2_count == 0:
            utils_logger.info(f"Data Count matched in both the versions")
        else:
            if missing_in_df2_count > 0:
                utils_logger.info(f"Rows missing in the df2: {missing_in_df2_count}")
            if additional_in_df2_count > 0:
                utils_logger.info(f"Additional rows in the df2 : {additional_in_df2_count}")
            raise AssertionError(f"Data check Failed. {missing_in_df2_count} row(s) is/are missing in the 2nd dataframe. {additional_in_df2_count} additional row(s) is/are present in the 2nd dataframe")
    except Exception as e:
        if "NUM_COLUMNS_MISMATCH" in str(e):
            utils_logger.error(f"Column mismatch error: {e.__str__()}")
            raise AssertionError(f"this method only be performed on inputs with the same number of columns")
        else:
            utils_logger.error(f"An error occurred: {e.__str__()}")
            raise Exception(f"An error occurred while compare_data_count: {e.__str__()}")

def compare_dataframes(df1: DataFrame, df2: DataFrame, key_columns:list, date_column: str = None, start_date: str = None, end_date: str = None, ignore_columns: list = [], select_columns: list =[])-> DataFrame:
    """
    Compare two DataFrames based on key columns and return the differences.

    Args:
        df1 (DataFrame): The first DataFrame to compare.
        df2 (DataFrame): The second DataFrame to compare.
        key_columns (list): List of columns to join on.
        date_column (str, optional): Column name for date filtering. Defaults to None.
        start_date (str, optional): Start date for filtering. Defaults to None.
        end_date (str, optional): End date for filtering. Defaults to None.
        ignore_columns (list, optional): List of columns to ignore during comparison. Defaults to [].
        select_columns (list, optional): List of columns to select during comparison. Defaults to [].

    Returns:
		DataFrame: A DataFrame containing the differences between df1 and df2 with additional column indicating uniformity.

    Raises:
		Exception: If an error occurs during the comparison process.

    Example:
		>>> from pyspark.sql import SparkSession
		>>> from pyspark.sql.functions import col, when
		>>> spark = SparkSession.builder.appName("example").getOrCreate()
		>>> data1 = [(1, '2021-01-01', 10, 'A'), (2, '2021-01-02', 20, 'B')]
		>>> data2 = [(1, '2021-01-01', 15, 'B'), (2, '2021-01-02', 20, 'B')]
		>>> input_schema = StructType([StructField('id', IntegerType(), True), StructField('date', StringType(), True),  StructField('value', IntegerType(), True), StructField('tag', StringType(), True)]) 
		>>> df1 = spark.createDataFrame(data1, input_schema)
		>>> df2 = spark.createDataFrame(data2, input_schema)
		>>> result = compare_dataframes(df1, df2, key_columns=['id'], date_column='date', start_date='2021-01-01', end_date='2021-01-02',ignore_columns=['tag'],select_columns=['date','value'])
		>>> result.show()
		+---+----------+----------+---------------+---------+---------+----------------+
		| id|  date_df1|  date_df2|date_uniformity|value_df1|value_df2|value_uniformity|
		+---+----------+----------+---------------+---------+---------+----------------+
		|  1|2021-01-01|2021-01-01|           true|       10|       15|           false|
		+---+----------+----------+---------------+---------+---------+----------------+
    """
    try:
        # Filter data based on the date range if date_column, start_date, and end_date are provided
        if date_column and start_date and end_date:
            df1 = df1.filter((col(date_column) >= start_date) & (col(date_column) <= end_date))
            df2 = df2.filter((col(date_column) >= start_date) & (col(date_column) <= end_date))
        else:
            df1 = df1
            df2 = df2

        if select_columns == []:
            compare_columns = [c for c in df1.columns if c in df2.columns and c not in ignore_columns + key_columns]
        else:
            compare_columns = [c for c in df1.columns if c in df2.columns and c not in ignore_columns + key_columns and c in select_columns]

        df1 = df1.select(compare_columns + key_columns)
        df2 = df2.select(compare_columns + key_columns)

        missing_in_df2 = df1.subtract(df2)
        missing_in_df1 = df2.subtract(df1)

        missing_in_df1 = missing_in_df1.select(key_columns + [col(c).alias(f"{c}_df2") for c in missing_in_df1.columns if c not in key_columns])
        missing_in_df2 = missing_in_df2.select(key_columns + [col(c).alias(f"{c}_df1") for c in missing_in_df2.columns if c not in key_columns])

        df1 = df1.select(key_columns + [col(c).alias(f"{c}_df1") for c in df1.columns if c not in key_columns])
        df2 = df2.select(key_columns + [col(c).alias(f"{c}_df2") for c in df2.columns if c not in key_columns])

        diff_df2 = missing_in_df2.join(df2, how='left', on=key_columns)
        diff_df1 = missing_in_df1.join(df1, how='left', on=key_columns)

        diff = diff_df1.unionByName(diff_df2, allowMissingColumns=True)
        diff = diff.dropDuplicates(key_columns)

        for c in compare_columns:
            comparison_col = f"{c}_uniformity"
            diff = diff.withColumn(comparison_col, expr(f"coalesce(cast({c}_df1 as string), '-1') = coalesce(cast({c}_df2 as string), '-1')"))
        sorted_columns = key_columns + sorted([col for col in diff.columns if col not in key_columns])
        diff = diff.select(sorted_columns).sort(*key_columns)
        return diff
       
    except Exception as e:
        print(f"An error occurred: {e.__str__()}")
        raise Exception(f"An error occurred while compare_dataframes: {e.__str__()}")