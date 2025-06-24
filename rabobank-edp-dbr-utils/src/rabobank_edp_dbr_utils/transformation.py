from pyspark.sql import DataFrame
from pyspark.sql.functions import col, base64, coalesce, lit
from rabobank_edp_dbr_utils.logger import logging
from rabobank_edp_dbr_utils.util_functions import ValidationUtils
from rabobank_edp_dbr_utils.utils_historization import insert_columns_if_not_exists

utils_logger = logging.getLogger(__name__)

def covert_columns_in_upper_case(
        input_df: DataFrame) -> DataFrame:
    """
    Convert columns of input dataframe in upper case

    Args:
        input_df (DataFrame): Source DataFrame

    Returns:
        DataFrame: A DataFrame with all columns in Upper Case

    Raises:
        ValueError(f"upper case conversation failed: {e.__str__()}")  

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.transformation import covert_columns_in_upper_case
        >>> data = [(1, "A", "2021-01-01"), (2, "B", "2021-01-02"), (3, "C", "2021-01-03")]
        >>> schema = ["id", "value", "date"]
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +---+-----+----------+
        | id|value|      date|
        +---+-----+----------+
        |  1|    A|2021-01-01|
        |  2|    B|2021-01-02|
        |  3|    C|2021-01-03|
        +---+-----+----------+
        >>> upper_case_df = covert_columns_in_upper_case(df)
        >>> upper_case_df.show()
        +---+-----+----------+
        | ID|VALUE|      DATE|
        +---+-----+----------+
        |  1|    A|2021-01-01|
        |  2|    B|2021-01-02|
        |  3|    C|2021-01-03|
        +---+-----+----------+
    """

    utils_logger.info("Converting column to upper case")
    
    try:
        result_df = input_df.toDF(*[column_name.upper() for column_name in input_df.columns])
    except Exception as e:
        utils_logger.error("upper case conversation failed")
        raise ValueError(f"upper case conversation failed: {e.__str__()}")  
    
    utils_logger.info("upper case conversation is successful")
    return result_df

def covert_columns_in_lower_case(
        input_df: DataFrame) -> DataFrame:
    """
    Convert columns of input dataframe in lower case

    Args:
        input_df (DataFrame): Source DataFrame

    Returns:
        DataFrame: A DataFrame with all columns in Upper Case

    Raises:
        ValueError(f"lower case conversation failed: {e.__str__()}")  

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.transformation import covert_columns_in_lower_case
        >>> data = [(1, "A", "2021-01-01"), (2, "B", "2021-01-02"), (3, "C", "2021-01-03")]
        >>> schema = ["ID", "VALUE", "DATE"]
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +---+-----+----------+
        | ID|VALUE|      DATE|
        +---+-----+----------+
        |  1|    A|2021-01-01|
        |  2|    B|2021-01-02|
        |  3|    C|2021-01-03|
        +---+-----+----------+
        >>> lower_case_df = covert_columns_in_lower_case(df)
        >>> lower_case_df.show()
        +---+-----+----------+
        | id|value|      date|
        +---+-----+----------+
        |  1|    A|2021-01-01|
        |  2|    B|2021-01-02|
        |  3|    C|2021-01-03|
        +---+-----+----------+
    """
    
    utils_logger.info("Converting column to lower case")

    try:
        result_df = input_df.toDF(*[column_name.lower() for column_name in input_df.columns])
    except Exception as e:
        utils_logger.error("lower case conversation failed")
        raise ValueError(f"lower case conversation failed: {e.__str__()}")  
    
    utils_logger.info("lower case conversation is successful")
    return result_df

def rename_columns(input_df, columns_dict, validate_cols=False):
    """
    Renames columns in a DataFrame based on a provided dictionary.

    Parameters:
        df (DataFrame): The input DataFrame whose columns need to be renamed.
        columns_dict (dict): A dictionary where keys are the current column names and values are the new column names.
        validate_cols (bool)[default False]: If True, validates that all keys in columns_dict are present in the DataFrame columns.

    Note: When validate_cols=False, if a column to be renamed is not found in the DataFrame, it will be ignored.

    Returns:
        DataFrame: A new DataFrame with the columns renamed.

    Raises:
        ValueError: If columns_dict is not a dictionary or if a column to be renamed is not found in the DataFrame (when validate_cols is True).	

    Example:
    --------
        >>> df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> columns_dict = {"id": "user_id", "name": "username"}
        >>> df = rename_columns(df, columns_dict)
        >>> df.show()
        +-------+--------+
        |user_id|username|
        +-------+--------+
        |      1|   Alice|
        |      2|     Bob|
        +-------+--------+
    """
    utils_logger.info("Rename columns")

    # Check if columns_dict is a dictionary
    if not isinstance(columns_dict, dict):
        raise ValueError("columns_dict should be a dictionary of columns to be renamed")
    # Check if all columns are present in the input dataframe
    if validate_cols:
        ValidationUtils.cols_present_check(input_df.columns,columns_dict.keys())

    # Rename attributes if present
    for key, value in columns_dict.items():
        try:
            input_df = input_df.withColumnRenamed(key, value)
        except Exception as e:
            if "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(e):
                continue
            else:
                raise ValueError(f"Error while renaming columns : {e.__str__()}")

    utils_logger.info("Successfully renamed columns")
    return input_df

def select_notNull_column(input_df, multi_case_columns):
    """	
    Applies coalesce on specified columns in a DataFrame to select the first non-null value.

    Parameters:
        input_df (DataFrame): The input DataFrame.
        multi_case_columns (dict): A dictionary where keys are the new column names and values are lists of column names to be coalesced.

    Note: Since coalese needs all the columns to be present in the DataFrame, 
    if a column to be coalesced is not found in the DataFrame, it will be added with NULL values.
    
    Returns:
        DataFrame: A new DataFrame with the coalesced columns.

    Raises:
        ValueError: If multi_case_columns is not a dictionary or if there is an error in applying coalesce.

    Example:
    --------
        >>> df = spark.createDataFrame([(None, "Alice", "A"), (2, None, "B")], ["id1", "id2", "name"])
        >>> multi_case_columns = {"id": ["id1", "id2"]}
        >>> df = select_notNull_column(df, multi_case_columns)
        >>> df.show()
        +----+-----+----+-----+
        | id1|  id2|name|   id|
        +----+-----+----+-----+
        |NULL|Alice|   A|Alice|
        |   2| NULL|   B|    2|
        +----+-----+----+-----+
    """
    utils_logger.info("Apply coalesce on multi case columns")

    # Check if multi_case_columns is a dictionary
    if not isinstance(multi_case_columns, dict):
        raise ValueError(f"multi_case_columns should be a dictionary of columns to be coalesced")

    # Check and insert columns if not exists
    default_dict = {value: lit(None).cast('string') for values in multi_case_columns.values() for value in values}
    updated_df = insert_columns_if_not_exists(input_df, default_dict)
    try:
        for key, value in multi_case_columns.items():
            updated_df = updated_df.withColumn(key, coalesce(*[updated_df[column] for column in value]))
    except Exception as e:
        raise ValueError(f"Error in applying coalesce on {multi_case_columns} : {e.__str__()}")
    
    utils_logger.info("Successfully applied coalesce on multi case columns")
    return updated_df

def mask_column_base64(input_df, mask_columns, validate_cols=True):
    """	
    Applies base64 encoding to specified columns in a DataFrame.

    Parameters:
        df (DataFrame): The input DataFrame.
        mask_columns (list): A list of column names to be masked with base64 encoding.
        validate_cols (bool)[default True]: If True, validates that all columns in mask_columns list are present in the DataFrame columns.

    Note: When validate_cols=False, if a column to be masked is not found in the DataFrame, it will be ignored.

    Returns:
        DataFrame: A new DataFrame with the specified columns masked using base64 encoding.

    Raises:
        ValueError: If mask_columns is not a list or if there is an error in applying base64 encoding.

    Example:
    --------
        >>> df = spark.createDataFrame([("Alice", "12345"), ("Bob", "67890")], ["name", "id"])
        >>> mask_columns = ["id"]
        >>> df = mask_column_base64(df, mask_columns)
        >>> df.show()
        +-----+---------+
        | name|       id|
        +-----+---------+
        |Alice|MTIzNDU=|
        |  Bob|Njc4OTA=|
        +-----+---------+
    """
    utils_logger.info("Apply base64 encoding on columns")
    # Check if mask_columns is a list
    if not isinstance(mask_columns, list):
        raise ValueError(f"mask_columns should be a list of columns to be masked with base64 encoding")
    # Check if all columns are present in the input dataframe
    if validate_cols:
        ValidationUtils.cols_present_check(input_df.columns,mask_columns)

    for column in mask_columns:
        try:
            input_df = input_df.withColumn(column, base64(col(column)))
        except Exception as e:
            if "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(e):
                continue
            else:
                utils_logger.error(f"Error while masking columns: {mask_columns} with base64 encoding")
                raise ValueError(f"Error while masking columns: {mask_columns} with base64 encoding")
            
    utils_logger.info(f"Successfully masked columns: {mask_columns} with base64 encoding")
    return input_df