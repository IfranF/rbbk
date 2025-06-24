"""Deduplicate Module

This module contains functions to help deduplicate dataframes 
based on primary keys and order specifications.
"""

# pylint: disable=import-error
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from rabobank_edp_dbr_utils.util_functions import ValidationUtils
from rabobank_edp_dbr_utils.utils_order_spec import OrderSpecsValidator, OrderSpecsFormatter
from rabobank_edp_dbr_utils.logger import logging

utils_logger = logging.getLogger(__name__)

def deduplicate_with_order_specs(
        input_df: DataFrame,
        primary_keys: list,
        order_specs:list,
        source_data_obj:str) -> DataFrame:
    """Deduplicate the input dataframe based on the primary keys and order specification

    Args:
        input_df (DataFrame): Input data to be deduyplciated
        primary_keys (list): List of primary keys based on which the data should be deduplicated
        order_specs (list): List of rules on which the data should be ordered before deduplicated. 
        Example [('col1','asc'),('col2','desc')]
        source_data_obj (str): Name of the data object

    Raises:
        ValueError: "Primary keys not provided or not list of strings"
        ValueError: "Error in Deduplication of data-object {source_data_obj}: {e}"

    Returns:
        DataFrame: Dataframe with deduplicated data

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.deduplicate import deduplicate_with_order_specs
        >>> input_df = spark.createDataFrame([(1, 'A', 10), (1, 'A', 40), \
(2, 'B', 20), (3, 'C', 30)], ['id', 'name', 'value'])
        >>> primary_keys = ['id']
        >>> order_specs = [('value', 'desc')]
        >>> source_data_obj = 'example_data'
        >>> deduplicate_with_order_specs(input_df, primary_keys, \
order_specs, source_data_obj).show()
        +---+----+-----+
        | id|name|value|
        +---+----+-----+
        |  1|   A|   40|
        |  2|   B|   20|
        |  3|   C|   30|
        +---+----+-----+
    """
    utils_logger.info(f"De-duplicating data-object: {source_data_obj} \
         based on primary keys and order specifications")

    try:
        if primary_keys and isinstance(primary_keys,list):
            #column check for primary keys
            ValidationUtils.cols_present_check(input_df.columns,primary_keys)

            #validate and format order_specs
            valid_order_specs = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
            order_specs_formatted = OrderSpecsFormatter().format_order_specs_spark_functions_tuples(valid_order_specs)

            #column check for order_specs
            ValidationUtils.cols_present_check(input_df.columns,valid_order_specs.columns)

            deduplicated_df = __deduplicate(input_df,primary_keys,order_specs_formatted)
        else:
            raise ValueError("Primary keys not provided or not list of strings")
    except Exception as e:
        utils_logger.error(f"De-duplication failed for data-object: {source_data_obj}")
        raise ValueError(f"Error in Deduplication of data-object {source_data_obj}: {e.__str__()}")

    utils_logger.info(f"De-duplication successful for data-object: {source_data_obj}")
    return deduplicated_df


def deduplicate_without_order_specs(
        input_df: DataFrame,
        primary_keys: list,
        source_data_obj:str) -> DataFrame:
    """Deduplicate the input dataframe based on the primary keys.
    To make the output deterministic, the function will order the data based on 
    the columns that are not primary keys(non-primary keys).
    Default ordering of non primary keys is ascending

    Args:
        input_df (DataFrame): Input data to be deduyplciated
        primary_keys (list): List of primary keys based on which the data should be deduplicated
        source_data_obj (str): Name of the data object

    Raises:
        ValueError: "Primary keys not provided or not list of strings"
        ValueError: "Error in Deduplication of data-object {source_data_obj}: {e}"

    Returns:
        DataFrame: Dataframe with deduplicated data

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.deduplicate import deduplicate_without_order_specs
        >>> input_df = spark.createDataFrame([(1, 'A', 10), (1, 'A', 40), \
    (2, 'B', 20), (3, 'C', 30)], ['id', 'name', 'value'])
        >>> primary_keys = ['id']
        >>> source_data_obj = 'example_data'
        >>> deduplicate_without_order_specs(input_df, primary_keys, source_data_obj).show()
        +---+----+-----+
        | id|name|value|
        +---+----+-----+
        |  1|   A|   10|
        |  2|   B|   20|
        |  3|   C|   30|
        +---+----+-----+
    """
    utils_logger.info(f"De-duplicating data-object: {source_data_obj} based on primary keys and order specifications")
    try:
        #non-empty pks and list check
        if primary_keys and isinstance(primary_keys,list):
            ValidationUtils.cols_present_check(input_df.columns,primary_keys)
            non_primary_keys = [ col for col in input_df.columns if col not in primary_keys]
            deduplicated_df = __deduplicate(input_df,primary_keys, non_primary_keys)
        else:
            raise ValueError("Primary keys not provided or not list of strings")

    except Exception as e:
        utils_logger.error(f"De-duplication failed for data-object: {source_data_obj}")
        raise ValueError(f"Error in Deduplication of data-object {source_data_obj}: {e.__str__()}")

    utils_logger.info(f"De-duplication successful for data-object: {source_data_obj}")
    return deduplicated_df


def __deduplicate(input_df: DataFrame,
                    primary_keys:list,
                    order_specs:list) -> DataFrame:
    """Deduplicate the input dataframe based on the primary keys and order specification
    Precondition:
    1) Function expects columns in the order_specs to be in the input dataframe
    2) Function expects columns in the primary_keys to be in the input dataframe

    Args:
        input_df (DataFrame): Input data to be deduyplciated
        primary_keys (list): List of primary keys based on which the data should be deduplicated
        order_specs (list): List of rules on which the data should be ordered before deduplicated. 
        ``Example [('col1','asc'),('col2','desc')]``

    Raises:
        Exception: Re throws the exception if any exception occurs during deduplication

    Returns:
        DataFrame: Dataframe with deduplicated data
    """
    try:
        window_spec = Window.partitionBy(*primary_keys).orderBy(*order_specs)
        df_dups = input_df.withColumn("row_number",row_number().over(window_spec))
        df_no_dups = df_dups.filter(df_dups.row_number == 1)
        primary_keys_str = ','.join(primary_keys)
        order_specs_str = ','.join([str(os) for os in order_specs])
        utils_logger.info(f"primary_keys: {primary_keys_str}, order_specs: {order_specs_str}")
        utils_logger.info("De-duplication successful.....")
    except Exception as e:
        utils_logger.error("De-duplication failed!")
        utils_logger.error(f"Error-Details: {e.__str__()}")
        raise e

    return df_no_dups.drop(df_no_dups.row_number)
