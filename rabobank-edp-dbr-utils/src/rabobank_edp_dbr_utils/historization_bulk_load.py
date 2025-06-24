from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, lit, expr
from pyspark.sql.functions import when, to_timestamp, lag, row_number, desc
from pyspark.sql.window import Window
from delta import DeltaTable
from datetime import datetime
from rabobank_edp_dbr_utils.common.shared_spark_session import get_spark
from rabobank_edp_dbr_utils.logger import logging
from rabobank_edp_dbr_utils.util_functions import ValidationUtils
from rabobank_edp_dbr_utils.utils_historization import prepare_insert_update_delete_columns
from rabobank_edp_dbr_utils.utils_historization import prepare_scd2_history_columns
from rabobank_edp_dbr_utils.utils_historization import validate_input_kwargs
from rabobank_edp_dbr_utils.utils_historization import insert_columns_if_not_exists
from rabobank_edp_dbr_utils.utils_historization import get_order_by_cols

utils_logger = logging.getLogger(__name__)
spark = get_spark("historization_initial")

def generic_scd1_bulk_history_load(
        source_df: DataFrame,
        destination: str,
        partition_name: str,
        partition_column: str,
        sorting_cols: dict,
        processing_date = datetime.now().strftime('%Y-%m-%d'),
        **kwargs) -> None:

    """
    Performs an initial load for Slowly Changing Dimension Type 1 (SCD1) data into a Delta table.

    Parameters:
        source_df (DataFrame): The source DataFrame containing the data to be loaded.
        destination (str): The path to the Delta table where the data will be saved.
        partition_name (str): The name of the column to partition the data by.
        partition_column (str): The column used for partitioning the data.
        sorting_cols (dict): (key: column name, value: sorting order 'desc' or 'asc'). 
                             These columns are sorted by order 'desc'/'asc' to get the most recent row.
        processing_date (str, optional): The date of processing, defaulting to the current date in 'YYYY-MM-DD' format.
    
    Keyword Arguments:
        primary_key_hash (str): The column name for the primary key hash.

    Raises:
        Exception: If there is an error in validating the keyword arguments or during the upsert process.

    Usage example:
    --------------
        >>> generic_scd1_bulk_history_load(source_df, '/path/to/delta/table', 'partition_col', 'partition_col', {'sort_col1':'desc', 'sort_col2':'asc'})
    """

    utils_logger.info("Applying SCD1 Initial Load")

    try:
        # Validate Kwargs
        valid_kwargs = validate_input_kwargs(**kwargs)
        # Get values from kwargs
        primary_key_hash = valid_kwargs["primary_key_hash"]
    except Exception as e:
        utils_logger.error("Error occured in validating kwargs")
        raise Exception(f"Error occured in validating kwargs: {e.__str__()}")

    # Prepare insert and update columns
    insert_col, _, _ = prepare_insert_update_delete_columns(source_df
                                                            , partition_name
                                                            , partition_column
                                                            , 'SCD1'
                                                            , processing_date
                                                            , **valid_kwargs)

    try:
        order_by_cols = get_order_by_cols(sorting_cols)
        window_spec  = Window.partitionBy(primary_key_hash).orderBy(*order_by_cols)
        source_df=source_df.withColumn("row_number",row_number().over(window_spec))
        source_df=source_df.filter(source_df['row_number']==1)
        source_df=source_df.drop('row_number')

        source_df = insert_columns_if_not_exists(source_df, insert_col)
        source_df.write.format("delta").mode("overwrite").partitionBy(partition_name).save(destination)

    except Exception as e:
        utils_logger.error(f"Error occured during upsert: {e.__str__()}")
        raise Exception(f"SCD1 Initial Load failed: {e.__str__()}")

    utils_logger.info("SCD1 Initial Load function is successful")

def generic_scd2_bulk_history_load(
        source_df: DataFrame,
        destination: str,
        partition_name: str,
        partition_column: str,
        scd2_history_column: str,
        sorting_cols: dict,
        **kwargs) -> None:

    """
    Performs a bulk history load for Slowly Changing Dimension Type 2 (SCD2) data into a Delta table.

    Parameters:
        source_df (DataFrame): The source DataFrame containing the data to be loaded.
        destination (str): The path to the Delta table where the data will be saved.
        partition_name (str): The name of the column to partition the data by.
        partition_column (str): The column used for partitioning the data.
        scd2_history_column (str): The column used for SCD2 historization reference.
        sorting_cols (dict): (key: column name, value: sorting order 'desc' or 'asc'). 
                            These columns are sorted by order 'desc'/'asc' to get the most recent row.
    
    Keyword Arguments:
        primary_key_hash (str): The column name for the primary key hash.
        non_primary_key_hash (str): The column name for the non-primary key hash.

    Raises:
        Exception: If there is an error in validating the keyword arguments or during the upsert process.

    Usage example:
    --------------
        >>> generic_scd2_bulk_history_load(source_df, '/path/to/delta/table', 'partition_col', 'partition_col', 'history_col', {'sort_col1':'desc', 'sort_col2':'asc'})
    """

    utils_logger.info("Applying SCD2 Bulk History Load")

    try:
        # Validate Kwargs
        valid_kwargs = validate_input_kwargs(**kwargs)
        # Get values from kwargs
        primary_key_hash = valid_kwargs["primary_key_hash"]
        non_primary_key_hash = valid_kwargs["non_primary_key_hash"]
    except Exception as e:
        utils_logger.error("Error occured in validating kwargs")
        raise Exception(f"Error occured in validating kwargs: {e.__str__()}")

    try:
        #deduplication
        windowspec_deduplication  = Window.partitionBy(primary_key_hash).orderBy([*sorting_cols.keys()])
        source_df=source_df.withColumn("LAG_VAL", lag(non_primary_key_hash,1).over(windowspec_deduplication))
        source_df=source_df.withColumn("MARK_DUPLICATE", when(col(non_primary_key_hash)==col('LAG_VAL'), 1).otherwise(0))
        inimitable_df= source_df.filter(col('MARK_DUPLICATE')==0)
        inimitable_df=inimitable_df.drop('MARK_DUPLICATE','LAG_VAL')

        #historization
        order_by_cols = get_order_by_cols(sorting_cols)
        windowspec_historization = Window.partitionBy(primary_key_hash).orderBy(*order_by_cols)
        inimitable_df=inimitable_df.withColumn("RANK",row_number().over(windowspec_historization))
        open_timestamp = to_timestamp(lit('9999-12-31 00:00:00.000'), 'yyyy-MM-dd HH:mm:ss.SSS')
        
        inimitable_df=inimitable_df.withColumn("edl_active_flag",
                                        when(col('RANK')==1, lit('Y'))\
                                        .otherwise(lit('N')))

        inimitable_df=inimitable_df.withColumn("edl_valid_to_datetime", 
                                        when(col('RANK')==1, 
                                                open_timestamp)\
                                        .otherwise(to_timestamp(lag(col(scd2_history_column),1)\
                                                .over(windowspec_historization)) - expr('INTERVAL 1 SECOND')))
        result_df=inimitable_df.drop('RANK')
        # Prepare insert columns
        insert_col = prepare_scd2_history_columns(result_df, partition_name, partition_column, scd2_history_column , **valid_kwargs)

        result_df = insert_columns_if_not_exists(result_df, insert_col)
        result_df=result_df.drop('edl_active_flag', 'edl_valid_to_datetime')
        result_df.write.format("delta").mode("overwrite").partitionBy(partition_name).save(destination)

    except Exception as e:
        utils_logger.error(f"Error occured during upsert: {e.__str__()}")
        raise Exception(f"SCD2 Bulk History Load failed: {e.__str__()}")
    
    utils_logger.info("SCD2 Bulk History Load is successful")
