from pyspark.sql import DataFrame
from delta import DeltaTable
from datetime import datetime
from rabobank_edp_dbr_utils.common.shared_spark_session import get_spark
from rabobank_edp_dbr_utils.logger import logging
from rabobank_edp_dbr_utils.util_functions import ValidationUtils
from rabobank_edp_dbr_utils.utils_historization import prepare_insert_update_delete_columns
from rabobank_edp_dbr_utils.utils_historization import validate_input_kwargs
from rabobank_edp_dbr_utils.utils_historization import insert_columns_if_not_exists

utils_logger = logging.getLogger(__name__)
spark = get_spark("historization")

def create_merge_key(
        source_df: DataFrame,
        target: DeltaTable,
        primary_key: str,
        non_primary_key: str,
        active_record_column: str) -> DataFrame:
    
    """Identifies the records in the source DataFrame that have different values in a specified non-primary key column compared to the target DataFrame, 
    and generates a new DataFrame with a merge key. (this function is used to identify the updates while executing SCD2)

    Args:
        source_df (DataFrame): Source Dataframe
        target_df (DeltaTable): Target DeltaTable/file
        primary_key (str): Column name used as the primary key for joining the DataFrames.(preferably primary_key_hash column)
        non_primary_key (str): Column name used to identify differing records between the DataFrames.(preferably non_primary_key_hash column)
        active_record_column (str): Column name in the target DataFrame that indicates active records (with a value of 'Y')

    Returns:
        DataFrame: A DataFrame with an additional _merge_key column

    Raises:
        ValueError(f"mergeKey generation failed: {e.__str__()}")    

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.historization import create_merge_key
        >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
        >>> from delta import DeltaTable
        >>> data_source = [(1, "John", "2021-01-01", "FR"), (2, "Jane", "2021-01-02", "US"), (4, "Joe", "2024-01-02", "JPN")]
        >>> schema_source = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("START_DATE", StringType(), True),
            StructField("COUNTRY_CODE", StringType(), True)
            ])
        >>> source_df = spark.createDataFrame(data_source, schema_source)       
        >>> target = DeltaTable.forPath(spark, destination_path)
        >>> target.show()
        +-----------+----+----------+------------+--------------+
        |CUSTOMER_ID|NAME|START_DATE|COUNTRY_CODE|EDL_ACTIVE_FLG|
        +-----------+----+----------+------------+--------------+
        |          1|John|2021-01-01|         IND|             Y|
        |          2|Jane|2021-01-02|          US|             Y|
        |          3| Doe|2021-01-03|          NL|             Y|
        +-----------+----+----------+------------+--------------+
        >>> result_df = create_merge_key(source_df, target, "CUSTOMER_ID", "COUNTRY_CODE", "active_flag")
        >>> result_df.show()
        +----------+-----------+----+----------+------------+
        |_merge_key|CUSTOMER_ID|NAME|START_DATE|COUNTRY_CODE|
        +----------+-----------+----+----------+------------+
        |      NULL|          1|John|2021-01-01|          FR|
        |         1|          1|John|2021-01-01|          FR|
        |         2|          2|Jane|2021-01-02|          US|
        |         4|          4| Joe|2024-01-02|         JPN|
        +----------+-----------+----+----------+------------+
    
    """
    utils_logger.info(f"Generating merge_key using {primary_key} and {non_primary_key}")

    try:
        # Validate input
        if not isinstance(target, DeltaTable):
            raise ValueError(f"target should be a DeltaTable")
        # Identify updated records
        new_df = source_df.alias("src").join(\
            target.toDF().filter(f"{active_record_column} == 'Y'").alias("tgt"), primary_key)\
                .where(f"src.{non_primary_key} <> tgt.{non_primary_key}")\
                    .select('src.*')
        # Create merge key
        union_df = new_df.selectExpr("NULL as _merge_key", *source_df.columns)\
            .union(source_df.selectExpr(f"{primary_key} as _merge_key", *source_df.columns))
    except Exception as e:
        utils_logger.error(f"mergeKey generation failed: {e.__str__()}")
        raise Exception("mergeKey generation failed, check log for more info")

    utils_logger.info(f"mergeKey generation is successful")
    return union_df

def __check_merge_schema_and_validate(merge_schema: bool
                                        , source_df: DataFrame
                                        , target_df: DataFrame) -> None:
    """
    Check if schema should be merged 
        and validate the schema of the source and target DataFrames in case merge_schema is False

    Args:
        merge_schema (bool): If True, (changed)schema will be merged.
                             If False, schema will not be merged.
        source_df (DataFrame): Source DataFrame
        target_df (DataFrame): Target DataFrame
    """

    # spark configuration
    automerge_schema = "spark.databricks.delta.schema.autoMerge.enabled"

    # Merge schema and schema validation
    if merge_schema is True:
        spark.conf.set(automerge_schema,'true')
    else:
        spark.conf.set(automerge_schema,'false')
        ValidationUtils.cols_present_check(target_df.columns, source_df.columns)

def __apply_scd1_merge(source_df: DataFrame
                        , target: DeltaTable
                        , mode: str
                        , primary_key_hash: str
                        , non_primary_key_hash: str
                        , insert_col: dict
                        , update_col: dict
                        , delete_col: dict
                        ) -> None:
    """
    Apply SCD1 merge operation on the target DeltaTable using the source DataFrame

    Args:
        source_df (DataFrame): Source Dataframe
        target (DeltaTable): Target DeltaTable
        mode (str): Mode 'incremental' or 'snapshot'
        primary_key_hash (str): Name of the primary key hash column
        non_primary_key_hash (str): Name of the non primary key hash column
        insert_col (dict): Dictionary of columns to be inserted when not matched 
            (key: column name, value: column value)
        update_col (dict): Dictionary of columns to be updated when matched by source 
            (key: column name, value: column value)
        delete_col (dict): Dictionary of columns to be updated when not merged by source 
            (key: column name, value: column value)

    Raises:
        Exception: If mode is not 'incremental' or 'snapshot'
    """

    if mode == "incremental":
        # Delta Upsert with SCD1 incremental logic
        target.alias("tgt")\
            .merge(source_df.alias("src"), f"src.{primary_key_hash} == tgt.{primary_key_hash}")\
                .whenMatchedUpdate(f"src.{non_primary_key_hash} != tgt.{non_primary_key_hash}",
                    set = update_col)\
                .whenNotMatchedInsert(values = insert_col)\
                .execute()
    elif mode == "snapshot":
        # Delta Upsert with SCD1 snapshot logic
        target.alias("tgt")\
            .merge(source_df.alias("src"), f"src.{primary_key_hash} == tgt.{primary_key_hash}")\
                .whenMatchedUpdate(f"src.{non_primary_key_hash} != tgt.{non_primary_key_hash}",
                    set = update_col)\
                .whenNotMatchedInsert(values = insert_col)\
                .whenNotMatchedBySourceUpdate(set = delete_col)\
                .execute()
    else:
        utils_logger.error("Invalid mode. It should be incremental or snapshot")
        raise Exception("Invalid mode. Mode should be incremental or snapshot")

def __apply_scd2_merge(source_df: DataFrame
                        , target: DeltaTable
                        , mode: str
                        , primary_key_hash: str
                        , non_primary_key_hash: str
                        , edl_active_flag: str
                        , edl_valid_to_datetime: str
                        , insert_col: dict
                        , update_col: dict
                        , delete_col: dict
                        ) -> None:
    """
    Apply SCD2 merge operation on the target DeltaTable using the source DataFrame

    Args:
        source_df (DataFrame): Source Dataframe
        target (DeltaTable): Target DeltaTable
        mode (str): Mode 'incremental' or 'snapshot'
        primary_key_hash (str): Name of the primary key hash column
        non_primary_key_hash (str): Name of the non primary key hash column
        edl_active_flag (str): Name of the column that holds 
                                'Y' if the record is active 
                                and 'N' if the record is inactive
        edl_valid_to_datetime (str): Name of the column to capture end datetime of a record
        insert_col (dict): Dictionary of columns to be inserted when not matched
            (key: column name, value: column value)
        update_col (dict): Dictionary of columns to be updated when matched by source
            (key: column name, value: column value)
        delete_col (dict): Dictionary of columns to be updated when not merged by source
            (key: column name, value: column value)

    Raises:
        Exception: If mode is not 'incremental' or 'snapshot'
    """

    # Open date value
    open_date = '9999-12-31'

    # Create merge key
    union_df = create_merge_key(source_df
                                , target
                                , primary_key_hash
                                , non_primary_key_hash
                                , edl_active_flag)

    if mode == "incremental":
        # Delta Upsert with SCD2 incremental logic
        target.alias("tgt").\
            merge(union_df.alias("src"), f"src._merge_key == tgt.{primary_key_hash}")\
                .whenMatchedUpdate(
                    condition = f"src.{non_primary_key_hash} != tgt.{non_primary_key_hash}\
                        and to_date(tgt.{edl_valid_to_datetime}) == '{open_date}'",
                        set = update_col
                    ).whenNotMatchedInsert(values = insert_col).execute()
    elif mode == "snapshot":
        # Delta Upsert with SCD2 snapshot logic
        target.alias("tgt").\
            merge(union_df.alias("src"), f"src._merge_key == tgt.{primary_key_hash}\
                                            and tgt.{edl_active_flag} ==  'Y'")\
                .whenMatchedUpdate(
                    condition = f"src.{non_primary_key_hash} != tgt.{non_primary_key_hash}\
                        and to_date(tgt.{edl_valid_to_datetime}) == '{open_date}'",
                        set = update_col
                    ).whenNotMatchedBySourceUpdate(
                        condition = f"tgt.{edl_active_flag} == 'Y'",
                        set = delete_col
                ).whenNotMatchedInsert(values = insert_col).execute()
    else:
        utils_logger.error("Invalid mode, it should be incremental or snapshot")
        raise Exception("Invalid mode. Mode has to be be incremental or snapshot")

def generic_scd1(
        source_df: DataFrame,
        destination: str,
        partition_name: str,
        partition_column: str,
        mode: str,
        processing_date = datetime.now().strftime('%Y-%m-%d'),
        **kwargs) -> None:
    
    """Performs an incremental load of a source DataFrame into a Delta table, handling Slowly Changing Dimension Type 1 (SCD1) logic. 
    It updates existing records and inserts new ones based on specified key columns and non key columns.

    Args:
        source_df (DataFrame): Input dataframe with incremental source data
        destination (str): Path of the final delta table
        partition_name (str): Name of the partition of final delta table
        partition_column (str): partition column to be used to patition the final delta table (it is generally same as partition_name)
        mode (str): Mode 'incremental' or 'snapshot'. If mode is 'snapshot' assumes full load as input and apply soft delete
        processing_date (str): Date of processing in the format 'YYYY-MM-DD'(in SCD1 incremental, it is not being used)
        
    Keyword Arguments:
        primary_key_hash (str): column name of primary_key_hash (generated by generateEDLHashColumn.py). Default - EDL_KEY_HASH
        non_primary_key_hash (str): column name of non_primary_key_hash (generated by generateEDLHashColumn.py). Default - EDL_NONKEY_HASH
        edl_load_datetime (str): column name to capture load datetime. Default - EDL_LOAD_DTS
        edl_act_datetime (str): column name to capture act datetime. Default - EDL_ACT_DTS
        edl_act_datetime_value (str): name of a functional datetime column in the data to be used in edl_act_datetime column.
        edl_act_datetime_utc (str): column name to capture act datetime UTC. Default - EDL_ACT_DTS_UTC
        edl_update_datetime (str): column name to capture last update datetime UTC. Default - EDL_LAST_UPDATE_DTS_UTC
        merge_schema (bool): If True, (changed)schema will be merged. If False, schema will not be merged.

    Raises:
        ValueError(f"Wrong argument: {e.__str__()}")
        ValueError(f"Argument list is not complete: {e.__str__()}")
        Exception(f"Error occured during upsert: {e.__str__()}")

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.historization import generic_SCD1
        >>> from rabobank_edp_dbr_utils.utils_historization import covert_columns_in_upper_case
        >>> from rabobank_edp_dbr_utils.utils_historization import generate_nonkey_column
        >>> from rabobank_edp_dbr_utils.utils_historization import create_content_hash
        >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        >>> # Assuming day 1 data was
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+
        |CUSTOMER_ID|NAME|START_DATE|COUNTRY_CODE|  SOME_BUSINESS_DTS|        EDL_KEY_HASH|     EDL_NONKEY_HASH|start_date_id|       EDL_LOAD_DTS|        EDL_ACT_DTS|    EDL_ACT_DTS_UTC|EDL_CHANGE_TYPE|EDL_LAST_UPDATE_DTS|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+
        |          1|John|2021-01-01|         IND|2021-01-01 00:00:00|6b86b273ff34fce19...|ad033df9dfb131d30...|   2021-01-01|2024-09-20 00:00:00|2021-01-01 00:00:00|2021-01-01 00:00:00|         Insert|2024-09-20 00:00:00|
        |          2|Jane|2021-01-02|          US|2021-01-02 00:00:00|d4735e3a265e16eee...|11a93af0dbbbbbc54...|   2021-01-02|2024-09-20 00:00:00|2021-01-02 00:00:00|2021-01-02 00:00:00|         Insert|2024-09-20 00:00:00|
        |          3| Doe|2021-01-03|          NL|2021-01-03 00:00:00|4e07408562bedb8b6...|cc5f7ee23292efe1c...|   2021-01-03|2024-09-20 00:00:00|2021-01-03 00:00:00|2021-01-03 00:00:00|         Insert|2024-09-20 00:00:00|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+        
        >>> # Input data
        >>> incr_data = [(1, "John", "2021-01-01", "FR"), (2, "Jane", "2021-01-02", "US"), (3, "Doe", "2021-01-03", "NL")]
        >>> schema = StructType([
        >>> StructField ("customer_id", IntegerType(), True),
        >>>     StructField("name", StringType(), True),
        >>>     StructField("start_date", StringType(), True),
        >>>     StructField("country_code", StringType(), True)
        >>> ])
        >>> incr_df = spark.createDataFrame(incr_data, schema)
        >>> upper_df = covert_columns_in_upper_case(incr_df) #optional
        >>> non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE']) #optional
        >>> source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
        >>> source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
        >>> source_key_nonkey_df.show()
        >>> # target table path
        >>> destination_path = 'base_path/target_table/delta_table_path'
        >>> try:
        >>>     generic_SCD1(source_key_nonkey_df, 
                destination_path, 
                'start_date_id', 
                'START_DATE', 
                '1900-01-01',
                'incremental',
                primary_key_hash='EDL_KEY_HASH', 
                non_primary_key_hash='EDL_NONKEY_HASH', 
                edl_update_datetime='EDL_LAST_UPDATE_DTS',
                edl_load_datetime='EDL_LOAD_DTS',
                edl_act_datetime_value='SOME_BUSINESS_DTS',
                merge_schema=True)
        >>> except Exception as e:
        >>>     print("Error: ", e)
        >>> result_df = spark.read.format("delta").load(destination_path)
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+
        |CUSTOMER_ID|NAME|START_DATE|COUNTRY_CODE|  SOME_BUSINESS_DTS|        EDL_KEY_HASH|     EDL_NONKEY_HASH|start_date_id|       EDL_LOAD_DTS|        EDL_ACT_DTS|    EDL_ACT_DTS_UTC|EDL_CHANGE_TYPE|EDL_LAST_UPDATE_DTS|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+
        |          1|John|2021-01-01|          FR|2021-01-01 00:00:00|6b86b273ff34fce19...|f2e9f873a6e52c443...|   2021-01-01|2024-09-20 00:00:00|2021-01-01 00:00:00|2021-01-01 00:00:00|         Update|2024-09-20 00:00:00|
        |          2|Jane|2021-01-02|          US|2021-01-02 00:00:00|d4735e3a265e16eee...|11a93af0dbbbbbc54...|   2021-01-02|2024-09-20 00:00:00|2021-01-02 00:00:00|2021-01-02 00:00:00|         Insert|2024-09-20 00:00:00|
        |          3| Doe|2021-01-03|          NL|2021-01-03 00:00:00|4e07408562bedb8b6...|cc5f7ee23292efe1c...|   2021-01-03|2024-09-20 00:00:00|2021-01-03 00:00:00|2021-01-03 00:00:00|         Insert|2024-09-20 00:00:00|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+
    """
    utils_logger.info(f"Applying SCD1 {mode}")

    try:
        # Validate Kwargs
        valid_kwargs = validate_input_kwargs(**kwargs)
        # Get values from kwargs
        primary_key_hash = valid_kwargs["primary_key_hash"]
        non_primary_key_hash = valid_kwargs["non_primary_key_hash"]
        edl_load_datetime = valid_kwargs["edl_load_datetime"]
        edl_act_datetime = valid_kwargs["edl_act_datetime"]
        edl_act_datetime_utc = valid_kwargs["edl_act_datetime_utc"]
        edl_update_datetime = valid_kwargs["edl_update_datetime"]
        merge_schema = valid_kwargs["merge_schema"] #boolean
    except Exception as e:
        utils_logger.error("Error occured in validating kwargs")
        raise Exception(f"Error occured in validating kwargs: {e.__str__()}")

    # Prepare insert and update columns
    insert_col, update_col, delete_col = prepare_insert_update_delete_columns(source_df
                                                                            , partition_name
                                                                            , partition_column
                                                                            , 'SCD1'
                                                                            , processing_date
                                                                            , **valid_kwargs)

    try:
        # Check for first day run
        if DeltaTable.isDeltaTable(spark, destination):
            target = DeltaTable.forPath(spark, destination)

            # Validate if required columns are present in the target table
            ValidationUtils.cols_present_check(target.toDF().columns, [primary_key_hash
                                                                       , non_primary_key_hash
                                                                       , edl_load_datetime
                                                                       , edl_act_datetime
                                                                       , edl_act_datetime_utc
                                                                       , edl_update_datetime])

            __check_merge_schema_and_validate(merge_schema, source_df, target.toDF())

            __apply_scd1_merge(source_df
                                , target
                                , mode
                                , primary_key_hash
                                , non_primary_key_hash
                                , insert_col
                                , update_col
                                , delete_col)
        else:
            # First day run
            source_df = insert_columns_if_not_exists(source_df, insert_col)
            source_df.write.format("delta").option("overwrite", "true")\
                .partitionBy(partition_name).save(destination)

    except Exception as e:
        utils_logger.error(f"Error occured during upsert: {e.__str__()}")
        raise Exception("Error occured during upsert: SCD1 failed")

    utils_logger.info(f"SCD1 {mode} function is successful")

def generic_scd2(
        source_df: DataFrame,
        destination: str,
        partition_name: str,
        partition_column: str,
        processing_date: str,
        mode: str,
        **kwargs) -> None:
    
    """Performs an incremental load of a source DataFrame into a Delta table, handling Slowly Changing Dimension Type 2 (SCD2) logic. 
    It updates existing records by keeping history and inserts new ones based on specified key columns and non key columns.

    Args:
        source_df (DataFrame): Input dataframe with incremental source data
        destination (str): Path of the final delta table
        partition_name (str): Name of the partition of final delta table
        partition_column (str): partition column to be used to patition the final delta table (it is generally same as partition_name)
        processing_date (str): Date of processing in format 'yyyy-MM-dd' (this is used to capture the record valid datetime)
        mode (str): Mode 'incremental' or 'snapshot'. If mode is 'snapshot' assumes full load as input and apply soft delete

    Keyword Arguments:
        primary_key_hash (str): column name of primary_key_hash (generated by generateEDLHashColumn.py). Example- EDL_KEY_HASH
        non_primary_key_hash (str): column name of non_primary_key_hash (generated by generateEDLHashColumn.py). Example- EDL_NONKEY_HASH
        edl_load_datetime (str): column name to capture load datetime. Example- EDL_LOAD_DTS
        edl_act_datetime (str): column name to capture act datetime. Default - EDL_ACT_DTS
        edl_act_datetime_value (str): name of a functional datetime column in the data to be used in edl_act_datetime column.
        edl_act_datetime_utc (str): column name to capture act datetime UTC. Default - EDL_ACT_DTS_UTC
        edl_update_datetime (str): column name to capture last update datetime UTC. Default - EDL_LAST_UPDATE_DTS_UTC        
        edl_valid_from_datetime (str): column name to capture start datetime of a record. Example- EDL_VALID_FROM_DTS
        edl_valid_to_datetime (str): column name to capture end datetime of a record. Example- EDL_VALID_TO_DTS
        edl_valid_from_datetime_utc (str): column name to capture start datetime UTC of a record. Example- EDL_VALID_FROM_DTS_UTC
        edl_valid_to_datetime_utc (str): column name to capture end datetime UTC of a record. Example- EDL_VALID_TO_DTS_UTC        
        edl_active_flag (str): column name that holds 'Y' if the record is active and 'N' if the record is inactive. Example- EDL_ACTIVE_FLG
        merge_schema (bool): If True, (changed)schema will be merged. If False, schema will not be merged.

    Raises:
        ValueError(f"Wrong argument: {e.__str__()}")
        ValueError(f"Argument list is not complete: {e.__str__()}")
        Exception(f"Error occured during upsert: {e.__str__()}")

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.historization import generic_SCD2
        >>> from rabobank_edp_dbr_utils.utils_historization import covert_columns_in_upper_case
        >>> from rabobank_edp_dbr_utils.utils_historization import generate_nonkey_column
        >>> from rabobank_edp_dbr_utils.utils_historization import create_content_hash
        >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        >>> # Assuming day 1 data was 
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+-------------------+-------------------+----------------------+----------------------+-------------------+------------+
        |CUSTOMER_ID|NAME|START_DATE|COUNTRY_CODE|  SOME_BUSINESS_DTS|        EDL_KEY_HASH|     EDL_NONKEY_HASH|start_date_id|       EDL_LOAD_DTS|        EDL_ACT_DTS|    EDL_ACT_DTS_UTC|EDL_LAST_UPDATE_DTS| EDL_VALID_FROM_DTS|EDL_VALID_FROM_DTS_UTC|   EDL_VALID_TO_DTS|EDL_VALID_TO_DTS_UTC|EDL_ACTIVE_FLG|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+-------------------+-------------------+----------------------+----------------------+-------------------+------------+
        |          1|John|2021-01-01|         IND|2021-01-01 00:00:00|6b86b273ff34fce19...|ad033df9dfb131d30...|   2021-01-01|2024-09-20 00:00:00|2021-01-01 00:00:00|2021-01-01 00:00:00|2024-09-20 00:00:00|2000-01-01 00:00:00|   2000-01-01 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        |          2|Jane|2021-01-02|          US|2021-01-02 00:00:00|d4735e3a265e16eee...|11a93af0dbbbbbc54...|   2021-01-02|2024-09-20 00:00:00|2021-01-02 00:00:00|2021-01-02 00:00:00|2024-09-20 00:00:00|2000-01-01 00:00:00|   2000-01-01 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        |          3| Doe|2021-01-03|          NL|2021-01-03 00:00:00|4e07408562bedb8b6...|cc5f7ee23292efe1c...|   2021-01-03|2024-09-20 00:00:00|2021-01-03 00:00:00|2021-01-03 00:00:00|2024-09-20 00:00:00|2000-01-01 00:00:00|   2000-01-01 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+-------------------+-------------------+----------------------+----------------------+-------------------+------------+
        >>> # Input data
        >>> incr_data = [(1, "John", "2021-01-01", "FR", datetime(2021, 1, 1, 0, 0, 0)),
                  (2, "Jane", "2021-01-02", "US", datetime(2021, 1, 2, 0, 0, 0)),
                  (4, "Joe", "2022-10-11", "JPN", datetime(2022, 10, 11, 0, 0, 0))]
        >>> schema = StructType([
                StructField("customer_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("start_date", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("some_business_dts", TimestampType(), True),
            ])
        >>> incr_df = spark.createDataFrame(incr_data, schema)
        >>> upper_df = covert_columns_in_upper_case(incr_df) #optional
        >>> non_keys = generate_nonkey_column(upper_df, ['CUSTOMER_ID'], ['START_DATE']) #optional
        >>> source_key_df = create_content_hash(upper_df, ['CUSTOMER_ID'], 'EDL_KEY_HASH')
        >>> source_key_nonkey_df = create_content_hash(source_key_df, non_keys, 'EDL_NONKEY_HASH')
        >>> source_key_nonkey_df.show()
        >>> # target table path
        >>> destination_path = 'base_path/target_table/delta_table_path'
        >>> try:
        >>>     # Call the function
        >>>     generic_SCD2(source_key_nonkey_df, 
                         destination_path, 
                         'start_date_id', 
                         'START_DATE',
                         '2000-01-02',
                         'incremental',
                         primary_key_hash='EDL_KEY_HASH', 
                         non_primary_key_hash='EDL_NONKEY_HASH',
                         edl_update_datetime='EDL_LAST_UPDATE_DTS',
                         edl_valid_from_date='EDL_VALID_FROM_DTS',
                         edl_valid_to_date="EDL_VALID_TO_DTS",
                         edl_valid_from_date_utc='EDL_VALID_FROM_DTS_UTC',
                         edl_valid_to_date_utc='EDL_VALID_TO_DTS_UTC',
                         edl_active_flag='EDL_ACTIVE_FLG',
                         edl_load_date='EDL_LOAD_DTS',
                         edl_act_datetime_value='SOME_BUSINESS_DTS',
                         merge_schema=True)
        >>> except Exception as e:
        >>>     print("Error: ", e)
	    >>> result_df = spark.read.format("delta").load(destination_path)
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+-------------------+----------------------+-------------------+--------------------+
        |CUSTOMER_ID|NAME|START_DATE|COUNTRY_CODE|  SOME_BUSINESS_DTS|        EDL_KEY_HASH|     EDL_NONKEY_HASH|start_date_id|       EDL_LOAD_DTS|        EDL_ACT_DTS|    EDL_ACT_DTS_UTC|EDL_LAST_UPDATE_DTS| EDL_VALID_FROM_DTS|EDL_VALID_FROM_DTS_UTC|   EDL_VALID_TO_DTS|EDL_VALID_TO_DTS_UTC|EDL_ACTIVE_FLG|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+-------------------+----------------------+-------------------+--------------------+
        |          1|John|2021-01-01|         IND|2021-01-01 00:00:00|6b86b273ff34fce19...|ad033df9dfb131d30...|   2021-01-01|2024-09-20 00:00:00|2021-01-01 00:00:00|2021-01-01 00:00:00|2024-09-20 00:00:00|2000-01-01 00:00:00|   2000-01-01 00:00:00|2000-01-01 23:59:59| 2000-01-01 23:59:59|             N|
        |          1|John|2021-01-01|          FR|2021-01-01 00:00:00|6b86b273ff34fce19...|f2e9f873a6e52c443...|   2021-01-01|2024-09-20 00:00:00|2021-01-01 00:00:00|2021-01-01 00:00:00|2024-09-20 00:00:00|2000-01-02 00:00:00|   2000-01-02 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        |          2|Jane|2021-01-02|          US|2021-01-02 00:00:00|d4735e3a265e16eee...|11a93af0dbbbbbc54...|   2021-01-02|2024-09-20 00:00:00|2021-01-02 00:00:00|2021-01-02 00:00:00|2024-09-20 00:00:00|2000-01-01 00:00:00|   2000-01-01 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        |          4| Joe|2022-10-11|         JPN|2022-10-11 00:00:00|4b227777d4dd1fc61...|b7a20c7865e4d872e...|   2022-10-11|2024-09-20 00:00:00|2022-10-11 00:00:00|2022-10-11 00:00:00|2024-09-20 00:00:00|2000-01-02 00:00:00|   2000-01-02 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        |          3| Doe|2021-01-03|          NL|2021-01-03 00:00:00|4e07408562bedb8b6...|cc5f7ee23292efe1c...|   2021-01-03|2024-09-20 00:00:00|2021-01-03 00:00:00|2021-01-03 00:00:00|2024-09-20 00:00:00|2000-01-01 00:00:00|   2000-01-01 00:00:00|9999-12-31 00:00:00| 9999-12-31 00:00:00|             Y|
        +-----------+----+----------+------------+-------------------+--------------------+--------------------+-------------+-------------------+-------------------+-------------------+---------------+-------------------+-------------------+----------------------+-------------------+--------------------+

    """
    utils_logger.info(f"Applying SCD2 {mode}")

    try:
        # Validate Kwargs
        valid_kwargs=validate_input_kwargs(**kwargs)
        # Get values from kwargs
        primary_key_hash = valid_kwargs["primary_key_hash"]
        non_primary_key_hash = valid_kwargs["non_primary_key_hash"]
        edl_load_datetime = valid_kwargs["edl_load_datetime"]
        edl_act_datetime = valid_kwargs["edl_act_datetime"]
        edl_act_datetime_utc = valid_kwargs["edl_act_datetime_utc"]
        edl_valid_from_datetime = valid_kwargs["edl_valid_from_datetime"]
        edl_valid_from_datetime_utc = valid_kwargs["edl_valid_from_datetime_utc"]
        edl_valid_to_datetime = valid_kwargs["edl_valid_to_datetime"]
        edl_valid_to_datetime_utc = valid_kwargs["edl_valid_to_datetime_utc"]
        edl_active_flag = valid_kwargs["edl_active_flag"]
        edl_deleted_flag = valid_kwargs["edl_deleted_flag"]
        edl_update_datetime = valid_kwargs["edl_update_datetime"]
        merge_schema = valid_kwargs["merge_schema"] #boolean
    except Exception as e:
        utils_logger.error("Error occured in validating kwargs")
        raise ValueError(f"Error occured in validating kwargs: {e.__str__()}")

    # Prepare insert columns
    insert_col, update_col, delete_col = prepare_insert_update_delete_columns(source_df
                                                                            , partition_name
                                                                            , partition_column
                                                                            , 'SCD2'
                                                                            , processing_date
                                                                            , **valid_kwargs)

    try:
        # Check for first day run
        if DeltaTable.isDeltaTable(spark, destination):
            target = DeltaTable.forPath(spark, destination)

            # Validate if required columns are present in the target table
            ValidationUtils.cols_present_check(target.toDF().columns, [primary_key_hash
                                                                       , non_primary_key_hash
                                                                       , edl_load_datetime
                                                                       , edl_act_datetime
                                                                       , edl_act_datetime_utc
                                                                       , edl_update_datetime
                                                                       , edl_valid_from_datetime
                                                                       , edl_valid_to_datetime
                                                                       , edl_valid_from_datetime_utc
                                                                       , edl_valid_to_datetime_utc
                                                                       , edl_active_flag
                                                                       , edl_deleted_flag
                                                                    ])

            __check_merge_schema_and_validate(merge_schema, source_df, target.toDF())

            __apply_scd2_merge(source_df
                                , target
                                , mode
                                , primary_key_hash
                                , non_primary_key_hash
                                , edl_active_flag
                                , edl_valid_to_datetime
                                , insert_col
                                , update_col
                                , delete_col)

        else:
            # First day run
            source_df = insert_columns_if_not_exists(source_df, insert_col)
            source_df.write.format("delta").option("overwrite", "true")\
                .partitionBy(partition_name).save(destination)

    except Exception as e:
        utils_logger.error(f"Error occured during upsert: {e.__str__()}")
        raise Exception(f"Error occured during upsert: SCD2 {mode} failed")    
    utils_logger.info(f"SCD2 {mode} function is successful")
