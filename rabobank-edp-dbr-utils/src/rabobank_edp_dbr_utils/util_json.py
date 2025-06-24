from pyspark.sql.functions import explode, explode_outer, col
from pyspark.sql.functions import DataFrame
from rabobank_edp_dbr_utils.logger import logging

utils_logger = logging.getLogger(__name__)

def prepare_column_selection(input_df,column_names):
    """
    Prepares a list of column selections for a DataFrame based on the provided column names.
    This function iterates through the keys of the `column_names` dictionary. For each column name:
    - If the column name contains a dot ('.'), it is considered a nested column.
        - If the column's data type starts with "array", it constructs a selection string for an exploded array column.
        - Otherwise, it constructs a selection string for a nested column.
    - If the column name does not contain a dot, it constructs a simple selection string.

    Parameters:
        input_df (DataFrame): The input DataFrame to be transformed.
        column_names (dict): (key: column name, value: alias) A dictionary where keys are column names and values are their corresponding aliases.

    Returns:
        tuple: A tuple containing two lists:
            - complex_col_list (list): List of complex column names
            - select_col_list (list): List of selection strings.
    """
    complex_col_list = []
    select_col_list = []
    for column in column_names.keys():
        if (column.__contains__('.')):
            nested_columns = column.split(".")
            if dict(input_df.dtypes)[column.split(".")[0]].startswith("array") :
                select_column = 'exploded_'+ nested_columns[len(nested_columns)-2] + '.' + nested_columns[len(nested_columns)-1] + " as " + column_names[column]
                complex_col_list.append(column)
            else:
                select_column = nested_columns[len(nested_columns)-2] + '.' + nested_columns[len(nested_columns)-1] +" as " + column_names[column]
        else:
            select_column = column + " as " + column_names[column]

        select_col_list.append(select_column)
    utils_logger.info("Successfully completed prepare_column_selection")
    return complex_col_list, select_col_list


def explode_and_select_columns(input_df, complex_cols:list, select_cols:list, outer:bool=False) -> DataFrame:
    """
    Explodes nested columns in a DataFrame and selects specified columns.

    This function iterates through a list of complex column names, determines the level of nesting,
    and applies the `explode` function to each nested level. It then selects the specified columns
    from the DataFrame.

    Parameters:
        input_df (DataFrame): The input DataFrame to be transformed.
        complex_cols (list): A list of complex column names to be exploded.
        select_cols (list): A list of column selection expressions.
        outer (bool): A boolean flag to indicate whether to use `explode_outer` instead of `explode`.

    Returns:
        DataFrame: The transformed DataFrame with exploded columns and selected expressions.
    """

    # Define explode function based on outer flag
    explode_func = explode_outer if outer else explode

    source_df = input_df
    for col in complex_cols:
        nested_count = col.count('.')
        for i in range(nested_count):
            explode_alias = 'exploded_'+ col.split('.')[i]
            if i== 0:
                explode_level = col.split('.')[i]
            else:
                explode_level = 'exploded_'+col.split('.')[i-1]+'.'+col.split('.')[i]
            if explode_alias not in source_df.schema.simpleString():
                source_df = source_df.select("*",explode_func(f"{explode_level}").alias(f"{explode_alias}"))
                
    result_df = source_df.selectExpr(*select_cols)
    utils_logger.info(f"Successfully completed exploding complex columns and selected {select_cols}")
    return result_df
