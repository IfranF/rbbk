"""Util Functions Module

This module contains utility functions that can be used across the package.
Classes on this module are:
- ``EnvironmentUtils``
- ``FolderUtils``
- ``DataFrameUtils``
- ``ValidationUtils``
- ``FileHandler``
"""
################################################################################
##### Never put from databricks.sdk.runtime import on the top of the file ######
################################################################################
##### Never put from databricks.sdk.runtime import * (or other modules)   ######
##### On top of the code otherwise packages with RDD functionality        ######
##### Will break when loading this package on the executors nodes         ######
##### When running a rdd function with a package that either includes     ######
##### a module with that imports the databricks runtime or loads a        ######
##### package which includes the runtime environment the executor node    ######
##### will crash. So only build databricks.sdk.runtime code in functions  ######
################################################################################

# pylint: disable=import-error
from os import environ
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .logger import logging
utils_logger = logging.getLogger(__name__)

class EnvironmentUtils:
    """This class contains functions to get environment details
    """
    @staticmethod
    def get_edl_load_dts_string() -> str:
        """Get the current date and time in the format 'YYYYMMDDTHHMMSSZ'

        Returns:
            str: The current date and time in the format 'YYYYMMDDTHHMMSSZ'

        Usage example:
        --------------
            >>> #Assuming the current date and time is April 5th, 2023, 12:34:56 UTC.
        
            >>> from rabobank_edp_dbr_utils.util_functions import EnvironmentUtils
            >>> current_edl_load_dts = EnvironmentUtils.get_edl_load_dts_string()
            >>> print(current_edl_load_dts)
            '20230405T123456Z'
        """
        return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')

    @staticmethod
    def get_databricks_environment() -> str:
        """Get the environment value from the databricks environment
        This is created during deployment pipeline or manually via admin settings
        The values can be dev, test, prd

        Returns:
            str: The environment value

        Usage example:
        --------------
            >>> #The function will return the environment value set in the databricks environment variable "rabo_environment".
            >>> from rabobank_edp_dbr_utils.util_functions import EnvironmentUtils
            >>> environment = EnvironmentUtils.get_databricks_environment()
            >>> print(environment)
            'dev'
        """
        try:
            environment_value = environ["rabo_environment"]
            print(f"The following environment has been identified and loaded: {environment_value}")
        except Exception as e:
            print("No Environment has been found in your Databricks Environment Global INIT Script.\
                 Please set this environment with a deployment pipeline or \
                    manually via admin settings. Reverting to dev")
            print(e)
            environment_value = "dev"
        return environment_value

class FolderUtils:
    """This class contains functions to get folder details

    Raises:
        ValueError: "No folders found {input_path}"
    """
    @staticmethod
    def get_the_latest_folder(input_path: str) -> str:
        """Get the latest folder from the input path

        Args:
            input_path (str): The input path

        Raises:
            ValueError: "No folders found {input_path}"

        Returns:
            str: The latest folder

        Usage example:
        --------------
            >>> #Assuming there are several folders in a directory with timestamps as their names:
            >>> #- '20230401'
            >>> #- '20230402'
            >>> #- '20230403'
            >>> from rabobank_edp_dbr_utils.util_functions import FolderUtils
            >>> directory_path = "/path/to/directory"
            >>> latest_folder = FolderUtils.get_the_latest_folder(directory_path)
            >>> print(latest_folder)
            '/path/to/directory/20230403'
        """
        from databricks.sdk.runtime import dbutils  
        folder_list = dbutils.fs.ls(input_path)
        folder_names = [folder.name for folder in folder_list]
        folder_names.sort(reverse=True)
        if len(folder_names) == 0:
            # Raise a ValueError if the folder list is empty
            raise ValueError(f"No folders found {input_path}")
        return folder_names[0]

    @staticmethod
    def create_abfss_path(storage_account: str,
                            container: str,
                            dataset: str,
                            version: int = -1,
                            ffolder: str = "data") -> str:
        """Create an abfss path string

        Args:
            storage_account (str): Name of the storage account
            container (str): Name of the container
            dataset (str): Name of the dataset
            version (int, optional): Version of the data. Defaults to -1.
            ffolder (str, optional): (data/testdata/metadata). Defaults to "data".

        Returns:
            str: The abfss path string

        Usage example:
        --------------
            >>> #Assuming the function is designed to create an ABFSS
            >>> #(Azure Blob File System Secure) path for accessing
            >>> #Azure Data Lake Storage Gen2 resources.
            
            >>> from rabobank_edp_dbr_utils.util_functions import FolderUtils
            >>> storage_account_name = "mystorageaccount"
            >>> file_system_name = "myfilesystem"
            >>> directory_path = "data/2023/04"
            >>> abfss_path = FolderUtils.create_abfss_path(storage_account_name, file_system_name, directory_path)
            >>> print(abfss_path)
            'abfss://myfilesystem@mystorageaccount.dfs.core.windows.net/data/2023/04'
        """
        from databricks.sdk.runtime import dbutils
        base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{dataset}"

        fversion = version
        if version == -1:
            # get latest version
            try:
                fversion = sorted([int(v.name.replace("/", "")) for v in dbutils.fs.ls(base_path)],
                                    reverse=True)[0]
            except Exception:
                # if no version found, set to 1
                fversion = 1
        path = f"{base_path}/{fversion}/{ffolder}/"

        return path

class DataFrameUtils:
    """This class contains functions to get dataframe details
    """
    @staticmethod
    def get_distinct_values_from_dfcolumn(spark_dataframe: DataFrame, column_name: str) -> list:
        """Get the distinct values from a dataframe column

        Args:
            spark_dataframe (DataFrame): The input dataframe
            column_name (str): The column name

        Returns:
            list: The list of distinct values

        Usage example:
        --------------
            >>> from pyspark.sql import SparkSession
            >>> from rabobank_edp_dbr_utils.util_functions import DataFrameUtils
            >>> spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
            >>> data = [('John', 28, 'New York'),
            ...         ('Anna', 24, 'Paris'),
            ...         ('John', 28, 'New York'),
            ...         ('Mike', 32, 'Boston')]
            >>> columns = ['Name', 'Age', 'City']
            >>> df = spark.createDataFrame(data, schema=columns)
            >>> distinct_names = DataFrameUtils.get_distinct_values_from_dfcolumn(df, 'Name')
            >>> print(distinct_names)
            3 number of distinct values found
            ['John', 'Anna', 'Mike']
        """
        distinct_values = spark_dataframe.select(column_name).distinct()
        distinct_list = [row[column_name] for row in distinct_values.collect()]
        print(f"{len(distinct_list)} number of distinct values found")
        return distinct_list

class ValidationUtils:
    """This class contains functions to validate data

    Raises:
        TypeError: "Parent list is not a list"
        TypeError: "Child list is not a list"
        ValueError: "Parent(main) list is empty"
        ValueError: "these column(s):{cols_child_not_in_parent_formatted} 
        not in parent-list: {parent_cols_formatted}"
    """
    @staticmethod
    def cols_present_check(parent_cols: list, child_cols: list):
        """Check if the child columns are present in the parent columns

        Args:
            parent_cols (list): List of parent columns
            child_cols (list): List of child columns

        Raises:
            TypeError: "Parent list is not a list"
            TypeError: "Child list is not a list"
            ValueError: "Parent(main) list is empty"
            ValueError: "these column(s):{cols_child_not_in_parent_formatted} 
            not in parent-list: {parent_cols_formatted}"

        Usage example:
        --------------            
            >>> from rabobank_edp_dbr_utils.util_functions import ValidationUtils
            >>> parent_columns = ['Name', 'Age', 'City']
            >>> child_columns = ['123']
            >>> ValidationUtils.cols_present_check(parent_columns, child_columns)
            ValueError: these column(s):123 not in parent-list: name,age,city
            >>> #This example raises ValueError because the child column '123' is not in the parent columns list.
            >>> from rabobank_edp_dbr_utils.util_functions import ValidationUtils
            >>> parent_columns = ['Name', 'Age', 'City']
            >>> child_columns = ['Name', 'Age', 'City']
            >>> ValidationUtils.cols_present_check(parent_columns, child_columns)
            >>> #This example does not raise any error because all the child columns are present in the parent columns list.
        """
        # Boundary-case
        if not isinstance(parent_cols, list):
            raise TypeError("Parent list is not a list")
        if not isinstance(child_cols, list):
            raise TypeError("Child list is not a list")
        if len(parent_cols) == 0:
            raise ValueError("Parent(main) list is empty")

        parent_cols = [i.lower() for i in parent_cols]
        cols_child_not_in_parent=[]
        for child_col in child_cols:
            if child_col.lower() not in parent_cols:
                cols_child_not_in_parent.append(child_col)

        cols_child_not_in_parent_formatted = ",".join(cols_child_not_in_parent)
        parent_cols_formatted = ",".join(parent_cols)
        if len(cols_child_not_in_parent) != 0:
            raise ValueError(f"these column(s):{cols_child_not_in_parent_formatted} not in parent-list: {parent_cols_formatted}")


class FileHandler:
    """
    It acts as a wrapper around dbutils.fs to handle file operations, with some extra functionalities.
    Attributes:
    -----------
    fs_utils (module): The file system utilities module from Databricks.

    Methods:
    --------
    __init__():
        Initializes the FileHandler class and sets up the file system utilities.
    _get_file_system_utils():
        Imports and returns the Databricks file system utilities module.
    is_file_present(file_path: str) -> bool: Checks if a file is present in the given path.
        file_path (str): The path of the file to be checked.
        bool: True if the file is present, False if the file is not present.

    Raises:
        Exception: If there is an error in checking file presence other than file not found.
        
    Example:
    --------
        >>> file_handler = FileHandler()
        >>> is_present = file_handler.is_file_present("/mnt/data/sample.txt")
        >>> print(is_present)  # Output: True or False
    """  

    def __init__(self):                  
        self.fs_utils = self._get_file_system_utils()
    
    def _get_file_system_utils(self):
        try:
            from databricks.sdk.runtime import dbutils
        except Exception as e:
            raise ImportError(f"Error in getting the file system utils: {e}")
        
        return dbutils.fs
    
    def is_file_present(self,file_path: str) -> bool:
        """	
        This function checks if a file is present in the given path.
        Args:
            file_path : The path of the file to be checked.
        Returns:
            bool : True if the file is present, False if the file is not present.

        """	
        try:
            self.fs_utils.ls(file_path)
            return True
        except Exception as e:
            if 'java.io.FileNotFoundException' in str(e):
                utils_logger.warning(f"File not found in path: {file_path}")
                return False
            else:
                raise Exception(f"Error in checking file presence: {e}")