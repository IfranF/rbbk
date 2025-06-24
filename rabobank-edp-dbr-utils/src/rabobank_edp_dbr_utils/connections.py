"""Connections Module

This module contains a classe ``ConnectADLS`` to connect to ADLS.
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
from pyspark.sql import SparkSession

class ConnectADLS:
    """Class to connect to ADLS using the service principal 
    by setting spark configurations on the cluster
    
    Args:
        service_credential_key (str): Name of the secret of the 
        service principal key in databricks scope
        storage_account (str): Name of the storage account
        application_id (str): Service principal application id
        directory_id (str): Tenant id
        spark_session (SparkSession): Spark session object
    """
    def __init__(self,
                service_credential_key: str,
                storage_account: str,
                application_id: str,
                directory_id: str,
                spark_session: SparkSession) -> None:
        self.service_credential_key = service_credential_key
        self.storage_account = storage_account
        self.application_id = application_id
        self.directory_id = directory_id
        self.spark = spark_session

    def connect_to_ADLS(self):
        """Connect to ADLS using the service principal by setting the 
        spark configurations on the cluster using parameters provided while creating the object.

        According to what is documented on:
        https://dev.azure.com/raboweb/Tribe%20Data%20and%20Analytics/_wiki/wikis/The%20Global%20Data%20Platform%20(GDP)/101617/2-Access-ADLS-Gen2

        Usage example:
        --------------
            >>> from pyspark.sql import SparkSession
            >>> from rabobank_edp_dbr_utils.connections import ConnectADLS
            >>> spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
            >>> connect_adls = ConnectADLS(service_credential_key="my_service_principal_key",
            ...                            storage_account="my_storage_account",
            ...                            application_id="my_application_id",
            ...                            directory_id="my_directory_id",
            ...                            spark_session=spark)
            >>> connect_adls.connect_to_ADLS()
        """
        from databricks.sdk.runtime import dbutils
        storage_account_url = f"{self.storage_account}.dfs.core.windows.net"
        self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account_url}",
                            "OAuth") 
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_url}",
                            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") 
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_url}",
                            self.application_id)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_url}",
                            dbutils.secrets.get(scope="connectedSecrets",
                            key = self.service_credential_key))
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_url}",
                            f"https://login.microsoftonline.com/{self.directory_id}/oauth2/token")

    def mount_storage(self, container: str, storage: str = None):
        """Mount the storage account to the dbfs using the container name and storage account name
        ``Mount points are being deprecated by Databricks``

        Args:
            container (str): Container name
            storage (str, optional): Storage account name. Defaults to None.
        """
        from databricks.sdk.runtime import dbutils       
        configs = {
                "fs.azure.account.auth.type":
                    "OAuth",
                "fs.azure.account.oauth.provider.type":
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id":
                    self.application_id,
                "fs.azure.account.oauth2.client.secret":
                    dbutils.secrets.get(scope="connectedSecrets",
                                        key = self.service_credential_key),
                "fs.azure.account.oauth2.client.endpoint":
                    f"https://login.microsoftonline.com/{self.directory_id}/oauth2/token"
                }

        if storage is None:
            if any(mount.mountPoint == f"/mnt/{container}" for mount in dbutils.fs.mounts()):
                print("mountpoint already in place. \
                Please delete old mountpoint before creating a new one")
            else:
                dbutils.fs.mount(
                source = f"abfss://{container}@{self.storage_account}.dfs.core.windows.net/",
                mount_point = f"/mnt/{container}",
                extra_configs = configs)
        else:
            if any(mount.mountPoint == f"/mnt/{storage}/{container}" for mount in dbutils.fs.mounts()):
                print("mountpoint already in place. \
                    Please delete old mountpoint before creating a new one")
            else:
                dbutils.fs.mount(
                source = f"abfss://{container}@{self.storage_account}.dfs.core.windows.net/",
                mount_point = f"/mnt/{storage}/{container}",
                extra_configs = configs)
