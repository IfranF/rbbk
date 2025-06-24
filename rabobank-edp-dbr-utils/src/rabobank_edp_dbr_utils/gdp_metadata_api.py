"""GDP Metadata API Module

This module contains the classes ``MetadataApiCaller`` to call the metadata API 
and the class ``CreateParamValues`` to create the parameter values for the metadata API.
"""

# pylint: disable=import-error
import sys
import logging
import json
import http.client
from http.client import HTTPResponse
from datetime import datetime, timezone
import requests
from rabobank_edp_dbr_utils.util_functions import EnvironmentUtils
from rabobank_edp_dbr_utils.logger import logging 

utils_logger = logging.getLogger(__name__)

class MetadataApiCaller:
    """
    This class is used to call the metadata API and write the response to a json file.

    For a successful call to the metadata API, the following parameters are required:
    1. environment: This is the environment where the metadata API is hosted
    2. metadata_config: This is a dictionary that contains the metadata configuration values
    metadata_config has the following keys:
    a. param_values: required for the GDP-metadata API call
    b. call_credentials: required for getting the token(from Microsoft) to call the metadata API
    c. runtime: required for the metadata API call
    Responsibility of metadata_config is from CreateParamValues class

    The class has the following methods:
    1. call_metadata_api: This method is used to call the metadata API
    2. __get_token: This method is used to get the token required to call the metadata API
    3. post_api: This method is used to post the metadata API
    4. write_response: This method is used to write the metadata response to a json file

    """
    def __init__(self,environment:str):
        self.service_credential_env = environment 
        self.metadata_config = {}

    def call_metadata_api(self, metadata_config: dict, **kwargs) -> HTTPResponse:
        """
        This function is used to call the metadata API and write the response to a json file

        Args:
            metadata_config: This is a dictionary that contains the metadata configuration values
        
        Keyword Arguments:
            write_single_file (bool): If True, writes the metadata API response to a single file. Defaults to False.
            fname_prefix (str): Prefix to be added to the filename. Defaults to "".
            fname_suffix (str): Suffix to be added to the filename. Defaults to "".
            
        Usage example:
        --------------
            >>> from rabobank_edp_dbr_utils.gdp_metadata_api import MetadataApiCaller
            >>> config_json = {
            ...     "dataProducerName": "ExampleProducer",
            ...     "scope": "https://example.com/metadata",
            ...     "clientId": "exampleClientId",
            ...     "relativeUrl": "/metadata/register",
            ...     "metadataAPI": "https://example.com/metadata/api"
            ... }
            >>> metadata_write_path = "/data/output/path"
            >>> metadata_api_caller = MetadataApiCaller(config_json, metadata_write_path)
            >>> metadata_config = {
            ...     "dataProducerName": "ProducerName",
            ...     "dataObjectTechnicalName": "ExampleDataObject",
            ...     "dataObjectLakeLayer": "defined",
            ...     "dataObjectType": "incremental",
            ...     "dataObjectTechnicalIdSourceObject":[],
            ...     "dataObjectProcessingJobPlannedFrequencyType": "Hourly",
            ...     "dataObjectDeliveryRetentionDatetime": "2023-12-31T23:59:59Z",
            ... }
            >>> delivery_location = "https://example/data/output"
            >>> job_id = "12345"
            >>> lines_written = 1000
            >>> extract_type = "Delta"
            >>> load_datetime = "2023-01-01T00:00:00Z"
            >>> metadata_params = CreateParamValues(metadata_config, lines_written, extract_type, load_datetime, delivery_location, job_id)
            >>> metadata_api_caller.call_metadata_api(metadata_params.param_values, load_datetime)
        """
        self.metadata_config = metadata_config

        token = self.__get_token()
        response = self.__post_api(token)
        self.__write_response(response, **kwargs)

        return response

    def __get_token(self) -> str:
        """Get token from Azure KeyVault REST API

        Returns:
            str: Azure KeyVault token
        """
        from databricks.sdk.runtime import dbutils       
        header = {
            "Content_Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
            }

        values = {
            "grant_type": "client_credentials",
            "scope": self.metadata_config["params_credentials"]["scope"],
            # Use client ID of your DB instance (Step 2)
            "client_id": self.metadata_config["params_credentials"]["clientId"],
            # Retreive secret from keyvault
            "client_secret": dbutils.secrets.get(
                scope = "connectedSecrets",
                key = f"edl-{self.metadata_config['params_credentials']['dataProducerName']}-{self.service_credential_env}-databricks"
            ),
        }

        try:
            api_url = "https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token"
            resp = requests.post(
                url = api_url,
                headers = header,
                data = values,
                timeout = 5*60 # 5 minutes timeout
            )

            # Check if the response is successful
            resp.raise_for_status()

        except Exception as e:
            logging.exception(f"ERROR: \
                Failed to retrieve a token from MICROSOFT with error message: {e.__str__()}")
            sys.exit(0)

        token = json.loads(resp.text)["access_token"]        
        return token

    def __post_api(self, token: str) -> HTTPResponse:
        """Makes a POST call to the Metadata API with the given token and parameters

        Args:
            token (str): API Token

        Raises:
            ValueError: "result statuscode is not 200 but : " + str(metadata_api_response.status)

        Returns:
            HTTPResponse: Metadata API response
        """
        try:
            metadata_api_response = self.__make_post_request(token)
            utils_logger.info(f"Metadata API response: {metadata_api_response.status}")     
            utils_logger.info(f"Metadata API response text: {metadata_api_response}") 
        except Exception as e:
            utils_logger.error(f"ERROR: Failed to retrieve a valid response from the request with error message: {e.__str__()}")
            sys.exit(0)

        self.__validate_post_response(metadata_api_response)

        return metadata_api_response

    def __make_post_request(self, token):
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        connection = http.client.HTTPSConnection(self.metadata_config["params_credentials"]["metadataAPI"])
        try:
            connection.request(
                "POST",
                self.metadata_config["params_credentials"]["relativeUrl"],
                json.dumps(self.metadata_config["params_metadata"]),
                headers
            )
            response_request = connection.getresponse()
            response = CustomResponse(response_request)
        except Exception as e:
            utils_logger.error(e.__str__())
            raise ValueError("Failed to make a POST request to the metadata API with error message: " + e.__str__())
        finally:
            connection.close()
        return response

    def __validate_post_response(self,response):
        if response.status != 200:
            raise ValueError("result statuscode is not 200 but : " 
                    + str(response.status) 
                    + " with reason: " + response.reason 
                    + " and response text: " + response.read().decode('utf-8'))

    def __write_response_single_file(self, metadata_output_json: dict, fname: str) -> None:
        """Writes the metadata API response to a single json file in the self.metadata_config["params_runtime"]["metadata_write_path"]

        Args:
            metadata_output_json (dict): Metadata output json as a dictionary
            fname (str): Filename to write the metadata API response
        """        
        # NOT RECOMMENDED FOR LARGE FILES
        from databricks.sdk.runtime import dbutils

        # create temp location to write single json output file
        metadata_output_splited = self.metadata_config["params_runtime"]["metadata_write_path"].split(".dfs.core.windows.net/")
        temp_specific = f'tmp/metadata_api_response_{self.metadata_config["params_metadata"]["dataProducerName"]}'
        if len(metadata_output_splited) > 1:
            temp_specific = f"{temp_specific}_{metadata_output_splited[1]}" 
        temp_dir_loc = f"dbfs:/mnt/{temp_specific}"

        # make sure the temp location exists
        dbutils.fs.mkdirs(temp_dir_loc)

        # write output to temp location
        temp_loc = f"{temp_dir_loc}/{fname}.json"
        print(f"Writing metadata API response to temp location {temp_loc}")
        with open(temp_loc.replace("dbfs:", "/dbfs"), "w") as f:
            json.dump(metadata_output_json, f)

        print("Moving temp location to final location")
        # for the first write we need to create the dir, otherwise it writes a file
        dbutils.fs.mkdirs(self.metadata_config["params_runtime"]["metadata_write_path"])
        dbutils.fs.mv(temp_loc, self.metadata_config["params_runtime"]["metadata_write_path"])

    def __write_response(self, response: HTTPResponse, **kwargs):
        """Writes the metadata API response to the metadata output path in storage account

        Args:
            response (HTTPResponse): Metadata API response

        Keyword Arguments:
            write_single_file (bool): If True, writes the metadata API response to a single file. Defaults to False.
            fname_prefix (str): Prefix to be added to the filename. Defaults to "".
            fname_suffix (str): Suffix to be added to the filename. Defaults to "".
        """
        from databricks.sdk.runtime import spark 

        write_single_file = kwargs.get("write_single_file", False)
        fname_prefix = kwargs.get("fname_prefix", "")
        fname_suffix = kwargs.get("fname_suffix", "")

        try:
            last_partition_date_formatted_for_filename = datetime.strptime(
                                    self.metadata_config["params_runtime"]["edl_load_dts"],
                                    "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%dT%H%M%SZ%z")
            fname = f"{fname_prefix}{last_partition_date_formatted_for_filename}{fname_suffix}"
            writeloc = f'{self.metadata_config["params_runtime"]["metadata_write_path"]}/{fname}'

            # get the response from the metadata API
            metadata_output_json = json.loads(response.read())

            print(f"Writing metadata API response to {writeloc}")
            if write_single_file:
                self.__write_response_single_file(metadata_output_json, fname)
            else:
                df = spark.read.json(spark.sparkContext.parallelize([metadata_output_json]))
                df.write.mode("overwrite").json(writeloc)
        except Exception as e:
            utils_logger.error(e.__str__())
            raise ValueError("Failed to write the metadata response to a json file with error message: " + e.__str__())        

class CreateParamValues:
    """Class to create the parameter values for the metadata API.
    When instantiated, the class will create a dictionary with the values to be passed to the metadata API.
    It will also call the ``get_delivery_location`` and ``get_run_id`` methods.

    Args:
        config_json_bytes (dict): Dict of configuration details for metadata API
        lines_written (int): Number of lines wirrten to the data object
        edl_load_dts (str): String representation of a date in the format %Y-%m-%dT%H:%M:%SZ
        delivery_location (str, optional): Link to data object location. Defaults to None.
        run_id (str, optional): Databricks Job run id. Defaults to None.

    Usage example:
    --------------
        >>> from rabobank_edp_dbr_utils.gdp_metadata_api import CreateParamValues
        >>> config_json = {
        ...     "dataProducerName": "ExampleProducer",
        ...     "dataObjectLakeLayer": "raw",
        ...     "dataObjectType": "incremental", # only for metadata v3
        ...     "dataObjectTechnicalName": "ExampleDataObject",
        ...     "dataObjectTechnicalIdSourceObject": "123456789",
        ...     "dataObjectProcessingJobPlannedFrequencyType": "Hourly"
        ... }
        >>> lines_written = 1000
        >>> extract_type = "parquet"
        >>> edl_load_dts = "2023-01-01T00:00:00Z"
        >>> delivery_location = "https://example/data/output"
        >>> run_id = "12345"
        >>> param_creator = CreateParamValues(config_json, lines_written, extract_type, edl_load_dts, delivery_location, run_id)
    """
    def __init__(self,
                config_json_bytes: dict,
                lines_written: int,
                edl_load_dts: str,
                metadata_write_path: str,
                delivery_location: str = None,
                run_id: str = None):

        self._config = config_json_bytes
        self.metadata_config = {"params_metadata":{},
                                "params_credentials":{},
                                "params_runtime":{}}
        self.lines_written = lines_written
        self.edl_load_dts = edl_load_dts
        self.metadata_write_path = metadata_write_path
        self.delivery_location = delivery_location
        self.run_id = run_id
        self.initialize_config()

    def initialize_config(self):
        """This function initializes the metadata configuration with the required values"""	

        try:
            self.metadata_config["params_metadata"]["dataProducerName"] = self._config["dataProducerName"]
            self.metadata_config["params_metadata"]["dataObjectLakeLayer"] = self._config["dataObjectLakeLayer"]
            self.metadata_config["params_metadata"]["dataObjectTechnicalName"] = self._config["dataObjectTechnicalName"]
            self.metadata_config["params_metadata"]["dataObjectTechnicalIdSourceObject"] = self._config["dataObjectTechnicalIdSourceObject"]
            self.metadata_config["params_metadata"]["dataObjectProcessingJobPlannedFrequencyType"] = self._config["dataObjectProcessingJobPlannedFrequencyType"]
            self.metadata_config["params_metadata"]["dataObjectDeliveryLocation"] = self.__get_delivery_location(self.delivery_location)
            self.metadata_config["params_metadata"]["dataObjectDeliveryRetentionDatetime"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            self.metadata_config["params_metadata"]["dataObjectType"] = self._config["dataObjectType"]
            self.metadata_config["params_metadata"]["fileFormat"] = self.__get_file_format()

            self.metadata_config["params_metadata"]["operationalMetadata"] = {
                                                        "jobId":self.__get_run_id(self.run_id),
                                                        "dataObjectProcessingJobNumberOfLinesWritten":self.lines_written,
                                                        "dataObjectProcessingExtractionType":self._config["operationalMetadata"]["dataObjectProcessingExtractionType"],
                                                        "dataObjectEDLLoadDatetime":self.edl_load_dts}
            
            #initialize call credentials in the same metadata config
            self.metadata_config["params_credentials"]["dataProducerName"] = self._config["dataProducerName"]
            self.metadata_config["params_credentials"]["scope"] =  self._config["scope"]
            self.metadata_config["params_credentials"]["clientId"] = self._config["clientId"]
            self.metadata_config["params_credentials"]["relativeUrl"] = self._config["relativeUrl"]
            self.metadata_config["params_credentials"]["metadataAPI"] = self._config["metadataAPI"]

            #runtime metadata config
            self.metadata_config["params_runtime"]["abfss_location"] = self._config["abfss_location"]
            self.metadata_config["params_runtime"]["metadata_write_path"] = self.metadata_write_path
            self.metadata_config["params_runtime"]["edl_load_dts"] = self.edl_load_dts
            self.metadata_config["params_runtime"]["lines_written"] = self.lines_written
        
        except Exception as e:
            utils_logger.error(e.__str__())
            logging.exception(f"ERROR: Failed to initialize metadata config with error message: {e}")
            sys.exit(0)

    def __str__(self) -> str:
        dict_to_string = json.dumps(self.metadata_config)
        utils_logger.info(f"Metadata config: {dict_to_string}")
        return dict_to_string

    def __get_file_format(self):
        """If file_format is provided, it gets the file_format from the config
        else it returns an empty string
        """	

        if "fileFormat" in self._config:
            return self._config["fileFormat"]
        else:
            return ""
    
    def __get_run_id(self,run_id):
        """If run_id is not provided, it gets the run_id from the context"""	

        if run_id is None:
            from databricks.sdk.runtime import dbutils
            context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
            context = json.loads(context_str)
            tags = context.get('tags', {})
            return tags.get('jobRunId', "run-id-not-present")
        else:
            return run_id

    def __get_delivery_location(self,location):
        """If delivery_location is not provided, it gets the delivery_location from the config"""
        if location is None:
            return self._config["dataObjectDeliveryLocation"]
        else:
            return location

class CustomResponse:
    def __init__(self,HttpResponse:HTTPResponse):
        self._response = HttpResponse
        self.status = HttpResponse.status
        self.reason = HttpResponse.reason
        self.text = HttpResponse.read()
        self.headers = HttpResponse.getheaders()

    def read(self):
        return self.text