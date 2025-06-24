"""REST API Module

This module contains the classes ``MetadataApiCaller`` to call the metadata API 
and the class ``CreateParamValues`` to create the parameter values for the metadata API.
"""

# pylint: disable=import-error
import sys
import logging
import json
import requests
from rabobank_edp_dbr_utils.util_functions import EnvironmentUtils
from rabobank_edp_dbr_utils.logger import logging 

utils_logger = logging.getLogger(__name__)

class RESTApiCaller:
    """Class to call the REST API"""
    def __init__(self, api_url: str, api_scope: str, client_id: str, client_secret: str):
        self.api_url = api_url
        self.api_scope = api_scope
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__token = self.__get_azure_token()

    def __get_azure_token(self) -> str:
        """Get token from Azure REST API

        Returns:
            str: Azure token
        
        Raises:
            Exception: If the request fails, the program will exit.
        """
        header = {
            "Content_Type": "application/json",
            "cache-control": "no-cache",
            }

        values = {
            "grant_type": "client_credentials",
            "scope": self.api_scope,
            "client_id": self.__client_id,
            "client_secret": self.__client_secret
        }

        try:
            api_url = "https://login.microsoftonline.com/6e93a626-8aca-4dc1-9191-ce291b4b75a1/oauth2/v2.0/token"
            response = requests.post(
                url = api_url,
                headers = header,
                data = values,
                timeout = 5*60 # 5 minutes timeout
            )

            # Check if the response is successful
            response.raise_for_status()

        except Exception as e:
            utils_logger.exception(f"ERROR: \
                Failed to retrieve a token from MICROSOFT with error message: {e.__str__()}")
            raise e

        token = json.loads(response.text)["access_token"]        
        return token

    def __make_post_request(self, body: dict, header: dict={}) -> requests.Response:
        """Make a POST request to the REST API

        Args:
            body (dict): The body of the POST request.
            header (dict, optional): Optional headers to include in the request. Defaults to an empty dictionary.
            requests.Response: The response object from the POST request.

        Returns:
            requests.Response: API response

        Raises:
            Exception: If the request fails, the program will exit.
        """
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json",
        }
        headers.update(header) # Update the header with optional header
        try:
            response = requests.post(
                self.api_url
                , headers=headers
                , json=body
            )
            # Check if the response is successful
            response.raise_for_status()

        except Exception as e:
            utils_logger.exception(f"ERROR: \
                Failed to make a POST request to the API with error message: {e.__str__()}")
            raise e

        return response

    def post_api(self, body: dict, header: dict={}) -> requests.Response:
        """Makes a POST call to API with the given token and parameters

        Args:
            body (dict): The JSON body to be sent in the POST request

        Returns:
            requests.Response: API response

        Raises:
            ValueError: "result statuscode is not successful but : " + str(api_response.status_code)
        """
        api_response = self.__make_post_request(body, header)
        utils_logger.info(f"API response status code: {api_response.status_code}")

        self.__validate_response(api_response)

        return api_response

    def __make_get_request(self, header: dict={}) -> requests.Response:
        """Make a GET request to the REST API.

        Args:
            header (dict, optional): Additional headers to include in the request. Defaults to an empty dictionary.

        Returns:
            requests.Response: The response object from the GET request.

        Raises:
            Exception: If the request fails, the program will exit.
        """
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json",
        }
        headers.update(header)
        try:
            response = requests.get(
                self.api_url
                , headers=headers
            )
            # Check if the response is successful
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            utils_logger.exception(f"ERROR: Failed to retrieve a valid response from the request with error message: {e}")
            raise e
        return response

    def get_api(self, header: dict={}) -> requests.Response:
        """Makes a GET call to API with the given token and parameters

        Args:
            hedaer (dict): Optional Header

        Returns:
            requests.Response: API response
        
        Raises:
            ValueError: "result statuscode is not successful but : " + str(api_response.status_code)
        """
        api_response = self.__make_get_request(header)
        utils_logger.info(f"API response status code: {api_response.status_code}")

        self.__validate_response(api_response)

        return api_response

    def __validate_response(self, response: requests.Response):
        """Validate the response from the API

        Args:
            response (requests.Response): The response object from the API

        Raises:
            ValueError: If the response status code is not successful.
        """
        status_code = int(response.status_code) 
        if status_code < 200 or status_code >= 300:
            raise ValueError("result statuscode is not successful but : " 
                    + str(status_code) 
                    + " with reason: " + response.reason 
                    + " and response text: " + response.text)
