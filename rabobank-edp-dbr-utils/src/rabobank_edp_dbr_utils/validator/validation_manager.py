import os
from typing import List
import time
from rabobank_edp_dbr_utils.validator.validation_read_files import ValidationFileLoader

class ValidationManager:
    """
    Manages the validation of files using a series of validators.

    Attributes:
        validators (list): A list of validator instances. Each validator should be a callable that takes a file path as input and returns a tuple (is_valid, message).
        error_on_validation_fail (bool): If True, raises an error when any validation fails. Defaults to True.

    example usage:
        from rabobank_panacea_utils.validator.validation_manager import ValidationManager
        from rabobank_panacea_utils.validator.validators import GeoTiffPixelValidator, GeoTiffCrsValidator   
        validators = [GeoTiffPixelValidator(expected_pixel_width = 0, expected_pixel_height = 0), GeoTiffCrsValidator(expected_crs="EPSG:28992")]
        manager = ValidationManager(validators)
        print(manager.validate("file_path"))
    """

    def __init__(self, validators: list, error_on_validation_fail: bool = True) -> None:
        """
        Initializes the ValidationManager with a list of validators and an error handling flag.

        Args:
            validators (list): A list of validator instances. Each validator should be a callable that takes a file path as input and returns a tuple (is_valid, message).
            error_on_validation_fail (bool): If True, raises an error when any validation fails. Defaults to True.
        """
        self.validators = validators
        self.error_on_validation_fail = error_on_validation_fail

    def validate(self, file_path: str, input_parameters: dict) -> list:
        """
        Validates a file using a series of validators and returns the results.

        Args:
            file_path (str): The path to the file to be validated.

        Returns:
            dict: A dictionary containing the validation results for each validator. 
              Each entry includes:
              - "validator": The name of the validator class.
              - "is_valid": A boolean indicating if the file passed the validation.
              - "message": A message from the validator.
              - "filename": The name of the file.
              - "file_path": The path to the file.
              - "runtime_in_sec_validator": The time taken to run the validator in seconds.
              - "runtime_in_sec_load_file": The time taken to load the file in seconds.
        """
        filename = os.path.basename(file_path)
        start_time_load_file = time.time()   
        dict_of_objects = ValidationFileLoader.load_file(file_path, **input_parameters) if input_parameters else ValidationFileLoader.load_file(file_path)
        runtime_load_file = time.time() - start_time_load_file
        results = []
        for validator in self.validators:
            start_time = time.time()
            is_valid, message = validator(file_path=file_path, dict_of_objects=dict_of_objects)
            runtime = time.time() - start_time
            results.append({
            "validator": validator.__class__.__name__,
            "is_valid": is_valid,
            "message": message,
            "filename": filename,
            "file_path": file_path,
            "runtime_in_sec_validator": runtime,
            "runtime_in_sec_load_file": runtime_load_file
            })
        self.check_for_errors(filename, results) 
        return results
    
    def check_for_errors(self, filename: str, results) -> None:
        """
        Checks for validation errors in the provided results and raises an exception if any errors are found.

        Args:
            filename (str): The name of the file being validated.
            results (list): A list of dictionaries containing validation results. Each dictionary should have the keys:
            - "is_valid" (bool): Indicates if the validation passed.
            - "filename" (str): The name of the file that was validated.
            - "validator" (str): The name of the validator that was used.
            - "message" (str): The validation error message.

        Raises:
            ValueError: If any validation errors are found and `self.error_on_validation_fail` is True.
        """
        list_of_errors = []
        for result in results:
            if not result["is_valid"]:
                list_of_errors.append(f"Validation failed for file: {result['filename']} - {result['validator']} - {result['message']}")
        if list_of_errors and self.error_on_validation_fail:
            raise ValueError(f"Validation errors for file {filename}:\n" + "\n".join(list_of_errors))
