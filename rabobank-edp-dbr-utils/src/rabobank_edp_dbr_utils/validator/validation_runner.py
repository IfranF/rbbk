from rabobank_edp_dbr_utils.validator.config_loader import ConfigLoader
from rabobank_edp_dbr_utils.validator.validation_manager import ValidationManager
import itertools
import pandas as pd
from typing import Tuple, List
import os
from rabobank_edp_dbr_utils.validator.validators import UnknownValidation
from typing import Optional

class ValidationRun:
    """
    A class to manage and run validation processes on a given file.
    Attributes:
        file_location (str): The location of the file to be validated.
        error_on_validation_fail (bool): Flag to indicate if an error should be raised on validation failure.
        validators (list): A list of validators to be used in the validation process. The validators are defined in the config file.
        outcome_single_run (Optional[dict]): The outcome of a single validation run.
        input_parameters_read (Optional[dict]): Input parameters read for validation.
        requires_file_read (bool): Flag to indicate if file reading is necessary for validation.
    Methods:
        run_validation() -> None:
            Executes the validation process using the provided validators.
        get_outcome_single_run() -> Optional[dict]:
            Returns the outcome of the last validation run.
        add_validators(validators) -> None:
            Adds a list of validators to the validation process and determines if file reading is necessary.
        determine_file_read_necessary() -> None:
            Checks if any of the validators require file reading and sets the flag accordingly.
        set_input_parameters_read(input_parameters_read) -> None:
            Sets the input parameters read for validation.
    Example:
        >>> validation_run = ValidationRun("/path/to/file", True)
        >>> validation_run.add_validators([Validator1, Validator2])
        >>> validation_run.set_input_parameters_read({"param1": "value1"})
        >>> validation_run.run_validation()
        >>> outcome = validation_run.get_outcome_single_run()
        >>> print(outcome)
    """
    def __init__(self, file_location, error_on_validation_fail):
        self.file_location = file_location
        self.error_on_validation_fail = error_on_validation_fail
        self.validators = []
        self.outcome_single_run = None
        self.input_parameters_read = None
        self.requires_file_read = False
    
    def run_validation(self) -> None:
        """
        Executes the validation process using the provided validators and input parameters.

        This method initializes a ValidationManager with the validators and error handling
        configuration, then runs the validation on the specified file location with the given
        input parameters. The outcome of the validation is stored in the `outcome_single_run` attribute.

        Returns:
            None
        """
        validation_manager = ValidationManager(self.validators, self.error_on_validation_fail)
        self.outcome_single_run = validation_manager.validate(self.file_location, self.input_parameters_read)

    def get_outcome_single_run(self) -> Optional[dict]:
        """
        Retrieve the outcome of a single validation run.

        Returns:
            Optional[dict]: The outcome of the single validation run if available, otherwise None.
        """
        return self.outcome_single_run
    
    def add_validators(self, validators) -> None:
        """
        Adds a list of validators to the current list of validators and 
        determines if file read is necessary.

        Args:
            validators (list): A list of validator objects to be added.
        
        Returns:
            None
        """
        self.validators.extend(validators)
        self.determine_file_read_necessary()

    def determine_file_read_necessary(self) -> None:
        """
        Determines if reading a file is necessary based on the validators.

        Iterates through the list of validators and sets the `requires_file_read`
        attribute to True if any validator requires a file read. This attribute is set by the validator itself on initialization.

        Returns:
            None
        """
        for validator in self.validators:
            if validator.requires_file_read:
                self.requires_file_read = True

    def set_input_parameters_read(self, input_parameters_read: dict) -> None:
        """
        Sets the input parameters read by the validation runner (expects a dict with the read properties).

        Args:
            input_parameters_read (Any): The input parameters that have been read and need to be set.
        """
        self.input_parameters_read = input_parameters_read



class ValidationRunner:
    def __init__(self, config_path: str, file_locations: List[str],rules: List[str], file_read_settings: str = None ,error_on_validation_fail: bool = True) -> None:
        """
        Initializes the ValidationRunner with the given configuration path, file locations, rules, and optional settings.

        Args:
            config_path (str): The path to the configuration file.
            file_locations (List[str]): A list of file locations to be validated.
            rules (List[str]): A list of validation rules to be applied.
            file_read_settings (str, optional): Settings for reading files. Defaults to None.
            error_on_validation_fail (bool, optional): Flag to raise an error if validation fails. Defaults to True.

        Example:
            >>> validation_runner = ValidationRunner(
            ...     config_path='/path/to/config.yaml',
            ...     file_locations=['/path/to/file1.csv', '/path/to/file2.csv', '/dbfs/mnt/liwo/path/to/file2.csv],
            ...     rules=['rule1', 'rule2'],
            ...     file_read_settings='read_settings',
            ...     error_on_validation_fail=True
            ...     )
            >>> validation_runner.run_validation()
        """
        self.config_loader = ConfigLoader(config_path)
        self.all_validators = self.config_loader.get_validators()
        self.all_file_read_settings = self.config_loader.get_file_read_settings()
        self.file_locations = file_locations
        self.rules = rules
        self.file_read_settings = file_read_settings
        self.error_on_validation_fail = error_on_validation_fail
        self.outcome = None

    def run_validation(self) -> None:
        """
        Executes the validation process for a list of file locations.
        This method iterates over each file location provided in `self.file_locations` and performs the following steps:
        1. Initializes a `ValidationRun` object for the current file location.
        2. Adds validators to the `ValidationRun` object based on the rules specified in `self.rules`.
        3. If the validation requires reading the file, sets the input parameters for reading based on `self.file_read_settings`.
        4. Runs the validation process for the current file location.
        5. Collects the outcome of the validation run and appends it to the `outcome` list.
        Finally, the method flattens the list of outcomes (which is a list of lists) into a single list using `itertools.chain` and assigns it to `self.outcome`.
        
        Attributes:
            self.file_locations (list): List of file locations to be validated.
            self.error_on_validation_fail (bool): Flag to determine if an error should be raised on validation failure.
            self.rules (list): List of rules to be applied during validation.
            self.file_read_settings (dict): Settings for reading the input files.
            self.outcome (list): Flattened list of validation outcomes.
        
        Example: of output: 
        [
        {'validator': 'validator1', 'is_valid': True, 'message': 'Valid', 'filename': 'file1.csv', 'file_path': 'path/to/file1.csv', 'runtime_in_sec_validator': 0.1, 'runtime_in_sec_load_file': 0.1},
        {'validator': 'validator2', 'is_valid': False, 'message': 'Invalid', 'filename': 'file1.csv', 'file_path': 'path/to/file1.csv', 'runtime_in_sec_validator': 0.2, 'runtime_in_sec_load_file': 0.2}
        ]    
        """
        outcome = []
        for file_location in self.file_locations:
            validation_run = ValidationRun(file_location, self.error_on_validation_fail)
            #pick up all the rules that are in the config file
            for rule in self.rules:
                validation_run.add_validators(self._get_validators_for_rule(rule))
            if validation_run.requires_file_read:
                validation_run.set_input_parameters_read(self._get_input_arguments_of_read(self.file_read_settings))
            validation_run.run_validation()
            outcome.append(validation_run.get_outcome_single_run())
        #because the results are a list of lists, we need to flatten it to a single list (so that we can make a table out of it). That is why itertools.chain is used here
        self.outcome = list(itertools.chain(*outcome))


    def _get_input_arguments_of_read(self, file_read_setting) -> dict | None:
        """
        Retrieve the input arguments for a specific file read setting.

        Args:
            file_read_setting (str): The file read setting to search for.

        Returns:
            dict | None: A dictionary of parameters if the file read setting is found, 
                         otherwise None.

        Example:
            >>> input_args = validation_runner._get_input_arguments_of_read('read_settings')
            >>> print(input_args)
            {'delimiter': ',', 'header': 0}
        """
        for setting in self.all_file_read_settings:
            if setting['file_read_setting'] == file_read_setting:
                return setting['params']
        return None

    def _get_validators_for_rule(self, rule) -> list:
        """
        Retrieve the validators associated with a given rule.

        This method checks if the specified rule exists in the `all_validators` dictionary.
        If the rule is found, it returns the associated validators.
        If the rule is not found, it returns a list containing an instance of `UnknownValidation`.

        Args:
            rule (str): The rule for which validators are to be retrieved.

        Returns:
            a list of all validators otherwise a list containing `UnknownValidation`.

        Example Output:
            [<rabobank_panacea_utils.validator.validators.ExtensionValidator object at 0x000001BD09A5CA50>, <rabobank_panacea_utils.validator.validators.FileSizeValidator object at 0x000001BD09A5CB10>]
            or    
            [UnknownValidation()]
        """
        if rule in self.all_validators:
            return self.all_validators[rule]['validators']
        else:
            return [UnknownValidation()]

    def get_results_as_pandas(self) -> pd.DataFrame:
        """
        Converts the validation outcome to a pandas DataFrame. This can be used for easy analysis. 

        Returns:
            pd.DataFrame: A DataFrame containing the validation results.

        Raises:
            ValueError: If no validation results are available.
        """
        if self.outcome is None:
            raise ValueError("No validation results available. Run the validation first.")
        return pd.DataFrame(self.outcome)
    
    def get_outcome(self) -> List:
        """
        Retrieve the outcome of the validation process. This could be used for api calls or other applications. 

        Returns:
            List: The outcome of the validation process.

        Raises:
            ValueError: If the validation has not been run and no results are available.
        """
        if self.outcome is None:
            raise ValueError("No validation results available. Run the validation first.")
        return self.outcome
    
    def get_rules(self) -> List[str]:
        """
        Retrieve the list of rules.

        Returns:
            List[str]: A list containing the rules.
        """
        return self.rules

    def get_read_settings(self) -> dict:
        """
        Retrieve the configuration for file read settings.

        Returns:
            dict: A dictionary containing the file read settings configuration.
        """
        return self.all_file_read_settings