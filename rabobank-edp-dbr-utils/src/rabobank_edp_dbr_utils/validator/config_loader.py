import yaml
from typing import Dict, List
from rabobank_edp_dbr_utils.validator.validators import VALIDATOR_REGISTRY

class ConfigLoader:
    """
    ConfigLoader is a class responsible for loading and parsing a YAML configuration file for validators.
    
    Attributes:
        config_path (str): The path to the YAML configuration file.
        config (dict): The contents of the YAML configuration file.
        validator_childs (dict): The "validators" section of the configuration.
        validator_rules (dict): The "rules" section within the "validators" configuration.
        validators (Dict[str, dict[list, dict]]): A dictionary mapping filenames to their respective
        file_read_settings (dict): Settings related to file reading from the configuration.
    Methods:
        load_config() -> dict:
        parse_validator_config() -> Dict[str, dict[list, dict]]:
        create_validator_instances(checks: list) -> list:
        get_validators() -> Dict[str, dict[list, dict]]:
            Returns the parsed validators configuration.
        get_file_read_settings() -> dict:
            Returns the file read settings from the configuration.
    """
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config()
        self.validator_childs = self.config.get("validators", {})
        self.validator_rules = self.validator_childs.get("rules", {})
        self.validators = self.parse_validator_config()
        self.file_read_settings = self.validator_childs.get("file_read_settings", {})

    def load_config(self) -> dict:
        """
        Loads a YAML configuration file and returns its contents as a dictionary.

        Returns:
            dict: The contents of the YAML configuration file.
        """
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def parse_validator_config(self) -> Dict[str, dict[list, dict]]:
        """
        Parses the validator configuration and returns an overview of validators.
        This method iterates through the validator rules provided in the instance,
        checks for the presence of required keys ('rule' and 'checks'), and creates
        validator instances based on the 'checks' key. It then constructs a dictionary
        mapping each rule to its corresponding validators. These rules are then used 
        to pass files through the validators.
        Raises:
            KeyError: If a validator configuration is missing the 'rule' or 'checks' key.
        Returns:
            Dict[str, dict[list, dict]]: A dictionary where each key is a rule and the value
            is another dictionary containing the list of validator instances.
        """
        validators_overview = {}
        for validator_config in self.validator_rules:
            if "rule" not in validator_config:
                raise KeyError(f"Missing 'rule' key in validator configuration: {validator_config}")
            if "checks" not in validator_config:
                raise KeyError(f"Missing 'checks' key in validator configuration: {validator_config}")
            
            rule = validator_config["rule"]
            validator_instances = self.create_validator_instances(validator_config["checks"])
            validators_overview[rule] = {"validators": validator_instances}
        return validators_overview

    def create_validator_instances(self, checks: list) -> list:
        """
        Creates instances of validators based on the provided checks.

        Args:
            checks (list): A list of checks from the configuration.

        Returns:
            list: A list of validator instances.

        Raises:
            KeyError: If the 'type' key is missing in a check.
            ValueError: If the validator type is not found in the VALIDATOR_REGISTRY.
            TypeError: If there is an error during the instantiation of a validator.
        """
        validator_instances = []
        for type_config in checks:
            if "type" not in type_config:
                raise KeyError(f"Missing 'type' key in check configuration: {type_config}")

            validator_type = type_config["type"]
            params = type_config.get("params", {})

            if validator_type not in VALIDATOR_REGISTRY:
                raise ValueError(f"Validator type '{validator_type}' not found in VALIDATOR_REGISTRY are you sure this is a valid validator?")

            validator_class = VALIDATOR_REGISTRY[validator_type]

            # Check if the validator class can be instantiated with the given params
            try:
                validator_instance = validator_class(**params)
            except TypeError as e:
                raise TypeError(f"Error instantiating validator '{validator_type}' with params {params}") from e

            validator_instances.append(validator_instance)
        return validator_instances
    
    def get_validators(self) -> Dict[str, dict[list]]:
        """
        Retrieve the validators.

        Returns:
            Dict[str, dict[list, dict]]: A dictionary where the keys are strings and the values are dictionaries containing lists and dictionaries.
        example output:
        {'rule1': {'validators': 
        [<rabobank_panacea_utils.validator.validators.ExtensionValidator object at 0x000002AE1F8E2D10>]}, 
        'rule2': {'validators': [<rabobank_panacea_utils.validator.validators.FileSizeValidator object at 0x000002AE1FC2F810>]}, 
        'rule3_csv': {'validators': [<rabobank_panacea_utils.validator.validators.DataFrameRowCountValidator object at 0x000002AE1FC9CF10>]},
        validate_geopackage': {'validators': [<rabobank_panacea_utils.validator.validators.CRSVectorDataValidator object at 0x000002AE1FC9DE90>]}}  
        """
        return self.validators
    
    def get_file_read_settings(self) -> dict:
        """
        Retrieve the file read settings.

        Returns:
            dict: A dictionary containing the file read settings.
        """
        return self.file_read_settings