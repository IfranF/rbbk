from typing import Tuple, Protocol, Type, runtime_checkable
import os
import hashlib
import os
from ..logger import logging
import pandas as pd
from typing import Tuple
from pathlib import Path


utils_logger = logging.getLogger(__name__)

try:
    import rasterio
except ImportError:
    utils_logger.warning("rasterio is not installed. You will not be able to use the GeoTiffPixelValidator and GeoTiffCrsValidator.")
try:
    import geopandas as gpd
except ImportError:
    utils_logger.warning("geopandas is not installed. You will not be able to use the CRSVectorDataValidator and DataFrameRowCountValidator.")

# Validator registry. This is used so that other modules can load all the available validators with a single import.
#from rabobank_panacea_utils.validator.validators import VALIDATOR_REGISTRY
VALIDATOR_REGISTRY = {}

def register_validator(cls: Type) -> Type:
    """Decorator to register a validator class. This is used to make sure that the classes are available in the VALIDATOR_REGISTRY."""
    VALIDATOR_REGISTRY[cls.__name__] = cls
    return cls

@runtime_checkable
class Validator(Protocol):
    """
    Validator Protocol class for file validation.

    This class is intended to be used as a runtime-checkable protocol for 
    validators that check the validity of a file given its path.

    Methods:
    --------
    __call__(file_path: str) -> Tuple[bool, str]:
        Validates the file at the given path.

        Args:
            file_path (str): The path to the file to be validated.

        Returns:
            Tuple[bool, str]: A tuple where the first element is a boolean 
            indicating whether the file is valid, and the second element is 
            a string containing a message or reason.
    """
    def __call__(self, file_path: str, dict_of_objects: dict) -> Tuple[bool, str]:
        """
        Validate the given file path. This is just the protocol definition and should be implemented in the concrete classes.
        Args:
            file_path (str): The path to the file to be validated.
        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the validation result
                              and a string with a message or error description.
        """   
        pass

@register_validator
class ExtensionValidator(Validator):
    """
    A validator class to check if a file's extension is within the allowed extensions.
    Attributes:
        allowed_extensions (list): A list of allowed file extensions.
    """
    
    def __init__(self, allowed_extensions: list) -> None:
        self.allowed_extensions = allowed_extensions
        self.requires_file_read = False
    
    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        extension = os.path.splitext(file_path)[1].lower()
        if extension not in self.allowed_extensions:
            return False, f"File extension {extension} not allowed for the validator settings"
        return True, f"File extension is valid expected and actual {extension}"
    
@register_validator
class GeoTiffPixelValidator(Validator): 
    def __init__(self, expected_pixel_width: int = 0, expected_pixel_height: int = 0) -> None:
        """
        Initializes the GeoTiffPixelValidator with expected pixel dimensions.

        Args:
            expected_pixel_width (int, optional): The expected width of the pixels. Defaults to 0.
            expected_pixel_height (int, optional): The expected height of the pixels. Defaults to 0.

        Raises:
            ImportError: If the rasterio library is not installed.
        """
        try:
            import rasterio
            self.rasterio = rasterio
        except ImportError:
            raise ImportError("rasterio is required for GeoTiffPixelValidator but is not installed.")
        self.expected_pixel_width = expected_pixel_width
        self.expected_pixel_height = expected_pixel_height
        self.requires_file_read = False

    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        """
        Validates the pixel dimensions of a GeoTIFF file.

        This method checks if the pixel dimensions of the provided GeoTIFF file meet or exceed
        the expected dimensions specified during initialization.

        Args:
            file_path (str): The path to the GeoTIFF file to be validated.

        Returns:
            Tuple[bool, str]:   A tuple containing a boolean indicating whether the validation was 
                                successful and a message describing the result. If an error occurs
                                during processing, the boolean is False and the message contains the error details.

        Raises:
            Exception: If an error occurs while processing the file.
        """
        try:
            with self.rasterio.open(file_path) as dataset:
                actual_pixel_width = dataset.width
                actual_pixel_height = dataset.height
                if actual_pixel_width >= self.expected_pixel_width and actual_pixel_height >= self.expected_pixel_height:
                    return True, f"The GeoTIFF has at least the expected pixel dimensions. The expected pixel values are: width:{self.expected_pixel_width}, height:{self.expected_pixel_height}. The actual values are width:{actual_pixel_width}, height:{actual_pixel_height}"
                return False, f"The GeoTIFF does not have the expected pixel dimensions. The expected pixel values are: width:{self.expected_pixel_width}, height:{self.expected_pixel_height}. The actual values are width:{actual_pixel_width}, height:{actual_pixel_height}"
        except Exception as e:
            return False, f"An error occurred while processing the file: {str(e)}"

@register_validator        
class GeoTiffCrsValidator(Validator):
    """
    A validator class to check if a GeoTIFF file has the expected Coordinate Reference System (CRS).
    Attributes:
        expected_crs (str): The expected CRS string that the GeoTIFF file should have.
        rasterio (module): The rasterio module used for reading GeoTIFF files.
    
    Raises:
        ImportError: If the rasterio library is not installed.
    """
    def __init__(self, expected_crs: str, dict_of_objects: dict=None) -> None:
        try:
            import rasterio
            self.rasterio = rasterio
        except ImportError:
            raise ImportError("rasterio is required for GeoTiffCrsValidator but is not installed.")
        self.expected_crs = expected_crs
        self.requires_file_read = False

    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        """
        Validates the Coordinate Reference System (CRS) of a given GeoTIFF file.

        Args:
            file_path (str): The path to the GeoTIFF file to be validated.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the CRS is as expected,
                              and a message providing details about the validation result.
                              If the CRS matches the expected CRS, the boolean is True and the message
                              confirms the match. If the CRS does not match, the boolean is False and
                              the message provides the expected and actual CRS values. If an error occurs
                              during processing, the boolean is False and the message contains the error details.
        """
        try:
            with self.rasterio.open(file_path) as dataset:
                actual_crs = dataset.crs.to_string()
                if actual_crs == self.expected_crs:
                    return True, f"The GeoTIFF has the expected CRS {self.expected_crs}"
                return False, f"The GeoTIFF does not have the expected CRS. The expected CRS is: {self.expected_crs}. The actual CRS is: {actual_crs}"
        except Exception as e:
            return False, f"An error occurred while processing the file: {str(e)}"

@register_validator
class FileSizeValidator(Validator):
    """
    A validator class to check if a file meets a minimum size requirement.
    Attributes:
        min_size_mb (int): The minimum file size in megabytes.
    """
    def __init__(self, min_size_mb: int) -> None:
        self.min_size_mb = min_size_mb
        self.requires_file_read = False

    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        """
        Check if the file size meets the minimum required size.

        Args:
            file_path (str): The path to the file to be validated.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the file size meets the minimum requirement,
                              and a string message providing details about the validation result.
        """
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)  # Convert bytes to megabytes
        if file_size_mb >= self.min_size_mb:
            return True, f"The file size meets the minimum requirement. The minimum required size is {self.min_size_mb} MB. The actual file size is {file_size_mb:.2f} MB"
        return False, f"The file size does not meet the minimum requirement. The minimum required size is {self.min_size_mb} MB. The actual file size is {file_size_mb:.2f} MB"

@register_validator
class FileHashValidator(Validator):
    """
    A validator class that checks if the hash of a given file matches the hash of a reference file.
    Attributes:
        reference_file_path (str): The path to the reference file.
        hash_algorithm (str): The hash algorithm to use (default is 'sha256').
        reference_hash (str): The hash of the reference file. This is the file where you want to compare the current file too. 
    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating whether the file hash matches the reference file hash,
                          and a string message providing details about the validation result.
    """
    def __init__(self, reference_file_path: str, hash_algorithm: str = 'sha256') -> None:
        self.reference_file_path = reference_file_path
        self.hash_algorithm = hash_algorithm
        self.reference_hash = self.calculate_hash(reference_file_path)
        self.requires_file_read = False

    def calculate_hash(self, file_path: str, dict_of_objects: dict=None) -> str:
        """
        Calculate the hash of a file using the specified hash algorithm.

        Args:
            file_path (str): The path to the file for which the hash is to be calculated.

        Returns:
            str: The hexadecimal digest of the hash.

        Raises:
            FileNotFoundError: If the file at the specified path does not exist.
            ValueError: If the specified hash algorithm is not supported.
        """
        hash_func = hashlib.new(self.hash_algorithm)
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        """
        Validates the hash of a given file against a reference hash.
        Args:
            file_path (str): The path to the file to be validated.
        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the file hash matches the reference hash,
                              and a string message describing the result.
        Raises:
            FileNotFoundError: If the file at the specified path does not exist.
            Exception: If an error occurs while processing the file.
        """
        try:
            file_hash = self.calculate_hash(file_path)
        except FileNotFoundError:
            return False, f"The file at path '{file_path}' was not found."
        except Exception as e:
            return False, f"An error occurred while processing the file at path '{file_path}': {str(e)}"
        
        if file_hash == self.reference_hash:
            return True, "The file hash matches the reference file hash."       
        return False, "The file hash does not match the reference file hash. Did the copy happen correctly?"

@register_validator
class UnknownValidation(Validator):
    """
    A validator class that always returns an unknown validation result.
    This class is used when the validation configuration is missing or unknown.

    Args:
            file_path (str): The path to the file for which the hash is to be calculated.
    """
    def __init__(self) -> None:
        self.requires_file_read = False
    
    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        return False, "Unknown validation result due to missing configuration."

@register_validator
class DataFrameRowCountValidator(Validator):
    """
    A validator class to check if a DataFrame meets a minimum row count requirement.
    Attributes:
        min_row_count (int): The minimum number of rows required.
    """
    def __init__(self, min_row_count: int) -> None:
        self.min_row_count = min_row_count
        self.requires_file_read = True

    def __call__(self, file_path: str, dict_of_objects: dict=None) -> Tuple[bool, str]:
        """
        Check if the DataFrame contains at least the minimum number of rows.

        Args:
            df (pd.DataFrame): The DataFrame to be validated.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the DataFrame meets the minimum row count,
                              and a string message providing details about the validation result.
        """
        check_pandas_keys(dict_of_objects, file_path, self.__class__.__name__)
        df = dict_of_objects.get('pandas')
        row_count = len(df)
        if df is None:
            return False, "The DataFrame object with the key 'pandas' was not found in the provided dictionary."
        if row_count >= self.min_row_count:
            return True, f"The DataFrame meets the minimum row count requirement. The minimum required row count is {self.min_row_count}. The actual row count is {row_count}."
        return False, f"The DataFrame does not meet the minimum row count requirement. The minimum required row count is {self.min_row_count}. The actual row count is {row_count}."
    
@register_validator
class CRSVectorDataValidator:
    """
    A validator class to check if the Coordinate Reference System (CRS) of GeoDataFrames matches the expected CRS.
    Attributes:
    -----------
    expected_crs (str): The expected CRS string.

    Methods:
    --------
    __call__(file_path: str, dict_of_objects: dict = None) -> Tuple[bool, str]:
    Validates the CRS of GeoDataFrames contained in the provided dictionary.
    Returns a tuple where the first element is a boolean indicating if the CRS matches the expected CRS,
    and the second element is a string message describing the result.
    Expects either a single geopandas GeoDataFrame (with key: geopandas) or a dictionary of geopandas GeoDataFrames
    with layer names as keys (with key: dict_of_geopandas).

    Example usage:
    --------------
        >>> #example 1: a single geodataframe
        >>> validator = CRSVectorDataValidator(expected_crs="EPSG:4326")
        >>> gdf = geopandas.read_file("path_to_vector_file.shp")
        >>> result, message = validator(file_path="path_to_vector_file.shp", dict_of_objects={"geopandas": gdf})
        >>> print(result, message)
        >>> #example 2: a dictionary of geodataframes
        >>> validator = CRSVectorDataValidator(expected_crs="EPSG:4326")
        >>> gdf1 = geopandas.read_file("path_to_vector_file1.shp")
        >>> gdf2 = geopandas.read_file("path_to_vector_file2.shp")
        >>> #your file reader is responsible for creating the geodataframes and adding them to the dictionary
        >>> result, message = validator(file_path="path_to_vector_file1.shp", dict_of_objects={"dict_of_geopandas": {"layer1": gdf1, "layer2": gdf2}})
    """
    def __init__(self,  expected_crs: str):
        self.expected_crs = expected_crs.upper()
        self.requires_file_read = True

    def validate_crs(self, gdf, layer=None) -> Tuple[bool, str]:
        """
        Validates the Coordinate Reference System (CRS) of a GeoDataFrame against an expected CRS.

        Parameters:
            gdf (GeoDataFrame): The GeoDataFrame whose CRS is to be validated.
            layer (str, optional): The name of the layer being validated. Defaults to None for a single geopandas.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating if the CRS matches the expected CRS,
                                and a string message describing the result.
        """
        if not gdf.crs:
            return False, f"No CRS found for the layer: {layer}"
                
        crs_str = gdf.crs.to_string().upper()
        if self.expected_crs and crs_str != self.expected_crs:
            if layer:
                return False, f"CRS mismatch in layer {layer}: expected {self.expected_crs}, found {crs_str}."
            else:
                return False, f"CRS mismatch: expected {self.expected_crs}, found {crs_str}."
        return True, f"CRS match: expected {self.expected_crs}, found {crs_str}."
    
    def __call__(self, file_path: str, dict_of_objects: dict = None) -> Tuple[bool, str]:
        check_geopandas_keys(dict_of_objects, file_path, self.__class__.__name__)
        
        gdfs = dict_of_objects.get('dict_of_geopandas', False) 
        if gdfs:
            for layer, gdf in gdfs.items():
                is_valid, message = self.validate_crs(gdf, layer)
                if not is_valid:
                    return is_valid, message
            return True, f"CRS match for all layers, expected {self.expected_crs}."
        
        gdf = dict_of_objects.get('geopandas', False) 
        if isinstance(gdf, gpd.GeoDataFrame):
            return self.validate_crs(gdf)
        return False, "Unknown error with the vallidation of the CRS. Please double check the input data."

@register_validator
class XMLSchemaValidator(Validator):

    def __init__(self,xsd_file_path: str) -> None:
        self.xsd_file_path = self._file_present_check(xsd_file_path)
        self.requires_file_read = True
        self.xmlschema = self._import_xmlschema()

    def _import_xmlschema(self):
        try:
            import xmlschema
            return xmlschema
        except ImportError:
            raise ImportError("xmlschema is required for XMLSchemaValidator but is not installed.")

    def _file_present_check(self,file_name_path:str):
        if Path(file_name_path).is_file():
            return file_name_path
        else:
            raise FileNotFoundError(f"File {file_name_path} not found")


    def validate_xml_xsd(self,xml_file_path: str,
                        xsd_file_path: str) -> bool:
        """
        Validates an XML file against an XSD schema.
        Args:
            xml_file_path (str): The path to the XML file to be validated.
            xsd_file_path (str): The path to the XSD schema file.
        Returns:
            bool: A boolean indicating whether the XML file is valid.
        """
        xml_file_path_present = self._file_present_check(xml_file_path)

        try:            
            schema = self.xmlschema.XMLSchema(xsd_file_path)
            schema.validate(xml_file_path_present)
            return True, "XML file is valid"
        except Exception as e:
            utils_logger.error(f"XML file at {xml_file_path} is invalid.")
            return False,f"Error Message: {str(e)}"

    def __call__(self, file_path: str,dict_of_objects: dict = None) -> Tuple[bool, str]:
        """
        Call validate function.
        Args:
            file_path (str): The path to the XML file to be validated.            
        Returns:
            is_valid (tuple): A boolean indicating whether the XML file is valid and a message.
        """

        return self.validate_xml_xsd(file_path, self.xsd_file_path)
       


def check_geopandas_keys(dict_of_objects: dict, file_path: str, class_name: str) -> None:
    """
    Checks if 'dict_of_geopandas' or 'geopandas' exists in the dict_of_objects.
    Raises an error if neither is present.
    """
    if 'dict_of_geopandas' not in dict_of_objects and 'geopandas' not in dict_of_objects:
        _, extension = os.path.splitext(file_path)
        raise ValueError(f"No Dataframe provided for validation {class_name}. please check the compatability of the {extension} extension with the validator")
    
def check_pandas_keys(dict_of_objects: dict, file_path: str, class_name: str) -> None:
    """
    Checks if 'dict_of_geopandas' or 'geopandas' exists in the dict_of_objects.
    Raises an error if neither is present.
    """
    if 'pandas' not in dict_of_objects:
        _, extension = os.path.splitext(file_path)
        raise ValueError(f"No Dataframe provided for validation {class_name}. please check the compatability of the {extension} extension with the validator")    