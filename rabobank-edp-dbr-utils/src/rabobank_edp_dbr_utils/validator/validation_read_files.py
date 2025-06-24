import os 
import pandas as pd
from rabobank_edp_dbr_utils.logger import logging
# from databricks.sdk.runtime import spark

utils_logger = logging.getLogger(__name__)

try:
    import geopandas as gpd
    import fiona
except ImportError:
    utils_logger.warning("geopandas / fiona is not installed. You will not be able to read .gpkg files.")

try:
    import openpyxl
except ImportError:
    utils_logger.warning("openpyxl is not installed. You will not be able to read .xls or .xlsx files.")


class ValidationFileLoader:
    """
    A class used to load validation files based on their file type. 
    Every instance of the ValidationFileLoader class will have access to all registered loaders 
    because the loaders are stored in class-level attributes (loaders and default_loader). 
    These attributes are shared across all instances of the class.
    
    Attributes
    ----------
    loaders : dict
        A dictionary mapping file types to their respective loader functions. This is so that the 
        appropriate loader function can be used based on the file type.
    default_loader : function
        The default loader function to use if no specific loader is registered for a file type. This is empty by default.
    Methods
    -------
    register_loader(file_type)
        Registers a loader function for a specific file type. It add the loader function to the loaders dictionary. The dictionary key
        is the file type in lowercase. The loader function should take a file path as input and return the loaded object in a dictionairy.
    register_default_loader(loader)
        Registers the default loader function. This is necessary if the file doesn't have any input parameters, but we still want to process the validator (such as filesize)
    get_file_type(file_path)
        Returns the file type (extension) of the given file path. This is just a helper function
    load_file(file_path, **kwargs)
        Loads a file using the appropriate loader function based on its file type.
    """
    loaders = {}
    default_loader = None

    @classmethod
    def register_loader(cls, file_type):
        """
        Registers a loader function for a specific file type.

        This method is used as a decorator to associate a loader function with a 
        particular file type. The file type is case-insensitive.

        Args:
            file_type (str): The type of file that the loader function can handle.

        Returns:
            function: A decorator that registers the loader function for the specified file type.
        """
        def decorator(loader):
            cls.loaders[file_type.lower()] = loader
            return loader
        return decorator

    @classmethod
    def register_default_loader(cls, loader):
        """
        Registers a default loader for the class.

        This method assigns the provided loader to the class attribute `default_loader`.

        Args:
            cls (type): The class to which the loader is being registered.
            loader (callable): The loader function or object to be set as the default loader.

        Returns:
            callable: The loader that was registered.
        """
        cls.default_loader = loader
        return loader

    @classmethod
    def get_file_type(cls, file_path):
        """
        Determine the file type based on the file extension.

        Args:
            file_path (str): The path to the file.

        Returns:
            str: The file extension in lowercase without the leading dot.
        """
        _, ext = os.path.splitext(file_path)
        return ext.lower().lstrip('.')

    @classmethod
    def load_file(cls, file_path, **kwargs):
        """
        Loads a file based on its type and returns the loaded content.

        Note that you can only have on loader per data type. If you need multiple data objects (like a geopandas and something else)
        please implement it within a single loader function and return a dictionary with the different objects (which you can pick up
        in the validator).

        Args:
            file_path (str): The path to the file to be loaded.
            **kwargs: Additional keyword arguments to be passed to the loader function.

        Returns:
            The content of the loaded file as determined by the specific loader function.

        Raises:
            ValueError: If the file type is not supported by any loader.
        """
        file_type = cls.get_file_type(file_path)
        loader = loader = cls.loaders.get(file_type, cls.default_loader)
        return loader(file_path, **kwargs)


@ValidationFileLoader.register_loader('csv')
def load_csv(file_path, **kwargs) -> dict:
    return {"pandas": pd.read_csv(file_path, **kwargs) if kwargs else pd.read_csv(file_path)}

@ValidationFileLoader.register_loader('xlsx')
@ValidationFileLoader.register_loader('xls')
def load_excel(file_path, **kwargs) -> pd.DataFrame:
    return {"pandas": pd.read_excel(file_path, engine='openpyxl', **kwargs)}

@ValidationFileLoader.register_loader('gpkg')
def load_geopackage(file_path, **kwargs) -> dict:
    layers = fiona.listlayers(file_path)
    gdfs = {layer: gpd.read_file(file_path, layer=layer, **kwargs) for layer in layers}
    return {"dict_of_geopandas": gdfs}

@ValidationFileLoader.register_loader('xml')
def load_xml(xml_file_path:str) -> str:
    return xml_file_path


@ValidationFileLoader.register_default_loader
def load_default(file_path, **kwargs) -> dict:
    return {}