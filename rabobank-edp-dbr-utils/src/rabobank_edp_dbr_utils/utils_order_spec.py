"""Util Order Spec Module

This module contains classes to validate and format the order specifications
The classes are:
- ``OrderSpecsValidator``
- ``OrderSpecsFormatter``
"""

# pylint: disable=import-error
from pyspark.sql import functions as sparkFunctions
from rabobank_edp_dbr_utils.logger import logging 

utils_logger = logging.getLogger(__name__)

class OrderSpecsValidator:
    """	
    Class to validate and format the order specifications.
    Order specifications are used to sort the data before deduplication.
    Order specifications are a list of tuples with column name and sort order.
    Example: `[("col1","asc"),("col2","desc")]`
    """
    def __init__(self):
        self.order_specs = []
        self.columns = []

    def validate_order_specs_tuples(self, order_specs: list):
        """Check if the order specification is valid
        
            - Check-1: order_specs is not empty
            - Check-2: Implicit checks for each tuple in order_specs: for loop
            - Check-3: sort order is 'asc' or 'desc'
            - Check-4: No duplicate columns in order_specs

        Raises:
            ValueError: "Column or Sort-order cannot be empty in the input order_specs. 
            column:{col}, sort_order:{sort_order}"
            ValueError: "Invalid order specification: {sort_order}. Order must be 'desc' or 'asc'"
            ValueError: "There are duplicate columns in the input for order_spec. 
            Please remove duplicates"
            ValueError: "order_specs are empty"
            ValueError: "Error while validating order specifications: {e}"

        Args:
            order_specs (list): List of tuples with column name and sort order. 
            Example: [("col1","asc"),("col2","desc")]

        Returns:
            self: OrderSpecsValidator object

        Usage example:
        --------------
            >>> from rabobank_edp_dbr_utils.utils_order_spec import OrderSpecsValidator
            >>> order_specs_validator = OrderSpecsValidator()
            >>> order_specs_tuples = [('col1','asc'),('col2','desc')]
            >>> order_specs_validator.validate_order_specs_tuples(order_specs_tuples)
        """
        utils_logger.info("Validating order specifications...")
        self.order_specs = order_specs

        try:
            # Check-1
            if len(self.order_specs) == 0:
                raise ValueError("order_specs are empty")

            # Check-2 and Check-3
            for col,sort_order in order_specs:
                if not col or not sort_order:
                    raise ValueError(f"Column or Sort-order cannot be empty in the input order_specs. column:{col}, sort_order:{sort_order}")
                if not sort_order.strip().lower().startswith("asc") \
                    and not sort_order.strip().lower().startswith("desc"):
                    raise ValueError(f"Invalid order specification: {sort_order}. Order must be 'desc' or 'asc'")
                self.columns.append(col.strip().lower())

            # Check-4
            if len(self.columns) != len(set(self.columns)):
                raise ValueError("There are duplicate columns in the input for order_spec. Please remove duplicates")

        except Exception as e:
            raise ValueError(f"Error while validating order specifications: {e}")

        utils_logger.info("Order specifications validated successfully...")
        return self

class OrderSpecsFormatter():
    """Class to format the order specifications
    """
    def format_order_specs_spark_functions_tuples(self,
                valid_order_specs: OrderSpecsValidator) -> list:
        """Create a list of order specifications for order by clause in spark.

        Example input/output: [("col1","asc"),("col2","desc")] 
        -> [asc(col("col1")),desc(col("col2"))]

        Args:
            valid_order_specs (OrderSpecsValidator): OrderSpecsValidator object

        Raises:
            ValueError: "Invalid order specification: {order}"
            ValueError: "Error while formatting order specifications: {e}"

        Returns:
            list: List of order specifications for order by clause in spark

        Usage example:
        --------------
            >>> from rabobank_edp_dbr_utils.utils_order_spec import OrderSpecsValidator
            >>> from rabobank_edp_dbr_utils.utils_order_spec import OrderSpecsFormatter
            >>> order_specs_validator = OrderSpecsValidator()
            >>> order_specs_tuples = [('col1','asc'),('col2','desc')]
            >>> order_specs_validator.validate_order_specs_tuples(order_specs_tuples)
            >>> order_specs_formatter = OrderSpecsFormatter()
            >>> formatted_order_specs = order_specs_formatter.format_order_specs_spark_functions_tuples(order_specs_validator)
            >>> print(formatted_order_specs)
            [Column<'col1 ASC NULLS FIRST'>, Column<'col2 DESC NULLS LAST'>]
        """
        order_specs_formatted = []

        try:
            for col,order in valid_order_specs.order_specs:
                if order.strip().lower().startswith("a"):
                    order_specs_formatted.append(sparkFunctions.asc(col))
                elif order.strip().lower().startswith("d"):
                    order_specs_formatted.append(sparkFunctions.desc(col))
                else:
                    raise ValueError(f"Invalid order specification: {order}")
        except Exception as e:
            raise ValueError(f"Error while formatting order specifications: {e}")

        order_specs_formatted_str = ','.join([os.__str__() for os in order_specs_formatted])
        utils_logger.info(f"Formatted order spec is: {order_specs_formatted_str}")

        return order_specs_formatted
