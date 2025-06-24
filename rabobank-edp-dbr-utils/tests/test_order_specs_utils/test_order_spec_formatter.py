import pytest
import sys
import os
from pathlib import Path

if (__name__=="__main__"):
    sys.path.append(os.path.dirname(os.getcwd()))
    sys.path.append(os.getcwd())
    # print(os.path.dirname(os.getcwd()))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.utils_order_spec import OrderSpecsValidator, OrderSpecsFormatter
from pyspark.sql import functions as sparkFunctions

def test_format_order_specs():
    order_specs = [("col1", "asc"), ("col2", "desc")]

    valid_order_specs = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    formatted_specs = OrderSpecsFormatter().format_order_specs_spark_functions_tuples(valid_order_specs)

    expected_specs = [sparkFunctions.asc("col1"), sparkFunctions.desc("col2")]

    assert '.'.join([os.__str__() for os in formatted_specs]) == '.'.join([os.__str__() for os in expected_specs])