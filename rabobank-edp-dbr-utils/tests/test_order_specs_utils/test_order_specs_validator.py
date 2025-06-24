import pytest
import sys
import os
from pathlib import Path

if (__name__=="__main__"):
    sys.path.append(os.path.dirname(os.getcwd()))
    sys.path.append(os.getcwd())
    print(os.path.dirname(os.getcwd()))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.utils_order_spec import OrderSpecsValidator

def test_empty_order_specs():
    order_specs = []
    try:
        order_spec_obj = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()    

    assert error_message == f"Error while validating order specifications: order_specs are empty"

def test_order_specs_not_tuple():
    order_specs = ["col1", "asc"]
    try:
        order_spec_obj = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()
    
    assert error_message == f"Error while validating order specifications: too many values to unpack (expected 2)"

def test_order_specs_col_empty():
    order_specs = [("", "asc")]
    try:
        order_spec_obj = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()
    
    assert error_message == f"Error while validating order specifications: Column or Sort-order cannot be empty in the input order_specs. column:, sort_order:asc"

def test_order_specs_sort_order_empty():
    order_specs = [("col1", "")]
    try:
        order_spec_obj = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()
    
    assert error_message == f"Error while validating order specifications: Column or Sort-order cannot be empty in the input order_specs. column:col1, sort_order:"

def test_order_specs_duplicate_cols():
    order_specs = [("col1", "asc"), ("COl1", "desc")]
    try:
        order_spec_obj =OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()
    
    
    assert error_message == f"Error while validating order specifications: There are duplicate columns in the input for order_spec. Please remove duplicates"

def test_order_specs_invalid_sort_order():
    order_specs = [("col1", "dedencing")]
    try:
        order_spec_obj = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()
    
    assert error_message == f"Error while validating order specifications: Invalid order specification: dedencing. Order must be 'desc' or 'asc'"

def test_order_specs_valid():
    order_specs = [("col1", "Ascending"), ("col2", "descENding")]
    error_message = None
    try:
        order_spec_obj = OrderSpecsValidator().validate_order_specs_tuples(order_specs)
    except ValueError as e:
        error_message = e.__str__()
    
    assert error_message == None
    assert order_spec_obj.columns == ["col1", "col2"]