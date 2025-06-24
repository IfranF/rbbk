import os
import sys
from pathlib import Path

if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.getcwd()))
    print(os.path.dirname(os.getcwd()))
else:
    sys.path.append(str(Path(__file__).parent.parent))

print(sys.path)
from rabobank_edp_dbr_utils.util_functions import ValidationUtils

def test_no_cols_present():
    parent_cols = ['BAG_ID','HOUSE_NR','POST_CODE']
    cols_to_be_checked = ['CLT_OBJ_NO','REAL_ESTT_BAG_ADDRB_OBJ_ID']
    error_message = ""          
    
    try:
        actual = ValidationUtils.cols_present_check(parent_cols,cols_to_be_checked)
    except Exception as ex:
        error_message = ex.__str__()
      
    expected_cols_not_in_parent_formatted = ",".join(['CLT_OBJ_NO','REAL_ESTT_BAG_ADDRB_OBJ_ID'])
    parent_cols_formatted = ",".join(col.lower() for col in parent_cols)

    assert error_message == f"these column(s):{expected_cols_not_in_parent_formatted} not in parent-list: {parent_cols_formatted}"

def test_some_cols_present():
    parent_cols = ['BAG_ID','HOUSE_NR','POST_CODE']
    cols_to_be_checked = ['CLT_OBJ_NO','REAL_ESTT_BAG_ADDRB_OBJ_ID','BAG_ID']
    error_message = ""          
    
    try:
        actual = ValidationUtils.cols_present_check(parent_cols,cols_to_be_checked)
    except Exception as ex:
        error_message = ex.__str__()
      
    expected_cols_not_in_parent_formatted = ",".join(['CLT_OBJ_NO','REAL_ESTT_BAG_ADDRB_OBJ_ID'])
    parent_cols_formatted = ",".join(col.lower() for col in parent_cols)

    assert error_message == f"these column(s):{expected_cols_not_in_parent_formatted} not in parent-list: {parent_cols_formatted}"

def test_child_empty():
    parent_cols = ['BAG_ID','HOUSE_NR','POST_CODE']
    cols_to_be_checked = []
    error_message = ""          
    
    try:
        actual = ValidationUtils.cols_present_check(parent_cols,cols_to_be_checked)
    except Exception as ex:
        error_message = ex.__str__()

    assert error_message == ""

def test_parent_empty():
    parent_cols = []
    cols_to_be_checked = ['BAG_ID','HOUSE_NR']
    error_message = ""          
    
    try:
        actual = ValidationUtils.cols_present_check(parent_cols,cols_to_be_checked)
    except Exception as ex:
        error_message = ex.__str__()

    assert error_message == f"Parent(main) list is empty"

def test_positive_all_cols_present():
    parent_cols = ['BAG_ID','HOUSE_NR','POST_CODE']
    cols_to_be_checked = ['bag_ID','house_nr'] #also check case insensitive col names
    error_message = ""          
    
    try:
        actual = ValidationUtils.cols_present_check(parent_cols,cols_to_be_checked)
    except Exception as ex:
        error_message = ex.__str__()

    assert error_message == ""