import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, ArrayType
from pyspark.sql import functions as sparkFunctions
from decimal import Decimal
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame

if (__name__=="__main__"):
    base_path = Path(os.getcwd()).parent.parent
    #print("base_path...........", base_path)
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))
from rabobank_edp_dbr_utils.utils_historization import validate_input_kwargs
# Test cases are listed below

def test_duplicate_keys():
    with pytest.raises(ValueError, match="Duplicate keys found in the keyword arguments"):
        validate_input_kwargs(primary_key_hash='EDL_KEY_HASH1',PRIMARY_KEY_HASH='abc')

def test_correct_default_values():
    validated_kwargs = validate_input_kwargs(primary_key_hash='EDL_KEY_HASH1')
    non_primary_key_hash = validated_kwargs.get("non_primary_key_hash")
    primary_key_hash = validated_kwargs.get("primary_key_hash")
    assert non_primary_key_hash == "EDL_NONKEY_HASH"
    assert primary_key_hash =='EDL_KEY_HASH1'

def test_merge_input_with_default_keys():
    validated_kwargs = validate_input_kwargs(content_hash='CONTENT_HASH')
    actual_keys_list = list(validated_kwargs.keys())
    expected_key_list = ['primary_key_hash', 'non_primary_key_hash', 'edl_load_datetime', 'edl_act_datetime', 'edl_act_datetime_utc', 'edl_deleted_flag', 'edl_valid_from_datetime', 'edl_valid_from_datetime_utc', 'edl_valid_to_datetime', 'edl_valid_to_datetime_utc', 'edl_update_datetime', 'merge_schema', 'edl_active_flag', 'content_hash']
    actual_keys_list.sort()
    expected_key_list.sort()
    assert actual_keys_list == expected_key_list

test_duplicate_keys()
test_correct_default_values()
test_merge_input_with_default_keys()