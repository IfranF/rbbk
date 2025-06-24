import pytest
import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as sparkFunctions

if (__name__=="__main__"):
    sys.path.append(os.path.dirname(os.getcwd()))
    sys.path.append(os.getcwd())
    print(os.path.dirname(os.getcwd()))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.deduplicate import __deduplicate


def test_deduplicate_multiple_pks_duplicates_with_nulls():
    primary_keys = ['CLT_OBJ_NO','REAL_ESTT_BAG_ADDRB_OBJ_ID']    
    order_specs = [sparkFunctions.asc("HOUSE_NR"),sparkFunctions.desc("EXTRA_COL")]
    input_schema = StructType([StructField("CLT_OBJ_NO",StringType(),True),
                         StructField("REAL_ESTT_BAG_ADDRB_OBJ_ID",StringType(),True),
                         StructField("HOUSE_NR",IntegerType(),True),
                         StructField("POST_CODE",StringType(),True),
                         StructField("EXTRA_COL",StringType(),True)
                         ])    
    input_data = [("1111",None,1,"2735AA","extra1"),
            ("1111","01111",2,"2735AA","extra1"),
            ("1111","01111",3,"2735AA","extra1"),
            ("2222",None,1,"2735AA","extra1"),
            ("2222",None,1,"2735AA","extra1"),
            ("2222",None,1,"2735AA","extra1"),
            ("3333","03333",1,"2735AA","extra1"),
            (None,"04444",1,"5585XA","extra1"),
            (None,"04444",1,"5585XA","extra2"),
            ("4444",None,1,"5585XB","extra2"),
            ("4444",None,1,"5585XB",None),
            ]
    df_input = spark.createDataFrame(input_data,input_schema)
    

    
    expected_schema = StructType([StructField("CLT_OBJ_NO",StringType(),True),
                         StructField("REAL_ESTT_BAG_ADDRB_OBJ_ID",StringType(),True),
                         StructField("HOUSE_NR",IntegerType(),True),
                         StructField("POST_CODE",StringType(),True),
                         StructField("EXTRA_COL",StringType(),True)
                        ])
    
    expected_data = [("1111",None,1,"2735AA","extra1"),
                    ("1111","01111",2,"2735AA","extra1"),
                    ("2222",None,1,"2735AA","extra1"),
                    ("3333","03333",1,"2735AA","extra1"),
                    (None,"04444",1,"5585XA","extra2"),
                    ("4444",None,1,"5585XB","extra2"),
                    ]
    df_expected = spark.createDataFrame(expected_data,expected_schema)
    df_expected_sorted = df_expected.sort(*df_expected.columns).collect()

    df_actual = __deduplicate(df_input,primary_keys,order_specs)
    df_actual_sorted = df_actual.sort(*df_actual.columns).collect()
    
    assert df_actual_sorted == df_expected_sorted