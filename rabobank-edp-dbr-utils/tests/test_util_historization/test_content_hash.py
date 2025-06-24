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
from rabobank_edp_dbr_utils.utils_historization import create_content_hash
# Test cases are listed below

def test_empty_dataframe():
    schema = StructType([
    StructField("pand_id", StringType(), True),
    StructField("Bag_id", StringType(), True),
    StructField("addressable_object_id", StringType(), True),
    StructField("polygon", StringType(), True),
])
    input_data=[]
    include_column_list = ['pand_id','Bag_id']
    df = spark.createDataFrame(input_data,schema)
    result_data = create_content_hash(df,include_column_list )
    assert result_data.count() == 0, "DataFrame is empty"

def test_hash_null_value():
    
    input_schema = StructType([
        StructField("polygon", StringType(), True),
        StructField("random_attribute_1", IntegerType(), True),
        StructField("random_attribute_2", IntegerType(), True)
    ])
    
    # Create test data with null values
    input_data = [
        (None, 44, 83),
        ("", 44, 83),
        (None, 5, 47),
        ('abcd', 59, 447)
    ]
    
    input_df = spark.createDataFrame(input_data, input_schema)
    include_column_list = ['polygon',"random_attribute_1","random_attribute_2"]
    hash_col = create_content_hash(input_df, include_column_list)
    hash_col_sorted = hash_col.sort(*hash_col.columns).collect()
    
    # Create test data with null values
    expected_data = [
        (None, 44, 83,'693778654191ba948331ec6f8bbe9c0765874b3c5a772782de08c20b7ec9c192'),
        ("", 44, 83,'010b88a4f9742e9c01514741e8c517e9b9f2a4f0736b91191a30e9d482c5b0d4'),
        (None, 5, 47,'4e847b889984c71922568191fca6fba0494d77544f4ffc0fd45dadbc8fdb6cf6'),
        ('abcd', 59, 447,'ad2571bada5e0b2c6637d01897e0e5bad2bafc748e636fb6da612d7d1644f6a1')
    ]

    expected_schema = StructType(input_schema.fields + [StructField("content_hash", StringType(), True)])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    expected_df_sorted = expected_df.sort(*expected_df.columns).collect()
    assert hash_col_sorted == expected_df_sorted, "Resulting DataFrame  match expected DataFrame"

    
def test_hash_case_sensitivity():

    schema = StructType([
        StructField("polygon", StringType(), True),
        StructField("random_attribute_1", IntegerType(), True),
        StructField("random_attribute_2", IntegerType(), True)
    ])
        
    data = [
        ("[['TestinG','dataSet'],['TesT', 'hasHfunC']]",  44, 83),
        ("[['ClimAta', 'DATA'],['wiLdFiRe', 'dataseT']]", 59, 47),
        (None, 5, 47),
        (None, 59, 447)
    ]

    df = spark.createDataFrame(data, schema)

    include_column_list_1 = ['POLYGON','RANDOM_ATTRIBUTE_1','RANDOM_ATTRIBUTE_2']
    include_column_list_2 = ['polygon','random_attribute_1','random_attribute_2']
    hash_col_upper = create_content_hash(df, include_column_list_1)
    hash_col_upper_sorted = hash_col_upper.sort(*hash_col_upper.columns).collect()  
    hash_col_lower = create_content_hash(df, include_column_list_2)
    hash_col_lower_sorted = hash_col_lower.sort(*hash_col_lower.columns).collect()  
    assert hash_col_upper_sorted == hash_col_lower_sorted, "Hashes for columns with different cases should not be different"

def test_hash_different_data_types():
    # Define schema with different data types
    schema = StructType([
        StructField("array_int_col", ArrayType(IntegerType()), True),
        StructField("array_str_col", ArrayType(StringType()), True),
        StructField("decimal_col", DecimalType(10, 2), True),
        StructField("integer_col", IntegerType(), True),
        StructField("string_col", StringType(), True),
        StructField("date_col", DateType(), True)
    ])
    
    # Create test data
    input_data = [
        ([1, 2, 3], ["a", "b", "c"], Decimal(123.45), 10, "test", date(2023,1,1)),
        ([4, 5, 6], ["d", "e", "f"], Decimal(678.90), 20, "example", date(2023,1,2))
    ]
    input_df = spark.createDataFrame(input_data, schema)
    
    # Columns to include in hash
    include_column_list = ['ARRAY_INT_COL', 'array_str_col', 'decimal_col', 'integer_col', 'string_col', 'date_col']
    
    # Compute hash
    result_data = create_content_hash(input_df, include_column_list)
    result_data_sorted = result_data.sort(*result_data.columns).collect()
    
    # Define expected DataFrame (example, adjust as needed)
    expected_data = [
        ([1, 2, 3], ["a", "b", "c"], Decimal(123.45), 10, "test", date(2023,1,1), "7812f87a2da910faff50488d2b056fb927eb23898e32c11a73241f06b9fb0cf3"),
        ([4, 5, 6], ["d", "e", "f"],Decimal(678.90), 20, "example",  date(2023,1,2), "05f1ccebdd183e245298e9c4878f1857d77d30ad432d3449a0bff5bbb508c815")
    ]
    expected_schema = StructType(schema.fields + [StructField("content_hash", StringType(), True)])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    expected_df_sorted = expected_df.sort(*expected_df.columns).collect()
    
    # Assertions
    #sort the dataframe after collect:
    assert result_data_sorted == expected_df_sorted, "Resulting DataFrame must match expected DataFrame"

def test_hash_different_delimiters():
        
        input_schema = StructType([
            StructField("polygon", StringType(), True),
            StructField("polygon2", StringType(), True),
            StructField("polygon3", StringType(), True),
             StructField("polygon4", StringType(), True),
            StructField("random_attribute_1", IntegerType(), True),
            StructField("random_attribute_2", IntegerType(), True)
        ])
        
        #Create test data with different delimiters
        input_data = [
            ('abcd','efgh','delim', 'test', 44, 83),
            ('abc', 'defgh','delimt', 'est', 59, 47),
        ]
        include_column_list = ['polygon','polygon2','polygon3','polygon4']	
        
        input_df = spark.createDataFrame(input_data, input_schema)
        hash_col_comma = create_content_hash(input_df, include_column_list,delimiter=",")
        hash_col_comma_sorted = hash_col_comma.sort(*hash_col_comma.columns).collect()
        expected_data = [
        ('abcd', 'efgh', 'delim', 'test', 44, 83, '4fb9c3e1dd9059c5653be3905f54e005031c8b3e07796116c8afd18314ef092e'),
        ('abc', 'defgh', 'delimt', 'est', 59, 47, 'd51e8f3f77da275cd9c555febd259b3941aa68c683e6acbd1a3a08c4cea1377c'),
        ]

        expected_schema = StructType(input_schema.fields + [StructField("content_hash", StringType(), True)])
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        expected_df_sorted = expected_df.sort(*expected_df.columns).collect()
        assert hash_col_comma_sorted == expected_df_sorted, "Resulting DataFrame must match expected DataFrame"
      
def test_userdefined_delimiter():
        
        input_schema = StructType([
            StructField("polygon", StringType(), True),
            StructField("polygon2", StringType(), True),
            StructField("polygon3", StringType(), True),
            StructField("polygon4", StringType(), True),
            StructField("random_attribute_1", IntegerType(), True),
            StructField("random_attribute_2", IntegerType(), True)
        ])
        
        #Create test data with different delimiters
        input_data = [
            ('abc;','defgh',';delim', ';test', 44, 83),
            ('abc', ';defgh;','delim;', 'test', 59, 47),
        ]
        include_column_list = ['polygon','polygon2','polygon3','polygon4']	

        input_df= spark.createDataFrame(input_data, input_schema)
        hash_col_semicolon = create_content_hash(input_df, include_column_list,delimiter=";")
        hash_col_semicolon_sorted = hash_col_semicolon.sort(*hash_col_semicolon.columns).collect()
        hash_col_semicolon.show(truncate=False)
        expected_data = [
        ('abcd', 'efgh', 'delim', 'test', 44, 83, 'c84b77df463fe15a828d922dd86b05a2fd575347ad6aa1737f6029cea8e9fc49'),
        ('abc', 'defgh', 'delimt', 'est', 59, 47, '8040f6c888791780d83bb62e81c32fbfe2b6d4b4e103c400c211f91b7fca3262'),
        ]

        expected_schema = StructType(input_schema.fields + [StructField("content_hash", StringType(), True)])
        expected_df_semicolon = spark.createDataFrame(expected_data, expected_schema)
        expected_df_semicolon_sorted = expected_df_semicolon.sort(*expected_df_semicolon.columns).collect() 

        # assert hash_col_semicolon_sorted == expected_df_semicolon_sorted, "Resulting DataFrame  match expected DataFrame"    

def test_include_column_list_type():
    schema = StructType([
        StructField("pand_id", StringType(), True),
        StructField("Bag_id", StringType(), True),
        StructField("addressable_object_id", StringType(), True),
        StructField("polygon", StringType(), True),
    ])
    
    # Create test data
    input_data = [
        ("1", "A", "X", "polygon1"),
        ("2", "B", "Y", "polygon2")
    ]
    list_df = spark.createDataFrame(input_data, schema)

    # Test with valid list type
    include_column_list = {'pand_id', 'Bag_id'}
    with pytest.raises(TypeError, match="The included_column_list must be a list"):
        create_content_hash(list_df, include_column_list)

test_empty_dataframe()
test_hash_null_value()
test_hash_case_sensitivity()
test_hash_different_data_types()
test_hash_different_delimiters()
test_userdefined_delimiter()
test_include_column_list_type()