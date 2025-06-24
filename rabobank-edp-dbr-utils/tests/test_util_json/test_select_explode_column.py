import sys
import os
from pathlib import Path
from databricks.sdk.runtime import spark
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.testing import assertDataFrameEqual

if __name__=="__main__":
    base_path = Path(os.getcwd()).parent.parent
    src_path = base_path / 'src'
    sys.path.append(str(src_path))
else:
    sys.path.append(str(Path(__file__).parent.parent))

from rabobank_edp_dbr_utils.util_json import prepare_column_selection, explode_and_select_columns

def test_prepare_column_selection():

    # Create sample data
    data = [
        Row(conversationId="conv1", participants=[Row(participantId="part1"), Row(participantId="part2")]),
        Row(conversationId="conv2", participants=[Row(participantId="part3"), Row(participantId="part4")])
    ]

    # Create DataFrame
    input_df = spark.createDataFrame(data)

    column_names = {'conversationId' : 'DIM_CONVERSATION_ID','participants.participantId' : 'DIM_PARTICIPANT_ID'}  
    actual_complex_col_list, actual_select_col_list = prepare_column_selection(input_df,column_names)
    expected_complex_col_list = ['participants.participantId']
    expected_select_col_list = ['conversationId as DIM_CONVERSATION_ID', 'exploded_participants.participantId as DIM_PARTICIPANT_ID']
    assert actual_complex_col_list == expected_complex_col_list
    assert actual_select_col_list == expected_select_col_list

def test_explode_and_select_columns():

    # Create sample data
    data = [
        Row(conversationId="1c02fdb7-eabc-46aa-bdd9-09502940ca3d", participants=[
            Row(participantId="05cc744c-30e8-433c-a9d2-2db2e84615e5", sessions=[
                Row(sessionId="06643cdd-5133-4ac6-8d6a-80972494f8a2"),
                Row(sessionId="1cf1dc01-fa90-4f1f-808e-7ee04abafbb8")
            ])
        ])
    ]

    # Create DataFrame
    input_df = spark.createDataFrame(data)

    complex_col_list = ['participants.participantId','participants.sessions.sessionId']
    select_col_list = ['conversationId as DIM_CONVERSATION_ID', 
                       'exploded_participants.participantId as DIM_PARTICIPANT_ID', 
                       'exploded_sessions.sessionId as DIM_SESSION_ID']

    expected_schema = StructType([StructField("DIM_CONVERSATION_ID", StringType()),
                                  StructField("DIM_PARTICIPANT_ID", StringType()),
                                  StructField("DIM_SESSION_ID", StringType())])
    expected_data = [
        ("1c02fdb7-eabc-46aa-bdd9-09502940ca3d", "05cc744c-30e8-433c-a9d2-2db2e84615e5", "06643cdd-5133-4ac6-8d6a-80972494f8a2"), 
        ("1c02fdb7-eabc-46aa-bdd9-09502940ca3d", "05cc744c-30e8-433c-a9d2-2db2e84615e5", "1cf1dc01-fa90-4f1f-808e-7ee04abafbb8")]

    expected_result_df = spark.createDataFrame(expected_data, expected_schema)
    actual_result_df = explode_and_select_columns(input_df,complex_col_list,select_col_list)
    assertDataFrameEqual(actual_result_df, expected_result_df, "DataFrames do not match")

if __name__=="__main__":
    test_prepare_column_selection()
    test_explode_and_select_columns()
