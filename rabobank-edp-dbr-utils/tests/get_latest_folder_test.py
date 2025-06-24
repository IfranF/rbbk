# pylint: disable=import-error
import os
import sys
from pathlib import Path
from databricks.sdk.runtime import dbutils
from rabobank_edp_dbr_utils.util_functions import FolderUtils

dbutils.library.restartPython()

if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.getcwd()))
    root_dir = "file:" + str(os.path.dirname(os.getcwd()))
    print(os.path.dirname(os.getcwd()))
else:
    root_dir = str(Path(__file__).parent.parent)
    sys.path.append(root_dir)
    # when running via VScode root_dir will be in file system. Example file:/Workspace/....
    # But when running through CICD pipeline root_dir will be in dbfs. Example /dbfs/Workspace/...
    if "/dbfs/" in root_dir:
        root_dir = str(Path(__file__).parent.parent).replace("/dbfs/","")
    else:
        root_dir = "file:" + root_dir
    print(root_dir)

def test_get_the_latest_folder_positive():
    input_path = root_dir + "/tests/foldertests/vanillafolders"
    print(input_path)
    latest_folder = FolderUtils.get_the_latest_folder(input_path)
    assert latest_folder=="DELIVERY_QUARTER=2023Q4/"

def test_get_the_latest_folder_nofolder():
    input_path = root_dir+ "/tests/foldertests/nofolders"
    error_message=""
    dbutils.fs.mkdirs(input_path)
    try:
        FolderUtils.get_the_latest_folder(input_path)
    except Exception as ex:
        error_message = str(ex)
    assert error_message == f"No folders found {input_path}"