from rabobank_edp_dbr_utils.util_functions import FileHandler


def test_init_filehandler():
    filehandler = FileHandler()
    assert filehandler is not None

def test_file_not_exists():
    filehandler = FileHandler()
    assert filehandler.is_file_present("tests/test_data/test_filehandler.py") == False

def test_file_exists():
    folder_name = "tests_delete_folder"
    filehandler = FileHandler()
    filehandler.fs_utils.mkdirs(folder_name)

    assert filehandler.is_file_present(folder_name) == True
