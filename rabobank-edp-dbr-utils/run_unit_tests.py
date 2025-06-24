# Prepare to run pytest.
import sys, pytest, os
from pathlib import Path
from databricks.sdk.runtime import dbutils

# Restart the Python session
dbutils.library.restartPython()

if __name__ == "__main__":
    file_path = os.getcwd()
    file_path_clean = file_path.replace("dbfs:","/dbfs")
    root_dir = file_path_clean
    sys.path.append(root_dir)
else:
    sys.path.append(os.path.dirname(str(Path(__file__))))
    root_dir = os.path.dirname(str(Path(__file__)))

sys.path.append(root_dir + '/src')
sys.path.append(root_dir)
print(sys.path)

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main(["--junit-xml"
                       , f"tests/test-reports/test-results/TEST-libout.xml"
                       , "--verbose"
                       ,"--cov=src/"
                       ,f"--cov-report=xml:tests/test-reports/code-coverage/pytest-cobertura.xml"
                       ,"--cov-report=html:tests/test-reports/htmlcov"
                       ,"--capture=tee-sys",
                      f"{root_dir}/tests/"])
print(retcode)
# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."