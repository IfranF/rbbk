import setuptools
 
with open("README.md", "r") as fh:
    long_description = fh.read()
 
setuptools.setup(
    name="rabobank-edp-dbr-utils",
    version="0.3.12",
    author="Enterprise Data Products",
    author_email="puja.datta@rabobank.nl",
    description="Util functions for interacting with data using DataBricks",
    url="https://dev.azure.com/raboweb/Tribe%20Data%20and%20Analytics/_git/rabobank_edp_dbr_utils",
    package_dir={'rabobank_edp_dbr_utils': 'src/rabobank_edp_dbr_utils'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
    include_package_data=True
)
 
 