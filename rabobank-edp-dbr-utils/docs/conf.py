# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'EDP Utils'
copyright = '2024, Tribe EDP'
author = 'Tribe EDP'
release = '0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinxcontrib.confluencebuilder',
    'myst_parser'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

autodoc_mock_imports = ["pyspark", "databricks", "delta", "pandas"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'#'sphinx_rtd_theme'
html_static_path = ['_static']

# -- Options for Confluence output -------------------------------------------
confluence_publish = True
confluence_space_key = "DLD"
confluence_parent_page = 'v0.1'
confluence_server_url = 'https://confluence.dev.rabobank.nl/'
# If you want to use the user name and password, use below configuration-
confluence_server_user = '<username>'
confluence_server_pass = '<password>'
# If you want to use the PAT (Personal Access Token) from Confluence, use below configuration-
#confluence_publish_token = os.getenv("confluence_PAT")
#pip install --upgrade myst-parser