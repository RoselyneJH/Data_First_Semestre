# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Death_People'
copyright = '2025, R.JEAN'
author = 'R.JEAN'
release = '1.0'

# Ajout chemin vers modules python
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc',
            'sphinx.ext.napoleon',
            'sphinx.ext.viewcode']


templates_path = ['_templates']
exclude_patterns = []

language = 'fr'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# Thème
html_theme = 'sphinx_rtd_theme'

html_static_path = ['_static']
