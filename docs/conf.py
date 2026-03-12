# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from metro_eval import __version__ as version

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "metro-eval"
release = version
copyright = "2026, AG Ehresmann"
author = "AG Ehresmann"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "numpydoc",
    "sphinx.ext.autosummary",
    "sphinx_autodoc_typehints",
    "sphinx.ext.mathjax",
    "sphinx_copybutton",
    "sphinxarg.ext",
    "matplotlib.sphinxext.plot_directive",
    "IPython.sphinxext.ipython_console_highlighting",
    "nbsphinx",
]

autosummary_ignore_module_all = False
autosummary_generate = True
autoclass_content = "class"
html_show_sourcelink = False
set_type_checking_flag = True
nbsphinx_allow_errors = False
nbsphinx_execute = "always"
add_module_names = False

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

numpydoc_show_class_members = False
numpydoc_show_inherited_class_members = False
numpydoc_class_members_toctree = True
numpydoc_xref_param_type = True
numpydoc_use_plots = True

plot_include_source = True
plot_html_show_source_link = False
plot_formats = [("hires.png", 300)]
plot_html_show_formats = False

autosummary_generate = ["reference"]

copybutton_prompt_text = ">>> "


def autodoc_skip_member(app, what, name, obj, skip, options):
    return None


def setup(app):
    app.connect("autodoc-skip-member", autodoc_skip_member)


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
