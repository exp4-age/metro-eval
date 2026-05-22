metro-eval
==========

.. |version| image:: https://img.shields.io/badge/version-dev-blue
   :target: https://img.shields.io/badge/version-dev-blue
.. |license| image:: https://img.shields.io/github/license/exp4-age/metro-eval
   :alt: GitHub License

|version| |license|

Installation
------------

Install from GitHub with ::

    pip install git+https://github.com/exp4-age/metro-eval@main

The cython extension modules can be explicitly disabled
by installing with the option ::

    --config-settings=setup-args=-Dcythonize=disabled

For editable installs, the optional dependencies [dev] need to be
installed and build isolation needs to be disabled ::

    pip install -e .[dev] --no-build-isolation

When installing with uv, the `--no-build-isolation` flag can be added
to the pyproject.toml for persistency ::

    [tool.uv]
    no-build-isolation-package = ["metro-eval"]

