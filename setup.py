from setuptools import Extension, setup
from Cython.Build import cythonize
import numpy
import os

ext_modules = []

PURE_PYTHON = os.environ.get("PURE_PYTHON") == "1"

if not PURE_PYTHON:
    sorting_tdc = Extension(
        name="metro_eval.cli.sort_events.sorting_tdc",
        sources=["src/metro_eval/cli/sort_events/sorting_tdc.pyx"],
        include_dirs=[numpy.get_include()],
        optional=True,
    )

    index_ascii = Extension(
        name="metro_eval.cli.metro2hdf._index_ascii",
        sources=["src/metro_eval/cli/metro2hdf/_index_ascii.py"],
        optional=True,
    )

    ext_modules = cythonize([sorting_tdc, index_ascii])

setup(ext_modules=ext_modules)
